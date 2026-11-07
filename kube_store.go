package main

// An objectStore holds one watched collection in memory, so a pass
// reads the cluster without a request to the API server. The watch
// that owns the store fills it from a list and keeps it current from
// the watch's events.
//
// The store trims each object before it holds it: it decodes the
// object into the operator's Go type for the collection and holds that
// value encoded as JSON. The API server sends every field, and a Job
// carries its pod template, managedFields, and status. The operator
// watches every Job in the cluster, so a store of whole objects grows
// with every workload. The Go types hold only the fields the pass and
// the writes read, so the trimmed object keeps the key, the
// resourceVersion, and everything else the operator uses. The store
// holds JSON and not the typed value, so one store type holds every
// collection, and the pass decodes a snapshot into the type it needs.
//
// An object that does not decode is left out of the store, and every
// other object applies. One bad object then costs only itself: the
// pass does not see it, and the rest of its collection stays current.
//
// A change wakes a pass only when the trimmed object differs from the
// stored copy, leaving out the resourceVersion and the status the
// operator itself last wrote. Without the rule every write the
// operator makes comes back as an event that runs a pass with nothing
// to do, and so does every change to a field no pass reads, such as a
// Job's count of ready pods. The rule decides wakes only: every change
// still replaces the stored copy, because a write states the
// resourceVersion it read.

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
	"sync"
)

type objectStore struct {
	// trim returns the part of an object that the operator reads.
	trim func(json.RawMessage) (json.RawMessage, error)
	// skipped reports an object that does not decode, by its key and
	// the error.
	skipped func(key, reason string)

	mutex sync.Mutex
	// objects is keyed by namespace/name. A value is never changed in
	// place, so a snapshot shares the values without a copy.
	objects map[string]json.RawMessage
	// rejected holds the error of each object that did not decode, so
	// a relist that finds the same error reports nothing new.
	rejected map[string]string
	// written holds, by key, the status the operator last wrote, as its
	// trimmed type encodes it.
	written map[string]json.RawMessage
	// wakes, when set, says whether a change under a key may wake a pass
	// at all. It is set before the watch starts, and never after.
	wakes func(key string) bool
}

func newObjectStore(trim func(json.RawMessage) (json.RawMessage, error), skipped func(key, reason string)) *objectStore {
	return &objectStore{
		trim:     trim,
		skipped:  skipped,
		objects:  map[string]json.RawMessage{},
		rejected: map[string]string{},
		written:  map[string]json.RawMessage{},
	}
}

// trimTo decodes an object into T and encodes it again, which drops
// every field that T does not hold.
func trimTo[T any](object json.RawMessage) (json.RawMessage, error) {
	var typed T
	if err := json.Unmarshal(object, &typed); err != nil {
		return nil, err
	}
	return json.Marshal(typed)
}

// storedMeta is the part of an object that the store reads.
type storedMeta struct {
	Metadata struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
}

func readStoredMeta(object json.RawMessage) (storedMeta, error) {
	var meta storedMeta
	err := json.Unmarshal(object, &meta)
	return meta, err
}

func (meta storedMeta) key() string {
	return meta.Metadata.Namespace + "/" + meta.Metadata.Name
}

// replace sets the store to a list's items. An object that is not in
// the list is gone, even when the watch missed its DELETED event. An
// item whose metadata does not decode has no key, so it is reported
// under an empty key.
func (s *objectStore) replace(items []json.RawMessage) {
	objects := make(map[string]json.RawMessage, len(items))
	rejected := map[string]string{}
	for _, item := range items {
		key := ""
		trimmed, err := s.trim(item)
		if meta, metaErr := readStoredMeta(item); metaErr != nil {
			err = metaErr
		} else {
			key = meta.key()
		}
		if err != nil {
			rejected[key] = err.Error()
			continue
		}
		objects[key] = trimmed
	}
	s.mutex.Lock()
	previous := s.rejected
	s.objects, s.rejected = objects, rejected
	s.mutex.Unlock()
	for _, key := range slices.Sorted(maps.Keys(rejected)) {
		if previous[key] != rejected[key] {
			s.skipped(key, rejected[key])
		}
	}
}

// put stores an object from an ADDED or MODIFIED event, and reports
// whether the change wakes a pass. Events arrive in the order the API
// server made the changes, and resourceVersions are opaque, so the
// newest event always wins. An object that does not decode removes the
// older copy under its key, so the pass acts on no version that the
// cluster has replaced.
func (s *objectStore) put(key string, object json.RawMessage) bool {
	trimmed, err := s.trim(object)
	s.mutex.Lock()
	previous := s.rejected[key]
	stored, held := s.objects[key]
	wakes := held
	if err != nil {
		delete(s.objects, key)
		s.rejected[key] = err.Error()
	} else {
		wakes = !held || differs(stored, trimmed, s.written[key])
		s.objects[key] = trimmed
		delete(s.rejected, key)
	}
	s.mutex.Unlock()
	if err != nil && previous != err.Error() {
		s.skipped(key, err.Error())
	}
	return wakes && s.mayWake(key)
}

// remove forgets an object, and reports whether the store held it.
func (s *objectStore) remove(key string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, held := s.objects[key]
	delete(s.objects, key)
	delete(s.rejected, key)
	delete(s.written, key)
	return held && s.mayWake(key)
}

// wakeOnly limits the keys whose changes wake a pass to those wakes
// accepts, for a store that holds more of a collection than the passes
// act on. A pass still reads every object the store holds.
func (s *objectStore) wakeOnly(wakes func(key string) bool) {
	s.wakes = wakes
}

func (s *objectStore) mayWake(key string) bool {
	return s.wakes == nil || s.wakes(key)
}

// wroteStatus records the status the operator is about to write to the
// object under key, encoded as its trimmed type encodes it, so the
// write's own event wakes no pass. It is recorded before the write,
// because the watch can deliver the event before the write's answer
// arrives. A status that the API server stores in another form wakes
// one pass, which finds nothing to do.
func (s *objectStore) wroteStatus(key string, status json.RawMessage) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.written[key] = status
}

// differs reports whether next differs from stored in what a pass
// reads: any field but the resourceVersion, and the status unless it
// is the one the operator wrote.
func differs(stored, next, written json.RawMessage) bool {
	var before, after map[string]json.RawMessage
	if json.Unmarshal(stored, &before) != nil || json.Unmarshal(next, &after) != nil {
		return true
	}
	if written != nil && bytes.Equal(after["status"], written) {
		after["status"] = before["status"]
	}
	before["metadata"], after["metadata"] = withoutVersion(before["metadata"]), withoutVersion(after["metadata"])
	return !maps.EqualFunc(before, after, func(a, b json.RawMessage) bool { return bytes.Equal(a, b) })
}

// withoutVersion is metadata with no resourceVersion.
func withoutVersion(metadata json.RawMessage) json.RawMessage {
	var fields map[string]json.RawMessage
	if json.Unmarshal(metadata, &fields) != nil {
		return metadata
	}
	delete(fields, "resourceVersion")
	encoded, _ := json.Marshal(fields)
	return encoded
}

// get returns the object under key as the store holds it.
func (s *objectStore) get(key string) (json.RawMessage, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	object, held := s.objects[key]
	return object, held
}

// snapshot returns every object, in namespace/name order as a list
// returns them. The caller holds no lock, and a later event does not
// change what it holds.
func (s *objectStore) snapshot() []json.RawMessage {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	objects := make([]json.RawMessage, 0, len(s.objects))
	for _, key := range slices.Sorted(maps.Keys(s.objects)) {
		objects = append(objects, s.objects[key])
	}
	return objects
}

// decodeSnapshot decodes every object in a store into T.
func decodeSnapshot[T any](store *objectStore) ([]T, error) {
	objects := store.snapshot()
	decoded := make([]T, len(objects))
	for index, object := range objects {
		if err := json.Unmarshal(object, &decoded[index]); err != nil {
			return nil, err
		}
	}
	return decoded, nil
}
