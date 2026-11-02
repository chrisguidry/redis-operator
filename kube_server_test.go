package main

// fakeKube is a Kubernetes API server for tests. It holds objects of
// any resource in memory and answers the requests that kubeClient
// makes: get, list and watch with a label selector, create, a merge
// patch with a resourceVersion precondition, server-side apply on an object or on
// its status subresource, and delete. A test seeds objects, runs the
// code under test against fakeKube's client, and reads back what the
// code wrote.
//
// It keeps the API server's rules that the reconciler depends on:
//
//   - Every change moves one resourceVersion counter, and a change
//     that writes the same content moves nothing.
//   - A change outside metadata and status increments the generation.
//   - A create of a name that exists gets 409.
//   - A merge patch that states an old resourceVersion gets 409.
//   - An apply removes a status field that its field manager applied
//     before and no longer states.
//   - A delete sets deletionTimestamp while finalizers remain, and the
//     patch that removes the last finalizer removes the object.
//   - An object that is being deleted accepts no new finalizer.

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeKey struct {
	resource  kubeResource
	namespace string
	name      string
}

type fakeEvent struct {
	version int
	key     fakeKey
	kind    string
	object  map[string]any
	// previous is the object before the change, or nil for a create. A
	// watch with a selector reads it to tell an object that stops
	// matching.
	previous map[string]any
}

// fakeRequest is one request as the client sent it.
type fakeRequest struct {
	Method      string
	Path        string
	Query       url.Values
	ContentType string
	Body        string
}

type fakeKube struct {
	t *testing.T
	// client talks to this server. It reads no token.
	client *kubeClient
	// now stamps creationTimestamp and deletionTimestamp.
	now func() time.Time

	mutex   sync.Mutex
	version int
	// oldest is the first resourceVersion a watch can start after. A
	// watch from an earlier version gets 410 Gone.
	oldest  int
	objects map[fakeKey]map[string]any
	// owners holds, for each object and field manager, the status
	// fields that the manager applied last, and applied holds the
	// fields of a whole-object apply the same way.
	owners  map[fakeKey]map[string][][]string
	applied map[fakeKey]map[string][][]string
	// refusals holds the status code for each "METHOD path" a test
	// refuses.
	refusals map[string]int
	events   []fakeEvent
	log      []fakeRequest
	changed  chan struct{}
	dropping chan struct{}
}

func startFakeKube(t *testing.T) *fakeKube {
	t.Helper()
	f := &fakeKube{
		t:        t,
		now:      time.Now,
		objects:  map[fakeKey]map[string]any{},
		owners:   map[fakeKey]map[string][][]string{},
		applied:  map[fakeKey]map[string][][]string{},
		refusals: map[string]int{},
		changed:  make(chan struct{}),
		dropping: make(chan struct{}),
	}
	server := httptest.NewServer(f)
	// Open watches end first, because Close waits for every handler.
	t.Cleanup(func() {
		f.dropWatches()
		server.Close()
	})
	f.client = newKubeClient(server.URL, server.Client(), "")
	f.client.retryUnit = time.Millisecond
	return f
}

// create stores a new object, as a person or a controller would with
// kubectl create. The object is anything that encodes to JSON with
// metadata.name, and metadata.namespace for a namespaced resource.
func (f *fakeKube) create(resource kubeResource, object any) {
	f.t.Helper()
	stored := toObject(f.t, object)
	meta := metadataOf(stored)
	key := fakeKey{resource, stringAt(meta, "namespace"), stringAt(meta, "name")}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if _, exists := f.objects[key]; exists {
		f.t.Fatalf("%s %s/%s already exists", resource, key.namespace, key.name)
	}
	f.version++
	meta["uid"] = fmt.Sprintf("uid-%d", f.version)
	meta["generation"] = 1
	meta["creationTimestamp"] = f.now().UTC().Format(time.RFC3339)
	f.record(key, "ADDED", stored, nil)
}

// read decodes the stored object into out, and reports whether it
// exists.
func (f *fakeKube) read(resource kubeResource, namespace, name string, out any) bool {
	f.t.Helper()
	f.mutex.Lock()
	object, exists := f.objects[fakeKey{resource, namespace, name}]
	f.mutex.Unlock()
	if exists {
		fromObject(f.t, object, out)
	}
	return exists
}

// update changes a stored object in place, as a person would with
// kubectl edit.
func (f *fakeKube) update(resource kubeResource, namespace, name string, change func(object map[string]any)) {
	f.t.Helper()
	f.mutex.Lock()
	defer f.mutex.Unlock()
	key := fakeKey{resource, namespace, name}
	before, exists := f.objects[key]
	if !exists {
		f.t.Fatalf("%s %s/%s does not exist", resource, namespace, name)
	}
	after := clone(before)
	change(after)
	f.commit(key, before, after)
}

// delete deletes an object, as kubectl delete does. An object with
// finalizers gets a deletionTimestamp and stays until the last
// finalizer is gone.
func (f *fakeKube) delete(resource kubeResource, namespace, name string) {
	f.t.Helper()
	f.update(resource, namespace, name, func(object map[string]any) {
		meta := metadataOf(object)
		if _, deleting := meta["deletionTimestamp"]; !deleting {
			meta["deletionTimestamp"] = f.now().UTC().Format(time.RFC3339)
			meta["generation"] = numberAt(meta, "generation") + 1
		}
	})
}

// dropWatches ends every open watch stream, as a restarted API server
// or a broken connection would.
func (f *fakeKube) dropWatches() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	close(f.dropping)
	f.dropping = make(chan struct{})
}

// compact forgets every event so far. A watch from an older
// resourceVersion then gets 410 Gone, and must list again.
func (f *fakeKube) compact() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.oldest = f.version
	f.events = nil
}

// commit stores a change and records its event. A change to nothing
// moves no version. The caller holds the mutex.
func (f *fakeKube) commit(key fakeKey, before, after map[string]any) map[string]any {
	after = clone(after)
	meta := metadataOf(after)
	if finalizers, _ := meta["finalizers"].([]any); len(finalizers) == 0 {
		delete(meta, "finalizers")
	}
	if reflect.DeepEqual(before, after) {
		return after
	}
	if !reflect.DeepEqual(withoutMetaAndStatus(before), withoutMetaAndStatus(after)) {
		meta["generation"] = numberAt(meta, "generation") + 1
	}
	f.version++
	if _, deleting := meta["deletionTimestamp"]; deleting && meta["finalizers"] == nil {
		meta["resourceVersion"] = strconv.Itoa(f.version)
		delete(f.objects, key)
		delete(f.owners, key)
		delete(f.applied, key)
		f.notify(key, "DELETED", after, before)
		return after
	}
	f.record(key, "MODIFIED", after, before)
	return f.objects[key]
}

// record stores an object at the current version and records its
// event. A stored object is never changed in place, so a handler can
// encode it after it releases the mutex. The caller holds the mutex.
func (f *fakeKube) record(key fakeKey, kind string, object, previous map[string]any) {
	object = clone(object)
	metadataOf(object)["resourceVersion"] = strconv.Itoa(f.version)
	f.objects[key] = object
	f.notify(key, kind, object, previous)
}

func (f *fakeKube) notify(key fakeKey, kind string, object, previous map[string]any) {
	f.events = append(f.events, fakeEvent{f.version, key, kind, clone(object), previous})
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *fakeKube) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mutex.Lock()
	f.log = append(f.log, fakeRequest{r.Method, r.URL.Path, r.URL.Query(), r.Header.Get("Content-Type"), string(body)})
	f.mutex.Unlock()
	if f.refused(w, r) {
		return
	}

	key, status, parsed := parseKubePath(r.URL.Path)
	switch {
	case !parsed:
		writeKubeStatus(w, http.StatusNotFound, "the fake API server has no route for "+r.URL.Path)
	case r.Method == http.MethodGet && key.name == "" && r.URL.Query().Get("watch") == "true":
		f.serveWatch(w, r, key, parseSelector(r.URL.Query().Get("labelSelector")))
	case r.Method == http.MethodGet && key.name == "":
		f.serveList(w, key, parseSelector(r.URL.Query().Get("labelSelector")))
	case r.Method == http.MethodPost && key.name == "" && !status:
		f.serveCreate(w, key, body)
	case r.Method == http.MethodDelete && !status:
		f.serveDelete(w, key, body)
	case r.Method == http.MethodGet && !status:
		f.serveGet(w, key)
	case r.Method == http.MethodPatch:
		f.servePatch(w, r, key, status, body)
	default:
		writeKubeStatus(w, http.StatusMethodNotAllowed, r.Method+" "+r.URL.Path+" is not supported by the fake API server")
	}
}

// parseKubePath reads a path that kubeResource.path writes, with an
// optional /status after the name.
func parseKubePath(path string) (fakeKey, bool, bool) {
	var key fakeKey
	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case len(parts) >= 3 && parts[0] == "api":
		key.resource.Version, parts = parts[1], parts[2:]
	case len(parts) >= 4 && parts[0] == "apis":
		key.resource.Group, key.resource.Version, parts = parts[1], parts[2], parts[3:]
	default:
		return key, false, false
	}
	if len(parts) >= 3 && parts[0] == "namespaces" {
		key.namespace, parts = parts[1], parts[2:]
	}
	key.resource.Resource = parts[0]
	if len(parts) >= 2 {
		key.name, _ = url.PathUnescape(parts[1])
	}
	status := len(parts) == 3 && parts[2] == "status"
	return key, status, len(parts) <= 2 || status
}

func (f *fakeKube) serveGet(w http.ResponseWriter, key fakeKey) {
	f.mutex.Lock()
	object, exists := f.objects[key]
	f.mutex.Unlock()
	if !exists {
		writeKubeStatus(w, http.StatusNotFound, key.name+" not found")
		return
	}
	writeKubeJSON(w, object)
}

func (f *fakeKube) serveList(w http.ResponseWriter, collection fakeKey, selector map[string]string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	items := []map[string]any{}
	for _, key := range f.sortedKeys() {
		if collection.matches(key) && selected(selector, f.objects[key]) {
			items = append(items, f.objects[key])
		}
	}
	writeKubeJSON(w, map[string]any{
		"kind":     "List",
		"metadata": map[string]any{"resourceVersion": strconv.Itoa(f.version)},
		"items":    items,
	})
}

// matches reports whether an object is in this collection. A
// collection with no namespace holds every namespace.
func (collection fakeKey) matches(key fakeKey) bool {
	return key.resource == collection.resource && (collection.namespace == "" || key.namespace == collection.namespace)
}

func (f *fakeKube) sortedKeys() []fakeKey {
	return slices.SortedFunc(maps.Keys(f.objects), func(a, b fakeKey) int {
		return strings.Compare(a.namespace+"/"+a.name, b.namespace+"/"+b.name)
	})
}

// serveWatch streams the collection's events after the requested
// resourceVersion until the client leaves or dropWatches ends it. An
// empty version or "0" starts with an ADDED event for each object, as
// the API server does.
func (f *fakeKube) serveWatch(w http.ResponseWriter, r *http.Request, collection fakeKey, selector map[string]string) {
	requested := r.URL.Query().Get("resourceVersion")
	encoder := json.NewEncoder(w)
	w.Header().Set("Content-Type", jsonContentType)

	f.mutex.Lock()
	// The stream holds the channel that was current when it opened, so
	// a drop while it writes still ends it.
	dropping := f.dropping
	var pending []fakeEvent
	last, err := strconv.Atoi(requested)
	if requested == "" || requested == "0" {
		for _, key := range f.sortedKeys() {
			if collection.matches(key) && selected(selector, f.objects[key]) {
				pending = append(pending, fakeEvent{kind: "ADDED", object: f.objects[key]})
			}
		}
		last, err = f.version, nil
	}
	if err != nil || last < f.oldest {
		f.mutex.Unlock()
		_ = encoder.Encode(map[string]any{"type": "ERROR", "object": kubeStatus(http.StatusGone, "too old resource version: "+requested)})
		return
	}
	for {
		for _, event := range f.events {
			if event.version > last && collection.matches(event.key) {
				if sent, matched := selectedEvent(selector, event); matched {
					pending = append(pending, sent)
				}
			}
		}
		last = f.version
		changed := f.changed
		f.mutex.Unlock()

		for _, event := range pending {
			_ = encoder.Encode(map[string]any{"type": event.kind, "object": event.object})
		}
		pending = nil
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			return
		case <-dropping:
			return
		case <-changed:
		}
		// A select picks at random when both are ready. A dropped stream
		// writes nothing more, so a change made after the drop reaches
		// the client only through its next list.
		select {
		case <-dropping:
			return
		default:
		}
		f.mutex.Lock()
	}
}

func withoutMetaAndStatus(object map[string]any) map[string]any {
	rest := maps.Clone(object)
	delete(rest, "metadata")
	delete(rest, "status")
	return rest
}

func metadataOf(object map[string]any) map[string]any {
	meta, _ := object["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		object["metadata"] = meta
	}
	return meta
}

func stringAt(object map[string]any, name string) string {
	value, _ := object[name].(string)
	return value
}

// numberAt reads a number that JSON decoded as float64, or that the
// fake stored as an int.
func numberAt(object map[string]any, name string) int {
	switch value := object[name].(type) {
	case float64:
		return int(value)
	case int:
		return value
	}
	return 0
}

// clone copies an object through JSON, so the copy shares nothing
// with the original and holds only JSON types.
func clone(object map[string]any) map[string]any {
	encoded, _ := json.Marshal(object)
	var copied map[string]any
	_ = json.Unmarshal(encoded, &copied)
	return copied
}

func toObject(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	mustSucceed(t, err)
	var object map[string]any
	mustSucceed(t, json.Unmarshal(encoded, &object))
	return object
}

func fromObject(t *testing.T, object map[string]any, out any) {
	t.Helper()
	encoded, err := json.Marshal(object)
	mustSucceed(t, err)
	mustSucceed(t, json.Unmarshal(encoded, out))
}

func kubeStatus(code int, message string) map[string]any {
	return map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": code, "message": message}
}

func writeKubeStatus(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(kubeStatus(code, message))
}

func writeKubeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", jsonContentType)
	_ = json.NewEncoder(w).Encode(value)
}
