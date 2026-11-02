package main

import (
	"encoding/json"
	"testing"
)

// storedVersions returns the resourceVersion of each stored object,
// keyed by namespace/name.
func storedVersions(t *testing.T, store *objectStore) map[string]string {
	t.Helper()
	versions := map[string]string{}
	for _, object := range store.snapshot() {
		meta, err := readStoredMeta(object)
		mustSucceed(t, err)
		versions[meta.key()] = meta.Metadata.ResourceVersion
	}
	return versions
}

// newTestStore builds a store that trims with trim, and returns it
// with the keys of the objects it skipped. An object with no readable
// metadata has an empty key.
func newTestStore(trim func(json.RawMessage) (json.RawMessage, error)) (*objectStore, *[]string) {
	skipped := &[]string{}
	store := newObjectStore(trim, func(key, _ string) {
		*skipped = append(*skipped, key)
	})
	return store, skipped
}

func storedObject(namespace, name, version string) json.RawMessage {
	return json.RawMessage(`{"metadata":{"name":"` + name + `","namespace":"` + namespace + `","resourceVersion":"` + version + `"}}`)
}

// A list holds the whole collection, so replace drops what the list
// does not hold.
func TestReplaceDropsWhatTheListDoesNotHold(t *testing.T) {
	store, _ := newTestStore(trimTo[storedMeta])
	store.put("shop/gear", storedObject("shop", "gear", "1"))
	store.put("shop/bolt", storedObject("shop", "bolt", "2"))

	store.replace([]json.RawMessage{storedObject("shop", "gear", "3")})

	mustMatchMap(t, storedVersions(t, store), map[string]string{"shop/gear": "3"})
}

// A snapshot is the caller's to keep: a later change to the store does
// not reach it.
func TestASnapshotKeepsWhatItRead(t *testing.T) {
	store, _ := newTestStore(trimTo[storedMeta])
	store.put("shop/gear", storedObject("shop", "gear", "1"))
	store.put("house/bolt", storedObject("house", "bolt", "2"))
	snapshot := store.snapshot()

	store.remove("shop/gear")
	store.put("house/bolt", storedObject("house", "bolt", "3"))

	mustMatch(t, string(snapshot[0]), string(storedObject("house", "bolt", "2")))
	mustMatch(t, string(snapshot[1]), string(storedObject("shop", "gear", "1")))
}
