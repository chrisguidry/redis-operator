package main

// The writes that fakeKube accepts: create, a merge patch on an object,
// server-side apply on an object or on its status, and delete.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"
)

// serveCreate stores the object in the body under the name in its
// metadata, in the collection of the path.
func (f *fakeKube) serveCreate(w http.ResponseWriter, collection fakeKey, body []byte) {
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		writeKubeStatus(w, http.StatusBadRequest, err.Error())
		return
	}
	meta := metadataOf(object)
	key := fakeKey{collection.resource, collection.namespace, stringAt(meta, "name")}
	meta["namespace"] = key.namespace
	if key.namespace == "" {
		delete(meta, "namespace")
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if _, exists := f.objects[key]; exists {
		writeKubeStatus(w, http.StatusConflict, key.name+" already exists")
		return
	}
	f.version++
	meta["uid"] = fmt.Sprintf("uid-%d", f.version)
	meta["generation"] = 1
	meta["creationTimestamp"] = f.now().UTC().Format(time.RFC3339)
	f.record(key, "ADDED", object, nil)
	writeKubeJSON(w, f.objects[key])
}

func (f *fakeKube) servePatch(w http.ResponseWriter, r *http.Request, key fakeKey, status bool, body []byte) {
	var patch map[string]any
	if err := json.Unmarshal(body, &patch); err != nil {
		writeKubeStatus(w, http.StatusBadRequest, err.Error())
		return
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	contentType := r.Header.Get("Content-Type")
	before, exists := f.objects[key]
	if !exists && contentType == applyContentType && !status {
		writeKubeJSON(w, f.applyNew(key, patch, r.URL.Query().Get("fieldManager")))
		return
	}
	if !exists {
		writeKubeStatus(w, http.StatusNotFound, key.name+" not found")
		return
	}
	var answer int
	var message string
	var after map[string]any
	switch {
	case contentType == mergePatchContentType && !status:
		after, answer, message = mergeObjectPatch(before, patch)
	case contentType == mergePatchContentType && status:
		after, answer, message = mergeStatusPatch(before, patch)
	case contentType == applyContentType && status:
		after, answer, message = f.applyStatus(key, before, patch, r.URL.Query().Get("fieldManager"))
	case contentType == applyContentType:
		after, answer, message = f.applyObject(key, before, patch, r.URL.Query().Get("fieldManager"))
	default:
		answer, message = http.StatusUnsupportedMediaType, "the fake API server does not support "+contentType+" here"
	}
	if answer != http.StatusOK {
		writeKubeStatus(w, answer, message)
		return
	}
	writeKubeJSON(w, f.commit(key, before, after))
}

// mergeObjectPatch applies a JSON merge patch (RFC 7386) to an object.
// The main resource ignores status, as it does for a resource with a
// status subresource.
func mergeObjectPatch(before, patch map[string]any) (map[string]any, int, string) {
	delete(patch, "status")
	patchMeta, _ := patch["metadata"].(map[string]any)
	meta := metadataOf(before)
	if version, stated := patchMeta["resourceVersion"]; stated && version != meta["resourceVersion"] {
		return nil, http.StatusConflict, fmt.Sprintf("the object has been modified: resourceVersion %v, want %v", meta["resourceVersion"], version)
	}
	after := clone(before)
	mergeInto(after, patch)
	if _, deleting := meta["deletionTimestamp"]; deleting {
		held, _ := meta["finalizers"].([]any)
		asked, _ := metadataOf(after)["finalizers"].([]any)
		for _, finalizer := range asked {
			if !slices.Contains(held, finalizer) {
				return nil, http.StatusUnprocessableEntity, fmt.Sprintf("no new finalizers can be added while the object is being deleted: %v", finalizer)
			}
		}
	}
	return after, http.StatusOK, ""
}

// mergeStatusPatch applies a JSON merge patch to the status
// subresource: only the status and the resourceVersion precondition
// count, as the API server ignores the rest of a status write.
func mergeStatusPatch(before, patch map[string]any) (map[string]any, int, string) {
	patchMeta, _ := patch["metadata"].(map[string]any)
	meta := metadataOf(before)
	if version, stated := patchMeta["resourceVersion"]; stated && version != meta["resourceVersion"] {
		return nil, http.StatusConflict, fmt.Sprintf("the object has been modified: resourceVersion %v, want %v", meta["resourceVersion"], version)
	}
	after := clone(before)
	status, _ := after["status"].(map[string]any)
	if status == nil {
		status = map[string]any{}
	}
	patchStatus, _ := patch["status"].(map[string]any)
	mergeInto(status, patchStatus)
	after["status"] = status
	return after, http.StatusOK, ""
}

// applyStatus merges an applied status into the object, and removes
// the status fields that the same field manager applied before and
// does not state now.
func (f *fakeKube) applyStatus(key fakeKey, before, applied map[string]any, manager string) (map[string]any, int, string) {
	if manager == "" {
		return nil, http.StatusBadRequest, "fieldManager is required for apply"
	}
	status, _ := applied["status"].(map[string]any)
	after := clone(before)
	stored, _ := after["status"].(map[string]any)
	if stored == nil {
		stored = map[string]any{}
	}
	fields := leafPaths(nil, status)
	for _, path := range f.owners[key][manager] {
		if !slices.ContainsFunc(fields, func(field []string) bool { return slices.Equal(field, path) }) {
			deletePath(stored, path)
		}
	}
	mergeInto(stored, status)
	after["status"] = stored
	if f.owners[key] == nil {
		f.owners[key] = map[string][][]string{}
	}
	f.owners[key][manager] = fields
	return after, http.StatusOK, ""
}

// mergeInto merges src into dst: maps merge by key, a null removes the
// key, and any other value replaces the one in dst.
func mergeInto(dst, src map[string]any) {
	for name, value := range src {
		inner, isMap := value.(map[string]any)
		existing, hasMap := dst[name].(map[string]any)
		switch {
		case value == nil:
			delete(dst, name)
		case isMap && hasMap:
			mergeInto(existing, inner)
		default:
			dst[name] = value
		}
	}
}

// leafPaths lists the paths to every value in a map that is not
// itself a map. A list is one value, as in a status field with
// listType atomic.
func leafPaths(prefix []string, object map[string]any) [][]string {
	var paths [][]string
	for name, value := range object {
		path := append(slices.Clone(prefix), name)
		if inner, isMap := value.(map[string]any); isMap && len(inner) > 0 {
			paths = append(paths, leafPaths(path, inner)...)
		} else {
			paths = append(paths, path)
		}
	}
	return paths
}

// deletePath removes one field, and each map above it that the removal
// leaves empty.
func deletePath(object map[string]any, path []string) {
	if len(path) > 1 {
		inner, _ := object[path[0]].(map[string]any)
		if inner == nil {
			return
		}
		deletePath(inner, path[1:])
		if len(inner) > 0 {
			return
		}
	}
	delete(object, path[0])
}

// applyNew creates an object from an apply, as the API server does for
// an apply to a name that does not exist. The caller holds the mutex.
func (f *fakeKube) applyNew(key fakeKey, applied map[string]any, manager string) map[string]any {
	f.applied[key] = map[string][][]string{manager: leafPaths(nil, applied)}
	meta := metadataOf(applied)
	f.version++
	meta["uid"] = fmt.Sprintf("uid-%d", f.version)
	meta["generation"] = 1
	meta["creationTimestamp"] = f.now().UTC().Format(time.RFC3339)
	f.record(key, "ADDED", applied, nil)
	return f.objects[key]
}

// applyObject merges an applied object into the stored one, and removes
// the fields that the same field manager applied before and does not
// state now. A field another writer set stays, and the applied value
// wins over it, as force gives it. A list is one field, as an atomic
// list is.
func (f *fakeKube) applyObject(key fakeKey, before, applied map[string]any, manager string) (map[string]any, int, string) {
	if manager == "" {
		return nil, http.StatusBadRequest, "fieldManager is required for apply"
	}
	after := clone(before)
	fields := leafPaths(nil, applied)
	for _, path := range f.applied[key][manager] {
		if !slices.ContainsFunc(fields, func(field []string) bool { return slices.Equal(field, path) }) {
			deletePath(after, path)
		}
	}
	mergeInto(after, applied)
	if f.applied[key] == nil {
		f.applied[key] = map[string][][]string{}
	}
	f.applied[key][manager] = fields
	return after, http.StatusOK, ""
}

// serveDelete deletes an object. One with finalizers gets a
// deletionTimestamp and stays until the last finalizer is gone. A uid
// or resourceVersion in the body's preconditions that is not the
// object's gets 409, as the API server answers a delete of an object
// made again under the same name, or changed since it was read. A uid
// that is present and empty is a uid too, as the API server reads it,
// and matches no object.
func (f *fakeKube) serveDelete(w http.ResponseWriter, key fakeKey, body []byte) {
	var options struct {
		Preconditions struct {
			UID             *string `json:"uid"`
			ResourceVersion string  `json:"resourceVersion"`
		} `json:"preconditions"`
	}
	_ = json.Unmarshal(body, &options)
	f.mutex.Lock()
	defer f.mutex.Unlock()
	before, exists := f.objects[key]
	if !exists {
		writeKubeStatus(w, http.StatusNotFound, key.name+" not found")
		return
	}
	if uid := options.Preconditions.UID; uid != nil && *uid != metadataOf(before)["uid"] {
		writeKubeStatus(w, http.StatusConflict, "Precondition failed: UID in precondition: "+*uid)
		return
	}
	if version := options.Preconditions.ResourceVersion; version != "" && version != metadataOf(before)["resourceVersion"] {
		writeKubeStatus(w, http.StatusConflict, "Precondition failed: ResourceVersion in precondition: "+version)
		return
	}
	after := clone(before)
	meta := metadataOf(after)
	if _, deleting := meta["deletionTimestamp"]; !deleting {
		meta["deletionTimestamp"] = f.now().UTC().Format(time.RFC3339)
	}
	f.commit(key, before, after)
	writeKubeStatus(w, http.StatusOK, key.name+" deleted")
}
