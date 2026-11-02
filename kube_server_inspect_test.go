package main

// What a test reads back from fakeKube: the requests it received, and
// the version of each object it holds.

import "slices"

// requests returns every request the server received, in order.
func (f *fakeKube) requests() []fakeRequest {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return slices.Clone(f.log)
}

// versions returns the resourceVersion of each object of a resource
// that selector matches, keyed by namespace/name.
func (f *fakeKube) versions(resource kubeResource, selector string) map[string]string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	versions := map[string]string{}
	for key, object := range f.objects {
		if key.resource == resource && selected(parseSelector(selector), object) {
			versions[key.namespace+"/"+key.name] = stringAt(metadataOf(object), "resourceVersion")
		}
	}
	return versions
}

// requestCount counts the requests of one method to one path, the
// watches or the rest.
func (f *fakeKube) requestCount(method, path string, watch bool) int {
	count := 0
	for _, request := range f.requests() {
		if request.Method == method && request.Path == path && (request.Query.Get("watch") == "true") == watch {
			count++
		}
	}
	return count
}
