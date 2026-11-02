package main

// fakeKube can refuse a request, as the API server refuses one that
// RBAC or admission stops, so a test reaches the code that handles the
// refusal.

import "net/http"

// refuse answers every request of method to path with code from now on.
func (f *fakeKube) refuse(method, path string, code int) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.refusals[method+" "+path] = code
}

// refused answers r when a test asked to refuse it, and reports whether
// it did.
func (f *fakeKube) refused(w http.ResponseWriter, r *http.Request) bool {
	f.mutex.Lock()
	code, found := f.refusals[r.Method+" "+r.URL.Path]
	f.mutex.Unlock()
	if found {
		writeKubeStatus(w, code, r.Method+" "+r.URL.Path+" is refused by the test")
	}
	return found
}

// allow stops refusing "method path".
func (f *fakeKube) allow(method, path string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	delete(f.refusals, method+" "+path)
}
