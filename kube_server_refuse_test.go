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

// refuseNext answers the next times requests of method to path with
// code, and then lets them through, as an API server that restarts
// refuses a few requests.
func (f *fakeKube) refuseNext(method, path string, code, times int) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.refusals[method+" "+path] = code
	f.refusalsLeft[method+" "+path] = times
}

// hangUp marks in refusals a request the fake never answers.
const hangUp = -1

// hang holds every request of method to path from now on, and answers
// none, as an API server that stopped answering does. The request ends
// when the client gives up.
func (f *fakeKube) hang(method, path string) {
	f.refuse(method, path, hangUp)
}

// refused answers r when a test asked to refuse it, and reports whether
// it did.
func (f *fakeKube) refused(w http.ResponseWriter, r *http.Request) bool {
	f.mutex.Lock()
	request := r.Method + " " + r.URL.Path
	code, found := f.refusals[request]
	if left, counted := f.refusalsLeft[request]; counted {
		f.refusalsLeft[request] = left - 1
		if left == 1 {
			delete(f.refusals, request)
			delete(f.refusalsLeft, request)
		}
	}
	f.mutex.Unlock()
	if found && code == hangUp {
		<-r.Context().Done()
		return true
	}
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
