package main

// The scenarios every watch loop must pass, from liken's operator
// guide. Each one runs the loop against a scripted API server that can
// answer slowly, hold a stream open, and cut a connection, and checks
// the requests the loop sent and the waits between them.
//
// A scenario whose correct result is "at once" sets the watch's pause
// to an hour, so a loop that waits instead never reaches the request
// the test waits for.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// watchStep is how the scripted server answers one watch request.
type watchStep struct {
	// delay is how long the server waits before it answers.
	delay time.Duration
	// status is the response's code. Zero is 200, a stream.
	status int
	// before is written at once, then the stream is held for hold, then
	// after is written, then the stream ends as end says.
	before []string
	hold   time.Duration
	after  []string
	end    streamFinish
}

type streamFinish int

const (
	// finishOpen holds the stream until the client leaves.
	finishOpen streamFinish = iota
	// finishClose ends the response cleanly.
	finishClose
	// finishReset cuts the connection in the middle of the response.
	finishReset
)

// scriptedRequest is one request the loop sent: a list, or a watch from
// a version, with when it arrived and when its response ended.
type scriptedRequest struct {
	what    string
	arrived time.Time
	ended   time.Time
}

// scriptedAPI answers each watch with the next step of its script, and
// holds every watch after the last step open. Each list answers the
// version 100 times the number of lists so far.
type scriptedAPI struct {
	t *testing.T

	mutex    sync.Mutex
	steps    []watchStep
	repeat   bool
	log      []scriptedRequest
	listings int
}

func startScriptedAPI(t *testing.T, repeat bool, steps ...watchStep) (*scriptedAPI, *kubeClient) {
	t.Helper()
	api := &scriptedAPI{t: t, steps: steps, repeat: repeat}
	return api, testKubeClient(t, api.serve)
}

func (s *scriptedAPI) serve(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	index := len(s.log)
	if r.URL.Query().Get("watch") != "true" {
		s.listings++
		version := strconv.Itoa(100 * s.listings)
		s.log = append(s.log, scriptedRequest{what: "list", arrived: time.Now(), ended: time.Now()})
		s.mutex.Unlock()
		writeKubeJSON(w, map[string]any{"metadata": map[string]any{"resourceVersion": version}, "items": []any{}})
		return
	}
	s.log = append(s.log, scriptedRequest{what: "watch " + r.URL.Query().Get("resourceVersion"), arrived: time.Now()})
	step := watchStep{}
	if len(s.steps) > 0 {
		step = s.steps[0]
		if !s.repeat {
			s.steps = s.steps[1:]
		}
	}
	s.mutex.Unlock()
	defer func() {
		s.mutex.Lock()
		s.log[index].ended = time.Now()
		s.mutex.Unlock()
	}()
	s.answer(w, r, step)
}

func (s *scriptedAPI) answer(w http.ResponseWriter, r *http.Request, step watchStep) {
	if !waitOrLeave(r.Context(), step.delay) {
		return
	}
	if step.status != 0 && step.status != http.StatusOK {
		writeKubeStatus(w, step.status, "scripted")
		return
	}
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(http.StatusOK)
	write := func(events []string) {
		for _, event := range events {
			_, _ = fmt.Fprintln(w, event)
		}
		w.(http.Flusher).Flush()
	}
	write(step.before)
	if !waitOrLeave(r.Context(), step.hold) {
		return
	}
	write(step.after)
	switch step.end {
	case finishOpen:
		<-r.Context().Done()
	case finishReset:
		panic(http.ErrAbortHandler)
	}
}

// waitOrLeave waits, and reports false when the client left first.
func waitOrLeave(ctx context.Context, wait time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(wait):
		return true
	}
}

func (s *scriptedAPI) requests() []scriptedRequest {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return slices.Clone(s.log)
}

// waitForRequests waits until the loop has sent count requests, and
// returns them.
func (s *scriptedAPI) waitForRequests(count int) []scriptedRequest {
	s.t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if log := s.requests(); len(log) >= count {
			return log[:count]
		}
		time.Sleep(time.Millisecond)
	}
	s.t.Fatalf("wanted %d requests, got %v", count, requestNames(s.requests()))
	return nil
}

func requestNames(log []scriptedRequest) string {
	names := make([]string, len(log))
	for index, request := range log {
		names[index] = request.what
	}
	return strings.Join(names, ", ")
}

// scenarioWatch runs a widget watch from version 1 until the test
// ends. A watch that lives for life counts as one that ran.
func scenarioWatch(t *testing.T, client *kubeClient, pause, backoff, life time.Duration) *widgetWatch {
	t.Helper()
	w := newWidgetWatch(client)
	w.pause, w.backoff, w.minLife = pause, backoff, life
	w.start(t, "1")
	return w
}

func addedEvent(name, version string) string {
	return `{"type":"ADDED","object":{"metadata":{"namespace":"shop","name":"` + name + `","resourceVersion":"` + version + `"}}}`
}

func errorEvent(code int) string {
	return `{"type":"ERROR","object":{"kind":"Status","apiVersion":"v1","status":"Failure","code":` + strconv.Itoa(code) + `}}`
}

const (
	undecodableLine = `{"type":"ADDED","object":{"metadata":`
	undecodableMeta = `{"type":"ADDED","object":{"metadata":"gear"}}`
)

// mustWaitBetween fails unless the later request arrived at least wait
// after the earlier one. The server cannot see exactly when the client
// let go of a response, so the wait counts from the earlier request's
// arrival, and a caller adds the time the server took to answer.
func mustWaitBetween(t *testing.T, earlier, later scriptedRequest, wait time.Duration) {
	t.Helper()
	if gap := later.arrived.Sub(earlier.arrived); gap < wait {
		t.Errorf("%s came %v after %s, want at least %v", later.what, gap, earlier.what, wait)
	}
}

// never is a pause no test waits out.
const never = time.Hour

// A watch that ran and then closed, cleanly or with a reset
// connection, opens the next one at once from the last version it
// delivered, with no list.
func TestAWatchThatRanResumesAtOnceWithNoList(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		end  streamFinish
	}{
		{"a clean close", finishClose},
		{"a reset connection", finishReset},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			api, client := startScriptedAPI(t, false, watchStep{before: []string{addedEvent("gear", "5")}, hold: 30 * time.Millisecond, end: one.end})
			scenarioWatch(t, client, never, never, 10*time.Millisecond)

			mustMatch(t, requestNames(api.waitForRequests(2)), "watch 1, watch 5")
		})
	}
}

// A watch that closes sooner than a watch's life is a failure: the loop
// waits out the backoff, then opens the next watch from the last
// version, still with no list.
func TestAShortWatchWaitsThenResumesWithNoList(t *testing.T) {
	t.Parallel()
	const wait = 50 * time.Millisecond
	api, client := startScriptedAPI(t, false, watchStep{before: []string{addedEvent("gear", "5")}, end: finishClose})
	scenarioWatch(t, client, wait, wait, never)

	log := api.waitForRequests(2)

	mustMatch(t, requestNames(log), "watch 1, watch 5")
	mustWaitBetween(t, log[0], log[1], wait)
}

// A 410 Gone lists again at once, whether it is the response or an
// ERROR event in a stream, and whether the watch had run or not.
func TestAGoneWatchListsOnceAtOnce(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		step watchStep
	}{
		{"a 410 response", watchStep{status: http.StatusGone}},
		{"a 410 ERROR event", watchStep{before: []string{errorEvent(http.StatusGone)}}},
		{"a 410 ERROR event after the watch ran", watchStep{before: []string{addedEvent("gear", "5")}, hold: 30 * time.Millisecond, after: []string{errorEvent(http.StatusGone)}}},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			api, client := startScriptedAPI(t, false, one.step)
			scenarioWatch(t, client, never, never, 10*time.Millisecond)

			mustMatch(t, requestNames(api.waitForRequests(3)), "watch 1, list, watch 100")
		})
	}
}

// A 410 from the watch that a 410's list opened waits out the backoff
// before the next list, so an API server that answers 410 to every
// version is listed no faster than the backoff allows, and the waits
// grow.
func TestA410OnEveryVersionListsAtTheBackoffsPace(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		step watchStep
	}{
		{"a 410 response", watchStep{status: http.StatusGone}},
		{"a 410 ERROR event", watchStep{before: []string{errorEvent(http.StatusGone)}}},
		{"a slow 410 response", watchStep{delay: 30 * time.Millisecond, status: http.StatusGone}},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			const wait = 20 * time.Millisecond
			api, client := startScriptedAPI(t, true, one.step)
			scenarioWatch(t, client, wait, 4*wait, 10*time.Millisecond)

			log := api.waitForRequests(7)

			mustMatch(t, requestNames(log), "watch 1, list, watch 100, list, watch 200, list, watch 300")
			mustWaitBetween(t, log[2], log[3], one.step.delay+wait)
			mustWaitBetween(t, log[4], log[5], one.step.delay+2*wait)
		})
	}
}

// An ERROR event of another code, or an event that does not decode,
// closes the stream at once, though the server holds it open, then
// waits out the backoff, then lists. Go's client reads a body it closes
// early for a moment before it drops the connection, so the wait is
// long enough for the server to see the close well inside it.
func TestABrokenStreamClosesWaitsAndLists(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		event string
	}{
		{"an ERROR event that is not a 410", errorEvent(http.StatusInternalServerError)},
		{"a line that does not decode", undecodableLine},
		{"an object whose metadata does not decode", undecodableMeta},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			const wait = 300 * time.Millisecond
			api, client := startScriptedAPI(t, false, watchStep{before: []string{one.event}})
			scenarioWatch(t, client, wait, wait, 10*time.Millisecond)

			log := api.waitForRequests(3)

			mustMatch(t, requestNames(log), "watch 1, list, watch 100")
			mustMatch(t, log[0].ended.Sub(log[0].arrived) < wait, true)
			mustWaitBetween(t, log[0], log[1], wait)
		})
	}
}

// A refused watch backs off and tries the same version again, with no
// list, and once the API server accepts it the watch runs as usual.
func TestARefusedWatchBacksOffThenWatches(t *testing.T) {
	t.Parallel()
	const wait = 20 * time.Millisecond
	refused := watchStep{status: http.StatusForbidden}
	api, client := startScriptedAPI(t, false, refused, refused, refused, watchStep{before: []string{addedEvent("gear", "5")}})
	watch := scenarioWatch(t, client, wait, 8*wait, 10*time.Millisecond)

	log := api.waitForRequests(4)

	mustMatch(t, requestNames(log), "watch 1, watch 1, watch 1, watch 1")
	mustWaitBetween(t, log[0], log[1], wait)
	mustWaitBetween(t, log[1], log[2], 2*wait)
	mustWaitBetween(t, log[2], log[3], 4*wait)
	eventually(t, "the store to hold the widget", func() bool {
		return storedVersions(t, watch.store)["shop/gear"] == "5"
	})
}

// A refusal that is slow to arrive is still a watch that never ran, so
// the backoff grows across slow refusals as it does across quick ones.
func TestASlowRefusalStillGrowsTheBackoff(t *testing.T) {
	t.Parallel()
	const wait, slow = 20 * time.Millisecond, 30 * time.Millisecond
	api, client := startScriptedAPI(t, true, watchStep{delay: slow, status: http.StatusForbidden})
	scenarioWatch(t, client, wait, 8*wait, 10*time.Millisecond)

	log := api.waitForRequests(4)

	mustWaitBetween(t, log[0], log[1], slow+wait)
	mustWaitBetween(t, log[1], log[2], slow+2*wait)
	mustWaitBetween(t, log[2], log[3], slow+4*wait)
}

// A watch's life counts from the 200, not from the request, so a 200
// that is slow to arrive followed by a short stream is a failure, and
// the loop waits before the next watch.
func TestAWatchsLifeCountsFromThe200(t *testing.T) {
	t.Parallel()
	const wait, slow, held = 50 * time.Millisecond, 60 * time.Millisecond, 10 * time.Millisecond
	api, client := startScriptedAPI(t, false, watchStep{delay: slow, before: []string{addedEvent("gear", "5")}, hold: held, end: finishClose})
	scenarioWatch(t, client, wait, wait, 40*time.Millisecond)

	log := api.waitForRequests(2)

	mustMatch(t, requestNames(log), "watch 1, watch 5")
	mustWaitBetween(t, log[0], log[1], slow+held+wait)
}

// A 200 that is slow to arrive, then a stream that runs, delivers every
// event the stream holds.
func TestASlow200ThenAStreamDeliversEveryEvent(t *testing.T) {
	t.Parallel()
	_, client := startScriptedAPI(t, false, watchStep{
		delay: 50 * time.Millisecond, before: []string{addedEvent("gear", "5")},
		hold: 50 * time.Millisecond, after: []string{addedEvent("bolt", "6")},
	})
	watch := scenarioWatch(t, client, never, never, 10*time.Millisecond)

	eventually(t, "the store to hold both widgets", func() bool {
		stored := storedVersions(t, watch.store)
		return stored["shop/gear"] == "5" && stored["shop/bolt"] == "6"
	})
}

// A server that cannot be dialed is tried again with a wait that
// doubles up to the backoff.
func TestADeadServerGrowsTheBackoffToItsCap(t *testing.T) {
	t.Parallel()
	const wait = 10 * time.Millisecond
	var mutex sync.Mutex
	var dials []time.Time
	refuse := func(context.Context, string, string) (net.Conn, error) {
		mutex.Lock()
		defer mutex.Unlock()
		dials = append(dials, time.Now())
		return nil, errors.New("connection refused")
	}
	client := newKubeClient("http://api.example.com", &http.Client{Transport: &http.Transport{DialContext: refuse}}, "")
	scenarioWatch(t, client, wait, 4*wait, 10*time.Millisecond)

	var seen []time.Time
	eventually(t, "six dials", func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		seen = slices.Clone(dials)
		return len(seen) >= 6
	})

	for index, want := range []time.Duration{wait, 2 * wait, 4 * wait, 4 * wait, 4 * wait} {
		if gap := seen[index+1].Sub(seen[index]); gap < want {
			t.Errorf("dial %d came %v after the one before, want at least %v", index+2, gap, want)
		}
	}
}
