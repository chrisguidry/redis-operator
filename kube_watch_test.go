package main

import (
	"bufio"
	"maps"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadWatchEventsKeepsTheStore(t *testing.T) {
	t.Parallel()
	const (
		added    = `{"type":"ADDED","object":{"metadata":{"namespace":"shop","name":"gear","resourceVersion":"10"}}}`
		modified = `{"type":"MODIFIED","object":{"metadata":{"namespace":"shop","name":"gear","resourceVersion":"12"}}}`
		deleted  = `{"type":"DELETED","object":{"metadata":{"namespace":"shop","name":"gear","resourceVersion":"13"}}}`
		bookmark = `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"11"}}}`
		gone     = `{"type":"ERROR","object":{"kind":"Status","code":410}}`
		failed   = `{"type":"ERROR","object":{"kind":"Status","code":500}}`
		garbled  = `{"type":"ADDED","object":{"metadata":"gear"}}`
		later    = `{"type":"MODIFIED","object":{"metadata":{"namespace":"shop","name":"gear","resourceVersion":"99"}}}`
	)
	cases := []struct {
		name    string
		events  []string
		version string
		end     streamEnd
		wakes   int
		stored  map[string]string
	}{
		{"a bookmark moves only the version", []string{bookmark}, "11", streamClosed, 0, map[string]string{}},
		{"an added object is stored", []string{added}, "10", streamClosed, 1, map[string]string{"shop/gear": "10"}},
		{"a modified object replaces the stored one", []string{added, modified}, "12", streamClosed, 1, map[string]string{"shop/gear": "12"}},
		{"a deleted object leaves the store", []string{added, deleted}, "13", streamClosed, 1, map[string]string{}},
		{"a 410 ends the stream", []string{added, gone, later}, "10", streamGone, 1, map[string]string{"shop/gear": "10"}},
		{"another error ends the stream", []string{added, failed, later}, "10", streamBroken, 1, map[string]string{"shop/gear": "10"}},
		{"an event with no readable metadata ends the stream", []string{added, garbled, later}, "10", streamBroken, 1, map[string]string{"shop/gear": "10"}},
		{"a line that does not decode ends the stream", []string{added, `{"type":`, later}, "10", streamBroken, 1, map[string]string{"shop/gear": "10"}},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			store, _ := newTestStore(trimTo[storedMeta])
			wake := make(chan struct{}, 1)
			reader := bufio.NewReader(strings.NewReader(strings.Join(one.events, "\n") + "\n"))

			version, end, _ := readWatchEvents(reader, store, "1", wake)

			mustMatch(t, version, one.version)
			mustMatch(t, end, one.end)
			mustMatch(t, len(wake), one.wakes)
			mustMatchMap(t, storedVersions(t, store), one.stored)
		})
	}
}

// A line that the connection cut before its newline is a dropped
// stream, which resumes, not an event that does not decode.
func TestALineTheConnectionCutEndsTheStreamAsADrop(t *testing.T) {
	t.Parallel()
	store, _ := newTestStore(trimTo[storedMeta])
	reader := bufio.NewReader(strings.NewReader(`{"type":"ADDED","object":{"metadata":{"namespace":"shop","name":"gear","resourceVersion":"10"}}}` + "\n" + `{"type":"MODIFIED","obj`))

	version, end, _ := readWatchEvents(reader, store, "1", make(chan struct{}, 1))

	mustMatch(t, version, "10")
	mustMatch(t, end, streamClosed)
}

// widgetWatch is a watch on every namespace's widgets, with the channel
// it wakes and the count of its restarts. It waits a millisecond after
// a stream ends, so a test runs fast.
type widgetWatch struct {
	*collectionWatch
	wake     chan struct{}
	restarts atomic.Int32
}

func newWidgetWatch(client *kubeClient) *widgetWatch {
	w := &widgetWatch{wake: make(chan struct{}, 1)}
	w.collectionWatch = newCollectionWatch(client, widgets, "", "", trimTo[widget], w.wake, func() { w.restarts.Add(1) })
	w.pause = time.Millisecond
	w.backoff = 4 * time.Millisecond
	return w
}

// start runs the watch from resourceVersion until the test ends.
func (w *widgetWatch) start(t *testing.T, resourceVersion string) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		w.run(t.Context(), resourceVersion)
	}()
	t.Cleanup(func() {
		select {
		case <-stopped:
		case <-time.After(testTimeout):
			t.Error("the watch did not stop")
		}
	})
}

// listAndStart fills the store from a list and starts the watch from
// it, the way the controller starts each watch.
func (w *widgetWatch) listAndStart(t *testing.T) {
	t.Helper()
	version, err := w.list(t.Context())
	mustSucceed(t, err)
	w.start(t, version)
}

// startWidgetWatch runs a new widget watch from resourceVersion until
// the test ends.
func startWidgetWatch(t *testing.T, client *kubeClient, resourceVersion string) *widgetWatch {
	t.Helper()
	w := newWidgetWatch(client)
	w.start(t, resourceVersion)
	return w
}

// waitForStore waits until the store holds each widget at the version
// the fake API server holds.
func waitForStore(t *testing.T, api *fakeKube, store *objectStore) {
	t.Helper()
	eventually(t, "the store to hold the widgets", func() bool {
		return maps.Equal(storedVersions(t, store), api.versions(widgets, ""))
	})
}

func waitForWake(t *testing.T, wake <-chan struct{}) {
	t.Helper()
	select {
	case <-wake:
	case <-time.After(testTimeout):
		t.Fatal("the watch did not wake the loop")
	}
}

// waitForWatchFrom waits until the fake API server has a watch request
// from resourceVersion.
func waitForWatchFrom(t *testing.T, api *fakeKube, resourceVersion string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		for _, request := range api.requests() {
			if request.Query.Get("watch") == "true" && request.Query.Get("resourceVersion") == resourceVersion {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no watch from resourceVersion %s in %v", resourceVersion, api.requests())
}

func TestAChangeWakesTheLoop(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	watch := startWidgetWatch(t, api.client, "0")
	waitForWatchFrom(t, api, "0")

	api.create(widgets, newWidget("shop", "gear"))

	waitForWake(t, watch.wake)
}

func TestTheWatchAsksForBookmarks(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	startWidgetWatch(t, api.client, "0")
	waitForWatchFrom(t, api, "0")

	mustMatch(t, api.requests()[0].Query.Get("allowWatchBookmarks"), "true")
}

// A create, an update, and a delete in the cluster each reach the
// store through the watch.
func TestChangesReachTheStore(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		change func(api *fakeKube)
		stored map[string]string
	}{
		{"a create", func(api *fakeKube) {
			api.create(widgets, newWidget("shop", "bolt"))
		}, map[string]string{"shop/bolt": "small", "shop/gear": "small"}},
		{"an update", func(api *fakeKube) {
			api.update(widgets, "shop", "gear", func(object map[string]any) { object["spec"] = map[string]any{"size": "large"} })
		}, map[string]string{"shop/gear": "large"}},
		{"a delete", func(api *fakeKube) {
			api.delete(widgets, "shop", "gear")
		}, map[string]string{}},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			api := startFakeKube(t)
			api.create(widgets, newWidget("shop", "gear"))
			watch := newWidgetWatch(api.client)
			watch.listAndStart(t)
			waitForWatchFrom(t, api, "1")

			one.change(api)

			waitForStore(t, api, watch.store)
			mustMatchMap(t, widgetSizes(t, watch.store), one.stored)
		})
	}
}

// widgetSizes returns each stored widget's spec.size, keyed by
// namespace/name.
func widgetSizes(t *testing.T, store *objectStore) map[string]string {
	t.Helper()
	stored, err := decodeSnapshot[widget](store)
	mustSucceed(t, err)
	sizes := map[string]string{}
	for _, one := range stored {
		sizes[one.Metadata.Namespace+"/"+one.Metadata.Name], _ = one.Spec["size"].(string)
	}
	return sizes
}

// A dropped stream resumes from the last version it delivered, with
// no list, and the store keeps what it held.
func TestADroppedWatchResumesWithNoList(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	watch := startWidgetWatch(t, api.client, "0")
	waitForWatchFrom(t, api, "0")
	api.create(widgets, newWidget("shop", "gear"))
	api.create(widgets, newWidget("shop", "bolt"))
	waitForStore(t, api, watch.store)

	api.dropWatches()

	waitForWatchFrom(t, api, "2")
	mustMatch(t, api.requestCount("GET", "/apis/example.com/v1/widgets", false), 0)
	mustMatch(t, watch.restarts.Load(), 1)
	mustMatchMap(t, storedVersions(t, watch.store), map[string]string{"shop/gear": "1", "shop/bolt": "2"})
}

// An object deleted while the stream is down reaches the store through
// the watch that resumes from the version before the delete.
func TestADeleteWhileTheStreamIsDownLeavesTheStore(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	api.create(widgets, newWidget("shop", "bolt"))
	watch := newWidgetWatch(api.client)
	watch.pause = 50 * time.Millisecond
	watch.listAndStart(t)
	waitForWatchFrom(t, api, "2")

	api.dropWatches()
	api.delete(widgets, "shop", "bolt")

	waitForWatchFrom(t, api, "2")
	waitForStore(t, api, watch.store)
	mustMatchMap(t, storedVersions(t, watch.store), map[string]string{"shop/gear": "1"})
	mustMatch(t, api.requestCount("GET", "/apis/example.com/v1/widgets", false), 1)
}

// A 410 Gone means the API server no longer has the watch's version.
// The watch lists again for a current one, and the list replaces the
// store.
func TestAGoneWatchRelistsAndResumes(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	watch := newWidgetWatch(api.client)
	version, err := watch.list(t.Context())
	mustSucceed(t, err)
	api.create(widgets, newWidget("shop", "bolt"))
	api.delete(widgets, "shop", "gear")
	api.compact()

	watch.start(t, version)

	waitForWatchFrom(t, api, "3")
	mustMatchMap(t, storedVersions(t, watch.store), map[string]string{"shop/bolt": "2"})
}

// A refused watch or a failed list is tried again, with a wait between
// tries.
func TestARefusedWatchIsTriedAgain(t *testing.T) {
	t.Parallel()
	var watches atomic.Int32
	client := testKubeClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" {
			watches.Add(1)
		}
		writeKubeStatus(w, http.StatusForbidden, "widgets is forbidden")
	})

	startWidgetWatch(t, client, "5")

	deadline := time.Now().Add(testTimeout)
	for watches.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	mustMatch(t, watches.Load() >= 3, true)
}
