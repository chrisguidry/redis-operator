package main

// The harness the reconcile tests share: the fake API server, a fake
// instance manager on each pod's address that serves the report a test
// sets, and a controller on a clock the test moves. The tests write
// what the kubelet, the StatefulSet controller, and the instance
// managers would write, and check what the operator decides.

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// testImages are the images every test's operator runs. The tests that
// start real servers find them on PATH, so these names reach no
// registry.
var testImages = images{
	operator: "registry.example.com/redis-operator:test",
	redis:    "registry.example.com/redis:default",
	valkey:   "registry.example.com/valkey:default",
	exporter: "registry.example.com/redis_exporter:default",
}

type harness struct {
	t       *testing.T
	api     *fakeKube
	clock   *testClock
	c       *controller
	log     *syncBuffer
	name    string
	port    int
	running bool

	mutex    sync.Mutex
	reports  map[string]instanceReport
	silenced map[string]bool
}

func startHarness(t *testing.T, instances int) *harness {
	t.Helper()
	h := &harness{
		t: t, api: startFakeKube(t), name: "cache", port: freePort(t),
		clock:    &testClock{at: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)},
		reports:  map[string]instanceReport{},
		silenced: map[string]bool{},
	}
	// The fake stamps creation times from the same clock the controller
	// reads, as the API server and the operator share one time.
	h.api.now = h.clock.now
	h.api.create(redisResource, Redis{
		APIVersion: apiVersion, Kind: redisKind,
		Metadata: ObjectMeta{Name: h.name, Namespace: testNamespace},
		Spec:     RedisSpec{Instances: instances},
	})
	for i := range instances {
		node := fmt.Sprintf("node-%d", i)
		h.api.create(nodesResource, Node{Metadata: ObjectMeta{Name: node}})
		name := instanceName(h.name, i)
		h.api.create(podsResource, Pod{
			Metadata: ObjectMeta{Name: name, Namespace: testNamespace, Labels: map[string]string{clusterLabel: h.name, revisionLabel: "rev-1"}},
			Spec:     PodSpec{NodeName: node},
			Status:   PodStatus{Phase: "Running", PodIP: h.ip(name), Conditions: []PodCondition{{Type: "Ready", Status: "True"}}},
		})
		h.serveReport(name)
	}
	h.api.create(statefulSetsResource, StatefulSet{
		Metadata: ObjectMeta{Name: h.name, Namespace: testNamespace, Labels: map[string]string{clusterLabel: h.name}},
		Status:   StatefulSetStatus{UpdateRevision: "rev-1"},
	})
	h.log = &syncBuffer{}
	h.c = newController(h.api.client, newMetrics("dev"), testImages, h.clock.now, h.log)
	h.c.statusPort = h.port
	h.c.timings.pollTimeout = 200 * time.Millisecond
	h.c.poller = &http.Client{Timeout: h.c.timings.pollTimeout}
	return h
}

func (h *harness) ip(name string) string {
	return fmt.Sprintf("127.0.0.%d", 2+ordinal(name))
}

// serveReport starts the fake instance manager of name.
func (h *harness) serveReport(name string) {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", h.ip(name), h.port))
	mustSucceed(h.t, err)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h.mutex.Lock()
		report, silenced := h.reports[name], h.silenced[name]
		h.mutex.Unlock()
		if silenced {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(report)
	})}
	go func() { _ = server.Serve(listener) }()
	h.t.Cleanup(func() { _ = server.Close() })
}

func (h *harness) setReport(report instanceReport) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.reports[report.Name] = report
}

// silence makes the manager of name stop answering, as a dead pod's
// does.
func (h *harness) silence(name string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.silenced[name] = true
}

// steady sets the reports and the Lease of a healthy Redis whose
// primary is primary.
func (h *harness) steady(primary string, instances int) {
	h.holdLease(primary)
	for i := range instances {
		name := instanceName(h.name, i)
		if name == primary {
			h.setReport(instanceReport{Name: name, Role: reportPrimary, HoldsLease: true, Target: primary, Offset: 100, HasData: true, Ready: true, SyncedReplicas: instances - 1})
			continue
		}
		h.setReport(instanceReport{Name: name, Role: reportReplica, Target: primary, Offset: 100, LinkUp: true, Following: h.ip(primary), HasData: true, Ready: true})
	}
}

// holdLease writes the primary Lease with holder, as its instance
// manager does.
func (h *harness) holdLease(holder string) {
	var lease Lease
	if !h.api.read(leasesResource, testNamespace, primaryLeaseName(h.name), &lease) {
		h.api.create(leasesResource, Lease{
			Metadata: ObjectMeta{Name: primaryLeaseName(h.name), Namespace: testNamespace, Labels: map[string]string{clusterLabel: h.name}},
			Spec:     LeaseSpec{HolderIdentity: holder},
		})
		return
	}
	h.api.update(leasesResource, testNamespace, primaryLeaseName(h.name), func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		if holder == "" {
			delete(spec, "holderIdentity")
		} else {
			spec["holderIdentity"] = holder
		}
	})
}

// pass runs one pass once the controller's stores hold what the fake
// API server holds, so a test's change reaches the pass the way a watch
// event would.
func (h *harness) pass() {
	h.t.Helper()
	h.passWith(h.t.Context())
}

// passWith runs one pass on ctx, as pass does. The watches and the
// Event recorder run on the test's context, so a pass on a context that
// ended, as after the operator lost its Lease, ends only that pass.
func (h *harness) passWith(ctx context.Context) {
	h.t.Helper()
	if !h.running {
		h.running = true
		versions, err := h.c.list(h.t.Context())
		mustSucceed(h.t, err)
		var running sync.WaitGroup
		h.c.watch(h.t.Context(), &running, versions)
		running.Go(func() { h.c.events.run(h.t.Context(), time.Millisecond, time.Second) })
		h.t.Cleanup(running.Wait)
	}
	eventually(h.t, "the stores hold the cluster", h.synced)
	h.c.pass(ctx)
}

func (h *harness) synced() bool {
	for resource, watch := range h.c.watches {
		if !maps.Equal(storedVersions(h.t, watch.store), h.api.versions(resource, watch.selector)) {
			return false
		}
	}
	return true
}

// passFor runs a pass after each step of the clock until it has moved
// by total.
func (h *harness) passFor(total, step time.Duration) {
	h.t.Helper()
	for moved := time.Duration(0); moved < total; moved += step {
		h.clock.advance(step)
		h.pass()
	}
}

func (h *harness) redis() Redis {
	var r Redis
	if !h.api.read(redisResource, testNamespace, h.name, &r) {
		h.t.Fatal("the Redis is gone")
	}
	return r
}

func (h *harness) pod(name string) Pod {
	var pod Pod
	if !h.api.read(podsResource, testNamespace, name, &pod) {
		h.t.Fatalf("pod %s is gone", name)
	}
	return pod
}

func (h *harness) editPod(name string, change func(object map[string]any)) {
	h.api.update(podsResource, testNamespace, name, change)
}

// linesWith counts the operator's log lines that contain text.
func (h *harness) linesWith(text string) int {
	n := 0
	for _, line := range h.log.lines() {
		if strings.Contains(line, text) {
			n++
		}
	}
	return n
}
