package main

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const testNamespace = "example"

// managerHarness runs instance managers against real servers and the
// fake API server. Each instance binds its own loopback address, the
// way each pod has its own IP, and every instance uses the same server
// port.
type managerHarness struct {
	t         *testing.T
	api       *fakeKube
	name      string
	port      int
	statusAt  int
	instances map[string]*runningManager
	// backupDir and restoreDir stand in for a volume every instance
	// mounts, as a ReadWriteMany claim or an NFS export is.
	backupDir  string
	restoreDir string
	// tlsSecret is the certificate's Secret of each instance that start
	// runs next, or empty for an instance without TLS.
	tlsSecret string
	tlsPort   int
	// leaseReadPeriod and slowReadPeriod are the read periods of each
	// instance that start runs next. Zero reads on every step.
	leaseReadPeriod time.Duration
	slowReadPeriod  time.Duration
}

type runningManager struct {
	manager *manager
	clock   *skippableClock
	stop    context.CancelFunc
	done    chan error
	dir     string
}

// skippableClock is the wall clock plus an offset that a test moves
// forward, so a manager's deadline passes without the test waiting it
// out. Each manager has its own: a skip on the clock of the instance
// that holds the Lease would make it miss its renewals.
type skippableClock struct {
	mutex  sync.Mutex
	offset time.Duration
}

func (c *skippableClock) now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return time.Now().Add(c.offset)
}

func (c *skippableClock) skip(d time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.offset += d
}

func startManagerHarness(t *testing.T, engine string, instances int) *managerHarness {
	t.Helper()
	return startManagerHarnessOn(t, startFakeKube(t), "cache", engine, instances, nil)
}

// startManagerHarnessOn declares a Redis named name on api, with its
// pods, and lets change edit its spec first.
func startManagerHarnessOn(t *testing.T, api *fakeKube, name, engine string, instances int, change func(*RedisSpec)) *managerHarness {
	t.Helper()
	h := &managerHarness{
		t: t, api: api, name: name,
		port: freePort(t), statusAt: freePort(t), tlsPort: freePort(t),
		instances: map[string]*runningManager{},
		backupDir: t.TempDir(), restoreDir: t.TempDir(),
	}
	spec := RedisSpec{Engine: engine, Instances: instances, Memory: "64Mi"}
	if change != nil {
		change(&spec)
	}
	h.api.create(redisResource, Redis{
		APIVersion: apiVersion, Kind: redisKind,
		Metadata: ObjectMeta{Name: h.name, Namespace: testNamespace},
		Spec:     spec,
	})
	for i := range instances {
		h.api.create(podsResource, Pod{
			Metadata: ObjectMeta{Name: instanceName(h.name, i), Namespace: testNamespace, Labels: map[string]string{clusterLabel: h.name}},
			Status:   PodStatus{PodIP: h.ip(instanceName(h.name, i))},
		})
	}
	return h
}

// ip is the loopback address of the instance named name.
func (h *managerHarness) ip(name string) string {
	return fmt.Sprintf("127.0.0.%d", 2+ordinal(name))
}

// setTarget writes status.targetPrimary, as the operator does.
func (h *managerHarness) setTarget(target string) {
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		status, _ := object["status"].(map[string]any)
		if status == nil {
			status = map[string]any{}
		}
		status["targetPrimary"] = target
		object["status"] = status
	})
}

// switchTo starts a switchover from the holder to target, as the
// operator does: the target, and status.switchover.
func (h *managerHarness) switchTo(target string) {
	from := h.holder()
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		status, _ := object["status"].(map[string]any)
		if status == nil {
			status = map[string]any{}
		}
		status["targetPrimary"] = target
		status["switchover"] = map[string]any{"from": from, "to": target, "reason": "Test"}
		object["status"] = status
	})
}

// setCurrentPrimary writes status.currentPrimary, as the operator does
// once a primary holds the Lease.
func (h *managerHarness) setCurrentPrimary(name string) {
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		status, _ := object["status"].(map[string]any)
		status["currentPrimary"] = name
		object["status"] = status
	})
}

// publishInstances writes status.instances from each manager's current
// report, as the operator's pass does.
func (h *managerHarness) publishInstances() {
	var instances []any
	for i := range len(h.instances) {
		report := h.report(instanceName(h.name, i))
		entry := map[string]any{"name": report.Name, "role": report.Role, "ready": report.Ready}
		if report.MinReplicasToWrite != nil {
			entry["minReplicasToWrite"] = *report.MinReplicasToWrite
		}
		instances = append(instances, entry)
	}
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["status"].(map[string]any)["instances"] = instances
	})
}

// start runs the manager of the instance named name, in dir, or in a
// new directory when dir is empty.
func (h *managerHarness) start(name, dir string) {
	h.t.Helper()
	if dir == "" {
		dir = h.t.TempDir()
	}
	clock := &skippableClock{}
	m := newManager(managerConfig{
		namespace: testNamespace, redisName: h.name, podName: name, podIP: h.ip(name),
		port: h.port, statusPort: h.statusAt, dataDir: dir,
		configPath: filepath.Join(dir, "server.conf"), password: testPassword,
		backupDir: h.backupDir, restoreDir: h.restoreDir,
		tlsSecret: h.tlsSecret, tlsDir: filepath.Join(dir, "tls"), tlsPort: h.tlsPort,
		leaseDuration: 1500 * time.Millisecond, renewDeadline: time.Second,
		retryPeriod: 200 * time.Millisecond, stepPeriod: 100 * time.Millisecond,
		leaseReadPeriod: h.leaseReadPeriod, slowReadPeriod: h.slowReadPeriod,
		failoverTimeout: 3 * time.Second, handoverWait: 3 * time.Second,
		demotionHold: 5 * time.Second, routeSettle: 500 * time.Millisecond, pauseGap: time.Second,
		clock: clock.now,
	}, h.api.client, &syncBuffer{})
	stop, cancel := context.WithCancel(context.Background())
	running := &runningManager{manager: m, clock: clock, stop: cancel, done: make(chan error, 1), dir: dir}
	go func() { running.done <- m.run(stop) }()
	h.instances[name] = running
	h.t.Cleanup(func() { h.stopWithoutHandover(name) })
}

// skip moves the clock of the manager of name forward by d.
func (h *managerHarness) skip(name string, d time.Duration) {
	h.instances[name].clock.skip(d)
}

// fastForward waits until done, as waitFor does, and moves the clock of
// the manager of name ahead by stride at each of its steps, so its
// deadlines pass at that pace. Only that manager's clock moves, so it
// suits an instance that has lost the Lease or is losing it.
func (h *managerHarness) fastForward(name, what string, stride time.Duration, done func() bool) {
	h.t.Helper()
	running := h.instances[name]
	deadline := time.Now().Add(20 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting until %s", what)
		}
		running.clock.skip(stride)
		time.Sleep(running.manager.config.stepPeriod)
	}
}

// stopWithoutHandover stops the manager of name as stop does, and moves
// its clock past the handover wait at each step until it exits, for a
// test that does not exercise the handover.
func (h *managerHarness) stopWithoutHandover(name string) {
	running, ok := h.instances[name]
	if !ok {
		return
	}
	delete(h.instances, name)
	running.stop()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-running.done:
			return
		case <-deadline:
			h.t.Errorf("the manager of %s did not stop", name)
			return
		case <-time.After(running.manager.config.stepPeriod):
			running.clock.skip(running.manager.config.handoverWait)
		}
	}
}

// stop stops the manager of name, as SIGTERM does, and waits for it.
func (h *managerHarness) stop(name string) {
	running, ok := h.instances[name]
	if !ok {
		return
	}
	delete(h.instances, name)
	running.stop()
	select {
	case <-running.done:
	case <-time.After(15 * time.Second):
		h.t.Errorf("the manager of %s did not stop", name)
	}
}

// crash stops the server of name without its manager's part in it, so
// the manager exits the way a crashed container does: holding whatever
// Lease it held.
func (h *managerHarness) crash(name string) {
	h.t.Helper()
	running := h.instances[name]
	delete(h.instances, name)
	_, _ = h.client(name).do(context.Background(), "SHUTDOWN", "NOSAVE")
	select {
	case <-running.done:
	case <-time.After(10 * time.Second):
		h.t.Fatalf("the manager of %s did not exit after its server stopped", name)
	}
	running.stop()
}

func (h *managerHarness) report(name string) instanceReport {
	return h.instances[name].manager.currentReport()
}

func (h *managerHarness) holder() string {
	var lease Lease
	if !h.api.read(leasesResource, testNamespace, primaryLeaseName(h.name), &lease) {
		return ""
	}
	return lease.Spec.HolderIdentity
}

func (h *managerHarness) client(name string) *server {
	s := &server{addr: fmt.Sprintf("%s:%d", h.ip(name), h.port), password: testPassword}
	h.t.Cleanup(s.close)
	return s
}

func (h *managerHarness) waitFor(what string, done func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting until %s", what)
}

// holdsFor fails the test if held stops being true within d.
func (h *managerHarness) holdsFor(what string, d time.Duration, held func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !held() {
			h.t.Fatalf("%s stopped holding", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// twoInstances starts a primary and a replica that holds its data.
func twoInstances(t *testing.T, engine string) *managerHarness {
	t.Helper()
	h := startManagerHarness(t, engine, 2)
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.start("cache-1", "")
	// The replica's link comes up before the primary counts it toward
	// min-replicas-to-write, so the wait is for the primary's count.
	h.waitFor("cache-0 streams to cache-1", func() bool {
		return h.report("cache-1").LinkUp && h.report("cache-0").Ready && h.report("cache-0").SyncedReplicas == 1
	})
	h.setCurrentPrimary("cache-0")
	_, err := h.client("cache-0").do(context.Background(), "SET", "key", "value")
	mustSucceed(t, err)
	return h
}

// outage makes the API server refuse every request the managers make,
// as an API server that is down does.
func (h *managerHarness) outage() {
	lease := leasesResource.path(testNamespace, primaryLeaseName(h.name))
	for _, request := range [][2]string{
		{"GET", redisResource.path(testNamespace, h.name)},
		{"GET", lease},
		{"PATCH", lease},
		{"GET", podsResource.path(testNamespace, "")},
	} {
		h.api.refuse(request[0], request[1], 503)
	}
}

// A single instance has no replica to hand its role to, so it stops at
// once instead of waiting for a switchover that cannot come.
func TestAStoppingSingleInstanceStopsWithoutWaitingForAHandover(t *testing.T) {
	t.Parallel()
	h := startManagerHarness(t, "redis", 1)
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })

	started := time.Now()
	h.stop("cache-0")

	if waited := time.Since(started); waited >= time.Second {
		t.Fatalf("cache-0 took %s to stop", waited)
	}
}
