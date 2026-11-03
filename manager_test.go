package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
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

func TestTheTargetTakesTheLeaseAndServesAsPrimary(t *testing.T) {
	t.Parallel()
	h := startManagerHarness(t, "redis", 1)
	h.setTarget("cache-0")

	h.start("cache-0", "")

	h.waitFor("cache-0 is a ready primary", func() bool {
		report := h.report("cache-0")
		return report.Role == reportPrimary && report.Ready
	})
	mustMatch(t, h.holder(), "cache-0")
	_, err := h.client("cache-0").do(context.Background(), "SET", "key", "value")
	mustSucceed(t, err)
}

func TestThePrimaryReportsTheReplicasItStreamsTo(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.waitFor("cache-0 reports one synced replica", func() bool { return h.report("cache-0").SyncedReplicas == 1 })
	mustMatch(t, h.report("cache-1").SyncedReplicas, 0)
}

func TestAnInstanceThatIsNotTheTargetFollowsTheHolder(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			h := startManagerHarness(t, engine, 2)
			h.setTarget("cache-0")

			h.start("cache-0", "")
			h.start("cache-1", "")

			h.waitFor("cache-1 replicates cache-0", func() bool {
				report := h.report("cache-1")
				return report.Role == reportReplica && report.LinkUp && report.Following == h.ip("cache-0")
			})
			mustMatch(t, h.holder(), "cache-0")
		})
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

func TestMovingTheTargetHandsThePrimaryRoleOver(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			h := twoInstances(t, engine)

			h.switchTo("cache-1")

			h.waitFor("cache-1 holds the lease as a ready primary", func() bool {
				return h.holder() == "cache-1" && h.report("cache-1").Ready && h.report("cache-1").Role == reportPrimary
			})
			h.waitFor("cache-0 follows cache-1", func() bool {
				report := h.report("cache-0")
				return report.Role == reportReplica && report.Following == h.ip("cache-1")
			})
			got, err := h.client("cache-1").do(context.Background(), "GET", "key")
			mustSucceed(t, err)
			mustMatch(t, got, any("value"))
		})
	}
}

// FAILOVER leaves the old primary a replica of the new one, and the
// new primary counts it toward min-replicas-to-write. If the old
// primary let go of that link while the Lease was empty, the new
// primary would refuse writes with NOREPLICAS until it came back.
func TestTheOldPrimaryKeepsStreamingFromTheNewOneAcrossASwitchover(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			h := twoInstances(t, engine)

			h.switchTo("cache-1")

			h.waitFor("cache-0 follows cache-1 with its link up", func() bool {
				report := h.report("cache-0")
				return h.holder() == "cache-1" && report.LinkUp && report.Following == h.ip("cache-1")
			})
			mustMatch(t, partialSyncs(t, h.client("cache-1")), 1)
		})
	}
}

// partialSyncs is how many partial resynchronizations the server
// accepted from its replicas. A replica that reconnects to the same
// primary resynchronizes partially, so this counts the cuts in its link.
func partialSyncs(t *testing.T, s *server) int {
	t.Helper()
	reply, err := s.do(context.Background(), "INFO", "stats")
	mustSucceed(t, err)
	for line := range strings.SplitSeq(reply.(string), "\n") {
		if count, found := strings.CutPrefix(strings.TrimSpace(line), "sync_partial_ok:"); found {
			n, err := strconv.Atoi(count)
			mustSucceed(t, err)
			return n
		}
	}
	t.Fatal("INFO stats has no sync_partial_ok")
	return 0
}

func TestAPendingTargetDetachesEveryInstance(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.setTarget(targetPending)

	h.waitFor("both instances detach and nobody holds the lease", func() bool {
		return h.report("cache-0").Role == reportDetached && h.report("cache-1").Role == reportDetached && h.holder() == ""
	})
}

func TestTheNewTargetTakesTheLeaseOfADeadPrimaryOnlyAfterItExpires(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")
	h.crash("cache-0")
	h.setTarget(targetPending)
	h.waitFor("cache-1 detaches", func() bool { return h.report("cache-1").Role == reportDetached })

	h.setTarget("cache-1")
	took := time.Now()

	h.waitFor("cache-1 holds the lease", func() bool { return h.holder() == "cache-1" })
	if waited := time.Since(took); waited < time.Second {
		t.Fatalf("cache-1 took the lease after %s, before it could have expired", waited)
	}
	h.waitFor("cache-1 is a ready primary with the data", func() bool { return h.report("cache-1").Ready })
	got, err := h.client("cache-1").do(context.Background(), "GET", "key")
	mustSucceed(t, err)
	mustMatch(t, got, any("value"))
}

func TestAnInstanceThatRestartsEmptyGivesUpThePrimaryLease(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.crash("cache-0")
	h.start("cache-0", "")

	h.waitFor("nobody holds the lease, and cache-0 stays detached", func() bool {
		return h.holder() == "" && h.report("cache-0").Role == reportDetached
	})
	mustMatch(t, h.report("cache-0").HasData, false)
}

// A server with no volume can still leave a snapshot in its data
// directory: a backup's BGSAVE or a replica's sync writes one there. The
// manager reports such an instance as empty after a restart, so its
// server must not load that snapshot and serve the keys in it.
func TestAnInstanceWithNoStorageRestartsWithNoKeys(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			h := twoInstances(t, engine)
			primary := h.client("cache-0")
			// SAVE refuses while a background save runs, such as the one
			// a replica's sync starts.
			h.waitFor("cache-0 saved a snapshot", func() bool {
				_, err := primary.do(context.Background(), "SAVE")
				return err == nil
			})
			dir := h.instances["cache-0"].dir

			h.crash("cache-0")
			h.start("cache-0", dir)

			h.waitFor("cache-0 answers", func() bool { return h.report("cache-0").Role != reportStarting })
			keys, err := h.client("cache-0").do(context.Background(), "DBSIZE")
			mustSucceed(t, err)
			mustMatch(t, keys, any(int64(0)))
			mustMatch(t, h.report("cache-0").HasData, false)
		})
	}
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

func TestAPrimaryKeepsServingThroughAnAPIOutageWhileItsPeersAgree(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.outage()
	h.fastForward("cache-0", "cache-0 passes its renew deadline", 250*time.Millisecond, func() bool {
		return h.instances["cache-0"].clock.now().Sub(time.Now()) > 2500*time.Millisecond
	})

	mustMatch(t, h.report("cache-0").Role, reportPrimary)
	_, err := h.client("cache-0").do(context.Background(), "SET", "during", "outage")
	mustSucceed(t, err)
}

func TestAPrimaryThatCannotRenewAndCannotReachAPeerFencesItself(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.crash("cache-1")
	h.outage()

	h.fastForward("cache-0", "cache-0 detaches", time.Second, func() bool { return h.report("cache-0").Role == reportDetached })
	_, err := h.client("cache-0").do(context.Background(), "SET", "after", "fencing")
	mustMatch(t, err != nil, true)
}

func TestAStoppingPrimaryAsksForASwitchoverAndHandsOver(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")
	stopped := make(chan struct{})
	go func() {
		h.stop("cache-0")
		close(stopped)
	}()

	h.waitFor("cache-0's pod asks for a switchover", func() bool {
		var pod Pod
		return h.api.read(podsResource, testNamespace, "cache-0", &pod) && pod.Metadata.Annotations[switchoverAnnotation] != ""
	})
	h.switchTo("cache-1")

	<-stopped
	h.waitFor("cache-1 holds the lease as a ready primary", func() bool {
		return h.holder() == "cache-1" && h.report("cache-1").Ready
	})
	got, err := h.client("cache-1").do(context.Background(), "GET", "key")
	mustSucceed(t, err)
	mustMatch(t, got, any("value"))
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
