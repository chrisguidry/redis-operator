package main

import (
	"context"
	"fmt"
	"path/filepath"
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
}

type runningManager struct {
	manager *manager
	stop    context.CancelFunc
	done    chan error
	dir     string
}

func startManagerHarness(t *testing.T, engine string, instances int) *managerHarness {
	t.Helper()
	h := &managerHarness{
		t: t, api: startFakeKube(t), name: "cache",
		port: freePort(t), statusAt: freePort(t),
		instances: map[string]*runningManager{},
	}
	h.api.create(redisResource, Redis{
		APIVersion: apiVersion, Kind: redisKind,
		Metadata: ObjectMeta{Name: h.name, Namespace: testNamespace},
		Spec:     RedisSpec{Engine: engine, Instances: instances, Memory: "64Mi"},
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
	var ordinal int
	_, _ = fmt.Sscanf(name, h.name+"-%d", &ordinal)
	return fmt.Sprintf("127.0.0.%d", 2+ordinal)
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
	m := newManager(managerConfig{
		namespace: testNamespace, redisName: h.name, podName: name, podIP: h.ip(name),
		port: h.port, statusPort: h.statusAt, dataDir: dir,
		configPath: filepath.Join(dir, "server.conf"), password: testPassword,
		leaseDuration: 1500 * time.Millisecond, renewDeadline: time.Second,
		retryPeriod: 200 * time.Millisecond, stepPeriod: 100 * time.Millisecond,
		failoverTimeout: 3 * time.Second, handoverWait: 3 * time.Second,
	}, h.api.client, &syncBuffer{})
	stop, cancel := context.WithCancel(context.Background())
	running := &runningManager{manager: m, stop: cancel, done: make(chan error, 1), dir: dir}
	go func() { running.done <- m.run(stop) }()
	h.instances[name] = running
	h.t.Cleanup(func() { h.stop(name) })
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

func TestAnInstanceThatIsNotTheTargetFollowsTheHolder(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
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
	h.waitFor("cache-1 replicates cache-0", func() bool {
		return h.report("cache-1").LinkUp && h.report("cache-0").Ready
	})
	h.setCurrentPrimary("cache-0")
	_, err := h.client("cache-0").do(context.Background(), "SET", "key", "value")
	mustSucceed(t, err)
	return h
}

func TestMovingTheTargetHandsThePrimaryRoleOver(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			h := twoInstances(t, engine)

			h.setTarget("cache-1")

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

func TestAPendingTargetDetachesEveryInstance(t *testing.T) {
	h := twoInstances(t, "redis")

	h.setTarget(targetPending)

	h.waitFor("both instances detach and nobody holds the lease", func() bool {
		return h.report("cache-0").Role == reportDetached && h.report("cache-1").Role == reportDetached && h.holder() == ""
	})
}

func TestTheNewTargetTakesTheLeaseOfADeadPrimaryOnlyAfterItExpires(t *testing.T) {
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
	h := twoInstances(t, "redis")

	h.crash("cache-0")
	h.start("cache-0", "")

	h.waitFor("nobody holds the lease, and cache-0 stays detached", func() bool {
		return h.holder() == "" && h.report("cache-0").Role == reportDetached
	})
	mustMatch(t, h.report("cache-0").HasData, false)
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
	h := twoInstances(t, "redis")

	h.outage()
	time.Sleep(2500 * time.Millisecond)

	mustMatch(t, h.report("cache-0").Role, reportPrimary)
	_, err := h.client("cache-0").do(context.Background(), "SET", "during", "outage")
	mustSucceed(t, err)
}

func TestAPrimaryThatCannotRenewAndCannotReachAPeerFencesItself(t *testing.T) {
	h := twoInstances(t, "redis")

	h.crash("cache-1")
	h.outage()

	h.waitFor("cache-0 detaches", func() bool { return h.report("cache-0").Role == reportDetached })
	_, err := h.client("cache-0").do(context.Background(), "SET", "after", "fencing")
	mustMatch(t, err != nil, true)
}

func TestAStoppingPrimaryAsksForASwitchoverAndHandsOver(t *testing.T) {
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
	h.setTarget("cache-1")

	<-stopped
	h.waitFor("cache-1 holds the lease as a ready primary", func() bool {
		return h.holder() == "cache-1" && h.report("cache-1").Ready
	})
	got, err := h.client("cache-1").do(context.Background(), "GET", "key")
	mustSucceed(t, err)
	mustMatch(t, got, any("value"))
}
