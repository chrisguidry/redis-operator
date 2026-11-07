package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

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

// A scale-down removes the highest ordinals, and a replica there may
// have acknowledged a write the replicas that stay have not taken yet.
// It detaches first, so its offset is final, and the operator shrinks
// once the replicas that stay are past it.
func TestAReplicaThatAScaleDownRemovesDetaches(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["instances"] = 1
	})
	h.waitFor("cache-0 runs with no replica to write to", func() bool {
		applied := h.report("cache-0").MinReplicasToWrite
		return applied != nil && *applied == 0
	})
	h.publishInstances()

	h.waitFor("cache-1 detaches with its data", func() bool {
		report := h.report("cache-1")
		return report.Role == reportDetached && report.HasData
	})
	_, err := h.client("cache-0").do(context.Background(), "SET", "after", "detach")
	mustSucceed(t, err)
}

// The primary takes the lower min-replicas-to-write on its own step, so
// until the operator has seen it run that value, the replica a
// scale-down removes keeps following: detached sooner, it would leave
// a primary that refuses every write with NOREPLICAS.
func TestAReplicaThatAScaleDownRemovesFollowsWhileThePrimaryCountsOnIt(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["instances"] = 1
		object["status"].(map[string]any)["instances"] = []any{
			map[string]any{"name": "cache-0", "role": "primary", "ready": true, "minReplicasToWrite": 1},
			map[string]any{"name": "cache-1", "role": "replica", "ready": true},
		}
	})

	h.holdsFor("cache-1 following cache-0", 1500*time.Millisecond, func() bool {
		return h.report("cache-1").Role == reportReplica
	})
}

// A scale-down to one instance that waits keeps its replicas, and a
// primary that stops then still has one to hand its role to, so it asks
// for a switchover as it would at any other scale.
func TestAStoppingPrimaryWhoseScaleDownWaitsAsksForASwitchover(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["instances"] = 1
		object["status"].(map[string]any)["instances"] = []any{
			map[string]any{"name": "cache-0", "role": "primary", "ready": true},
			map[string]any{"name": "cache-1", "role": "replica", "ready": true},
		}
	})
	stopped := make(chan struct{})
	go func() {
		h.stop("cache-0")
		close(stopped)
	}()

	h.waitFor("cache-0's pod asks for a switchover", func() bool {
		var pod Pod
		return h.api.read(podsResource, testNamespace, "cache-0", &pod) && pod.Metadata.Annotations[switchoverAnnotation] != ""
	})
	select {
	case <-stopped:
		t.Fatal("cache-0 stopped before it handed its role over")
	case <-time.After(1500 * time.Millisecond):
	}
	h.switchTo("cache-1")
	<-stopped
}

// slowPeers serves a peer's /status on each of ips, at one port, and
// answers after delay with cache-0 as the target. It returns the port.
func slowPeers(t *testing.T, delay time.Duration, ips ...string) int {
	t.Helper()
	port := freePort(t)
	for _, ip := range ips {
		listener, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
		mustSucceed(t, err)
		peer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
			_ = json.NewEncoder(w).Encode(instanceReport{Name: "peer", Target: "cache-0"})
		})}
		go func() { _ = peer.Serve(listener) }()
		t.Cleanup(func() { _ = peer.Close() })
	}
	return port
}

// The holder asks its peers at once, so peers that each answer within
// the peer timeout agree within it, however many there are.
func TestPeersAgreeWithinOnePeerTimeout(t *testing.T) {
	t.Parallel()
	port := slowPeers(t, peerTimeout*6/10, "127.0.0.3", "127.0.0.4")
	m := newManager(managerConfig{podName: "cache-0", statusPort: port}, nil, &syncBuffer{})
	m.instances = 3
	m.podIPs = map[string]string{"cache-0": "127.0.0.2", "cache-1": "127.0.0.3", "cache-2": "127.0.0.4"}

	started := time.Now()
	agreed := m.peersAgree(context.Background())

	mustMatch(t, agreed, true)
	mustMatch(t, time.Since(started) < peerTimeout, true)
}

// writeWaits says whether a write to s waits instead of running, as
// one does while the server holds its writes.
func writeWaits(s *server) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := s.do(ctx, "SET", "probe", "held")
	return errors.Is(err, os.ErrDeadlineExceeded)
}

// lonelyHolder runs a primary server for cache-0, whose manager holds
// the Lease on api and whose one peer refuses connections.
func lonelyHolder(t *testing.T, api *fakeKube) (*manager, *server) {
	t.Helper()
	ip, port := "127.0.0.2", freePort(t)
	s := startManagedServer(t, "redis", ip, port)
	_, err := s.do(context.Background(), "REPLICAOF", "NO", "ONE")
	mustSucceed(t, err)
	m := newManager(managerConfig{
		namespace: testNamespace, redisName: "cache", podName: "cache-0", podIP: ip, port: port,
		password: testPassword, statusPort: freePort(t),
		renewDeadline: 10 * time.Second, retryPeriod: 3 * time.Second,
		demotionHold: time.Minute, routeSettle: time.Minute, failoverTimeout: time.Minute,
	}, api.client, &syncBuffer{})
	t.Cleanup(m.server.close)
	m.lease.observed.HolderIdentity = "cache-0"
	m.lease.exists = true
	m.instances = 2
	m.podIPs = map[string]string{"cache-0": ip, "cache-1": "127.0.0.3"}
	return m, s
}

// A holder past its renew deadline holds its writes before it reads the
// API server, so a read that waits out its own timeout never keeps it
// serving after the deadline.
func TestAHolderPastItsRenewDeadlineHoldsWritesBeforeItReads(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	m, s := lonelyHolder(t, api)
	m.lastRenew = time.Now().Add(-time.Minute)
	heldDuringRead := make(chan bool, 1)
	api.duringNext("GET", redisResource.path(testNamespace, "cache"), func() { heldDuringRead <- writeWaits(s) })

	m.step(context.Background())

	mustMatch(t, <-heldDuringRead, true)
}
