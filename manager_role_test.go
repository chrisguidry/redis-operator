package main

import (
	"context"
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

	h.waitFor("cache-1 detaches with its data", func() bool {
		report := h.report("cache-1")
		return report.Role == reportDetached && report.HasData
	})
	_, err := h.client("cache-0").do(context.Background(), "SET", "after", "detach")
	mustSucceed(t, err)
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
