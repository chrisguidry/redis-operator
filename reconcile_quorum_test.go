package main

import (
	"testing"
	"time"
)

// failQuorum makes a steady Redis of instances whose primary is
// cache-0, with spec.minReplicasToWrite set to minReplicas, then
// silences cache-0 and runs passes until the failover is pending.
func (h *harness) failQuorum(instances, minReplicas int) {
	h.t.Helper()
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["minReplicasToWrite"] = minReplicas
	})
	h.steady("cache-0", instances)
	h.pass()
	h.silence("cache-0")
	h.passFor(8*time.Second, 2*time.Second)
	mustMatch(h.t, h.redis().Status.TargetPrimary, targetPending)
}

// unsilence makes the manager of name answer again, as a pod's does
// once a partition heals.
func (h *harness) unsilence(name string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	delete(h.silenced, name)
}

// annotate sets an annotation on the Redis, as a person does with
// kubectl annotate.
func (h *harness) annotate(key, value string) {
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		metadata := object["metadata"].(map[string]any)
		annotations, _ := metadata["annotations"].(map[string]any)
		if annotations == nil {
			annotations = map[string]any{}
		}
		annotations[key] = value
		metadata["annotations"] = annotations
	})
}

// detached is the report of an instance that detached for a failover
// with its data at offset.
func detached(name string, offset int64) instanceReport {
	return instanceReport{Name: name, Role: reportDetached, Target: targetPending, Offset: offset, HasData: true}
}

func TestAFailoverNamesAPrimaryOnlyOnceItReachedEnoughInstances(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		instances   int
		minReplicas int
		reports     []instanceReport
		want        string
	}{
		{"both replicas of three", 3, 1, []instanceReport{detached("cache-1", 90), detached("cache-2", 120)}, "cache-2"},
		{"one replica of three", 3, 1, []instanceReport{detached("cache-1", 90)}, targetPending},
		{"one replica of three that each write reaches both of", 3, 2, []instanceReport{detached("cache-1", 90)}, "cache-1"},
		{"the only replica of two", 2, 1, []instanceReport{detached("cache-1", 90)}, "cache-1"},
		{"three replicas of five", 5, 2, []instanceReport{detached("cache-1", 90), detached("cache-2", 80), detached("cache-3", 70)}, "cache-1"},
		{"two replicas of five", 5, 2, []instanceReport{detached("cache-1", 90), detached("cache-2", 80)}, targetPending},
		{"a replica that has not detached", 3, 1, []instanceReport{
			detached("cache-1", 90),
			{Name: "cache-2", Role: reportReplica, Target: targetPending, Offset: 80, HasData: true},
		}, targetPending},
		{"a replica that restarted with no data", 3, 1, []instanceReport{
			detached("cache-1", 90),
			{Name: "cache-2", Role: reportDetached, Target: targetPending, Offset: 500},
		}, targetPending},
		{"a replica that is stopping", 3, 1, []instanceReport{
			detached("cache-1", 90),
			{Name: "cache-2", Role: reportDetached, Target: targetPending, Offset: 80, HasData: true, Stopping: true},
		}, targetPending},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, c.instances)
			h.failQuorum(c.instances, c.minReplicas)
			answered := map[string]bool{}
			for _, report := range c.reports {
				h.setReport(report)
				answered[report.Name] = true
			}
			for i := 1; i < c.instances; i++ {
				if name := instanceName(h.name, i); !answered[name] {
					h.silence(name)
				}
			}

			h.passFor(10*time.Second, 2*time.Second)

			mustMatch(t, h.redis().Status.TargetPrimary, c.want)
		})
	}
}

func TestAFailoverWithoutItsQuorumSaysWhy(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.setReport(detached("cache-1", 90))
	h.silence("cache-2")

	h.passFor(10*time.Second, 2*time.Second)

	ready := findReady(t, h.redis())
	mustMatch(t, ready.Status, ConditionFalse)
	mustMatch(t, ready.Reason, "QuorumNotReached")
	want := "1 of the 2 instances a failover needs have detached with their data. " +
		"With fewer, the next primary could lack a write confirmed with WAIT 1. " +
		"To promote the best of them anyway, annotate the Redis with redis.guid.foo/failover-without-quorum=true"
	mustMatch(t, ready.Message, want)
	mustContainLine(t, h.redisEvents(), "Warning QuorumNotReached: Ready is False: "+want)
}

// The detach wait comes first: until it passes, a failover that has
// not heard from every replica is in progress, not short of a quorum.
func TestAFailoverIsNotShortOfAQuorumBeforeTheDetachWait(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.setReport(detached("cache-1", 90))
	h.silence("cache-2")

	h.passFor(2*time.Second, 2*time.Second)

	mustMatch(t, findReady(t, h.redis()).Reason, "FailoverInProgress")
}

func TestAnInstanceThatComesBackCompletesTheQuorum(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.setReport(detached("cache-1", 90))
	h.silence("cache-2")
	h.passFor(30*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)

	h.unsilence("cache-2")
	h.setReport(detached("cache-2", 120))
	h.passFor(2*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-2")
}

func TestAnAnnotationForcesOneFailoverWithoutItsQuorum(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.setReport(detached("cache-1", 90))
	h.silence("cache-2")
	h.passFor(10*time.Second, 2*time.Second)

	h.annotate(forceFailoverAnnotation, "true")
	h.passFor(2*time.Second, 2*time.Second)

	r := h.redis()
	mustMatch(t, r.Status.TargetPrimary, "cache-1")
	_, kept := r.Metadata.Annotations[forceFailoverAnnotation]
	mustMatch(t, kept, false)
	mustContainLine(t, h.redisEvents(), "Warning FailoverForced: cache-1 is the next primary without the quorum: 1 of the 2 instances it needs had detached with their data, so a write only the others held is lost")
}

func TestAForcedFailoverStillNeverNamesAnInstanceWithoutData(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.annotate(forceFailoverAnnotation, "true")

	h.setReport(detached("cache-1", 90))
	h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Offset: 500, HasData: false})
	h.passFor(2*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-1")
}

// Only the value true forces a failover, so a mistyped value leaves the
// quorum in place.
func TestAnAnnotationOtherThanTrueForcesNothing(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.annotate(forceFailoverAnnotation, "yes")
	h.setReport(detached("cache-1", 90))
	h.silence("cache-2")

	h.passFor(10*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)
}

// The quorum counts every instance that could have acknowledged a
// write, so while a scale-down leaves more pods than spec.instances,
// it counts the pods.
func TestTheQuorumCountsThePodsAScaleDownHasNotRemoved(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.setInstances(2)
	h.setReport(detached("cache-1", 90))
	h.silence("cache-2")

	h.passFor(10*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)
}

// A replica that restarted with no data and has not synced since holds
// none of the writes it acknowledged, so while the old primary is
// unreachable, a failover that reached only one replica with data waits.
// The old primary, once reachable and detached with its data, is one of
// the instances the quorum counts, so its return completes the quorum.
func TestAnOldPrimaryThatComesBackWithItsDataCompletesTheQuorum(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.holdLease("")
	h.setReport(instanceReport{Name: "cache-1", Role: reportDetached, Target: targetPending, Offset: 20})
	h.setReport(detached("cache-2", 117))
	h.passFor(30*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)
	mustMatch(t, findReady(t, h.redis()).Reason, "QuorumNotReached")

	h.unsilence("cache-0")
	h.setReport(detached("cache-0", 120))
	h.passFor(2*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
}
