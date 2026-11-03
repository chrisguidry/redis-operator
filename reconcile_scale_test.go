package main

import (
	"strings"
	"testing"
	"time"
)

// setInstances changes spec.instances, as a user's edit does.
func (h *harness) setInstances(n int) {
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["instances"] = n
	})
}

// setTarget writes status.targetPrimary, as an earlier pass did.
func (h *harness) setTarget(target string) {
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		status, _ := object["status"].(map[string]any)
		if status == nil {
			status = map[string]any{}
		}
		status["targetPrimary"] = target
		object["status"] = status
	})
}

// statefulSet reads the StatefulSet's spec as the operator applied it.
func (h *harness) statefulSetSpec() map[string]any {
	h.t.Helper()
	var held map[string]any
	if !h.api.read(statefulSetsResource, testNamespace, h.name, &held) {
		h.t.Fatal("the StatefulSet is gone")
	}
	spec, _ := held["spec"].(map[string]any)
	return spec
}

func (h *harness) replicas() int {
	h.t.Helper()
	replicas, _ := h.statefulSetSpec()["replicas"].(float64)
	return int(replicas)
}

func TestScalingDownPastThePrimarySwitchesOverBeforeTheStatefulSetShrinks(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		primary   string
		instances int
		offsets   map[string]int64
		want      string
		during    int
	}{
		{"to one instance", "cache-1", 1, map[string]int64{"cache-0": 100, "cache-2": 100}, "cache-0", 2},
		{"to the most caught-up instance that stays", "cache-2", 2, map[string]int64{"cache-0": 100, "cache-1": 150}, "cache-1", 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, 3)
			h.setTarget(c.primary)
			h.steady(c.primary, 3)
			for name, offset := range c.offsets {
				report := h.reports[name]
				report.Offset = offset
				h.setReport(report)
			}
			h.pass()

			h.setInstances(c.instances)
			h.pass()

			status := h.redis().Status
			mustMatch(t, status.TargetPrimary, c.want)
			mustMatch(t, status.Switchover.Reason, reasonScaleDown)
			mustMatch(t, h.replicas(), c.during)

			h.steady(c.want, 3)
			h.pass()

			mustMatch(t, h.redis().Status.Switchover == nil, true)
			mustMatch(t, h.replicas(), c.instances)
			mustMatch(t, findReady(t, h.redis()).Reason, "ScalingDown")
		})
	}
}

func TestScalingDownAboveThePrimaryShrinksAtOnce(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()

	h.setInstances(1)
	h.pass()

	mustMatch(t, h.redis().Status.Switchover == nil, true)
	mustMatch(t, h.replicas(), 1)
}

func TestTheBudgetOnThePrimaryExistsOnlyWithMoreThanOneInstance(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	var budget map[string]any

	h.setInstances(1)
	h.pass()
	mustMatch(t, h.api.read(budgetsResource, testNamespace, "cache-primary", &budget), false)
	mustMatch(t, h.api.read(budgetsResource, testNamespace, "cache-replicas", &budget), true)

	h.setInstances(3)
	h.pass()
	mustMatch(t, h.api.read(budgetsResource, testNamespace, "cache-primary", &budget), true)
}

func TestTheStatefulSetDeletesTheVolumeOfAnInstanceItScalesAway(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["storage"] = map[string]any{"size": "1Gi"}
	})

	h.pass()

	policy, _ := h.statefulSetSpec()["persistentVolumeClaimRetentionPolicy"].(map[string]any)
	mustMatch(t, policy["whenScaled"], any("Delete"))
	mustMatch(t, policy["whenDeleted"], any("Retain"))
}

// The StatefulSet removes its highest ordinals first, and while no
// primary is settled one of them may hold the only copy of a write
// confirmed with WAIT, so a scale-down waits.
func TestAScaleDownWaitsWhileThePrimaryIsNotSettled(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		unsettle func(h *harness)
		reason   string
		message  string
	}{
		{"a failover is pending", func(h *harness) {
			h.failQuorum(3, 1)
			h.setReport(detached("cache-1", 90))
			h.silence("cache-2")
			h.passFor(10*time.Second, 2*time.Second)
		}, "QuorumNotReached", "The StatefulSet keeps its 3 pods until the next primary holds the lease, then scales down to 1"},
		{"no instance holds the lease", func(h *harness) {
			h.steady("cache-0", 3)
			h.pass()
			h.holdLease("")
		}, "NoPrimary", "The StatefulSet keeps its 3 pods until the next primary holds the lease, then scales down to 1"},
		{"a switchover is in flight", func(h *harness) {
			h.steady("cache-0", 3)
			h.pass()
			h.editPod("cache-0", func(object map[string]any) {
				metadataOf(object)["annotations"] = map[string]any{switchoverAnnotation: "now"}
			})
			h.pass()
			mustMatch(t, h.redis().Status.Switchover != nil, true)
		}, "SwitchoverInProgress", "The StatefulSet keeps its 3 pods until the next primary holds the lease, then scales down to 1"},
		{"the primary restarted before its lease expired", func(h *harness) {
			h.steady("cache-0", 3)
			h.pass()
			h.editPod("cache-0", func(object map[string]any) {
				object["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Ready", "status": "False"}}
			})
			h.setReport(instanceReport{Name: "cache-0", Role: reportReplica})
		}, "ScalingDown", "The StatefulSet keeps its 3 pods until it answers, then scales down to 1"},
		{"the primary's manager stopped answering", func(h *harness) {
			h.steady("cache-0", 3)
			h.pass()
			h.silence("cache-0")
		}, "ScalingDown", "The StatefulSet keeps its 3 pods until it answers, then scales down to 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, 3)
			c.unsettle(h)

			h.setInstances(1)
			h.passFor(4*time.Second, 2*time.Second)

			mustMatch(t, h.replicas(), 3)
			ready := findReady(t, h.redis())
			mustMatch(t, ready.Reason, c.reason)
			mustMatch(t, strings.HasSuffix(ready.Message, c.message), true)
		})
	}
}

// A failover during a scale-down still reaches every pod, and may name
// one beyond spec.instances. Once that primary holds the Lease, the
// scale-down moves it to an instance that stays, then shrinks.
func TestAFailoverDuringAScaleDownCanNameAnInstanceTheScaleDownRemoves(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.setReport(detached("cache-1", 90))
	h.silence("cache-2")
	h.setInstances(1)
	h.passFor(10*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)

	h.unsilence("cache-2")
	h.setReport(detached("cache-2", 120))
	h.passFor(2*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-2")
	mustMatch(t, h.replicas(), 3)

	h.unsilence("cache-0")
	h.steady("cache-2", 3)
	h.pass()
	status := h.redis().Status
	mustMatch(t, status.TargetPrimary, "cache-0")
	mustMatch(t, status.Switchover.Reason, reasonScaleDown)
	mustMatch(t, h.replicas(), 3)

	h.steady("cache-0", 3)
	h.pass()
	mustMatch(t, h.redis().Status.Switchover == nil, true)
	mustMatch(t, h.replicas(), 1)
}

// The budgets count the pods the StatefulSet keeps, so a drain during
// a held scale-down still evicts one replica at a time.
func TestTheBudgetsCountThePodsAHeldScaleDownKeeps(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.failQuorum(3, 1)
	h.setReport(detached("cache-1", 90))
	h.silence("cache-2")

	h.setInstances(1)
	h.passFor(4*time.Second, 2*time.Second)

	var budget map[string]any
	h.api.read(budgetsResource, testNamespace, "cache-replicas", &budget)
	mustMatch(t, budget["spec"].(map[string]any)["minAvailable"], any(float64(1)))
	mustMatch(t, h.api.read(budgetsResource, testNamespace, "cache-primary", &budget), true)
}
