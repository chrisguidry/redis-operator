package main

import (
	"testing"
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
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()

	h.setInstances(1)
	h.pass()

	mustMatch(t, h.redis().Status.Switchover == nil, true)
	mustMatch(t, h.replicas(), 1)
}

func TestTheBudgetOnThePrimaryExistsOnlyWithMoreThanOneInstance(t *testing.T) {
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
	h := startHarness(t, 3)
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["storage"] = map[string]any{"size": "1Gi"}
	})

	h.pass()

	policy, _ := h.statefulSetSpec()["persistentVolumeClaimRetentionPolicy"].(map[string]any)
	mustMatch(t, policy["whenScaled"], any("Delete"))
	mustMatch(t, policy["whenDeleted"], any("Retain"))
}
