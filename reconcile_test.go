package main

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestANewRedisGetsItsObjectsAndAFirstTarget(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)

	h.pass()

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
	for _, object := range []struct {
		resource kubeResource
		name     string
	}{
		{statefulSetsResource, "cache"},
		{servicesResource, "cache-rw"},
		{servicesResource, "cache-ro"},
		{servicesResource, "cache-nodes"},
		{secretsResource, "cache-app"},
		{serviceAccountsResource, "cache"},
		{rolesResource, "cache"},
		{roleBindingsResource, "cache"},
		{budgetsResource, "cache-primary"},
		{budgetsResource, "cache-replicas"},
	} {
		var held map[string]any
		if !h.api.read(object.resource, testNamespace, object.name, &held) {
			t.Fatalf("%s %s was not created", object.resource, object.name)
		}
	}
}

func TestTheSecretKeepsItsPasswordAcrossPasses(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.pass()
	var first map[string]any
	h.api.read(secretsResource, testNamespace, "cache-app", &first)

	h.c = newController(h.api.client, newMetrics("dev"), testImages, h.clock.now, h.log)
	h.c.statusPort = h.port
	h.running = false
	h.pass()

	var second map[string]any
	h.api.read(secretsResource, testNamespace, "cache-app", &second)
	mustMatch(t, second["metadata"].(map[string]any)["resourceVersion"], first["metadata"].(map[string]any)["resourceVersion"])
}

func TestTheRoleLabelsFollowTheLease(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.api.update(redisResource, testNamespace, "cache", func(object map[string]any) {
		object["status"] = map[string]any{"targetPrimary": "cache-1"}
	})
	h.steady("cache-1", 3)

	h.pass()

	mustMatch(t, h.pod("cache-1").Metadata.Labels[roleLabel], rolePrimary)
	mustMatch(t, h.pod("cache-0").Metadata.Labels[roleLabel], roleReplica)
	mustMatch(t, h.redis().Status.CurrentPrimary, "cache-1")
	ready := findReady(t, h.redis())
	mustMatch(t, ready.Status, ConditionTrue)
}

func TestTheHolderLosesThePrimaryLabelInThePassThatMovesTheTarget(t *testing.T) {
	t.Parallel()
	moves := []struct {
		name string
		move func(h *harness)
	}{
		{"switchover", func(h *harness) {
			h.api.update(nodesResource, "", "node-0", func(object map[string]any) {
				object["spec"] = map[string]any{"unschedulable": true}
			})
			h.pass()
		}},
		{"failover", func(h *harness) {
			h.silence("cache-0")
			h.passFor(8*time.Second, 2*time.Second)
		}},
	}
	for _, move := range moves {
		t.Run(move.name, func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, 3)
			h.steady("cache-0", 3)
			h.pass()
			mustMatch(t, h.pod("cache-0").Metadata.Labels[roleLabel], rolePrimary)

			move.move(h)

			mustMatch(t, h.redis().Status.TargetPrimary != "cache-0", true)
			for i := range 3 {
				mustMatch(t, h.pod(instanceName("cache", i)).Metadata.Labels[roleLabel], roleReplica)
			}
		})
	}
}

func findReady(t *testing.T, r Redis) Condition {
	t.Helper()
	condition, ok := findCondition(r.Status.Conditions, readyCondition)
	if !ok {
		t.Fatal("the Redis has no Ready condition")
	}
	return condition
}

func TestNoFailoverStartsBeforeAnyPrimaryHeldTheLease(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	for i := range 3 {
		h.silence(instanceName(h.name, i))
	}

	h.passFor(20*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
}

// A pass that runs after the operator lost its Lease sends every
// request on a canceled context, so no manager answers it. That
// silence is the operator's, and the pass decides and counts nothing.
func TestAPassAfterTheLeaseIsLostDecidesAndCountsNothing(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	h.clock.advance(10 * time.Second)
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errLostLease)

	h.passWith(ctx)

	mustMatch(t, h.linesWith("failing over"), 0)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
	samples := scrape(t, h.c.readings)
	mustContainLine(t, samples, "redis_operator_failovers_total 0")
	mustContainLine(t, samples, "redis_operator_reconcile_errors_total 0")
}

func TestASilentPrimaryStartsAFailoverToTheReplicaWithTheMostData(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()

	h.silence("cache-0")
	h.passFor(8*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)

	h.setReport(instanceReport{Name: "cache-1", Role: reportDetached, Target: targetPending, Offset: 90, HasData: true})
	h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Target: targetPending, Offset: 120, HasData: true})
	h.passFor(2*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-2")
}

func TestACordonedPrimaryNodeStartsASwitchover(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()

	h.api.update(nodesResource, "", "node-0", func(object map[string]any) {
		object["spec"] = map[string]any{"unschedulable": true}
	})
	h.pass()

	status := h.redis().Status
	mustMatch(t, status.TargetPrimary == "cache-1" || status.TargetPrimary == "cache-2", true)
	mustMatch(t, status.Switchover.Reason, reasonNodeCordoned)
	mustMatch(t, status.Switchover.From, "cache-0")
}

func TestASwitchoverEndsWhenTheNewPrimaryHoldsTheLease(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 2)
	h.steady("cache-0", 2)
	h.pass()
	h.editPod("cache-0", func(object map[string]any) {
		metadataOf(object)["annotations"] = map[string]any{switchoverAnnotation: "now"}
	})
	h.pass()
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-1")

	h.steady("cache-1", 2)
	h.pass()

	status := h.redis().Status
	mustMatch(t, status.Switchover == nil, true)
	mustMatch(t, status.CurrentPrimary, "cache-1")
	mustMatch(t, h.pod("cache-1").Metadata.Labels[roleLabel], rolePrimary)
}

func TestASwitchoverThatRunsTooLongIsGivenUp(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 2)
	h.steady("cache-0", 2)
	h.pass()
	h.api.update(nodesResource, "", "node-0", func(object map[string]any) {
		object["spec"] = map[string]any{"unschedulable": true}
	})
	h.pass()

	h.passFor(32*time.Second, 2*time.Second)

	status := h.redis().Status
	mustMatch(t, status.TargetPrimary, "cache-0")
	mustMatch(t, status.Switchover == nil, true)
	failed, _ := findCondition(status.Conditions, "SwitchoverFailed")
	mustMatch(t, failed.Status, ConditionTrue)
}

func TestNoSwitchoverStartsWithoutAReplicaThatFollowsThePrimary(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 2)
	h.steady("cache-0", 2)
	h.setReport(instanceReport{Name: "cache-1", Role: reportReplica, Target: "cache-0", LinkUp: false, Following: h.ip("cache-0"), HasData: true})
	h.pass()

	h.api.update(nodesResource, "", "node-0", func(object map[string]any) {
		object["spec"] = map[string]any{"unschedulable": true}
	})
	h.pass()

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
}

func TestARolloutRollsTheReplicasFirstThenSwitchesOver(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()

	h.api.update(statefulSetsResource, testNamespace, "cache", func(object map[string]any) {
		object["status"] = map[string]any{"updateRevision": "rev-2"}
	})
	h.pass()

	var gone Pod
	mustMatch(t, h.api.read(podsResource, testNamespace, "cache-2", &gone), false)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")

	// The StatefulSet controller makes the pod again on the new
	// revision, and it becomes ready.
	h.api.create(podsResource, Pod{
		Metadata: ObjectMeta{Name: "cache-2", Namespace: testNamespace, Labels: map[string]string{clusterLabel: h.name, revisionLabel: "rev-2"}},
		Spec:     PodSpec{NodeName: "node-2"},
		Status:   PodStatus{Phase: "Running", PodIP: h.ip("cache-2"), Conditions: []PodCondition{{Type: "Ready", Status: "True"}}},
	})
	h.pass()
	mustMatch(t, h.api.read(podsResource, testNamespace, "cache-1", &gone), false)
	h.api.create(podsResource, Pod{
		Metadata: ObjectMeta{Name: "cache-1", Namespace: testNamespace, Labels: map[string]string{clusterLabel: h.name, revisionLabel: "rev-2"}},
		Spec:     PodSpec{NodeName: "node-1"},
		Status:   PodStatus{Phase: "Running", PodIP: h.ip("cache-1"), Conditions: []PodCondition{{Type: "Ready", Status: "True"}}},
	})

	h.pass()

	status := h.redis().Status
	mustMatch(t, status.Switchover.Reason, reasonRollout)
	mustMatch(t, status.TargetPrimary == "cache-1" || status.TargetPrimary == "cache-2", true)
	mustMatch(t, h.api.read(podsResource, testNamespace, "cache-0", &gone), true)
}

// A target waits out the old holder's Lease before it takes it, so a
// Lease with no holder right after a failover named it is no new
// failure.
func TestANewTargetHasTimeToTakeTheLeaseBeforeAnotherFailover(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	h.silence("cache-0")
	h.passFor(8*time.Second, 2*time.Second)
	h.holdLease("")
	h.setReport(instanceReport{Name: "cache-1", Role: reportDetached, Offset: 120, HasData: true})
	h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Offset: 90, HasData: true})
	h.passFor(2*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-1")

	h.passFor(14*time.Second, 2*time.Second)

	failovers := 0
	for _, line := range h.log.lines() {
		if strings.Contains(line, "failing over") {
			failovers++
		}
	}
	mustMatch(t, failovers, 1)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-1")
}

// The replicas' budget lets one replica go at a time. A maxUnavailable
// budget on part of a StatefulSet's pods is counted against the
// StatefulSet's whole scale, and allows no eviction at all, so the
// budget states the replicas that must stay.
func TestTheReplicaBudgetKeepsAllButOneReplica(t *testing.T) {
	t.Parallel()
	cases := []struct {
		instances int
		want      int
	}{{2, 0}, {3, 1}, {5, 3}}
	for _, c := range cases {
		t.Run(strconv.Itoa(c.instances), func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, c.instances)
			h.steady("cache-0", c.instances)

			h.pass()
			h.pass()

			var budget map[string]any
			h.api.read(budgetsResource, testNamespace, "cache-replicas", &budget)
			spec := budget["spec"].(map[string]any)
			mustMatch(t, spec["minAvailable"], any(float64(c.want)))
			_, hasMax := spec["maxUnavailable"]
			mustMatch(t, hasMax, false)
		})
	}
}

// A frozen primary keeps its Lease until it expires, so after a
// failover names the next primary, the old holder's silence is no new
// failure: the operator waits on the target it named.
func TestASilentOldHolderDoesNotRestartAFailoverItAlreadyStarted(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	h.silence("cache-0")
	h.passFor(8*time.Second, 2*time.Second)
	h.setReport(instanceReport{Name: "cache-1", Role: reportDetached, Offset: 120, HasData: true})
	h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Offset: 90, HasData: true})
	h.passFor(2*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-1")

	h.passFor(14*time.Second, 2*time.Second)

	failovers := 0
	for _, line := range h.log.lines() {
		if strings.Contains(line, "failing over") {
			failovers++
		}
	}
	mustMatch(t, failovers, 1)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-1")
}

func TestARedisWhoseInstancesAllRestartEmptyStartsAnEmptyDataset(t *testing.T) {
	t.Parallel()
	for _, instances := range []int{1, 3} {
		t.Run(strconv.Itoa(instances), func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, instances)
			h.steady("cache-0", instances)
			h.pass()

			h.holdLease("")
			for i := range instances {
				name := instanceName("cache", i)
				h.setReport(instanceReport{Name: name, Role: reportDetached, Target: "cache-0"})
			}
			h.passFor(10*time.Second, 2*time.Second)

			status := h.redis().Status
			mustMatch(t, status.TargetPrimary, "cache-0")
			mustMatch(t, status.CurrentPrimary, "")
			lost, _ := findCondition(status.Conditions, "DataLost")
			mustMatch(t, lost.Status, ConditionTrue)
			mustMatch(t, lost.Reason, "AllInstancesEmpty")
		})
	}
}

// A single instance has no other to fail over to, so its silence while
// its pod restarts starts no failover, and it comes back as primary
// with whatever data it kept.
func TestASilentSingleInstanceStaysTheTarget(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.pass()

	h.silence("cache-0")
	h.passFor(30*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
}
