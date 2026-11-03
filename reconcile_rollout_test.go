package main

import (
	"testing"
	"time"
)

// setImage changes spec.image, as a person does.
func (h *harness) setImage(image string) {
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["image"] = image
	})
}

// templateImage is the image the StatefulSet's pod template runs.
func (h *harness) templateImage() string {
	var set map[string]any
	h.api.read(statefulSetsResource, testNamespace, h.name, &set)
	containers := set["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	return containers[0].(map[string]any)["image"].(string)
}

// newRevision makes the StatefulSet report revision as its update
// revision, as its controller does after a template change.
func (h *harness) newRevision(revision string) {
	h.api.update(statefulSetsResource, testNamespace, h.name, func(object map[string]any) {
		object["status"] = map[string]any{"updateRevision": revision}
	})
}

// recreate makes pod name again on revision, as the StatefulSet
// controller does after the operator deleted it, ready or not.
func (h *harness) recreate(name, revision string, ready bool, waiting string) {
	status := "False"
	if ready {
		status = "True"
	}
	pod := Pod{
		Metadata: ObjectMeta{Name: name, Namespace: testNamespace, Labels: map[string]string{clusterLabel: h.name, revisionLabel: revision}},
		Spec:     PodSpec{NodeName: "node-" + name[len(name)-1:]},
		Status:   PodStatus{Phase: "Running", PodIP: h.ip(name), Conditions: []PodCondition{{Type: "Ready", Status: status}}},
	}
	if waiting != "" {
		var cs ContainerStatus
		cs.Name = "redis"
		cs.State.Waiting = &struct {
			Reason  string `json:"reason,omitempty"`
			Message string `json:"message,omitempty"`
		}{Reason: waiting}
		pod.Status.ContainerStatuses = []ContainerStatus{cs}
	}
	h.api.create(podsResource, pod)
}

func (h *harness) podGone(name string) bool {
	var pod Pod
	return !h.api.read(podsResource, testNamespace, name, &pod)
}

func TestANewImageRollsOnlyTheCanaryFirst(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	old := h.templateImage()

	h.setImage("redis:9.0.0")
	h.pass()
	h.newRevision("rev-2")
	h.pass()

	ro := h.redis().Status.Rollout
	mustMatch(t, ro.Phase, rolloutCanary)
	mustMatch(t, ro.Canary, "cache-2")
	mustMatch(t, h.templateImage(), "redis:9.0.0")
	mustMatch(t, h.redis().Status.Image, old)
	mustMatch(t, h.podGone("cache-2"), true)
	mustMatch(t, h.podGone("cache-1"), false)
}

func TestAProvenCanaryRollsTheRestAndTheImageLands(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	h.setImage("redis:9.0.0")
	h.pass()
	h.newRevision("rev-2")
	h.pass()
	h.recreate("cache-2", "rev-2", true, "")

	h.passFor(32*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.Rollout.Phase, rolloutRolling)
	mustMatch(t, h.podGone("cache-1"), true)

	h.recreate("cache-1", "rev-2", true, "")
	h.pass()
	mustMatch(t, h.redis().Status.Switchover.Reason, reasonRollout)
	target := h.redis().Status.TargetPrimary
	h.steady(target, 3)
	h.pass()
	h.pass()
	mustMatch(t, h.podGone("cache-0"), true)
	h.recreate("cache-0", "rev-2", true, "")
	h.pass()

	mustMatch(t, h.redis().Status.Rollout == nil, true)
	mustMatch(t, h.redis().Status.Image, "redis:9.0.0")
}

func TestACanaryThatNeverHoldsTheDataIsRolledBack(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	old := h.templateImage()
	h.setImage("redis:does-not-exist")
	h.pass()
	h.newRevision("rev-2")
	h.pass()
	h.recreate("cache-2", "rev-2", false, "ImagePullBackOff")

	h.passFor(6*time.Minute, 30*time.Second)

	status := h.redis().Status
	mustMatch(t, status.Rollout.Phase, rolloutFailed)
	mustMatch(t, status.Rollout.Reason, "ImagePullBackOff")
	mustMatch(t, status.Image, old)
	mustMatch(t, h.templateImage(), old)
	failed, _ := findCondition(status.Conditions, "RolloutFailed")
	mustMatch(t, failed.Status, ConditionTrue)
	mustMatch(t, h.podGone("cache-1"), false)
	mustMatch(t, h.pod("cache-0").Metadata.Labels[revisionLabel], "rev-1")

	// The template is back on the old image, so its revision is the old
	// one again, and the broken canary rolls back to it.
	h.newRevision("rev-1")
	h.pass()

	mustMatch(t, h.podGone("cache-2"), true)
}

func TestRevertingTheImageAfterAFailureEndsTheRollout(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	old := h.templateImage()
	h.setImage("redis:does-not-exist")
	h.pass()
	h.newRevision("rev-2")
	h.pass()
	h.recreate("cache-2", "rev-2", false, "ImagePullBackOff")
	h.passFor(6*time.Minute, 30*time.Second)

	h.setImage(old)
	h.pass()

	mustMatch(t, h.redis().Status.Rollout == nil, true)
}
