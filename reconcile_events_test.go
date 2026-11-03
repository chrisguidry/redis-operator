package main

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// settleEvents waits until the recorder wrote every Event posted so
// far. It posts a marker last, and the recorder writes its queue in
// order, so the marker's arrival means every earlier Event arrived.
func (h *harness) settleEvents() {
	h.t.Helper()
	marker := objectReference{APIVersion: apiVersion, Kind: "Marker", Namespace: testNamespace, Name: "settled"}
	h.c.events.normal(marker, "Settled", time.Now().String())
	eventually(h.t, "the recorder wrote its queue", func() bool {
		return len(h.api.eventsAbout("Marker", "settled")) > 0
	})
	h.api.mutex.Lock()
	for key := range h.api.objects {
		if key.resource == eventsResource && strings.HasPrefix(key.name, "settled.") {
			delete(h.api.objects, key)
		}
	}
	h.api.mutex.Unlock()
}

// redisEvents returns "Type Reason: message" for each Event about the
// Redis, once the recorder wrote every Event posted so far.
func (h *harness) redisEvents() []string {
	h.t.Helper()
	h.settleEvents()
	return h.api.eventLines(redisKind, h.name)
}

func mustContainLine(t *testing.T, lines []string, want string) {
	t.Helper()
	if !slices.Contains(lines, want) {
		t.Errorf("no line %q in:\n%s", want, strings.Join(lines, "\n"))
	}
}

func mustNotContainReason(t *testing.T, lines []string, reason string) {
	t.Helper()
	for _, line := range lines {
		if strings.Contains(line, " "+reason+": ") {
			t.Errorf("wanted no %s Event, got %q", reason, line)
		}
	}
}

// failOver silences the primary cache-0 of a steady Redis of three,
// and runs passes until the operator names cache-2 the next primary.
func (h *harness) failOver() {
	h.steady("cache-0", 3)
	h.pass()
	h.silence("cache-0")
	h.passFor(8*time.Second, 2*time.Second)
	h.setReport(instanceReport{Name: "cache-1", Role: reportDetached, Target: targetPending, Offset: 90, HasData: true})
	h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Target: targetPending, Offset: 120, HasData: true})
	h.passFor(2*time.Second, 2*time.Second)
}

func TestAFailoverPostsItsStartAndTheChosenPrimary(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)

	h.failOver()

	lines := h.redisEvents()
	mustContainLine(t, lines, "Warning FailoverStarted: failing over from cache-0: the instance manager of cache-0 has not answered for 6s")
	mustContainLine(t, lines, "Warning FailoverInProgress: Ready is False: the primary failed, and the operator is choosing the next one")
	mustContainLine(t, lines, "Normal PrimaryChosen: cache-2 is the next primary, at offset 120")
}

func TestANewRedisPostsItsFirstReadinessAsNormal(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.pass()
	h.steady("cache-0", 3)

	h.pass()

	lines := h.redisEvents()
	mustContainLine(t, lines, "Normal NoPrimary: Ready is False: no instance holds the primary lease")
	mustContainLine(t, lines, "Normal AllInstancesReady: Ready is True: cache-0 is primary, and 3 of 3 instances are ready")
}

func TestAConditionWhoseMessageAloneChangesPostsNothing(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	h.editPod("cache-1", func(object map[string]any) {
		object["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Ready", "status": "False"}}
	})
	h.pass()

	h.editPod("cache-2", func(object map[string]any) {
		object["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Ready", "status": "False"}}
	})
	h.pass()

	notReady := 0
	for _, line := range h.redisEvents() {
		if strings.Contains(line, "InstancesNotReady") {
			notReady++
		}
	}
	mustMatch(t, notReady, 1)
}

func TestASwitchoverPostsItsStartWithItsCauseAndItsEnd(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		cause  func(h *harness)
		reason string
	}{
		{"cordon", func(h *harness) {
			h.api.update(nodesResource, "", "node-0", func(object map[string]any) {
				object["spec"] = map[string]any{"unschedulable": true}
			})
		}, reasonNodeCordoned},
		{"pod deleted", func(h *harness) {
			h.editPod("cache-0", func(object map[string]any) {
				metadataOf(object)["annotations"] = map[string]any{switchoverAnnotation: "now"}
			})
		}, reasonPodDeleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, 2)
			h.steady("cache-0", 2)
			h.pass()
			tc.cause(h)
			h.pass()

			h.steady("cache-1", 2)
			h.pass()

			lines := h.redisEvents()
			mustContainLine(t, lines, "Normal SwitchoverStarted: "+switchoverMessage("cache-0", "cache-1", tc.reason))
			mustContainLine(t, lines, "Normal SwitchoverDone: cache-1 is primary, in place of cache-0 ("+tc.reason+")")
		})
	}
}

func TestASwitchoverThatRunsTooLongPostsAWarning(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 2)
	h.steady("cache-0", 2)
	h.pass()
	h.api.update(nodesResource, "", "node-0", func(object map[string]any) {
		object["spec"] = map[string]any{"unschedulable": true}
	})
	h.pass()

	h.passFor(32*time.Second, 2*time.Second)

	mustContainLine(t, h.redisEvents(), "Warning Timeout: SwitchoverFailed is True: cache-1 did not take the primary role within 30s")
}

func TestARolloutPostsEachStep(t *testing.T) {
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

	lines := h.redisEvents()
	mustContainLine(t, lines, "Normal RolloutStarted: rolling out redis:9.0.0 on the canary cache-2 first")
	mustContainLine(t, lines, "Normal RollingInstance: deleted cache-2 so it starts on the new revision")
	mustContainLine(t, lines, "Normal CanaryPromoted: the canary cache-2 holds the data on redis:9.0.0, so the rest roll")
}

func TestAFailedCanaryPostsItsRollback(t *testing.T) {
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

	lines := h.redisEvents()
	mustContainLine(t, lines, "Warning CanaryRolledBack: the canary cache-2 failed on redis:does-not-exist: ImagePullBackOff; rolling it back to "+old)
	mustContainLine(t, lines, "Warning CanaryFailed: RolloutFailed is True: cache-2 did not hold the data on redis:does-not-exist within 5m0s: ImagePullBackOff")
}

func TestAStatusWriteThatConflictsPostsNothing(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	h.silence("cache-0")
	status := redisResource.path(testNamespace, h.name) + "/status"
	h.api.refuse("PATCH", status, 409)

	h.passFor(8*time.Second, 2*time.Second)

	mustNotContainReason(t, h.redisEvents(), "FailoverStarted")
}

func TestAnEventAPIThatHangsDoesNotSlowAPass(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.api.hang("POST", eventsResource.path(testNamespace, ""))
	h.steady("cache-0", 3)
	h.pass()
	h.silence("cache-0")

	started := time.Now()
	h.passFor(8*time.Second, 2*time.Second)

	mustMatch(t, time.Since(started) < testTimeout, true)
	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)
}

func TestConditionEventTypeWarnsOfABadStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		condition  Condition
		hadPrimary bool
		want       string
	}{
		{Condition{Type: readyCondition, Status: ConditionFalse, Reason: "FailoverInProgress"}, true, eventWarning},
		{Condition{Type: readyCondition, Status: ConditionFalse, Reason: "NoPrimary"}, true, eventWarning},
		{Condition{Type: readyCondition, Status: ConditionFalse, Reason: "NoPrimary"}, false, eventNormal},
		{Condition{Type: readyCondition, Status: ConditionFalse, Reason: "InstancesNotReady"}, true, eventNormal},
		{Condition{Type: readyCondition, Status: ConditionTrue, Reason: "AllInstancesReady"}, true, eventNormal},
		{Condition{Type: "DataLost", Status: ConditionTrue, Reason: "AllInstancesEmpty"}, true, eventWarning},
		{Condition{Type: "SwitchoverFailed", Status: ConditionFalse, Reason: "Started"}, true, eventNormal},
		{Condition{Type: "RolloutFailed", Status: ConditionTrue, Reason: "CanaryFailed"}, true, eventWarning},
		{Condition{Type: "SourceReachable", Status: ConditionFalse, Reason: "Unreachable"}, false, eventWarning},
		{Condition{Type: "Adopted", Status: ConditionTrue, Reason: "Promoted"}, true, eventNormal},
		{Condition{Type: "Adopted", Status: ConditionFalse, Reason: "SourceRefused"}, false, eventWarning},
	}
	for _, tc := range cases {
		t.Run(tc.condition.Type+"/"+tc.condition.Reason, func(t *testing.T) {
			mustMatch(t, conditionEventType(tc.condition, tc.hadPrimary), tc.want)
		})
	}
}

func TestABackupPostsItsStart(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.withBackupStore("https://s3.example.com", testBucket, "")
	h.newBackup("nightly")

	h.pass()
	h.pass()

	h.settleEvents()
	key := h.backup("nightly").Status.Key
	mustContainLine(t, h.api.eventLines(backupKind, "nightly"), "Normal Started: cache-0 takes the snapshot, to "+key)
}

func TestABackupTheOperatorFailsPostsOneFailure(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.newBackup("nightly")

	h.pass()
	h.pass()
	h.pass()

	h.settleEvents()
	lines := h.api.eventLines(backupKind, "nightly")
	mustMatch(t, strings.Join(lines, "\n"), "Warning Failed: redis cache names no backup store in spec.backup")
	mustMatch(t, h.api.eventsAbout(backupKind, "nightly")[0].Count, int32(1))
}

// finishBackup writes the end of a Backup, as its instance manager
// does.
func (h *harness) finishBackup(name string, status map[string]any) {
	h.api.update(backupsResource, testNamespace, name, func(object map[string]any) {
		held, _ := object["status"].(map[string]any)
		if held == nil {
			held = map[string]any{}
		}
		for field, value := range status {
			held[field] = value
		}
		object["status"] = held
	})
}

func TestABackupAnInstanceFinishesPostsItsEnd(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		status map[string]any
		want   string
	}{
		{"completed", map[string]any{"phase": backupCompleted, "size": 2048, "offset": 300},
			"Normal Completed: cache-0 wrote redis/example/cache/20261101T120000Z-nightly.rdb: 2048 bytes, at offset 300"},
		{"failed", map[string]any{"phase": backupFailed, "error": "the store refused the upload"},
			"Warning Failed: the store refused the upload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, 1)
			h.steady("cache-0", 1)
			h.withBackupStore("https://s3.example.com", testBucket, "")
			h.newBackup("nightly")
			h.pass()
			h.pass()

			h.finishBackup("nightly", tc.status)
			h.pass()
			h.pass()

			h.settleEvents()
			lines := h.api.eventLines(backupKind, "nightly")
			mustContainLine(t, lines, tc.want)
			mustMatch(t, len(lines), 2)
		})
	}
}

func TestABackupThatFinishedBeforeTheOperatorStartedPostsNothing(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.withBackupStore("https://s3.example.com", testBucket, "")
	h.newBackup("nightly")
	h.api.update(backupsResource, testNamespace, "nightly", func(object map[string]any) {
		metadataOf(object)["finalizers"] = []any{backupFinalizer}
	})
	h.finishBackup("nightly", map[string]any{"phase": backupCompleted, "instance": "cache-0", "key": "redis/example/cache/old.rdb"})

	h.pass()
	h.pass()

	h.settleEvents()
	mustMatch(t, len(h.api.eventLines(backupKind, "nightly")), 0)
}

func TestASwitchoverMessageSaysWhyInWords(t *testing.T) {
	t.Parallel()
	cases := []struct {
		reason string
		want   string
	}{
		{reasonPodDeleted, "moving the primary from cache-0 to cache-1, because its pod is being deleted (PodDeleted)"},
		{reasonNodeCordoned, "moving the primary from cache-0 to cache-1, because its node is cordoned (NodeCordoned)"},
		{reasonScaleDown, "moving the primary from cache-0 to cache-1, because spec.instances no longer keeps it (ScaleDown)"},
		{reasonRollout, "moving the primary from cache-0 to cache-1, because it is the last instance on the old revision (Rollout)"},
	}
	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			mustMatch(t, switchoverMessage("cache-0", "cache-1", tc.reason), tc.want)
		})
	}
}
