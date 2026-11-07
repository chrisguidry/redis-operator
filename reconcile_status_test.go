package main

import (
	"context"
	"slices"
	"testing"
)

// A status write that a newer change refused wakes the next pass at
// once, so the decision is made again without waiting for the backstop.
func TestAConflictOnTheStatusWriteWakesTheNextPass(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.settle()
	h.api.refuseNext("PATCH", redisResource.path(testNamespace, h.name)+"/status", 409, 1)
	h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Target: "cache-0", HasData: true})

	h.pass()

	mustMatch(t, h.woke(), true)
}

// The replication offsets move with every write, so the status holds
// none of them, and a pass whose only news is a later offset writes
// nothing.
func TestOffsetsThatMoveWriteNoStatus(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.settle()
	before := h.redis().Metadata.ResourceVersion

	h.steadyAt("cache-0", 3, 5000)
	h.pass()

	mustMatch(t, h.redis().Metadata.ResourceVersion, before)
}

// Each status write that lands is counted by its resource.
func TestAStatusWriteIsCounted(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)

	h.pass()

	mustMatch(t, slices.Contains(scrape(t, h.c.readings), `redis_operator_status_writes_total{resource="redis"} 1`), true)
}

// The loop counts each pass by what woke it.
func TestTheLoopCountsPassesByWhatWokeThem(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- h.c.run(ctx) }()
	counted := func(sample string) func() bool {
		return func() bool { return slices.Contains(scrape(t, h.c.readings), sample) }
	}
	eventually(t, "the first pass", counted(`redis_operator_passes_total{cause="start"} 1`))

	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["cpu"] = "250m"
	})

	eventually(t, "a pass for the change", func() bool { return !counted(`redis_operator_passes_total{cause="event"} 0`)() })
	stop()
	mustSucceed(t, <-done)
}
