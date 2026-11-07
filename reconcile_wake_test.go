package main

import (
	"testing"
	"time"
)

// settle runs passes until the operator's writes are done, then empties
// the wake channel, so a test reads only the wakes that its own change
// makes.
func (h *harness) settle() {
	h.t.Helper()
	for range 3 {
		h.pass()
	}
	eventually(h.t, "the stores hold the cluster", h.synced)
	h.drainWake()
}

func (h *harness) drainWake() {
	select {
	case <-h.c.wake:
	default:
	}
}

// woke reports whether a change woke the loop, once the stores hold
// it. A watch applies an event to the store just before it wakes the
// loop, so the check waits a moment after the stores agree.
func (h *harness) woke() bool {
	h.t.Helper()
	eventually(h.t, "the stores hold the cluster", h.synced)
	select {
	case <-h.c.wake:
		return true
	case <-time.After(20 * time.Millisecond):
		return false
	}
}

func TestOnlyAChangeThatAPassReadsWakesIt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		change func(h *harness)
		wakes  bool
	}{
		{"a renewal of the primary Lease", func(h *harness) {
			h.api.update(leasesResource, testNamespace, primaryLeaseName(h.name), func(object map[string]any) {
				object["spec"].(map[string]any)["renewTime"] = "2026-11-01T12:00:09.000000Z"
			})
		}, false},
		{"a new holder of the primary Lease", func(h *harness) { h.holdLease("cache-1") }, true},
		{"the operator's own status write", func(h *harness) {
			h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Target: "cache-0", HasData: true})
			h.pass()
		}, false},
		{"a change to the spec", func(h *harness) {
			h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
				object["spec"].(map[string]any)["cpu"] = "250m"
			})
		}, true},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, 3)
			h.steady("cache-0", 3)
			h.settle()

			one.change(h)

			mustMatch(t, h.woke(), one.wakes)
		})
	}
}

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
