package main

import (
	"fmt"
	"testing"
	"time"
)

// A new term's first pass applies every child, and each apply can wait
// on a slow API server. The managers answered the poll before those
// waits, so the wait is no silence of theirs.
func TestAnApplyThatWaitsAfterThePollStartsNoFailover(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	started := h.clock.now()
	h.api.duringNext("PATCH", statefulSetsResource.path(testNamespace, h.name), func() {
		h.clock.advance(7 * time.Second)
	})

	h.pass()

	mustMatch(t, h.clock.now().Sub(started), 7*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
	mustMatch(t, h.linesWith("failing over"), 0)
}

// A pass can run long after the one before it, as when another Redis's
// writes held it up. A primary that then misses one poll has missed it
// for no time at all; its silence counts from that poll.
func TestAPrimaryThatMissesOnePollAfterAGapFailsOverOnlyAfterTheThreshold(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	h.clock.advance(10 * time.Second)
	h.silence("cache-0")

	h.pass()
	h.passFor(4*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")

	h.passFor(2*time.Second, 2*time.Second)
	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)
}

// A poll that no manager answers says nothing about the primary: the
// operator's own network, or a policy that blocks the status port,
// looks the same. The operator says so once, and once again when a
// manager answers.
func TestAPollThatNoManagerAnswersStartsNoFailoverAndSaysSo(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	for i := range 3 {
		h.silence(instanceName(h.name, i))
	}

	h.passFor(20*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
	mustMatch(t, h.linesWith(fmt.Sprintf("status port %d", h.port)), 1)

	h.unsilence("cache-1")
	h.pass()
	mustMatch(t, h.linesWith("poll recovered"), 1)
}

// The poll asks only the pods that have an address, so the target is
// accounted for by name: a primary whose pod is gone still fails over.
func TestAPrimaryWhosePodIsGoneFailsOver(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()

	h.api.delete(podsResource, testNamespace, "cache-0")
	h.passFor(8*time.Second, 2*time.Second)

	mustMatch(t, h.redis().Status.TargetPrimary, targetPending)
}
