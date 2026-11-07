package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// startPoller is a poller of the harness's Redis that asks every pod,
// with cache-0 as the target, after one poll that every manager
// answered.
func startPoller(h *harness, instances int) *instancePoller {
	h.t.Helper()
	p := newInstancePoller(h.c, "redis example/cache")
	var pods []Pod
	for i := range instances {
		pods = append(pods, h.pod(instanceName(h.name, i)))
	}
	p.ask(pods, "cache-0")
	p.poll(h.t.Context())
	select {
	case <-h.c.pollWake:
	default:
	}
	return p
}

func pollWoke(h *harness) bool {
	select {
	case <-h.c.pollWake:
		return true
	default:
		return false
	}
}

// A poll wakes the pass only when it changed what a decision reads.
func TestAPollWakesThePassOnlyForAChangeADecisionReads(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		change func(h *harness)
		wakes  bool
	}{
		{"nothing", func(*harness) {}, false},
		{"an offset", func(h *harness) { h.steadyAt("cache-0", 3, 900) }, false},
		{"a role", func(h *harness) {
			h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Target: "cache-0", HasData: true})
		}, true},
		{"a link", func(h *harness) {
			h.setReport(instanceReport{Name: "cache-2", Role: reportReplica, Target: "cache-0", Offset: 100, Following: h.ip("cache-0"), HasData: true})
		}, true},
		{"an error", func(h *harness) {
			h.setReport(instanceReport{Name: "cache-2", Role: reportStarting, Error: "connection refused"})
		}, true},
		{"a manager that stops answering", func(h *harness) { h.silence("cache-2") }, false},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			h := startHarness(t, 3)
			h.steady("cache-0", 3)
			p := startPoller(h, 3)

			one.change(h)
			p.poll(t.Context())

			mustMatch(t, pollWoke(h), one.wakes)
		})
	}
}

// A target that stops answering wakes the pass when its silence reaches
// the failure threshold, so a failover need not wait for the backstop.
func TestASilentTargetWakesThePassAtTheThreshold(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	p := startPoller(h, 3)
	h.silence("cache-0")
	p.poll(t.Context())
	h.clock.advance(h.c.timings.failureThreshold - time.Second)
	p.poll(t.Context())
	early := pollWoke(h)

	h.clock.advance(time.Second)
	p.poll(t.Context())

	mustMatch(t, early, false)
	mustMatch(t, pollWoke(h), true)
	mustMatch(t, p.snapshot().silence("cache-0"), h.c.timings.failureThreshold)
}

// A poll whose context ended says nothing about silence, because a
// manager it could not finish asking may well be up.
func TestACanceledPollIsNotInformative(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	p := startPoller(h, 3)
	informed := p.snapshot().informedAt
	h.clock.advance(time.Second)
	ended, cancel := context.WithCancel(t.Context())
	cancel()

	p.poll(ended)

	mustMatch(t, p.snapshot().informedAt, informed)
	mustMatch(t, p.snapshot().silence("cache-0"), time.Duration(0))
}

// Inside the controller's loop each Redis's poller runs on its own, so
// a change in a report wakes the pass with no pass to poll for it, and
// the poller stops with the leader's context.
func TestTheLoopPollsBetweenPasses(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	var running sync.WaitGroup
	ctx, stop := context.WithCancel(t.Context())
	h.c.polling = &running
	h.c.timings.pollPeriod = 10 * time.Millisecond
	h.passWith(ctx)
	pollWoke(h)

	h.setReport(instanceReport{Name: "cache-2", Role: reportDetached, Target: "cache-0", HasData: true})

	eventually(t, "a poll to wake the pass", func() bool { return pollWoke(h) })
	stop()
	running.Wait()
}

// A Redis that is gone stops its poller.
func TestAGoneRedisStopsItsPoller(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	var running sync.WaitGroup
	h.c.polling = &running
	h.c.timings.pollPeriod = 10 * time.Millisecond
	h.pass()

	h.api.delete(redisResource, testNamespace, h.name)
	h.pass()

	running.Wait()
	mustMatch(t, len(h.c.pollers), 0)
}
