package main

// The poll of the instance managers, and what the operator learns from
// it about each one's silence.
//
// A poll is informative when its context was live when it finished and
// at least one manager answered it. Only such a poll tells the operator
// that a manager which did not answer is silent. A poll that reached no
// manager says nothing about the primary, because a fault on the
// operator's side looks the same: its own network, a policy that blocks
// the status port, or a context that ended when it lost its Lease.
//
// Silence is measured across informative polls, from the first one a
// manager missed to the latest one, and not as the time since it last
// answered. A pass that a slow API server or another Redis held up
// polls late, and a manager that misses that one poll has then been
// silent for no time at all.

import (
	"context"
	"fmt"
	"time"
)

// poll reads every instance manager's /status at once, records what
// each answered, and returns the time of the poll. It accounts for the
// silence of each pod it asks and of target, so a target whose pod is
// gone, which it cannot ask, still accumulates silence.
func (c *controller) poll(ctx context.Context, subject string, st *redisState, pods []Pod, target string) time.Time {
	now := c.now()
	type answer struct {
		name   string
		report instanceReport
		err    error
	}
	answers := make(chan answer, len(pods))
	accounted := map[string]bool{}
	if target != targetPending {
		accounted[target] = true
	}
	asked := 0
	for _, pod := range pods {
		if pod.Status.PodIP == "" || pod.Metadata.DeletionTimestamp != "" && pod.Status.Phase != "Running" {
			continue
		}
		asked++
		accounted[pod.Metadata.Name] = true
		go func() {
			address := fmt.Sprintf("%s:%d", pod.Status.PodIP, c.statusPort)
			report, err := readInstanceReport(ctx, c.poller, address)
			answers <- answer{pod.Metadata.Name, report, err}
		}()
	}
	heard := map[string]bool{}
	for range asked {
		a := <-answers
		if a.err != nil {
			continue
		}
		heard[a.name] = true
		st.lastSeen[a.name] = now
		st.reports[a.name] = a.report
	}
	switch {
	case ctx.Err() != nil:
	case len(heard) > 0:
		c.log.clear(subject, topicPoll)
		st.recordInformative(now, accounted, heard)
	case asked > 0:
		c.log.fault(subject, topicPoll, fmt.Sprintf("no instance manager answered on status port %d, so a silent primary starts no failover until one does", c.statusPort))
	}
	return now
}

// recordInformative records an informative poll at now: each accounted
// instance that did not answer missed it, unless it already missed an
// earlier one, and each that answered is silent no more.
func (st *redisState) recordInformative(now time.Time, accounted, heard map[string]bool) {
	st.informedAt = now
	for name := range st.missedSince {
		if !accounted[name] || heard[name] {
			delete(st.missedSince, name)
		}
	}
	for name := range accounted {
		if _, missed := st.missedSince[name]; !missed && !heard[name] {
			st.missedSince[name] = now
		}
	}
}

// silence is how long the manager of name has missed informative
// polls: from the first one it missed to the latest one, and 0 when it
// answered the latest one.
func (st *redisState) silence(name string) time.Duration {
	since, missed := st.missedSince[name]
	if !missed {
		return 0
	}
	return st.informedAt.Sub(since)
}

// fresh returns the report of name if its manager answered within
// within of polled, the time of a poll. The decisions pass the time of
// their own pass's poll, so a pass that waited on the API server after
// it polled does not make a report that just arrived look old.
func (st *redisState) fresh(name string, polled time.Time, within time.Duration) (instanceReport, bool) {
	seen, ok := st.lastSeen[name]
	if !ok || polled.Sub(seen) > within {
		return instanceReport{}, false
	}
	return st.reports[name], true
}
