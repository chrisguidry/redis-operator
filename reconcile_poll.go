package main

// The poll of the instance managers, and what the operator learns from
// it about each one's silence.
//
// Each Redis has a poller, keyed by its UID, that asks every instance
// manager's /status once a poll period on the leader's context, apart
// from the passes. A pass takes a snapshot of the poller's last whole
// poll, because some decisions, such as a scale-down's and a
// failover's choice of the next primary, need the reports of one poll
// together. The pass gives the poller the pods to ask and the target
// whose silence counts, and the poller wakes the pass when a report
// changes in a way a decision reads, or when the target's silence
// reaches the failure threshold, so a failover does not wait for the
// backstop.
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
// answered. A poll that ran late, as when the machine was busy, and a
// manager that misses that one poll has then been silent for no time
// at all.

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"
)

// pollView is what the poller learned, as of its last whole poll. A
// published view is never changed: the next poll publishes new maps.
type pollView struct {
	// at is the time of the poll, and zero before the first one.
	at time.Time
	// lastSeen is when each instance manager last answered, and reports
	// what it answered then.
	lastSeen map[string]time.Time
	reports  map[string]instanceReport
	// informedAt is the time of the latest informative poll, and
	// missedSince holds, for each instance, the first informative poll
	// it missed since it last answered.
	informedAt  time.Time
	missedSince map[string]time.Time
}

// instancePoller polls one Redis's instance managers.
type instancePoller struct {
	c       *controller
	subject string

	mutex  sync.Mutex
	pods   []Pod
	target string
	view   pollView
	// first is closed when the first poll is published.
	first chan struct{}
	stop  context.CancelFunc
}

func newInstancePoller(c *controller, subject string) *instancePoller {
	return &instancePoller{
		c:       c,
		subject: subject,
		view:    pollView{lastSeen: map[string]time.Time{}, reports: map[string]instanceReport{}, missedSince: map[string]time.Time{}},
		first:   make(chan struct{}),
	}
}

// ask sets the pods the next poll asks, and the target whose silence it
// counts.
func (p *instancePoller) ask(pods []Pod, target string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.pods, p.target = pods, target
}

func (p *instancePoller) snapshot() pollView {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.view
}

// run polls every period until ctx ends.
func (p *instancePoller) run(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		p.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// poll reads every instance manager's /status at once, and publishes
// what each answered. It accounts for the silence of each pod it asks
// and of the target, so a target whose pod is gone, which it cannot
// ask, still accumulates silence. It wakes the pass when the poll
// changed what a decision reads.
func (p *instancePoller) poll(ctx context.Context) {
	p.mutex.Lock()
	pods, target := p.pods, p.target
	p.mutex.Unlock()

	began := time.Now()
	now := p.c.now()
	type answer struct {
		name   string
		report instanceReport
		err    error
	}
	answers := make(chan answer, len(pods))
	accounted := map[string]bool{}
	if target != targetPending && target != "" {
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
			address := fmt.Sprintf("%s:%d", pod.Status.PodIP, p.c.statusPort)
			report, err := readInstanceReport(ctx, p.c.poller, address)
			answers <- answer{pod.Metadata.Name, report, err}
		}()
	}
	heard := map[string]instanceReport{}
	for range asked {
		if a := <-answers; a.err == nil {
			heard[a.name] = a.report
		}
	}

	p.mutex.Lock()
	previous := p.view
	next := pollView{at: now, lastSeen: maps.Clone(previous.lastSeen), reports: maps.Clone(previous.reports), informedAt: previous.informedAt, missedSince: previous.missedSince}
	for name, report := range heard {
		next.lastSeen[name], next.reports[name] = now, report
	}
	outcome := pollUnanswered
	switch {
	case ctx.Err() != nil:
		outcome = pollCanceled
	case len(heard) > 0:
		outcome = pollInformative
		next.recordInformative(now, accounted, heard)
	}
	p.view = next
	p.mutex.Unlock()
	if previous.at.IsZero() {
		close(p.first)
	}

	p.c.readings.observePoll(outcome, time.Since(began))
	switch {
	case outcome == pollInformative:
		p.c.log.clear(p.subject, topicPoll)
	case outcome == pollUnanswered && asked > 0:
		p.c.log.fault(p.subject, topicPoll, fmt.Sprintf("no instance manager answered on status port %d, so a silent primary starts no failover until one does", p.c.statusPort))
	}
	if outcome == pollInformative && pollChanged(previous, next, target, p.c.timings.failureThreshold) {
		poke(p.c.pollWake)
	}
}

// The outcomes of a poll, by the label that counts them.
const (
	pollInformative = "informative"
	pollUnanswered  = "unanswered"
	pollCanceled    = "canceled"
)

// pollChanged reports whether a poll changed what a decision reads: a
// manager that answered for the first time or again, a report whose
// role, link, readiness, or error changed, or a target whose silence
// reached the failure threshold.
func pollChanged(previous, next pollView, target string, threshold time.Duration) bool {
	for name, report := range next.reports {
		before, known := previous.reports[name]
		if !next.lastSeen[name].Equal(next.at) {
			continue
		}
		if !known || !previous.lastSeen[name].Equal(previous.at) || before.Role != report.Role || before.LinkUp != report.LinkUp || before.Ready != report.Ready || before.Error != report.Error {
			return true
		}
	}
	return previous.silence(target) < threshold && next.silence(target) >= threshold
}

// recordInformative records an informative poll at now: each accounted
// instance that did not answer missed it, unless it already missed an
// earlier one, and each that answered is silent no more.
func (v *pollView) recordInformative(now time.Time, accounted map[string]bool, heard map[string]instanceReport) {
	missed := map[string]time.Time{}
	for name := range accounted {
		if _, answered := heard[name]; answered {
			continue
		}
		since, already := v.missedSince[name]
		if !already {
			since = now
		}
		missed[name] = since
	}
	v.informedAt, v.missedSince = now, missed
}

// silence is how long the manager of name has missed informative
// polls: from the first one it missed to the latest one, and 0 when it
// answered the latest one.
func (v pollView) silence(name string) time.Duration {
	since, missed := v.missedSince[name]
	if !missed {
		return 0
	}
	return v.informedAt.Sub(since)
}

// fresh returns the report of name if its manager answered within
// within of polled, the time of a poll. The decisions pass the time of
// the poll their pass works from, so a pass that waited on the API
// server does not make a report that just arrived look old.
func (v pollView) fresh(name string, polled time.Time, within time.Duration) (instanceReport, bool) {
	seen, ok := v.lastSeen[name]
	if !ok || polled.Sub(seen) > within {
		return instanceReport{}, false
	}
	return v.reports[name], true
}

// polls returns the view the pass decides from for r. A pass outside
// the controller's loop, as in the tests, polls once itself. In the
// loop the poller runs on its own, and a pass waits only for a new
// poller's first poll, so a new leader's first pass decides from what
// the managers say.
func (c *controller) polls(ctx context.Context, r *Redis, pods []Pod, target string) pollView {
	p, running := c.pollers[r.Metadata.UID]
	if !running {
		p = newInstancePoller(c, "redis "+objectKey(r.Metadata))
		c.pollers[r.Metadata.UID] = p
	}
	p.ask(pods, target)
	if c.polling == nil {
		p.poll(ctx)
		return p.snapshot()
	}
	if !running {
		loop, stop := context.WithCancel(ctx)
		p.stop = stop
		c.polling.Go(func() { p.run(loop, c.timings.pollPeriod) })
	}
	select {
	case <-p.first:
	case <-ctx.Done():
	}
	return p.snapshot()
}

// forgetPollers stops the poller of each Redis that is gone.
func (c *controller) forgetPollers(live map[string]bool) {
	for uid, p := range c.pollers {
		if live[uid] {
			continue
		}
		if p.stop != nil {
			p.stop()
		}
		delete(c.pollers, uid)
	}
}
