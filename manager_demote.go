package main

// When a primary's server becomes a replica and keeps running, its
// clients still hold connections to it. A command that reaches a
// replica fails with READONLY, which a client takes for a fault in the
// application rather than a reason to reconnect. Redis Sentinel closes
// those connections when it demotes an instance, so its clients
// reconnect and find the new primary. The manager closes them too, but
// its clients find the primary through the <name>-rw Service, which
// selects on the role label the operator writes, so a client that
// reconnects while the label is still on this pod comes back to the
// same server.
//
// So a planned step-down, for a switchover or a pending failover, moves
// the routing first (stepDown). The operator takes the primary
// label off the holder as soon as the target names another instance or
// none. The manager pauses its server's writes, which a primary holds
// instead of refusing, and waits until it reads its pod without the
// label and kube-proxy has had routeSettle to apply the endpoint change.
// Then it closes every normal and pub/sub client, changes the role, and
// closes them again in the transaction that ends the pause, because a
// replica refuses with READONLY a write that arrives after the role
// changed, pause or not. A client's write ends with a closed
// connection, which clients retry, and its reconnect finds the new
// primary or no endpoint yet.
//
// An unplanned demotion, a fence or a primary that finds another
// instance holding the Lease, does not wait for routing (demote). The manager pauses
// writes across the role change and closes the clients in the
// transaction that ends the pause, then closes them again once it reads
// its pod without the label and once more after routeSettle. A manager
// that cannot read its pod closes them a last time after demotionHold.
//
// The replication links are other client types and stay, and a pause
// of writes does not stop a primary from sending its replicas what it
// already took. The server's own limit on a pause is longer than any
// wait here, and ends a pause that a crashed manager left.

import (
	"context"
	"time"
)

// leaving is a primary that holds its writes until routing moves away
// from its pod.
type leaving struct {
	started time.Time
	// routedAt is when the manager first read its pod without the
	// primary label.
	routedAt time.Time
}

// stepDown makes a primary a replica with change, once routing has
// moved away from it. It reports whether the server is a replica now,
// and the caller tries again on the next step until it is.
func (m *manager) stepDown(ctx context.Context, now time.Time, info replicationInfo, change func() error) (bool, error) {
	if info.role != "master" {
		m.leaving = nil
		return true, change()
	}
	l := m.leaving
	if l == nil {
		if err := m.server.pauseWrites(ctx, m.pauseLimit()); err != nil {
			return false, err
		}
		m.line(m.config.podName + ": holding writes until the primary label is off this pod")
		l = &leaving{started: now}
		m.leaving = l
	}
	if !m.labeledPrimary && l.routedAt.IsZero() {
		l.routedAt = now
	}
	settled := !l.routedAt.IsZero() && now.Sub(l.routedAt) >= m.config.routeSettle
	if !settled && now.Sub(l.started) < m.config.demotionHold {
		return false, nil
	}
	if err := m.server.killClients(ctx, false); err != nil {
		return false, err
	}
	err := change()
	m.leaving = nil
	if after, readErr := m.server.replication(ctx); readErr == nil && after.role == "master" {
		// The server kept the primary role, as it does when a FAILOVER
		// aborts, so its clients may write again.
		if unpauseErr := m.server.unpause(ctx); err == nil {
			err = unpauseErr
		}
		return false, err
	}
	if killErr := m.server.killClients(ctx, true); killErr != nil {
		// The pause outlives this step, so the unplanned path ends it.
		m.demoted = &demotion{started: now}
		if err == nil {
			err = killErr
		}
	}
	return err == nil, err
}

// resumeWrites ends a step-down that the operator called off, so the
// primary takes writes again.
func (m *manager) resumeWrites(ctx context.Context) error {
	if m.leaving == nil {
		return nil
	}
	m.leaving = nil
	return m.server.unpause(ctx)
}

// pauseLimit is the server's own limit on a pause.
func (m *manager) pauseLimit() time.Duration {
	return 2*m.config.demotionHold + m.config.failoverTimeout
}

// demotion is a server that stopped being primary without waiting for
// routing, and whose clients may still reconnect to it.
type demotion struct {
	started  time.Time
	routedAt time.Time
}

// demote makes a primary a replica with change at once, with its writes
// paused across the change.
func (m *manager) demote(ctx context.Context, info replicationInfo, change func() error) error {
	if info.role != "master" {
		return change()
	}
	m.leaving = nil
	if err := m.server.pauseWrites(ctx, m.pauseLimit()); err != nil {
		return err
	}
	err := change()
	if after, readErr := m.server.replication(ctx); readErr == nil && after.role == "master" {
		if unpauseErr := m.server.unpause(ctx); err == nil {
			err = unpauseErr
		}
		return err
	}
	m.demoted = &demotion{started: time.Now()}
	if killErr := m.server.killClients(ctx, true); err == nil {
		err = killErr
	}
	return err
}

// disconnectDemotedClients closes the clients of a server that demote
// changed, once its pod has lost the primary label and again after
// routeSettle. info is the server's role after this step acted.
func (m *manager) disconnectDemotedClients(ctx context.Context, now time.Time, apiErr error, info replicationInfo) error {
	d := m.demoted
	if d == nil {
		return nil
	}
	if info.role == "master" {
		// The server is primary again, and its clients are the primary's.
		m.demoted = nil
		return m.server.unpause(ctx)
	}
	routed := apiErr == nil && !m.labeledPrimary
	if routed && d.routedAt.IsZero() {
		d.routedAt = now
		return m.server.killClients(ctx, true)
	}
	if (routed && now.Sub(d.routedAt) >= m.config.routeSettle) || now.Sub(d.started) >= m.config.demotionHold {
		if err := m.server.killClients(ctx, true); err != nil {
			return err
		}
		m.demoted = nil
	}
	return nil
}
