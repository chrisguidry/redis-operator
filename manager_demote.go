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
// instance holding the Lease, takes the same steps (demote). Its server
// stays a paused primary until routing moves, which is as safe as a
// replica: it takes no write while paused, and a write that its lost
// replicas cannot confirm gets NOREPLICAS, never READONLY. A manager
// that cannot read its pod cannot tell when routing moves, so it waits
// demotionHold before it closes the clients and changes the role.
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
		if m.leaving != nil {
			// The role changed on an earlier step, and the transaction
			// that ends the pause failed there.
			if err := m.server.killClients(ctx, true); err != nil {
				return false, err
			}
			m.leaving = nil
		}
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
	if after, readErr := m.server.replication(ctx); readErr == nil && after.role == "master" {
		// The server kept the primary role, as it does when a FAILOVER
		// aborts, so its clients may write again.
		m.leaving = nil
		if unpauseErr := m.server.unpause(ctx); err == nil {
			err = unpauseErr
		}
		return false, err
	}
	if killErr := m.server.killClients(ctx, true); killErr != nil {
		// leaving stays, so the next step ends the pause.
		if err == nil {
			err = killErr
		}
		return false, err
	}
	m.leaving = nil
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

// demote makes a primary a replica with change once routing has moved
// off its pod, as stepDown does, for a demotion that nothing plans: a
// fence, or a primary that finds another instance holding the Lease. A
// replica answers every write with READONLY, so the server stays a
// paused primary until no client can reach it through the Service.
func (m *manager) demote(ctx context.Context, info replicationInfo, change func() error) error {
	_, err := m.stepDown(ctx, m.now(), info, change)
	return err
}
