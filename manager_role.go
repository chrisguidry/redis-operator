package main

// The instance manager's step for each role. The holder of the Lease
// runs the primary, hands the role over, or steps down, and fences
// itself when it cannot renew the Lease and a peer disagrees. Every
// other instance takes the Lease when it is the target and may, and
// follows the holder or detaches otherwise.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// stepAsHolder runs the primary, or hands the role over.
func (m *manager) stepAsHolder(ctx context.Context, now time.Time, apiErr error, info replicationInfo) error {
	if apiErr == nil && now.Sub(m.lastRenew) >= m.config.retryPeriod {
		if err := m.lease.renew(ctx, now); err == nil {
			m.lastRenew = now
		} else {
			apiErr = err
		}
	}
	if m.lastRenew.IsZero() {
		m.lastRenew = now
	}
	if now.Sub(m.lastRenew) > m.config.renewDeadline {
		return m.isolationCheck(ctx, info)
	}
	if apiErr != nil {
		// The decision cannot be read, and the Lease is recent, so the
		// primary keeps serving as it is.
		return nil
	}
	if m.stopping && m.target == m.config.podName && !m.askedHandover {
		m.askedHandover = true
		if err := m.askForHandover(ctx); err != nil {
			return err
		}
	}
	switch m.target {
	case m.config.podName:
		if m.replicaMode {
			return m.followSource(ctx, info)
		}
		if err := m.resumeWrites(ctx); err != nil {
			return err
		}
		if err := m.setMasterauth(ctx, m.config.password); err != nil {
			return err
		}
		if err := m.server.promote(ctx, info); err != nil {
			return err
		}
		m.line(m.config.podName + ": primary")
		return m.server.setMinReplicasToWrite(ctx, m.minReplicas)
	case targetPending, "":
		m.line(m.config.podName + ": stepping down, a failover is pending")
		return m.stepDownForFailover(ctx, now, info)
	default:
		if !m.switchingOver && !m.replicaMode {
			// A failover named the next primary while this instance, which
			// the API server still reaches, held the Lease. Its replicas
			// detached for the failover, and may not reach it at all, so a
			// FAILOVER to the next primary could never finish.
			m.line(fmt.Sprintf("%s: stepping down, a failover named %s", m.config.podName, m.target))
			return m.stepDownForFailover(ctx, now, info)
		}
		return m.handOver(ctx, now, info)
	}
}

// stepDownForFailover detaches the server once routing has moved off
// this pod, then releases the Lease.
func (m *manager) stepDownForFailover(ctx context.Context, now time.Time, info replicationInfo) error {
	detached, err := m.stepDown(ctx, now, info, func() error { return m.server.detach(ctx, info, m.config.port) })
	if !detached {
		return err
	}
	return m.lease.release(ctx, now)
}

// handOver moves the primary role to the target with FAILOVER, once
// routing has moved off this pod, then releases the Lease. An aborted
// FAILOVER leaves this instance primary, and the operator gives the
// switchover up.
func (m *manager) handOver(ctx context.Context, now time.Time, info replicationInfo) error {
	if m.replicaMode {
		// The holder is a replica of the source, so there is no primary
		// role to hand over: the next holder follows the source itself.
		m.say(fmt.Sprintf("%s: giving the lease to %s, which follows the source in its place", m.config.podName, m.target))
		return m.lease.release(ctx, now)
	}
	ip := m.podIPs[m.target]
	if ip == "" {
		return fmt.Errorf("cannot hand the primary role to %s: its pod has no IP", m.target)
	}
	handed, err := m.stepDown(ctx, now, info, func() error {
		port, err := m.peerPort(ctx, m.target)
		if err != nil {
			return err
		}
		m.say(fmt.Sprintf("%s: handing the primary role to %s", m.config.podName, m.target))
		return m.server.failoverTo(ctx, ip, port, m.config.failoverTimeout)
	})
	if err != nil {
		return fmt.Errorf("handing the primary role to %s: %w", m.target, err)
	}
	if !handed {
		return nil
	}
	return m.lease.release(ctx, m.now())
}

// askForHandover marks this pod, so the operator switches over before
// the pod stops.
func (m *manager) askForHandover(ctx context.Context) error {
	m.say(m.config.podName + ": asking the operator to move the primary role before this pod stops")
	return m.client.mergePatch(ctx, podsResource, m.config.namespace, m.config.podName, map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{switchoverAnnotation: time.Now().UTC().Format(time.RFC3339)}},
	}, nil)
}

// isolationCheck runs when the holder has not renewed its Lease for the
// renew deadline.
func (m *manager) isolationCheck(ctx context.Context, info replicationInfo) error {
	if m.peersAgree(ctx) {
		m.line(m.config.podName + ": cannot renew the primary lease, but every peer agrees this instance is primary; serving")
		return m.resumeWrites(ctx)
	}
	m.line(m.config.podName + ": cannot renew the primary lease and a peer disagrees or is unreachable; fencing")
	return m.demote(ctx, info, func() error { return m.server.detach(ctx, info, m.config.port) })
}

// peersAgree asks every other instance's manager which instance it
// takes for the target.
func (m *manager) peersAgree(ctx context.Context) bool {
	for name, ip := range m.podIPs {
		if name == m.config.podName {
			continue
		}
		report, err := m.readPeer(ctx, ip)
		if err != nil || report.Target != m.config.podName {
			return false
		}
	}
	return true
}

func (m *manager) readPeer(ctx context.Context, ip string) (instanceReport, error) {
	return readInstanceReport(ctx, m.peers, net.JoinHostPort(ip, strconv.Itoa(m.config.statusPort)))
}

// stepAsOther takes the Lease when this instance is the target and may,
// and follows the holder otherwise.
func (m *manager) stepAsOther(ctx context.Context, now time.Time, info replicationInfo) error {
	holder := m.lease.holder()
	fit := m.hasData || !m.everHadPrimary
	switch {
	case m.target == m.config.podName && fit && !m.stopping && m.lease.takeable(now):
		if err := m.lease.take(ctx, now); err != nil {
			if errors.Is(err, errConflict) {
				return nil
			}
			return err
		}
		m.lastRenew = now
		m.hasData = true
		m.say(m.config.podName + ": took the primary lease")
		if err := m.resumeWrites(ctx); err != nil {
			return err
		}
		if m.replicaMode {
			return m.followSource(ctx, info)
		}
		if err := m.server.promote(ctx, info); err != nil {
			return err
		}
		return m.server.setMinReplicasToWrite(ctx, m.minReplicas)
	case m.target == m.config.podName && info.role == "master":
		// FAILOVER made this server the primary, and the old holder has
		// not released the Lease yet.
		return nil
	case m.target == targetPending:
		m.line(m.config.podName + ": detached, a failover is pending")
		return m.demote(ctx, info, func() error { return m.server.detach(ctx, info, m.config.port) })
	case m.scaledAway() && holder != "" && holder == m.target:
		// A scale-down removes this instance. Detached, it acknowledges
		// no more writes, so its offset is final, and the operator
		// shrinks the StatefulSet once the instances that stay are past
		// it. A failover still counts it, with its data.
		m.line(m.config.podName + ": detached, the scale-down removes this instance")
		return m.server.detach(ctx, info, m.config.port)
	case holder != "" && holder != m.config.podName && m.podIPs[holder] != "":
		if err := m.setMasterauth(ctx, m.config.password); err != nil {
			return err
		}
		m.line(m.config.podName + ": following " + holder)
		return m.demote(ctx, info, func() error {
			port, err := m.peerPort(ctx, holder)
			if err != nil {
				return err
			}
			return m.server.follow(ctx, info, m.podIPs[holder], port)
		})
	case holder == "" && m.switchingOver && info.role == "slave" && m.podIPs[m.target] != "" && info.masterHost == m.podIPs[m.target]:
		// FAILOVER made this server a replica of the target, which takes
		// the empty Lease on its next step. The target counts this link
		// toward min-replicas-to-write, so detaching would make the new
		// primary refuse writes with NOREPLICAS until this instance
		// followed it again. Keeping the link loses nothing a detach
		// would protect: what the target takes streams to this instance,
		// which is the one the operator names again if the switchover
		// fails.
		m.line(m.config.podName + ": following " + m.target + ", which the switchover made primary")
		return nil
	default:
		m.line(m.config.podName + ": detached, no primary holds the lease")
		return m.demote(ctx, info, func() error { return m.server.detach(ctx, info, m.config.port) })
	}
}

// scaledAway reports whether a scale-down removes this instance: its
// ordinal is at or beyond spec.instances, and the min-replicas-to-write
// the operator names and the one the primary's server runs are both
// ones the instances that stay can meet, so the primary does not count
// on this one to take writes. The primary applies a new value on its
// own step, so the status's value alone would let this instance detach
// while the primary still refuses writes without it.
func (m *manager) scaledAway() bool {
	if ordinal(m.config.podName) < m.specInstances || m.minReplicas >= m.specInstances {
		return false
	}
	return m.primaryMinReplicas != nil && *m.primaryMinReplicas < m.specInstances
}
