package main

// What the instance manager reads from the API server, and how often.
// Every step reads the Redis, because a renewal must follow a fresh
// read of status.targetPrimary, and a replica must detach within a
// step of a pending failover. The rest changes rarely, so a step reads
// it only when it is due:
//
//   - The holder renews the primary Lease from the version of its own
//     last write, with no read, and reads it again only after a write
//     that the API server refused as stale or gone. The target reads it
//     every step, so it takes a released Lease at once. Any other
//     instance reads it every Lease read period.
//   - The pods are listed every slow read period, and on every step of a
//     primary that steps down, which waits for its pod to lose the
//     primary label. A step that lists no pods leaves labeledPrimary
//     true, so a stale list never lets a primary stop holding writes
//     early.
//   - The Backups, and the Secrets of the users and of TLS, are read
//     every slow read period, so a change to them applies within it.
//
// Each read is a direct request, which etcd answers with a quorum read,
// so a decision never rests on a copy older than the read.

import (
	"context"
	"errors"
	"time"
)

// readDecision reads what this step decides from: the Redis, the
// primary Lease when it is due, and the pods when they are due.
func (m *manager) readDecision(ctx context.Context, now time.Time) error {
	// Until a read of this pod says otherwise, it may still carry the
	// primary label.
	m.labeledPrimary = true
	var redis Redis
	if err := m.client.get(ctx, redisResource, m.config.namespace, m.config.redisName, &redis); err != nil {
		return err
	}
	m.target = redis.Status.TargetPrimary
	m.switchingOver = redis.Status.Switchover != nil
	m.everHadPrimary = redis.Status.CurrentPrimary != ""
	m.minReplicas = redis.minReplicasToWrite()
	m.primaryMinReplicas = nil
	for _, instance := range redis.Status.Instances {
		if instance.Role == reportPrimary {
			m.primaryMinReplicas = instance.MinReplicasToWrite
		}
	}
	// A scale-down that waits keeps pods beyond spec.instances, and
	// the operator lists every pod in the status.
	m.instances = max(redis.Spec.instances(), len(redis.Status.Instances))
	m.specInstances = redis.Spec.instances()
	m.replicaMode = redis.Status.replicating() && redis.Spec.Replica != nil
	if redis.Spec.Replica != nil {
		m.replicaSource = redis.Spec.Replica.Source
	}
	if err := m.readLease(ctx, now); err != nil {
		return err
	}
	if err := m.readPods(ctx, now); err != nil {
		return err
	}
	m.readAccessDecision(redis, m.pods)
	return nil
}

// readLease reads the primary Lease when this instance needs a fresh
// copy: it does not hold it, and it is the target or its read is due.
// The holder's copy is the answer to its own last write.
func (m *manager) readLease(ctx context.Context, now time.Time) error {
	if m.lease.held() && m.lease.exists {
		return nil
	}
	if m.target != m.config.podName && !due(now, m.leaseReadAt, m.config.leaseReadPeriod) {
		return nil
	}
	if err := m.lease.read(ctx, now); err != nil {
		return err
	}
	m.leaseReadAt = now
	return nil
}

// rereadLeaseAfter reads the Lease again after a write that the API
// server refused because the holder's copy was stale or the Lease is
// gone, so the next step acts on what the Lease holds now.
func (m *manager) rereadLeaseAfter(ctx context.Context, now time.Time, err error) {
	if errors.Is(err, errConflict) || errors.Is(err, errNotFound) {
		if m.lease.read(ctx, now) == nil {
			m.leaseReadAt = now
		}
	}
}

// readPods lists the pods when the list is due, or on every step while
// this primary steps down.
func (m *manager) readPods(ctx context.Context, now time.Time) error {
	if m.leaving == nil && !due(now, m.podsReadAt, m.config.slowReadPeriod) {
		return nil
	}
	var pods struct {
		Items []Pod `json:"items"`
	}
	if err := m.client.list(ctx, podsResource, m.config.namespace, clusterLabel+"="+m.config.redisName, &pods); err != nil {
		return err
	}
	ips := map[string]string{}
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" {
			ips[pod.Metadata.Name] = pod.Status.PodIP
		}
		if pod.Metadata.Name == m.config.podName {
			m.labeledPrimary = pod.Metadata.Labels[roleLabel] == rolePrimary
		}
	}
	m.pods, m.podIPs, m.podsReadAt = pods.Items, ips, now
	return nil
}

// due reports whether a read last made at last is due again at now. A
// read never made is due.
func due(now, last time.Time, period time.Duration) bool {
	return last.IsZero() || now.Sub(last) >= period
}

// apiContext bounds every API request of a step that this instance
// takes as the holder by its renew deadline, so a dial or an answer
// that hangs cannot carry the step past the moment the holder must
// stop serving or prove its peers agree. A holder past its deadline
// still renews, so it serves again once the API server answers, and
// each of its requests is bounded by a retry period, so every step
// still reaches the isolation check. It measures the deadline on the
// manager's clock and sets it on the wall clock.
func (m *manager) apiContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if !m.lease.held() || m.lastRenew.IsZero() {
		return context.WithCancel(ctx)
	}
	left := m.lastRenew.Add(m.config.renewDeadline).Sub(m.now())
	if left <= 0 {
		left = m.config.retryPeriod
	}
	return context.WithTimeout(ctx, left)
}
