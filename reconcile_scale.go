package main

import "time"

// Scaling down. The StatefulSet removes the highest ordinals first, so
// a smaller spec.instances would delete a primary whose ordinal is at or
// beyond the new count. The operator keeps the StatefulSet large enough
// for the Lease holder and the target, switches over to the most
// caught-up instance that stays (reason ScaleDown), and shrinks the
// StatefulSet once that instance holds the Lease.
//
// A write confirmed with WAIT is on the primary and on some of the
// replicas, and any of them may be beyond the new count. While the
// primary is not settled, the instance that holds such a write may be
// the one a failover must reach or name, so the StatefulSet keeps every
// pod it has until an instance holds the Lease, is the target, and
// answers as primary. A primary that has just died still holds the
// Lease, and a scale-down then would delete the only copies of its
// writes.

// replicasFor is the StatefulSet's scale: spec.instances, or more while
// the holder or the target has an ordinal beyond it, and never less
// than current, the StatefulSet's scale as read, while the primary is
// not settled.
func replicasFor(r *Redis, holder string, current int, settled bool) int {
	n := r.Spec.instances()
	for _, name := range []string{holder, r.Status.TargetPrimary} {
		n = max(n, ordinal(name)+1)
	}
	if !settled {
		n = max(n, current)
	}
	return n
}

// primarySettled reports whether an instance holds the Lease and is the
// target, its pod is ready and not being deleted, and its manager
// answered the poll at polled as primary. A primary that dies keeps the
// Lease until the Lease expires, and until then only its pod and its
// manager show that it is gone.
func primarySettled(target, holder string, pods map[string]Pod, st *redisState, polled time.Time) bool {
	if holder == "" || holder != target {
		return false
	}
	pod, ok := pods[holder]
	if !ok || !pod.ready() || pod.Metadata.DeletionTimestamp != "" {
		return false
	}
	report, ok := st.fresh(holder, polled, 0)
	return ok && report.Role == reportPrimary
}

// scaleDownHeld reports whether the StatefulSet keeps pods beyond
// spec.instances while the primary is not settled.
func (d *decision) scaleDownHeld() bool {
	return d.scale > d.r.Spec.instances() && !d.settled
}

// staysAfterScaling reports whether the instance named name is within
// spec.instances, so a scale-down keeps it.
func (d *decision) staysAfterScaling(name string) bool {
	return ordinal(name) < d.r.Spec.instances()
}
