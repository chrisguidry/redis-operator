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
// the holder or the target has an ordinal beyond it. While the primary
// is not settled it is current, the StatefulSet's scale as read, in
// both directions: a scale-down could delete the only copy of a
// confirmed write, and a scale-up would add empty pods that raise the
// failover's quorum and can never count toward it. A StatefulSet not
// yet made takes spec.instances.
func replicasFor(r *Redis, holder string, current int, settled, shrinkSafe bool) int {
	n := r.Spec.instances()
	if (!settled || !shrinkSafe && current > n) && current > 0 {
		n = current
	}
	for _, name := range []string{holder, r.Status.TargetPrimary} {
		n = max(n, ordinal(name)+1)
	}
	return n
}

// shrinkSafe reports whether the StatefulSet may remove the pods beyond
// spec.instances without losing a write confirmed with WAIT: each of
// them answered the poll at polled detached, so its offset is final,
// and each replica that stays answered linked to the holder at an
// offset at or past theirs. Every write a removed instance acknowledged
// is then on an instance that stays.
func shrinkSafe(r *Redis, holder string, pods map[string]Pod, st *redisState, polled time.Time) bool {
	instances := r.Spec.instances()
	var removed int64 = -1
	for name := range pods {
		if ordinal(name) < instances || name == holder {
			continue
		}
		report, ok := st.fresh(name, polled, 0)
		if !ok || report.Role != reportDetached {
			return false
		}
		removed = max(removed, report.Offset)
	}
	if removed < 0 {
		return true
	}
	for name := range pods {
		if ordinal(name) >= instances || name == holder {
			continue
		}
		report, ok := st.fresh(name, polled, 0)
		if !ok || report.Role != reportReplica || !report.LinkUp || report.Following != pods[holder].Status.PodIP || report.Offset < removed {
			return false
		}
	}
	return true
}

// waitsOnOffsets reports whether this pass waits for an offset that the
// managers report to move: a scale-down that only shrinkSafe holds,
// which shrinks once the replicas that stay reach the offset of those it
// removes. A scale-down that waits for a switchover off a removed
// instance waits on the switchover instead.
func (d *decision) waitsOnOffsets(shrinkSafe bool) bool {
	instances := d.r.Spec.instances()
	return d.settled && !shrinkSafe && d.scale > instances && ordinal(d.holder) < instances
}

// scaleAsRead is the StatefulSet's scale: the scale its watch read, or
// the scale this operator last applied when that is smaller, as when
// the watch has not yet delivered a shrink. Taking the smaller keeps a
// held scale from growing the StatefulSet back to a size it left.
func scaleAsRead(read, applied int) int {
	if applied == 0 {
		return read
	}
	return min(read, applied)
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

// scaleHeld reports whether the StatefulSet keeps a scale other than
// spec.instances while the primary is not settled.
func (d *decision) scaleHeld() bool {
	return d.scale != d.r.Spec.instances() && !d.settled
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
