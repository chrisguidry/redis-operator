package main

// Scaling down. The StatefulSet removes the highest ordinals first, so
// a smaller spec.instances would delete a primary whose ordinal is at or
// beyond the new count. The operator keeps the StatefulSet large enough
// for the Lease holder and the target, switches over to the most
// caught-up instance that stays (reason ScaleDown), and shrinks the
// StatefulSet once that instance holds the Lease.

// replicasFor is the StatefulSet's scale: spec.instances, or more while
// the holder or the target has an ordinal beyond it.
func replicasFor(r *Redis, holder string) int {
	n := r.Spec.instances()
	for _, name := range []string{holder, r.Status.TargetPrimary} {
		n = max(n, ordinal(name)+1)
	}
	return n
}

// staysAfterScaling reports whether the instance named name is within
// spec.instances, so a scale-down keeps it.
func (d *decision) staysAfterScaling(name string) bool {
	return ordinal(name) < d.r.Spec.instances()
}
