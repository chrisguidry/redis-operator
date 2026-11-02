package main

// The min-replicas-to-write the primary runs with. The spec's default
// is 1 with replicas and 0 without, so scaling a single instance up
// would make the primary refuse every write with NOREPLICAS until the
// first new replica synced: an outage caused by adding capacity. The
// operator records the value in status.minReplicasToWrite instead, and
// raises it toward the spec's only as far as replicas have synced.
// Once raised, it stays when replicas go away, because then the
// refusal is the guarantee: a primary that no replica follows must not
// take a write it could lose. It drops to the spec's at once, and a new
// Redis starts at the spec's, as it has no writes to keep taking.

// minReplicasToWrite sets status.minReplicasToWrite for this pass.
func (d *decision) minReplicasToWrite() {
	want := d.r.Spec.minReplicasToWrite()
	current := d.status.MinReplicasToWrite
	if current == nil || want <= *current {
		d.status.MinReplicasToWrite = &want
		return
	}
	value := max(*current, min(want, d.syncedReplicas()))
	d.status.MinReplicasToWrite = &value
}

// syncedReplicas is how many replicas the primary streams to, by its
// own count. A replica's report would not do: the report of an
// instance that a scale-down removed stays fresh for a moment, and a
// scale-up right after brings its name back.
func (d *decision) syncedReplicas() int {
	report, ok := d.st.fresh(d.holder, d.now, 2*d.c.timings.backstop)
	if !ok || report.Role != reportPrimary {
		return 0
	}
	return report.SyncedReplicas
}
