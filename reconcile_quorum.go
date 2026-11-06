package main

// The failover quorum. Redis replicates asynchronously, so the
// instances a failover does not reach may hold writes that the one it
// promotes lacks. A write a client confirmed with WAIT n is on the old
// primary and on at least n of the other instances, so any set of
// instances larger than the rest of them holds it. A failover names the
// next primary only once it has reached such a set, and the highest
// offset among them is then at least the offset of every such write.

import (
	"context"
	"errors"
	"fmt"
)

// forceFailoverAnnotation lets a person name the next primary without
// the quorum, when they know the instances the failover cannot reach
// will not come back with their data. The operator removes it once a
// failover names the next primary, so it covers one failover.
const forceFailoverAnnotation = "redis.guid.foo/failover-without-quorum"

// quorumAcks is the n of the WAIT n whose writes a failover keeps: the
// min-replicas-to-write the primary runs with, and at least one.
func (d *decision) quorumAcks() int {
	return max(1, d.r.minReplicasToWrite())
}

// quorum is how many instances a failover must reach. It counts every
// pod that could have acknowledged a write: each ordinal the
// StatefulSet keeps, which during a scale-down is more than
// spec.instances, and each pod beyond it that has not gone yet.
func (d *decision) quorum() int {
	total := max(d.scale, len(d.pods))
	return max(1, total-d.quorumAcks())
}

// countsTowardQuorum reports whether an instance's report proves what
// it holds. A detached server no longer follows the old primary, so
// its offset is final, where a replica that still follows could take
// more writes after it reported. An instance that restarted with no
// data lost what it acknowledged, and one that is stopping cannot be
// named.
func countsTowardQuorum(report instanceReport) bool {
	return report.Role == reportDetached && report.HasData && !report.Stopping
}

// forced reports whether a person annotated the Redis to fail over
// without the quorum.
func (d *decision) forced() bool {
	return d.r.Metadata.Annotations[forceFailoverAnnotation] == "true"
}

// lacksQuorum reports whether a failover that reached counted
// instances must wait for more, and records why for the Ready
// condition.
func (d *decision) lacksQuorum(counted int) bool {
	need := d.quorum()
	if counted >= need || d.forced() {
		d.c.log.clear(d.subject(), "quorum")
		return false
	}
	d.quorumShort = fmt.Sprintf("%d of the %d instances a failover needs have detached with their data. "+
		"With fewer, the next primary could lack a write confirmed with WAIT %d. "+
		"To promote the best of them anyway, annotate the Redis with %s=true",
		counted, need, d.quorumAcks(), forceFailoverAnnotation)
	d.c.log.fault(d.subject(), "quorum", "waiting for the failover quorum: "+d.quorumShort)
	return true
}

// noteForced records a failover that named a primary while the
// annotation was set, so the pass removes the annotation once the
// decision lands, and says so when the quorum was short.
func (d *decision) noteForced(best string, counted int) {
	if !d.forced() {
		return
	}
	d.clearForce = true
	if need := d.quorum(); counted < need {
		d.line("%s is the next primary without the quorum: %d of %d instances detached with their data", best, counted, need)
		d.note(eventWarning, "FailoverForced", fmt.Sprintf("%s is the next primary without the quorum: %d of the %d instances it needs had detached with their data, so a write only the others held is lost", best, counted, need))
	}
}

// clearForce removes the annotation that forced a failover. It runs
// only after the decision landed, so a decision that a newer version
// refused keeps the annotation for the next pass.
func (c *controller) clearForce(ctx context.Context, r *Redis) error {
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]any{forceFailoverAnnotation: nil}}}
	err := c.client.mergePatch(ctx, redisResource, r.Metadata.Namespace, r.Metadata.Name, patch, nil)
	if err != nil && !errors.Is(err, errNotFound) {
		return fmt.Errorf("removing the annotation %s: %w", forceFailoverAnnotation, err)
	}
	return nil
}
