package main

// Planned switchovers. The operator moves the primary role off an
// instance whose pod asked for it, whose node is cordoned, that a
// scale-down removes, or that is the last pod on an old revision, to
// the ready replica with the highest offset. A switchover ends when the
// new primary holds the Lease, and the operator gives up one that runs
// past its deadline.

import (
	"fmt"
	"time"
)

// switchover starts a planned switchover when the primary's node is
// cordoned, when the primary's pod asked for one, or when the primary
// is the last pod on an old revision. It ends one that finished, and
// gives up one that ran too long.
func (d *decision) switchover(updateRevision string) {
	if s := d.status.Switchover; s != nil {
		started, _ := time.Parse(time.RFC3339, s.StartedAt)
		switch {
		case d.holder == s.To:
			d.c.log.printf("%s: switchover from %s to %s done", d.subject(), s.From, s.To)
			d.note(eventNormal, "SwitchoverDone", fmt.Sprintf("%s is primary, in place of %s (%s)", s.To, s.From, s.Reason))
			d.status.Switchover = nil
		case d.now.Sub(started) > d.c.timings.switchoverDeadline:
			d.c.log.printf("%s: switchover from %s to %s did not finish in %s; %s stays primary", d.subject(), s.From, s.To, d.c.timings.switchoverDeadline, s.From)
			d.status.Switchover = nil
			d.status.TargetPrimary = s.From
			d.st.retryAfter = d.now.Add(d.c.timings.switchoverBackoff)
			d.setCondition("SwitchoverFailed", ConditionTrue, "Timeout", fmt.Sprintf("%s did not take the primary role within %s", s.To, d.c.timings.switchoverDeadline))
		}
		return
	}
	if d.holder == "" || d.holder != d.status.TargetPrimary || d.now.Before(d.st.retryAfter) {
		return
	}
	primary, ok := d.pods[d.holder]
	if !ok {
		return
	}
	reason := ""
	switch {
	case primary.Metadata.Annotations[switchoverAnnotation] != "":
		reason = reasonPodDeleted
	case d.nodes[primary.Spec.NodeName].Spec.Unschedulable:
		reason = reasonNodeCordoned
	case !d.staysAfterScaling(d.holder):
		reason = reasonScaleDown
	case updateRevision != "" && primary.Metadata.Labels[revisionLabel] != updateRevision && d.othersUpdated(updateRevision) &&
		(d.status.Rollout == nil || d.status.Rollout.Phase != rolloutCanary):
		reason = reasonRollout
	}
	if reason == "" {
		return
	}
	to := d.bestReplica(reason == reasonRollout, updateRevision)
	if to == "" {
		d.c.log.fault(d.subject(), "switchover", fmt.Sprintf("a switchover off %s is needed (%s), and no replica is ready to take it", d.holder, reason))
		return
	}
	d.c.log.clear(d.subject(), "switchover")
	d.c.log.printf("%s: switching over from %s to %s: %s", d.subject(), d.holder, to, reason)
	d.note(eventNormal, "SwitchoverStarted", switchoverMessage(d.holder, to, reason))
	d.c.readings.switchovers.WithLabelValues(reason).Inc()
	d.status.TargetPrimary = to
	d.status.Switchover = &SwitchoverStatus{From: d.holder, To: to, Reason: reason, StartedAt: d.now.UTC().Format(time.RFC3339)}
	d.setCondition("SwitchoverFailed", ConditionFalse, "Started", "")
}

// othersUpdated reports whether every pod but the primary runs the
// update revision, so the primary is the one left to roll.
func (d *decision) othersUpdated(updateRevision string) bool {
	for name, pod := range d.pods {
		if name != d.holder && pod.Metadata.Labels[revisionLabel] != updateRevision {
			return false
		}
	}
	return true
}

// bestReplica is the ready replica on a schedulable node that follows
// the primary with its link up and the highest offset, among the
// instances a scale-down keeps. A rollout takes only a replica on the
// update revision.
func (d *decision) bestReplica(updatedOnly bool, updateRevision string) string {
	primaryIP := d.pods[d.holder].Status.PodIP
	best, bestOffset := "", int64(-1)
	for name, pod := range d.pods {
		if name == d.holder || !pod.ready() || pod.Metadata.DeletionTimestamp != "" || !d.staysAfterScaling(name) {
			continue
		}
		if d.nodes[pod.Spec.NodeName].Spec.Unschedulable || pod.Metadata.Annotations[switchoverAnnotation] != "" {
			continue
		}
		if updatedOnly && pod.Metadata.Labels[revisionLabel] != updateRevision {
			continue
		}
		report, ok := d.st.fresh(name, d.now, 2*d.c.timings.backstop)
		if !ok || report.Role != reportReplica || !report.LinkUp || report.Following != primaryIP || report.Stopping {
			continue
		}
		if report.Offset > bestOffset || report.Offset == bestOffset && ordinal(name) < ordinal(best) {
			best, bestOffset = name, report.Offset
		}
	}
	return best
}
