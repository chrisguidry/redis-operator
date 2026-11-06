package main

// The staged rollout of a new image: a canary replica first, proven
// over a soak, then the rest. A canary that is not proven in time is
// rolled back, and the operator does not try that image again until
// spec.image changes.

import (
	"fmt"
	"time"
)

// The rollout's timings. They are fields of timings so a test can
// shorten them.
const (
	defaultCanarySoak    = 30 * time.Second
	defaultCanaryTimeout = 5 * time.Minute
)

func (d *decision) rollout(updateRevision string) {
	want := d.r.Spec.image(d.c.images)
	if d.status.Image == "" {
		d.status.Image = want
		return
	}
	ro := d.status.Rollout
	switch {
	case ro == nil && want == d.status.Image:
		return
	case ro == nil || (ro.Image != want && want != d.status.Image):
		d.startRollout(want)
		return
	case ro.Image != want && want == d.status.Image:
		d.c.log.printf("%s: spec.image is back to %s; the rollout to %s ends", d.subject(), want, ro.Image)
		d.note(eventNormal, "RolloutReverted", fmt.Sprintf("spec.image is back to %s, so the rollout to %s ends", want, ro.Image))
		d.status.Rollout = nil
		d.setCondition("RolloutFailed", ConditionFalse, "Reverted", "")
		return
	}
	switch ro.Phase {
	case rolloutCanary:
		d.watchCanary(ro, updateRevision)
	case rolloutRolling:
		if d.allOn(updateRevision) {
			d.c.log.printf("%s: every instance runs %s", d.subject(), ro.Image)
			d.note(eventNormal, "RolloutCompleted", fmt.Sprintf("every instance runs %s", ro.Image))
			d.status.Image = ro.Image
			d.status.Rollout = nil
			d.setCondition("RolloutFailed", ConditionFalse, "Completed", "")
		}
	}
}

func (d *decision) startRollout(image string) {
	ro := &RolloutStatus{Image: image, Phase: rolloutCanary, StartedAt: d.now.UTC().Format(time.RFC3339)}
	d.st.canarySince = time.Time{}
	if d.r.Spec.instances() == 1 {
		// A single instance has no replica to prove the image on.
		ro.Phase = rolloutRolling
	} else {
		ro.Canary = d.canaryName()
	}
	d.c.log.printf("%s: rolling out %s, canary %s", d.subject(), image, ro.Canary)
	if ro.Canary == "" {
		d.note(eventNormal, "RolloutStarted", fmt.Sprintf("rolling out %s, with no canary because there is one instance", image))
	} else {
		d.note(eventNormal, "RolloutStarted", fmt.Sprintf("rolling out %s on the canary %s first", image, ro.Canary))
	}
	d.status.Rollout = ro
}

// canaryName is the replica with the highest ordinal.
func (d *decision) canaryName() string {
	for i := d.r.Spec.instances() - 1; i >= 0; i-- {
		if name := instanceName(d.r.Metadata.Name, i); name != d.holder {
			return name
		}
	}
	return ""
}

// watchCanary passes the canary once it has been ready, linked, and
// caught up for the soak, and fails it after the timeout.
func (d *decision) watchCanary(ro *RolloutStatus, updateRevision string) {
	pod, ok := d.pods[ro.Canary]
	report, fresh := d.st.fresh(ro.Canary, d.polled, 2*d.c.timings.backstop)
	primary, _ := d.st.fresh(d.holder, d.polled, 2*d.c.timings.backstop)
	proven := ok && updateRevision != "" && pod.Metadata.Labels[revisionLabel] == updateRevision &&
		pod.ready() && fresh && report.LinkUp && report.Offset >= primary.Offset-d.c.timings.canaryLagBytes
	switch {
	case proven && d.st.canarySince.IsZero():
		d.st.canarySince = d.now
	case !proven:
		d.st.canarySince = time.Time{}
	}
	if proven && d.now.Sub(d.st.canarySince) >= d.c.timings.canarySoak {
		d.c.log.printf("%s: canary %s holds the data on %s; rolling the rest", d.subject(), ro.Canary, ro.Image)
		d.note(eventNormal, "CanaryPromoted", fmt.Sprintf("the canary %s holds the data on %s, so the rest roll", ro.Canary, ro.Image))
		ro.Phase = rolloutRolling
		return
	}
	started, _ := time.Parse(time.RFC3339, ro.StartedAt)
	if d.now.Sub(started) < d.c.timings.canaryTimeout {
		return
	}
	reason := "its pod did not become ready"
	switch {
	case ok && pod.waitingReason() != "":
		reason = pod.waitingReason()
	case fresh && report.Error != "":
		reason = report.Error
	case fresh && !report.LinkUp:
		reason = "it could not replicate from the primary; the new version may not read the primary's data"
	}
	d.c.log.printf("%s: canary %s failed on %s: %s; rolling it back to %s", d.subject(), ro.Canary, ro.Image, reason, d.status.Image)
	d.note(eventWarning, "CanaryRolledBack", fmt.Sprintf("the canary %s failed on %s: %s; rolling it back to %s", ro.Canary, ro.Image, reason, d.status.Image))
	ro.Phase = rolloutFailed
	ro.Reason = reason
	d.setCondition("RolloutFailed", ConditionTrue, "CanaryFailed", fmt.Sprintf("%s did not hold the data on %s within %s: %s", ro.Canary, ro.Image, d.c.timings.canaryTimeout, reason))
}

func (d *decision) allOn(updateRevision string) bool {
	if updateRevision == "" || len(d.pods) != d.r.Spec.instances() {
		return false
	}
	for _, pod := range d.pods {
		if pod.Metadata.Labels[revisionLabel] != updateRevision || !pod.ready() {
			return false
		}
	}
	return true
}
