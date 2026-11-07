package main

// The status a pass composes for one Redis, and its write. A pass
// writes only a status that differs from the copy it read, with that
// copy's resourceVersion, so a decision made from a stale read never
// lands.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// summarize fills in the instances, the ready count, and the Ready
// condition.
func (d *decision) summarize(pods []Pod) {
	var instances []InstanceStatus
	ready := 0
	for _, pod := range pods {
		report := d.st.reports[pod.Metadata.Name]
		role := report.Role
		if role == "" {
			role = reportStarting
		}
		instances = append(instances, InstanceStatus{Name: pod.Metadata.Name, Role: role, Ready: pod.ready(), MinReplicasToWrite: report.MinReplicasToWrite})
		if pod.ready() {
			ready++
		}
	}
	d.status.Instances = instances
	d.status.ReadyInstances = ready
	d.status.ObservedGeneration = d.r.Metadata.Generation
	want := d.r.Spec.instances()
	status, reason, message := ConditionTrue, "AllInstancesReady", fmt.Sprintf("%s is primary, and %d of %d instances are ready", d.holder, ready, want)
	switch {
	case d.status.TargetPrimary == targetPending && d.quorumShort != "":
		status, reason, message = ConditionFalse, "QuorumNotReached", d.quorumShort
	case d.status.TargetPrimary == targetPending:
		status, reason, message = ConditionFalse, "FailoverInProgress", "the primary failed, and the operator is choosing the next one"
	case d.holder == "":
		status, reason, message = ConditionFalse, "NoPrimary", "no instance holds the primary lease"
	case d.status.Switchover != nil:
		reason, message = "SwitchoverInProgress", fmt.Sprintf("moving the primary from %s to %s", d.status.Switchover.From, d.status.Switchover.To)
	case ready < want:
		status, reason, message = ConditionFalse, "InstancesNotReady", fmt.Sprintf("%d of %d instances are ready", ready, want)
	case d.scaleDownHeld():
		reason, message = "ScalingDown", fmt.Sprintf("%s holds the primary lease, but its pod is not ready or its manager did not answer as primary. "+
			"The StatefulSet keeps its %d pods until it answers, then scales down to %d", d.holder, d.scale, want)
	case len(pods) > want:
		reason, message = "ScalingDown", fmt.Sprintf("%s is primary, and the instances beyond %d are being removed", d.holder, want)
	}
	if d.scaleHeld() && reason != "ScalingDown" {
		direction := "down"
		if d.scale < want {
			direction = "up"
		}
		message += fmt.Sprintf(". The StatefulSet keeps its %d pods until the next primary holds the lease, then scales %s to %d", d.scale, direction, want)
	}
	d.setCondition(readyCondition, status, reason, message)
}

func (d *decision) setCondition(kind string, status ConditionStatus, reason, message string) {
	d.status.Conditions = withCondition(d.status.Conditions, Condition{
		Type: kind, Status: status, Reason: reason, Message: message,
		ObservedGeneration: d.r.Metadata.Generation,
		LastTransitionTime: d.now.UTC().Format(time.RFC3339),
	})
}

// writeStatus writes the status when it changed, with the
// resourceVersion read, so it fails rather than overwrite a newer
// decision. It reports whether the status as composed now stands: it
// was written, or it was already the status read.
func (c *controller) writeStatus(ctx context.Context, r *Redis, status RedisStatus) (bool, error) {
	if reflect.DeepEqual(status, r.Status) {
		return true, nil
	}
	body, err := json.Marshal(status)
	if err != nil {
		return false, err
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		return false, err
	}
	// A merge patch keeps a field it does not state, so a field that is
	// no longer set, such as the end of a switchover or the current
	// primary of a dataset that starts again empty, is a null.
	for _, optional := range []string{"switchover", "replica", "rollout", "currentPrimary"} {
		if _, set := fields[optional]; !set {
			fields[optional] = nil
		}
	}
	patch := map[string]any{
		"metadata": map[string]any{"resourceVersion": r.Metadata.ResourceVersion},
		"status":   fields,
	}
	c.watches[redisResource].store.wroteStatus(objectKey(r.Metadata), body)
	err = c.client.mergePatchStatus(ctx, redisResource, r.Metadata.Namespace, r.Metadata.Name, patch, nil)
	if err == nil {
		c.readings.wroteStatus(redisResource)
	}
	if errors.Is(err, errConflict) {
		// The Redis changed since this pass read it. The change can be
		// one whose event wakes no pass, such as this operator's own
		// last status write, so the next pass runs at once and decides
		// again from what the watch brought.
		poke(c.wake)
		return false, nil
	}
	return err == nil, err
}

func cloneSwitchover(s *SwitchoverStatus) *SwitchoverStatus {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}
