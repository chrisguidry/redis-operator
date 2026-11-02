package main

// The decisions for one Redis: which instance should be primary, when a
// failover starts, when a switchover starts and ends, and which pod
// rolls next. Each decision is a change to status.targetPrimary, which
// the operator writes with the resourceVersion it read, so a decision
// made from a stale read never lands.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// revisionLabel is the label the StatefulSet controller puts on each pod
// with the revision of the template it was made from.
const revisionLabel = "controller-revision-hash"

const readyCondition = "Ready"

func (c *controller) reconcile(ctx context.Context, r *Redis, w *world) error {
	if r.Metadata.DeletionTimestamp != "" {
		return nil
	}
	st := c.state(r)
	if err := c.ensureChildren(ctx, r, st); err != nil {
		return err
	}
	key := objectKey(r.Metadata)
	pods := w.pods[key]
	slices.SortFunc(pods, func(a, b Pod) int { return ordinal(a.Metadata.Name) - ordinal(b.Metadata.Name) })
	byName := map[string]Pod{}
	for _, pod := range pods {
		byName[pod.Metadata.Name] = pod
	}
	holder := w.leases[r.Metadata.Namespace+"/"+primaryLeaseName(r.Metadata.Name)].Spec.HolderIdentity
	c.poll(ctx, st, pods)
	now := c.now()

	status := r.Status
	status.Switchover = cloneSwitchover(r.Status.Switchover)
	if status.TargetPrimary == "" {
		status.TargetPrimary = instanceName(r.Metadata.Name, 0)
	}
	if holder != "" {
		status.CurrentPrimary = holder
		st.leaderlessSince = time.Time{}
	} else if st.leaderlessSince.IsZero() {
		st.leaderlessSince = now
	}
	if err := c.labelRoles(ctx, pods, holder); err != nil {
		return err
	}

	d := decision{c: c, r: r, st: st, status: &status, pods: byName, holder: holder, now: now, nodes: w.nodes}
	if r.Spec.instances() > 1 {
		d.failover()
		d.switchover(w.statefulSets[key].Status.UpdateRevision)
	}
	if err := c.roll(ctx, &d, w.statefulSets[key].Status.UpdateRevision); err != nil {
		return err
	}
	d.summarize(pods)
	return c.writeStatus(ctx, r, status)
}

// decision carries one pass's read of a Redis, and the status it
// changes.
type decision struct {
	c      *controller
	r      *Redis
	st     *redisState
	status *RedisStatus
	pods   map[string]Pod
	nodes  map[string]Node
	holder string
	now    time.Time
}

func (d *decision) subject() string { return "redis " + objectKey(d.r.Metadata) }

// failover starts a failover when the primary's manager stops
// answering, and names the next primary once every replica detached.
func (d *decision) failover() {
	t := d.c.timings
	if d.status.TargetPrimary == targetPending {
		d.choosePrimary()
		return
	}
	// Before the first primary takes the Lease, there is nothing to fail
	// over from: the pods are still starting.
	if d.status.CurrentPrimary == "" {
		return
	}
	primary := d.holder
	if primary == "" {
		primary = d.status.TargetPrimary
	}
	reason := ""
	switch {
	case d.st.silentFor(primary, d.now) > t.failureThreshold:
		reason = fmt.Sprintf("the instance manager of %s has not answered for %s", primary, t.failureThreshold)
	case d.holder == "" && d.status.CurrentPrimary != "" && !d.st.leaderlessSince.IsZero() && d.now.Sub(d.st.leaderlessSince) > t.failureThreshold:
		reason = fmt.Sprintf("no instance has held the primary lease for %s", t.failureThreshold)
	}
	if reason == "" {
		return
	}
	d.c.log.printf("%s: failing over: %s", d.subject(), reason)
	d.c.readings.failovers.Inc()
	d.status.TargetPrimary = targetPending
	d.status.Switchover = nil
	d.st.pendingSince = d.now
}

// choosePrimary names the reachable instance with data and the highest
// offset, once every reachable instance has detached or the wait for
// that is over.
func (d *decision) choosePrimary() {
	if d.st.pendingSince.IsZero() {
		d.st.pendingSince = d.now
	}
	within := 2 * d.c.timings.backstop
	allDetached, allAnswered, anyData := true, true, false
	best, bestOffset := "", int64(-1)
	for i := range d.r.Spec.instances() {
		name := instanceName(d.r.Metadata.Name, i)
		report, ok := d.st.fresh(name, d.now, within)
		if !ok {
			allAnswered = false
			continue
		}
		anyData = anyData || report.HasData
		if report.Role != reportDetached {
			allDetached = false
		}
		if !report.HasData || report.Stopping {
			continue
		}
		if report.Offset > bestOffset {
			best, bestOffset = name, report.Offset
		}
	}
	if !allDetached && d.now.Sub(d.st.pendingSince) < d.c.timings.pendingWait {
		return
	}
	if best == "" && allAnswered && !anyData {
		// Every instance answered, and none holds data: they all
		// restarted with no volume. The data is gone, so the dataset
		// starts again on instance 0.
		d.c.log.printf("%s: every instance restarted with no data; starting an empty dataset on %s", d.subject(), instanceName(d.r.Metadata.Name, 0))
		d.status.TargetPrimary = instanceName(d.r.Metadata.Name, 0)
		d.status.CurrentPrimary = ""
		d.st.pendingSince = time.Time{}
		d.setCondition("DataLost", ConditionTrue, "AllInstancesEmpty", "every instance restarted with no data, so the dataset started empty")
		return
	}
	if best == "" {
		return
	}
	d.c.log.printf("%s: %s is the next primary, at offset %d", d.subject(), best, bestOffset)
	d.status.TargetPrimary = best
	d.st.pendingSince = time.Time{}
}

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
	case updateRevision != "" && primary.Metadata.Labels[revisionLabel] != updateRevision && d.othersUpdated(updateRevision):
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
// the primary with its link up and the highest offset. A rollout takes
// only a replica on the update revision.
func (d *decision) bestReplica(updatedOnly bool, updateRevision string) string {
	primaryIP := d.pods[d.holder].Status.PodIP
	best, bestOffset := "", int64(-1)
	for name, pod := range d.pods {
		if name == d.holder || !pod.ready() || pod.Metadata.DeletionTimestamp != "" {
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

// roll deletes one replica that runs an old revision, when every pod is
// ready and no failover or switchover runs. The primary rolls after the
// switchover that switchover starts once the replicas are done. A
// single instance has no replica to switch to, so its pod is deleted.
func (c *controller) roll(ctx context.Context, d *decision, updateRevision string) error {
	if updateRevision == "" || d.status.Switchover != nil || d.status.TargetPrimary == targetPending || d.holder == "" {
		return nil
	}
	if len(d.pods) != d.r.Spec.instances() {
		return nil
	}
	for _, pod := range d.pods {
		if !pod.ready() || pod.Metadata.DeletionTimestamp != "" {
			return nil
		}
	}
	names := slices.SortedFunc(maps.Keys(d.pods), func(a, b string) int { return ordinal(b) - ordinal(a) })
	for _, name := range names {
		pod := d.pods[name]
		if pod.Metadata.Labels[revisionLabel] == updateRevision {
			continue
		}
		if name == d.holder && d.r.Spec.instances() > 1 {
			continue
		}
		c.log.printf("%s: rolling %s to the new revision", d.subject(), name)
		err := c.client.delete(ctx, podsResource, pod.Metadata.Namespace, name, pod.Metadata.UID, "")
		if err != nil && !errors.Is(err, errNotFound) {
			return fmt.Errorf("deleting pod %s: %w", name, err)
		}
		return nil
	}
	return nil
}

// labelRoles writes the role label on each pod from the Lease: primary
// on the holder, replica on the rest. The Services and the budgets
// select on it.
func (c *controller) labelRoles(ctx context.Context, pods []Pod, holder string) error {
	for _, pod := range pods {
		want := roleReplica
		if pod.Metadata.Name == holder {
			want = rolePrimary
		}
		if pod.Metadata.Labels[roleLabel] == want {
			continue
		}
		patch := map[string]any{"metadata": map[string]any{"labels": map[string]any{roleLabel: want}}}
		// A replica that took over clears its stale request, so a later
		// term as primary does not start a switchover by itself.
		if want == roleReplica && pod.Metadata.Annotations[switchoverAnnotation] != "" && pod.Metadata.DeletionTimestamp == "" {
			patch["metadata"].(map[string]any)["annotations"] = map[string]any{switchoverAnnotation: nil}
		}
		err := c.client.mergePatch(ctx, podsResource, pod.Metadata.Namespace, pod.Metadata.Name, patch, nil)
		if err != nil && !errors.Is(err, errNotFound) {
			return fmt.Errorf("labeling pod %s: %w", pod.Metadata.Name, err)
		}
	}
	return nil
}

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
		instances = append(instances, InstanceStatus{Name: pod.Metadata.Name, Role: role, Ready: pod.ready(), Offset: report.Offset})
		if pod.ready() {
			ready++
		}
	}
	d.status.Instances = instances
	d.status.ReadyInstances = ready
	d.status.ObservedGeneration = d.r.Metadata.Generation
	want := d.r.Spec.instances()
	switch {
	case d.status.TargetPrimary == targetPending:
		d.setCondition(readyCondition, ConditionFalse, "FailoverInProgress", "the primary failed, and the operator is choosing the next one")
	case d.holder == "":
		d.setCondition(readyCondition, ConditionFalse, "NoPrimary", "no instance holds the primary lease")
	case d.status.Switchover != nil:
		d.setCondition(readyCondition, ConditionTrue, "SwitchoverInProgress", fmt.Sprintf("moving the primary from %s to %s", d.status.Switchover.From, d.status.Switchover.To))
	case ready < want:
		d.setCondition(readyCondition, ConditionFalse, "InstancesNotReady", fmt.Sprintf("%d of %d instances are ready", ready, want))
	default:
		d.setCondition(readyCondition, ConditionTrue, "AllInstancesReady", fmt.Sprintf("%s is primary, and %d of %d instances are ready", d.holder, ready, want))
	}
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
// decision.
func (c *controller) writeStatus(ctx context.Context, r *Redis, status RedisStatus) error {
	if reflect.DeepEqual(status, r.Status) {
		return nil
	}
	patch := map[string]any{
		"metadata": map[string]any{"resourceVersion": r.Metadata.ResourceVersion},
		"status":   status,
	}
	if status.Switchover == nil {
		// A merge patch keeps a field it does not state, so the end of a
		// switchover is a null.
		patch["status"] = statusWithNullSwitchover(status)
	}
	err := c.client.mergePatchStatus(ctx, redisResource, r.Metadata.Namespace, r.Metadata.Name, patch, nil)
	if errors.Is(err, errConflict) {
		// The Redis changed since this pass read it. The watch brings
		// the change, and the next pass decides again from it.
		return nil
	}
	return err
}

func statusWithNullSwitchover(status RedisStatus) map[string]any {
	return map[string]any{
		"observedGeneration": status.ObservedGeneration,
		"targetPrimary":      status.TargetPrimary,
		"currentPrimary":     status.CurrentPrimary,
		"readyInstances":     status.ReadyInstances,
		"instances":          status.Instances,
		"switchover":         nil,
		"conditions":         status.Conditions,
	}
}

func cloneSwitchover(s *SwitchoverStatus) *SwitchoverStatus {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}

// ordinal is the number at the end of a pod name, which the StatefulSet
// gives it.
func ordinal(name string) int {
	index := strings.LastIndex(name, "-")
	n, err := strconv.Atoi(name[index+1:])
	if err != nil {
		return -1
	}
	return n
}
