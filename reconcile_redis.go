package main

// The decisions for one Redis: which instance should be primary, when a
// failover starts, when a switchover starts and ends, and which pod
// rolls next. Each decision is a change to status.targetPrimary, which
// the operator writes with the resourceVersion it read, so a decision
// made from a stale read never lands.

import (
	"context"
	"encoding/json"
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
	if err := c.ensureChildren(ctx, r, st, w); err != nil {
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
	// The decisions change these through their pointers, so they are
	// copies, or the change would compare equal to what was read and
	// never be written.
	if r.Status.Replica != nil {
		replica := *r.Status.Replica
		status.Replica = &replica
	}
	if r.Status.Rollout != nil {
		rollout := *r.Status.Rollout
		status.Rollout = &rollout
	}
	if status.TargetPrimary == "" {
		status.TargetPrimary = instanceName(r.Metadata.Name, 0)
	}
	if holder != "" {
		status.CurrentPrimary = holder
		st.leaderlessSince = time.Time{}
	} else if st.leaderlessSince.IsZero() {
		st.leaderlessSince = now
	}

	d := decision{c: c, r: r, st: st, status: &status, pods: byName, holder: holder, now: now, nodes: w.nodes}
	d.replica(ctx)
	// A single instance fails over too, to itself: when it restarts with
	// no data, only a failover starts the empty dataset it must serve.
	d.failover()
	// A Redis scaling down to one instance still has replicas to switch
	// over to until the StatefulSet shrinks.
	if r.Spec.instances() > 1 || len(pods) > 1 {
		d.switchover(w.statefulSets[key].Status.UpdateRevision)
	}
	d.minReplicasToWrite()
	d.rollout(w.statefulSets[key].Status.UpdateRevision)
	if err := c.roll(ctx, &d, w.statefulSets[key].Status.UpdateRevision); err != nil {
		return err
	}
	d.summarize(pods)
	landed, err := c.writeStatus(ctx, r, status)
	if err != nil {
		return err
	}
	if landed {
		c.postDecisions(r, &d)
	}
	return c.labelRoles(ctx, pods, holder, status.TargetPrimary)
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
	// notes are the Events of this pass's decisions, which the pass
	// posts once the status lands.
	notes []eventNote
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
	// The operator watches the instance it named. After a failover names
	// the next primary, the old holder can keep its Lease until it
	// expires, and its silence then is the failure already handled.
	primary := d.status.TargetPrimary
	reason := ""
	switch {
	case d.st.silentFor(primary, d.now) > t.failureThreshold && d.hasOtherCandidate(primary):
		reason = fmt.Sprintf("the instance manager of %s has not answered for %s", primary, t.failureThreshold)
	case d.holder == "" && d.targetCannotTake():
		reason = fmt.Sprintf("%s restarted with no data and cannot take the primary lease", d.status.TargetPrimary)
	case d.holder == "" && !d.st.leaderlessSince.IsZero() && d.now.Sub(d.st.leaderlessSince) > t.leaderlessThreshold:
		reason = fmt.Sprintf("no instance has held the primary lease for %s", t.leaderlessThreshold)
	}
	if reason == "" {
		return
	}
	d.c.log.printf("%s: failing over: %s", d.subject(), reason)
	d.note(eventWarning, "FailoverStarted", fmt.Sprintf("failing over from %s: %s", primary, reason))
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
		// starts again on instance 0. A Redis that replicates loses
		// nothing, because instance 0 copies the source again.
		first := instanceName(d.r.Metadata.Name, 0)
		d.status.TargetPrimary = first
		d.status.CurrentPrimary = ""
		d.st.pendingSince = time.Time{}
		if d.status.replicating() {
			d.c.log.printf("%s: every instance restarted with no data; %s copies the source again", d.subject(), first)
			d.note(eventWarning, "AllInstancesEmpty", fmt.Sprintf("every instance restarted with no data; %s copies the source again", first))
			return
		}
		d.c.log.printf("%s: every instance restarted with no data; starting an empty dataset on %s", d.subject(), first)
		d.note(eventWarning, "AllInstancesEmpty", fmt.Sprintf("every instance restarted with no data; starting an empty dataset on %s", first))
		d.setCondition("DataLost", ConditionTrue, "AllInstancesEmpty", "every instance restarted with no data, so the dataset started empty")
		return
	}
	if best == "" {
		return
	}
	d.c.log.printf("%s: %s is the next primary, at offset %d", d.subject(), best, bestOffset)
	d.note(eventNormal, "PrimaryChosen", fmt.Sprintf("%s is the next primary, at offset %d", best, bestOffset))
	d.status.TargetPrimary = best
	d.st.pendingSince = time.Time{}
	// The new target may wait out the old holder's Lease before it takes
	// it, so the wait for a holder starts again from here.
	d.st.leaderlessSince = d.now
}

// hasOtherCandidate reports whether a failover could name an instance
// other than primary. A single instance cannot, so its silence, as while
// its pod restarts, starts no failover: it comes back as the target, and
// takes the Lease again if it kept its data. If it did not, it says so,
// and that starts the failover that begins an empty dataset.
func (d *decision) hasOtherCandidate(primary string) bool {
	return d.r.Spec.instances() > 1 || !d.staysAfterScaling(primary)
}

// targetCannotTake reports whether the target answered that it has no
// data, so it will never take the Lease, and waiting for it is no use.
func (d *decision) targetCannotTake() bool {
	report, ok := d.st.fresh(d.status.TargetPrimary, d.now, 2*d.c.timings.backstop)
	return ok && !report.HasData && !report.HoldsLease
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
	// A replica that is down on an old revision rolls first and at once:
	// it serves nothing, so deleting it costs nothing, and it is how a
	// failed canary goes back to the image that works.
	for _, name := range slices.Sorted(maps.Keys(d.pods)) {
		pod := d.pods[name]
		if name != d.holder && !pod.ready() && pod.Metadata.DeletionTimestamp == "" && pod.Metadata.Labels[revisionLabel] != updateRevision {
			c.log.printf("%s: rolling %s, which is down, to the current revision", d.subject(), name)
			err := c.client.delete(ctx, podsResource, pod.Metadata.Namespace, name, pod.Metadata.UID, "")
			if err != nil && !errors.Is(err, errNotFound) {
				return fmt.Errorf("deleting pod %s: %w", name, err)
			}
			c.events.normal(referenceTo(redisKind, d.r.Metadata), "RollingInstance", fmt.Sprintf("deleted %s, which is down, so it starts on the current revision", name))
			return nil
		}
	}
	for _, pod := range d.pods {
		if !pod.ready() || pod.Metadata.DeletionTimestamp != "" {
			return nil
		}
	}
	names := slices.SortedFunc(maps.Keys(d.pods), func(a, b string) int { return ordinal(b) - ordinal(a) })
	if ro := d.status.Rollout; ro != nil && ro.Phase == rolloutCanary {
		// Until the canary holds the data, it is the only pod that rolls.
		names = []string{ro.Canary}
	}
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
		c.events.normal(referenceTo(redisKind, d.r.Metadata), "RollingInstance", fmt.Sprintf("deleted %s so it starts on the new revision", name))
		return nil
	}
	return nil
}

// labelRoles writes the role label on each pod from the Lease and the
// decision: primary on the holder while it is also the target, replica
// on the rest. The Services and the budgets select on it. Once the
// target names another instance, or a failover is pending, the holder
// loses the label before its manager steps down, so the <name>-rw
// Service stops sending clients to a server that is about to refuse
// writes (manager_demote.go). It runs after the status write, so a
// decision that did not land moves no label.
func (c *controller) labelRoles(ctx context.Context, pods []Pod, holder, target string) error {
	for _, pod := range pods {
		want := roleReplica
		if pod.Metadata.Name == holder && holder == target {
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
	case len(pods) > want:
		d.setCondition(readyCondition, ConditionTrue, "ScalingDown", fmt.Sprintf("%s is primary, and the instances beyond %d are being removed", d.holder, want))
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
	err = c.client.mergePatchStatus(ctx, redisResource, r.Metadata.Namespace, r.Metadata.Name, patch, nil)
	if errors.Is(err, errConflict) {
		// The Redis changed since this pass read it. The watch brings
		// the change, and the next pass decides again from it.
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
