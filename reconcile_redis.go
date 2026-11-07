package main

// The decisions for one Redis: which instance should be primary, when a
// failover starts, when a switchover starts and ends
// (reconcile_switchover.go), and which pod rolls next. Each decision is
// a change to status.targetPrimary, which the operator writes with the
// resourceVersion it read, so a decision made from a stale read never
// lands. A decision's log lines, counters, and Events wait for that
// write (reconcile_events.go).

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

	"github.com/prometheus/client_golang/prometheus"
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
	key := objectKey(r.Metadata)
	holder := w.leases[r.Metadata.Namespace+"/"+primaryLeaseName(r.Metadata.Name)].Spec.HolderIdentity
	pods := w.pods[key]
	slices.SortFunc(pods, func(a, b Pod) int { return ordinal(a.Metadata.Name) - ordinal(b.Metadata.Name) })
	byName := map[string]Pod{}
	for _, pod := range pods {
		byName[pod.Metadata.Name] = pod
	}
	target := r.Status.TargetPrimary
	if target == "" {
		target = instanceName(r.Metadata.Name, 0)
	}
	st.pollView = c.polls(ctx, r, pods, target)
	polled := st.at
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	settled := primarySettled(r.Status.TargetPrimary, holder, byName, st, polled)
	safe := shrinkSafe(r, holder, byName, st, polled)
	scale := replicasFor(r, holder, scaleAsRead(w.statefulSets[key].Spec.Replicas, st.appliedScale), settled, safe)
	if err := c.ensureChildren(ctx, r, st, w, scale, primaryLabeled(pods)); err != nil {
		return err
	}
	st.appliedScale = scale
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
	status.TargetPrimary = target
	if holder != "" {
		status.CurrentPrimary = holder
		st.leaderlessSince = time.Time{}
	} else if st.leaderlessSince.IsZero() {
		st.leaderlessSince = now
	}

	d := decision{c: c, r: r, st: st, status: &status, pods: byName, holder: holder, now: now, polled: polled, nodes: w.nodes, scale: scale, settled: settled}
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
		if d.clearForce {
			if err := c.clearForce(ctx, r); err != nil {
				return err
			}
		}
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
	// now is when the pass decides, for the operator's own timers, and
	// polled is when it polled the managers, for every check of whether
	// one answered recently.
	now    time.Time
	polled time.Time
	// scale is the StatefulSet's scale this pass applied.
	scale int
	// settled is primarySettled for this pass.
	settled bool
	// notes, lines, and counts are the Events, log lines, and counters
	// of this pass's decisions, which the pass posts, writes, and
	// increments once the status lands.
	notes  []eventNote
	lines  []string
	counts []prometheus.Counter
	// quorumShort says why a pending failover waits for its quorum, and
	// clearForce whether the pass removes the annotation that forced it.
	quorumShort string
	clearForce  bool
}

func (d *decision) subject() string { return "redis " + objectKey(d.r.Metadata) }

// failover starts a failover when the primary's manager has missed
// informative polls for failureThreshold (reconcile_poll.go), and names
// the next primary once every replica detached.
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
	case d.st.silence(primary) >= t.failureThreshold && d.hasOtherCandidate(primary):
		reason = fmt.Sprintf("the instance manager of %s has not answered for %s", primary, t.failureThreshold)
	case d.holder == "" && d.targetCannotTake():
		reason = fmt.Sprintf("%s restarted with no data and cannot take the primary lease", d.status.TargetPrimary)
	case d.holder == "" && !d.st.leaderlessSince.IsZero() && d.now.Sub(d.st.leaderlessSince) > t.leaderlessThreshold:
		reason = fmt.Sprintf("no instance has held the primary lease for %s", t.leaderlessThreshold)
	}
	if reason == "" {
		return
	}
	d.line("failing over: %s", reason)
	d.note(eventWarning, "FailoverStarted", fmt.Sprintf("failing over from %s: %s", primary, reason))
	d.count(d.c.readings.failovers)
	d.status.TargetPrimary = targetPending
	d.status.Switchover = nil
	d.st.pendingSince = d.now
}

// choosePrimary names the reachable instance with data and the highest
// offset, once every reachable instance has detached or the wait for
// that is over, and once enough instances detached with their data to
// hold every confirmed write (reconcile_quorum.go).
func (d *decision) choosePrimary() {
	if d.st.pendingSince.IsZero() {
		d.st.pendingSince = d.now
	}
	within := 2 * d.c.timings.backstop
	allDetached, allAnswered, anyData := true, true, false
	counted := 0
	best, bestOffset := "", int64(-1)
	// The StatefulSet keeps its pods while a failover is pending, so a
	// scale-down started during it still lets the failover reach and
	// name each of them.
	for i := range d.scale {
		name := instanceName(d.r.Metadata.Name, i)
		report, ok := d.st.fresh(name, d.polled, within)
		if !ok {
			allAnswered = false
			continue
		}
		anyData = anyData || report.HasData
		if report.Role != reportDetached {
			allDetached = false
		}
		if countsTowardQuorum(report) {
			counted++
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
			d.line("every instance restarted with no data; %s copies the source again", first)
			d.note(eventWarning, "AllInstancesEmpty", fmt.Sprintf("every instance restarted with no data; %s copies the source again", first))
			return
		}
		d.line("every instance restarted with no data; starting an empty dataset on %s", first)
		d.note(eventWarning, "AllInstancesEmpty", fmt.Sprintf("every instance restarted with no data; starting an empty dataset on %s", first))
		d.setCondition("DataLost", ConditionTrue, "AllInstancesEmpty", "every instance restarted with no data, so the dataset started empty")
		return
	}
	if d.lacksQuorum(counted) || best == "" {
		return
	}
	d.noteForced(best, counted)
	d.line("%s is the next primary, at offset %d", best, bestOffset)
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
	report, ok := d.st.fresh(d.status.TargetPrimary, d.polled, 2*d.c.timings.backstop)
	return ok && !report.HasData && !report.HoldsLease
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
