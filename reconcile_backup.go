package main

// The operator's part of backups: it starts each Backup by choosing the
// instance and the key, fails one that runs too long, makes Backups on
// each ScheduledBackup's schedule, deletes the Backups that retention
// no longer keeps, and deletes a deleted Backup's object from the
// store. The instance manager takes the snapshot (manager_backup.go).

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// backupDeadline is how long a Backup may run before the operator marks
// it failed.
const backupDeadline = time.Hour

func (c *controller) reconcileBackups(ctx context.Context, w *world) {
	byRedis := map[string]Redis{}
	for _, r := range w.redis {
		byRedis[objectKey(r.Metadata)] = r
	}
	c.forgetBackups(w.backups)
	for _, backup := range w.backups {
		c.observeBackup(backup)
		subject := "backup " + objectKey(backup.Metadata)
		if err := c.reconcileBackup(ctx, backup, byRedis, w.pods); err != nil {
			c.log.fault(subject, topicReconcile, errorText(err))
			continue
		}
		c.log.clear(subject, topicReconcile)
	}
	for _, scheduled := range w.scheduledBackups {
		subject := "scheduledbackup " + objectKey(scheduled.Metadata)
		if err := c.reconcileSchedule(ctx, scheduled); err != nil {
			c.log.fault(subject, topicReconcile, errorText(err))
			continue
		}
		c.log.clear(subject, topicReconcile)
	}
	for _, r := range w.redis {
		subject := "redis " + objectKey(r.Metadata)
		if err := c.applyRetention(ctx, r, w.backups); err != nil {
			c.log.fault(subject, "retention", errorText(err))
			continue
		}
		c.log.clear(subject, "retention")
	}
}

func (c *controller) reconcileBackup(ctx context.Context, backup Backup, byRedis map[string]Redis, pods map[string][]Pod) error {
	meta := backup.Metadata
	if meta.DeletionTimestamp != "" {
		return c.finishBackupDelete(ctx, backup, byRedis)
	}
	if !slices.Contains(meta.Finalizers, backupFinalizer) {
		return c.client.setFinalizers(ctx, backupsResource, meta.Namespace, meta.Name, meta.ResourceVersion, append(slices.Clone(meta.Finalizers), backupFinalizer), nil)
	}
	r, ok := byRedis[meta.Namespace+"/"+backup.Spec.Redis]
	switch backup.Status.Phase {
	case "":
		if !ok {
			return c.failBackup(ctx, backup, fmt.Sprintf("there is no Redis named %s in %s", backup.Spec.Redis, meta.Namespace))
		}
		if r.Spec.Backup == nil {
			return c.failBackup(ctx, backup, fmt.Sprintf("redis %s names no backup store in spec.backup", backup.Spec.Redis))
		}
		return c.startBackup(ctx, backup, r, pods[meta.Namespace+"/"+backup.Spec.Redis])
	case backupRunning:
		started, _ := time.Parse(time.RFC3339, backup.Status.StartedAt)
		if c.now().Sub(started) > backupDeadline {
			return c.failBackup(ctx, backup, fmt.Sprintf("%s did not finish the backup within %s", backup.Status.Instance, backupDeadline))
		}
		if reason := instanceLost(backup, started, pods[meta.Namespace+"/"+backup.Spec.Redis]); reason != "" {
			return c.failBackup(ctx, backup, reason)
		}
	}
	return nil
}

// instanceLost says why the instance that takes a running Backup can no
// longer finish it, or returns "" while it can. The snapshot runs in
// the instance's pod, so it ends with that pod. A pod made again under
// the same name must not take it up either: it may not have its data
// back yet, and its snapshot would be missing writes.
func instanceLost(backup Backup, started time.Time, pods []Pod) string {
	instance := backup.Status.Instance
	for _, pod := range pods {
		if pod.Metadata.Name != instance {
			continue
		}
		created, err := time.Parse(time.RFC3339, pod.Metadata.CreationTimestamp)
		if err == nil && created.After(started) {
			return fmt.Sprintf("the pod of %s, which took the backup, was made again after the backup started", instance)
		}
		return ""
	}
	return fmt.Sprintf("the pod of %s, which took the backup, is gone", instance)
}

// startBackup chooses the instance and the key, and marks the Backup
// running. A replica takes the snapshot, so the fork is not the
// primary's, unless no replica follows the primary.
func (c *controller) startBackup(ctx context.Context, backup Backup, r Redis, pods []Pod) error {
	st := c.state(&r)
	// Adding a volume to spec.backup rolls the pods, and a pod that still
	// has the old template has no mount to write the snapshot to. Such
	// a pod cannot take the Backup, so the choice waits for one that can.
	eligible := map[string]bool{}
	for _, pod := range pods {
		if r.Spec.Backup.Volume == nil || mountsBackupVolume(pod, *r.Spec.Backup.Volume) {
			eligible[pod.Metadata.Name] = true
		}
	}
	instance := ""
	var bestOffset int64 = -1
	// Ties go to the lowest ordinal, so the choice does not hang on the
	// order a map lists the instances in.
	names := slices.SortedFunc(maps.Keys(st.reports), func(a, b string) int { return ordinal(a) - ordinal(b) })
	for _, name := range names {
		report := st.reports[name]
		if _, fresh := st.fresh(name, c.now(), 2*c.timings.backstop); !fresh || report.Stopping || !eligible[name] {
			continue
		}
		if report.Role == reportReplica && report.LinkUp && report.Offset > bestOffset {
			instance, bestOffset = name, report.Offset
		}
	}
	if instance == "" && r.Status.CurrentPrimary != "" {
		if _, fresh := st.fresh(r.Status.CurrentPrimary, c.now(), 2*c.timings.backstop); fresh && eligible[r.Status.CurrentPrimary] {
			instance = r.Status.CurrentPrimary
		}
	}
	if instance == "" {
		// No instance answers, or none has the store mounted yet. The next
		// pass tries again.
		return nil
	}
	store := *r.Spec.Backup
	prefix := ""
	if store.Volume == nil {
		_, destinationPrefix, err := parseDestination(store.Destination)
		if err != nil {
			return c.failBackup(ctx, backup, err.Error())
		}
		prefix = destinationPrefix
	}
	stamp := c.now().UTC().Format("20060102T150405Z")
	parts := []string{backup.Metadata.Namespace, r.Metadata.Name, stamp + "-" + backup.Metadata.Name + ".rdb"}
	if prefix != "" {
		parts = append([]string{prefix}, parts...)
	}
	c.log.printf("backup %s: %s takes the snapshot", objectKey(backup.Metadata), instance)
	key := strings.Join(parts, "/")
	err := c.client.mergePatchStatus(ctx, backupsResource, backup.Metadata.Namespace, backup.Metadata.Name, map[string]any{
		"metadata": map[string]any{"resourceVersion": backup.Metadata.ResourceVersion},
		"status": BackupStatus{
			Phase: backupRunning, Instance: instance, Store: &store,
			Key: key, StartedAt: c.now().UTC().Format(time.RFC3339),
		},
	}, nil)
	if err == nil {
		c.readings.wroteStatus(backupsResource)
		c.events.normal(referenceTo(backupKind, backup.Metadata), "Started", fmt.Sprintf("%s takes the snapshot, to %s", instance, key))
	}
	return err
}

// mountsBackupVolume says whether the pod mounts v where the instance
// manager writes backups.
func mountsBackupVolume(pod Pod, v BackupVolume) bool {
	mounted := slices.ContainsFunc(pod.Spec.Containers, func(container PodContainer) bool {
		return slices.Contains(container.VolumeMounts, VolumeMount{Name: "backups", MountPath: backupMount})
	})
	return mounted && slices.ContainsFunc(pod.Spec.Volumes, func(volume PodVolume) bool {
		if volume.Name != "backups" {
			return false
		}
		if v.NFS != nil {
			return volume.NFS != nil && *volume.NFS == *v.NFS
		}
		return volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == v.ClaimName
	})
}

func (c *controller) failBackup(ctx context.Context, backup Backup, reason string) error {
	c.log.printf("backup %s: failed: %s", objectKey(backup.Metadata), reason)
	err := c.client.mergePatchStatus(ctx, backupsResource, backup.Metadata.Namespace, backup.Metadata.Name, map[string]any{
		"status": map[string]any{"phase": backupFailed, "error": reason, "completedAt": c.now().UTC().Format(time.RFC3339)},
	}, nil)
	if err == nil {
		c.readings.wroteStatus(backupsResource)
		// The pass that reads this phase back posts nothing more.
		c.backupPhases[backup.Metadata.UID] = backupFailed
		c.events.warning(referenceTo(backupKind, backup.Metadata), "Failed", reason)
	}
	return err
}

// finishBackupDelete deletes a deleted Backup's object, then lets the
// Backup go. A store that refuses the delete keeps the Backup, so no
// object is left behind unseen.
func (c *controller) finishBackupDelete(ctx context.Context, backup Backup, byRedis map[string]Redis) error {
	meta := backup.Metadata
	if !slices.Contains(meta.Finalizers, backupFinalizer) {
		return nil
	}
	if backup.Status.Store != nil && backup.Status.Store.Volume != nil && backup.Status.Key != "" {
		// The operator does not mount the volume. The primary's instance
		// manager deletes the file and the finalizer, unless the Redis is
		// gone, and then nothing can.
		if _, ok := byRedis[meta.Namespace+"/"+backup.Spec.Redis]; ok {
			return nil
		}
		c.log.printf("backup %s: redis %s is gone, so %s stays on its volume", objectKey(meta), backup.Spec.Redis, backup.Status.Key)
	} else if backup.Status.Store != nil && backup.Status.Key != "" {
		store, err := storeFor(ctx, c.client, meta.Namespace, *backup.Status.Store)
		if err != nil {
			return err
		}
		if err := store.delete(ctx, store.relativeKey(backup.Status.Key)); err != nil {
			return fmt.Errorf("deleting %s from the store: %w", backup.Status.Key, err)
		}
		c.log.printf("backup %s: deleted %s from the store", objectKey(meta), backup.Status.Key)
	}
	kept := slices.DeleteFunc(slices.Clone(meta.Finalizers), func(f string) bool { return f == backupFinalizer })
	err := c.client.setFinalizers(ctx, backupsResource, meta.Namespace, meta.Name, meta.ResourceVersion, kept, nil)
	if errors.Is(err, errNotFound) {
		return nil
	}
	return err
}

// reconcileSchedule makes a Backup when the schedule's next time after
// the last one has come. A schedule that missed several times while the
// operator was down makes one Backup, not one for each.
func (c *controller) reconcileSchedule(ctx context.Context, scheduled ScheduledBackup) error {
	schedule, err := parseCron(scheduled.Spec.Schedule)
	if err != nil {
		return err
	}
	last, err := time.Parse(time.RFC3339, scheduled.Status.LastScheduleTime)
	if err != nil {
		last, _ = time.Parse(time.RFC3339, scheduled.Metadata.CreationTimestamp)
	}
	if last.IsZero() {
		last = c.now()
	}
	due := schedule.next(last)
	if due.IsZero() || c.now().Before(due) {
		return nil
	}
	name := scheduled.Metadata.Name + "-" + strconv.FormatInt(c.now().Unix(), 10)
	err = c.client.create(ctx, backupsResource, scheduled.Metadata.Namespace, Backup{
		APIVersion: apiVersion, Kind: backupKind,
		Metadata: ObjectMeta{
			Name: name, Namespace: scheduled.Metadata.Namespace,
			Labels: map[string]string{apiGroup + "/scheduled-backup": scheduled.Metadata.Name},
		},
		Spec: BackupSpec{Redis: scheduled.Spec.Redis},
	}, nil)
	if err != nil && !errors.Is(err, errConflict) {
		return fmt.Errorf("creating backup %s: %w", name, err)
	}
	c.log.printf("scheduledbackup %s: created backup %s", objectKey(scheduled.Metadata), name)
	c.events.normal(referenceTo(scheduledBackupKind, scheduled.Metadata), "BackupCreated", fmt.Sprintf("created the Backup %s on the schedule", name))
	err = c.client.mergePatchStatus(ctx, scheduledBackupsResource, scheduled.Metadata.Namespace, scheduled.Metadata.Name, map[string]any{
		"status": ScheduledBackupStatus{LastScheduleTime: c.now().UTC().Format(time.RFC3339), LastBackup: name},
	}, nil)
	if err == nil {
		c.readings.wroteStatus(scheduledBackupsResource)
	}
	return err
}

// applyRetention deletes each completed Backup of r older than its
// retention, except the newest completed one.
func (c *controller) applyRetention(ctx context.Context, r Redis, backups []Backup) error {
	if r.Spec.Backup == nil || r.Spec.Backup.Retention == "" {
		return nil
	}
	keep, err := parseRetention(r.Spec.Backup.Retention)
	if err != nil {
		return err
	}
	var completed []Backup
	for _, backup := range backups {
		if backup.Metadata.Namespace == r.Metadata.Namespace && backup.Spec.Redis == r.Metadata.Name &&
			backup.Status.Phase == backupCompleted && backup.Metadata.DeletionTimestamp == "" {
			completed = append(completed, backup)
		}
	}
	slices.SortFunc(completed, func(a, b Backup) int { return strings.Compare(b.Status.CompletedAt, a.Status.CompletedAt) })
	for i, backup := range completed {
		finished, err := time.Parse(time.RFC3339, backup.Status.CompletedAt)
		if i == 0 || err != nil || c.now().Sub(finished) <= keep {
			continue
		}
		c.log.printf("redis %s: deleting backup %s, older than the retention of %s", objectKey(r.Metadata), backup.Metadata.Name, r.Spec.Backup.Retention)
		err = c.client.delete(ctx, backupsResource, backup.Metadata.Namespace, backup.Metadata.Name, backup.Metadata.UID, "")
		if err != nil && !errors.Is(err, errNotFound) {
			return err
		}
		c.events.normal(referenceTo(redisKind, r.Metadata), "BackupDeleted", fmt.Sprintf("deleted the Backup %s, older than the retention of %s", backup.Metadata.Name, r.Spec.Backup.Retention))
	}
	return nil
}

// parseRetention reads a duration in days, such as 7d, or any Go
// duration, such as 36h.
func parseRetention(text string) (time.Duration, error) {
	if days, found := strings.CutSuffix(text, "d"); found {
		n, err := strconv.Atoi(days)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("retention %q is not a whole number of days", text)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(text)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("retention %q is not a duration such as 7d or 36h", text)
	}
	return d, nil
}
