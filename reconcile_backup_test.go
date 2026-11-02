package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// withBackupStore names a store in the Redis's spec.backup, with its
// credentials Secret. endpoint is a real store, or any URL when the
// test never reaches it.
func (h *harness) withBackupStore(endpoint, retention string) BackupStore {
	h.t.Helper()
	h.api.create(secretsResource, map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "store", "namespace": testNamespace},
		"data":     map[string]any{accessKeyIDKey: []byte(testAccessKey), secretAccessKeyKey: []byte(testSecretKey)},
	})
	store := BackupStore{Destination: "s3://" + testBucket + "/redis", Endpoint: endpoint, Credentials: SecretRef{Secret: "store"}, Retention: retention}
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["backup"] = toObject(h.t, store)
	})
	return store
}

func (h *harness) newBackup(name string) {
	h.api.create(backupsResource, Backup{
		APIVersion: apiVersion, Kind: backupKind,
		Metadata: ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:     BackupSpec{Redis: h.name},
	})
}

func (h *harness) backup(name string) Backup {
	var backup Backup
	h.api.read(backupsResource, testNamespace, name, &backup)
	return backup
}

func TestABackupStartsOnTheReplicaWithTheMostData(t *testing.T) {
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.withBackupStore("https://s3.example.com", "")
	h.setReport(instanceReport{Name: "cache-2", Role: reportReplica, Offset: 150, LinkUp: true, Following: h.ip("cache-0"), HasData: true, Ready: true})
	h.newBackup("nightly")

	h.pass()
	h.pass()

	status := h.backup("nightly").Status
	mustMatch(t, status.Phase, backupRunning)
	mustMatch(t, status.Instance, "cache-2")
	mustMatch(t, strings.HasPrefix(status.Key, "redis/example/cache/20261101T120000Z-nightly"), true)
	mustMatch(t, status.Store.Destination, "s3://backups/redis")
	mustMatch(t, len(h.backup("nightly").Metadata.Finalizers), 1)
}

func TestABackupOfASingleInstanceRunsOnThePrimary(t *testing.T) {
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.withBackupStore("https://s3.example.com", "")
	h.newBackup("nightly")

	h.pass()
	h.pass()

	mustMatch(t, h.backup("nightly").Status.Instance, "cache-0")
}

func TestABackupOfARedisWithNoStoreFails(t *testing.T) {
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.newBackup("nightly")

	h.pass()
	h.pass()

	status := h.backup("nightly").Status
	mustMatch(t, status.Phase, backupFailed)
	mustMatch(t, status.Error, "redis cache names no backup store in spec.backup")
}

func TestABackupThatRunsTooLongFails(t *testing.T) {
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.withBackupStore("https://s3.example.com", "")
	h.newBackup("nightly")
	h.pass()
	h.pass()

	h.clock.advance(backupDeadline + time.Minute)
	h.pass()

	mustMatch(t, h.backup("nightly").Status.Phase, backupFailed)
}

// runningBackup starts a Backup on the replica cache-1 and returns once
// it runs there.
func (h *harness) runningBackup(name string) {
	h.t.Helper()
	h.steady("cache-0", 2)
	h.withBackupStore("https://s3.example.com", "")
	h.newBackup(name)
	h.pass()
	h.pass()
	mustMatch(h.t, h.backup(name).Status.Instance, "cache-1")
}

func TestABackupWhoseInstanceIsGoneFails(t *testing.T) {
	h := startHarness(t, 2)
	h.runningBackup("nightly")

	h.api.delete(podsResource, testNamespace, "cache-1")
	h.pass()

	status := h.backup("nightly").Status
	mustMatch(t, status.Phase, backupFailed)
	mustMatch(t, status.Error, "the pod of cache-1, which took the backup, is gone")
}

func TestABackupWhoseInstanceRestartedFails(t *testing.T) {
	h := startHarness(t, 2)
	h.runningBackup("nightly")

	h.api.delete(podsResource, testNamespace, "cache-1")
	h.clock.advance(time.Minute)
	h.recreate("cache-1", "", true, "")
	h.pass()

	status := h.backup("nightly").Status
	mustMatch(t, status.Phase, backupFailed)
	mustMatch(t, status.Error, "the pod of cache-1, which took the backup, was made again after the backup started")
}

func TestAScheduledBackupMakesABackupWhenItIsDue(t *testing.T) {
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.api.create(scheduledBackupsResource, ScheduledBackup{
		APIVersion: apiVersion, Kind: scheduledBackupKind,
		Metadata: ObjectMeta{Name: "hourly", Namespace: testNamespace},
		Spec:     ScheduledBackupSpec{Redis: h.name, Schedule: "0 * * * *"},
	})
	h.pass()
	mustMatch(t, len(h.api.versions(backupsResource, "")), 0)

	h.clock.advance(time.Hour)
	h.pass()

	mustMatch(t, len(h.api.versions(backupsResource, "")), 1)
	var scheduled ScheduledBackup
	h.api.read(scheduledBackupsResource, testNamespace, "hourly", &scheduled)
	mustMatch(t, scheduled.Status.LastBackup != "", true)
}

// completedBackup writes a Backup that finished at completedAt.
func (h *harness) completedBackup(name string, completedAt time.Time, store BackupStore) {
	h.api.create(backupsResource, Backup{
		APIVersion: apiVersion, Kind: backupKind,
		Metadata: ObjectMeta{Name: name, Namespace: testNamespace, Finalizers: []string{backupFinalizer}},
		Spec:     BackupSpec{Redis: h.name},
	})
	h.api.update(backupsResource, testNamespace, name, func(object map[string]any) {
		object["status"] = toObject(h.t, BackupStatus{
			Phase: backupCompleted, Store: &store, Key: "redis/example/cache/" + name + ".rdb",
			CompletedAt: completedAt.UTC().Format(time.RFC3339),
		})
	})
}

func TestRetentionDeletesOldBackupsButKeepsTheNewest(t *testing.T) {
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	endpoint := startTestStore(t)
	store := h.withBackupStore(endpoint, "1d")
	now := h.clock.now()
	h.completedBackup("old", now.Add(-72*time.Hour), store)
	h.completedBackup("older", now.Add(-96*time.Hour), store)
	h.completedBackup("recent", now.Add(-time.Hour), store)

	h.pass()
	h.pass()

	mustMatch(t, len(h.api.versions(backupsResource, "")), 1)
	mustMatch(t, h.backup("recent").Status.Phase, backupCompleted)
}

func TestRetentionKeepsTheNewestBackupHoweverOld(t *testing.T) {
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	store := h.withBackupStore("https://s3.example.com", "1d")
	h.completedBackup("only", h.clock.now().Add(-30*24*time.Hour), store)

	h.pass()

	mustMatch(t, h.backup("only").Status.Phase, backupCompleted)
}

func TestDeletingABackupDeletesItsObject(t *testing.T) {
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	endpoint := startTestStore(t)
	store := h.withBackupStore(endpoint, "")
	s3 := testStore(t, endpoint, store.Destination)
	mustSucceed(t, s3.put(context.Background(), "example/cache/gone.rdb", strings.NewReader("REDIS"), 5))
	h.completedBackup("gone", h.clock.now(), store)
	h.pass()

	h.api.delete(backupsResource, testNamespace, "gone")
	h.pass()

	mustMatch(t, len(h.api.versions(backupsResource, "")), 0)
	_, _, err := s3.get(context.Background(), "example/cache/gone.rdb")
	mustMatch(t, errors.Is(err, errNoSuchKey), true)
}
