package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// withStore names a SeaweedFS store in the Redis's spec.backup, with
// its credentials Secret, and returns the store.
func (h *managerHarness) withStore() (*s3Store, BackupStore) {
	h.t.Helper()
	endpoint := startTestStore(h.t)
	h.api.create(secretsResource, map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "store", "namespace": testNamespace},
		"data": map[string]any{
			accessKeyIDKey:     []byte(testAccessKey),
			secretAccessKeyKey: []byte(testSecretKey),
		},
	})
	store := BackupStore{Destination: "s3://" + testBucket + "/redis", Endpoint: endpoint, Credentials: SecretRef{Secret: "store"}}
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["backup"] = toObject(h.t, store)
	})
	return testStore(h.t, endpoint, store.Destination), store
}

// runBackup writes a Backup that the operator started on instance.
func (h *managerHarness) runBackup(name, instance string, store BackupStore) {
	h.api.create(backupsResource, Backup{
		APIVersion: apiVersion, Kind: backupKind,
		Metadata: ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:     BackupSpec{Redis: h.name},
	})
	h.api.update(backupsResource, testNamespace, name, func(object map[string]any) {
		object["status"] = toObject(h.t, BackupStatus{
			Phase: backupRunning, Instance: instance, Store: &store,
			Key: "redis/" + testNamespace + "/" + h.name + "/" + name + ".rdb", StartedAt: time.Now().UTC().Format(time.RFC3339),
		})
	})
}

func (h *managerHarness) backup(name string) Backup {
	var backup Backup
	h.api.read(backupsResource, testNamespace, name, &backup)
	return backup
}

func TestAReplicaUploadsTheSnapshotABackupNamesItFor(t *testing.T) {
	h := twoInstances(t, "redis")
	store, spec := h.withStore()

	h.runBackup("nightly", "cache-1", spec)

	h.waitFor("the backup completes", func() bool { return h.backup("nightly").Status.Phase == backupCompleted })
	status := h.backup("nightly").Status
	mustMatch(t, status.Size > 0, true)
	mustMatch(t, status.Offset > 0, true)
	body, size, err := store.get(context.Background(), store.relativeKey(status.Key))
	mustSucceed(t, err)
	defer body.Close()
	data, err := io.ReadAll(body)
	mustSucceed(t, err)
	mustMatch(t, size, status.Size)
	mustMatch(t, string(data[:5]), "REDIS")
}

func TestAReplicaThatStartsUnderABackupSnapshotsThePrimarysData(t *testing.T) {
	h := twoInstances(t, "redis")
	store, spec := h.withStore()
	_, err := h.client("cache-0").do(context.Background(), "SET", "kept", "by-the-backup")
	mustSucceed(t, err)
	h.stop("cache-1")
	h.runBackup("nightly", "cache-1", spec)

	h.start("cache-1", "")

	h.waitFor("the backup completes", func() bool { return h.backup("nightly").Status.Phase == backupCompleted })
	body, _, err := store.get(context.Background(), store.relativeKey(h.backup("nightly").Status.Key))
	mustSucceed(t, err)
	defer body.Close()
	data, err := io.ReadAll(body)
	mustSucceed(t, err)
	mustMatch(t, strings.Contains(string(data), "by-the-backup"), true)
}

func TestABackupThatCannotReachItsStoreFails(t *testing.T) {
	h := twoInstances(t, "redis")
	_, spec := h.withStore()
	spec.Endpoint = fmt.Sprintf("http://127.0.0.1:%d", freePort(t))

	h.runBackup("nightly", "cache-1", spec)

	h.waitFor("the backup fails", func() bool { return h.backup("nightly").Status.Phase == backupFailed })
	mustMatch(t, h.backup("nightly").Status.Error != "", true)
}

func TestANewRedisRestoresABackupOnItsFirstInstance(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprintf("persistent=%v", persistent), func(t *testing.T) {
			h := twoInstances(t, "redis")
			_, spec := h.withStore()
			for i := range 100 {
				_, err := h.client("cache-0").do(context.Background(), "SET", fmt.Sprintf("key-%d", i), "value")
				mustSucceed(t, err)
			}
			h.runBackup("nightly", "cache-1", spec)
			h.waitFor("the backup completes", func() bool { return h.backup("nightly").Status.Phase == backupCompleted })
			h.stop("cache-0")
			h.stop("cache-1")

			restored := startManagerHarnessOn(t, h.api, "restored", "redis", 1, func(spec *RedisSpec) {
				spec.Bootstrap = &Bootstrap{Recovery: &Recovery{Backup: "nightly"}}
				if persistent {
					spec.Storage = &StorageSpec{Size: "1Gi"}
				}
			})
			restored.setTarget("restored-0")
			restored.start("restored-0", "")

			restored.waitFor("restored-0 is a ready primary", func() bool { return restored.report("restored-0").Ready })
			got, err := restored.client("restored-0").do(context.Background(), "DBSIZE")
			mustSucceed(t, err)
			mustMatch(t, got, any(int64(101)))
			if persistent {
				restored.waitFor("the append-only file is on", func() bool {
					reply, err := restored.client("restored-0").do(context.Background(), "CONFIG", "GET", "appendonly")
					items, _ := reply.([]any)
					return err == nil && len(items) == 2 && items[1] == "yes"
				})
			}
		})
	}
}

func TestABackupToAVolumeWritesTheSnapshotThere(t *testing.T) {
	h := twoInstances(t, "redis")
	store := BackupStore{Volume: &BackupVolume{ClaimName: "backups"}}

	h.runBackup("nightly", "cache-1", store)

	h.waitFor("the backup completes", func() bool { return h.backup("nightly").Status.Phase == backupCompleted })
	data, err := os.ReadFile(filepath.Join(h.backupDir, h.backup("nightly").Status.Key))
	mustSucceed(t, err)
	mustMatch(t, int64(len(data)), h.backup("nightly").Status.Size)
	mustMatch(t, string(data[:5]), "REDIS")
}

func TestARedisRestoresABackupFromAVolume(t *testing.T) {
	h := twoInstances(t, "redis")
	store := BackupStore{Volume: &BackupVolume{ClaimName: "backups"}}
	h.runBackup("nightly", "cache-1", store)
	h.waitFor("the backup completes", func() bool { return h.backup("nightly").Status.Phase == backupCompleted })
	h.stop("cache-0")
	h.stop("cache-1")

	restored := startManagerHarnessOn(t, h.api, "restored", "redis", 1, func(spec *RedisSpec) {
		spec.Bootstrap = &Bootstrap{Recovery: &Recovery{Backup: "nightly"}}
	})
	// The restoring Redis mounts the Backup's volume where it restores
	// from.
	restored.restoreDir = h.backupDir
	restored.setTarget("restored-0")
	restored.start("restored-0", "")

	restored.waitFor("restored-0 is a ready primary", func() bool { return restored.report("restored-0").Ready })
	got, err := restored.client("restored-0").do(context.Background(), "GET", "key")
	mustSucceed(t, err)
	mustMatch(t, got, any("value"))
}

func TestThePrimaryDeletesADeletedVolumeBackupsFile(t *testing.T) {
	h := twoInstances(t, "redis")
	store := BackupStore{Volume: &BackupVolume{ClaimName: "backups"}}
	h.runBackup("nightly", "cache-1", store)
	h.waitFor("the backup completes", func() bool { return h.backup("nightly").Status.Phase == backupCompleted })
	path := filepath.Join(h.backupDir, h.backup("nightly").Status.Key)
	h.api.update(backupsResource, testNamespace, "nightly", func(object map[string]any) {
		metadataOf(object)["finalizers"] = []any{backupFinalizer}
	})

	h.api.delete(backupsResource, testNamespace, "nightly")

	h.waitFor("the Backup is gone", func() bool {
		var backup Backup
		return !h.api.read(backupsResource, testNamespace, "nightly", &backup)
	})
	_, err := os.Stat(path)
	mustMatch(t, os.IsNotExist(err), true)
}
