package main

// The instance manager's part of backups: it takes the snapshot that a
// running Backup names it for and uploads it, and the first instance of
// a new Redis downloads the snapshot it restores from before its server
// starts. The operator chooses the instance and the key; the data and
// the store's credentials never pass through the operator.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// snapshotFile is the server's default snapshot name, in its data
// directory.
const snapshotFile = "dump.rdb"

// backupTimeout bounds one snapshot and its upload.
const backupTimeout = 50 * time.Minute

// targetFor is where store keeps its snapshots: the directory a volume
// store is mounted at, or the object store a destination names.
func targetFor(ctx context.Context, client *kubeClient, namespace string, store BackupStore, mount string) (backupTarget, error) {
	if store.Volume != nil {
		return dirStore{root: mount}, nil
	}
	return storeFor(ctx, client, namespace, store)
}

// storeFor builds a client for store, with the keys from its
// credentials Secret.
func storeFor(ctx context.Context, client *kubeClient, namespace string, store BackupStore) (*s3Store, error) {
	var secret struct {
		Data map[string][]byte `json:"data"`
	}
	if err := client.get(ctx, secretsResource, namespace, store.Credentials.Secret, &secret); err != nil {
		return nil, fmt.Errorf("reading the backup credentials %s: %w", store.Credentials.Secret, err)
	}
	access, secretKey := string(secret.Data[accessKeyIDKey]), string(secret.Data[secretAccessKeyKey])
	if access == "" || secretKey == "" {
		return nil, fmt.Errorf("secret %s needs the keys %s and %s", store.Credentials.Secret, accessKeyIDKey, secretAccessKeyKey)
	}
	return newS3Store(store.Endpoint, store.Region, store.Destination, access, secretKey, namespace)
}

// objectKey is where a snapshot lands in the store: under the
// destination's prefix, the namespace, and the Redis.
func (s *s3Store) objectKey(namespace, redis, name string) string {
	parts := []string{namespace, redis, name}
	if s.prefix != "" {
		parts = append([]string{s.prefix}, parts...)
	}
	return strings.Join(parts, "/")
}

// relativeKey strips the destination's prefix from a key the status
// states in full, because the client adds the prefix itself.
func (s *s3Store) relativeKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return strings.TrimPrefix(key, s.prefix+"/")
}

// startBackups starts the snapshot of each running Backup that names
// this instance, one at a time, in the background, so a long upload
// never holds up the role in the step loop.
func (m *manager) startBackups(ctx context.Context) {
	if m.backupBusy.Load() {
		return
	}
	var list struct {
		Items []Backup `json:"items"`
	}
	if err := m.client.list(ctx, backupsResource, m.config.namespace, "", &list); err != nil {
		return
	}
	if m.lease.held() {
		m.deleteVolumeBackups(ctx, list.Items)
	}
	for _, backup := range list.Items {
		if backup.Spec.Redis != m.config.redisName || backup.Status.Phase != backupRunning ||
			backup.Status.Instance != m.config.podName || backup.Status.Store == nil || backup.Metadata.DeletionTimestamp != "" {
			continue
		}
		m.backupBusy.Store(true)
		go func() {
			defer m.backupBusy.Store(false)
			m.runBackup(backup)
		}()
		return
	}
}

// runBackup takes one snapshot, uploads it, and records the outcome on
// the Backup.
func (m *manager) runBackup(backup Backup) {
	ctx, cancel := context.WithTimeout(context.Background(), backupTimeout)
	defer cancel()
	m.say(fmt.Sprintf("%s: taking backup %s", m.config.podName, backup.Metadata.Name))
	size, offset, err := m.snapshotAndUpload(ctx, backup)
	status := map[string]any{"completedAt": time.Now().UTC().Format(time.RFC3339)}
	if err != nil {
		status["phase"], status["error"] = backupFailed, errorText(err)
		m.say(fmt.Sprintf("%s: backup %s failed: %s", m.config.podName, backup.Metadata.Name, errorText(err)))
	} else {
		status["phase"], status["size"], status["offset"] = backupCompleted, size, offset
		m.say(fmt.Sprintf("%s: backup %s completed, %d bytes at offset %d", m.config.podName, backup.Metadata.Name, size, offset))
	}
	if err := m.client.mergePatchStatus(ctx, backupsResource, m.config.namespace, backup.Metadata.Name, map[string]any{"status": status}, nil); err != nil {
		m.say(fmt.Sprintf("%s: cannot record backup %s: %s", m.config.podName, backup.Metadata.Name, errorText(err)))
	}
}

func (m *manager) snapshotAndUpload(ctx context.Context, backup Backup) (int64, int64, error) {
	store, err := targetFor(ctx, m.client, m.config.namespace, *backup.Status.Store, m.config.backupDir)
	if err != nil {
		return 0, 0, err
	}
	conn := m.server.another()
	defer conn.close()
	info, err := conn.replication(ctx)
	if err != nil {
		return 0, 0, err
	}
	if err := snapshot(ctx, conn); err != nil {
		return 0, 0, err
	}
	path := filepath.Join(m.config.dataDir, snapshotFile)
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	if err := store.put(ctx, store.relativeKey(backup.Status.Key), file, stat.Size()); err != nil {
		return 0, 0, fmt.Errorf("uploading the snapshot: %w", err)
	}
	return stat.Size(), info.offset, nil
}

// snapshot runs BGSAVE and waits until it finished: LASTSAVE moves past
// the time before the request, and the last background save succeeded.
func snapshot(ctx context.Context, conn *server) error {
	before, err := lastSave(ctx, conn)
	if err != nil {
		return err
	}
	if _, err := conn.do(ctx, "BGSAVE"); err != nil {
		// A save the server started on its own is as good as one asked
		// for, and the wait below sees it finish.
		if reply, ok := errors.AsType[replyError](err); !ok || !strings.Contains(string(reply), "in progress") {
			return fmt.Errorf("BGSAVE: %w", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
		after, err := lastSave(ctx, conn)
		if err != nil {
			return err
		}
		if after <= before {
			continue
		}
		reply, err := conn.do(ctx, "INFO", "persistence")
		if err != nil {
			return err
		}
		text, _ := reply.(string)
		if !strings.Contains(text, "rdb_last_bgsave_status:ok") {
			return errors.New("the server's background save failed; its log says why")
		}
		return nil
	}
}

func lastSave(ctx context.Context, conn *server) (int64, error) {
	reply, err := conn.do(ctx, "LASTSAVE")
	if err != nil {
		return 0, err
	}
	value, ok := reply.(int64)
	if !ok {
		return 0, fmt.Errorf("LASTSAVE answered %T, not a time", reply)
	}
	return value, nil
}

// restoreIfNeeded downloads the snapshot a new Redis restores from into
// the data directory, before the server starts. Only the instance with
// ordinal 0 restores, only before any primary has run, and only when
// its volume holds no data, so a restart never replaces data that
// changed since the restore. It reports whether it restored.
func (m *manager) restoreIfNeeded(ctx context.Context, redis Redis) (bool, error) {
	recovery := redis.Spec.Bootstrap
	if recovery == nil || recovery.Recovery == nil || redis.Status.CurrentPrimary != "" ||
		ordinal(m.config.podName) != 0 || dataOnDisk(m.config.dataDir) {
		return false, nil
	}
	store, key, err := m.recoverySource(ctx, redis)
	if err != nil {
		return false, err
	}
	client, err := targetFor(ctx, m.client, m.config.namespace, store, m.config.restoreDir)
	if err != nil {
		return false, err
	}
	m.say(fmt.Sprintf("%s: restoring %s", m.config.podName, key))
	body, size, err := client.get(ctx, client.relativeKey(key))
	if err != nil {
		return false, fmt.Errorf("downloading %s: %w", key, err)
	}
	defer body.Close()
	target := filepath.Join(m.config.dataDir, snapshotFile)
	partial, err := os.CreateTemp(m.config.dataDir, snapshotFile+".*")
	if err != nil {
		return false, err
	}
	defer os.Remove(partial.Name())
	written, err := io.Copy(partial, body)
	if closeErr := partial.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, fmt.Errorf("downloading %s: %w", key, err)
	}
	if size >= 0 && written != size {
		return false, fmt.Errorf("downloading %s: got %d of %d bytes", key, written, size)
	}
	if err := os.Rename(partial.Name(), target); err != nil {
		return false, err
	}
	m.say(fmt.Sprintf("%s: restored %s bytes from %s", m.config.podName, strconv.FormatInt(written, 10), key))
	return true, nil
}

// recoverySource is the store and key a Redis restores from: a Backup's,
// or its own spec.backup's with an explicit key.
func (m *manager) recoverySource(ctx context.Context, redis Redis) (BackupStore, string, error) {
	recovery := redis.Spec.Bootstrap.Recovery
	if recovery.Backup != "" {
		var backup Backup
		if err := m.client.get(ctx, backupsResource, m.config.namespace, recovery.Backup, &backup); err != nil {
			return BackupStore{}, "", fmt.Errorf("reading backup %s: %w", recovery.Backup, err)
		}
		if backup.Status.Phase != backupCompleted || backup.Status.Store == nil {
			return BackupStore{}, "", fmt.Errorf("backup %s is %s, not Completed", recovery.Backup, backup.Status.Phase)
		}
		return *backup.Status.Store, backup.Status.Key, nil
	}
	if redis.Spec.Backup == nil || recovery.Key == "" {
		return BackupStore{}, "", errors.New("bootstrap.recovery names neither a backup nor a key in spec.backup's store")
	}
	return *redis.Spec.Backup, recovery.Key, nil
}

// deleteVolumeBackups deletes the file of each deleted Backup of this
// Redis whose store is a volume, then its finalizer. The operator does
// not mount the volume, so the primary's manager does this.
func (m *manager) deleteVolumeBackups(ctx context.Context, backups []Backup) {
	for _, backup := range backups {
		meta := backup.Metadata
		if backup.Spec.Redis != m.config.redisName || meta.DeletionTimestamp == "" || !slices.Contains(meta.Finalizers, backupFinalizer) ||
			backup.Status.Store == nil || backup.Status.Store.Volume == nil {
			continue
		}
		if backup.Status.Key != "" {
			if err := (dirStore{root: m.config.backupDir}).delete(ctx, backup.Status.Key); err != nil {
				m.say(fmt.Sprintf("%s: cannot delete %s from the backup volume: %s", m.config.podName, backup.Status.Key, errorText(err)))
				continue
			}
		}
		kept := slices.DeleteFunc(slices.Clone(meta.Finalizers), func(f string) bool { return f == backupFinalizer })
		if err := m.client.setFinalizers(ctx, backupsResource, meta.Namespace, meta.Name, meta.ResourceVersion, kept, nil); err == nil {
			m.say(fmt.Sprintf("%s: deleted %s from the backup volume", m.config.podName, backup.Status.Key))
		}
	}
}
