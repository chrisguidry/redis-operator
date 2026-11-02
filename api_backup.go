package main

// The backup resources. A Redis names its object store in
// spec.backup, a Backup is one snapshot of it, and a ScheduledBackup
// makes Backups on a cron schedule. deploy/backups-crd.yaml and
// deploy/scheduledbackups-crd.yaml are their schemas.

var (
	backupsResource          = kubeResource{apiGroup, "v1alpha1", "backups"}
	scheduledBackupsResource = kubeResource{apiGroup, "v1alpha1", "scheduledbackups"}
)

const (
	backupKind          = "Backup"
	scheduledBackupKind = "ScheduledBackup"
)

// BackupStore is where a Redis keeps its snapshots: an S3-compatible
// object store, or a volume every instance mounts.
type BackupStore struct {
	// Destination is s3://bucket/prefix. Objects land under
	// <prefix>/<namespace>/<redis>/.
	Destination string `json:"destination,omitempty"`
	Endpoint    string `json:"endpoint,omitempty"`
	Region      string `json:"region,omitempty"`
	// Credentials names a Secret in the Redis's namespace with the keys
	// ACCESS_KEY_ID and SECRET_ACCESS_KEY.
	Credentials SecretRef `json:"credentials,omitempty"`
	// Volume is a volume that every instance mounts, in place of an
	// object store. Files land under <namespace>/<redis>/ in it.
	Volume *BackupVolume `json:"volume,omitempty"`
	// Retention is how long a completed Backup is kept, such as 7d or
	// 48h. The newest completed Backup is always kept.
	Retention string `json:"retention,omitempty"`
}

type SecretRef struct {
	Secret string `json:"secret,omitempty"`
}

// BackupVolume is a PersistentVolumeClaim that every instance can mount
// at once, so it must be ReadWriteMany, or an NFS export.
type BackupVolume struct {
	ClaimName string     `json:"claimName,omitempty"`
	NFS       *NFSVolume `json:"nfs,omitempty"`
}

type NFSVolume struct {
	Server string `json:"server"`
	Path   string `json:"path"`
}

// podVolume is the pod spec's volume source for v.
func (v BackupVolume) podVolume(name string) map[string]any {
	if v.NFS != nil {
		return map[string]any{"name": name, "nfs": map[string]any{"server": v.NFS.Server, "path": v.NFS.Path}}
	}
	return map[string]any{"name": name, "persistentVolumeClaim": map[string]any{"claimName": v.ClaimName}}
}

// Where an instance mounts the volume its backups go to, and the volume
// a new Redis restores from.
const (
	backupMount  = "/backups"
	restoreMount = "/restore"
)

// The keys of the credentials Secret.
const (
	accessKeyIDKey     = "ACCESS_KEY_ID"
	secretAccessKeyKey = "SECRET_ACCESS_KEY"
)

// Bootstrap says how a new Redis's first primary gets its data.
type Bootstrap struct {
	Recovery *Recovery `json:"recovery,omitempty"`
}

// Recovery restores a snapshot: the one a Backup in the same namespace
// took, or the object at key in the store that spec.backup names.
type Recovery struct {
	Backup string `json:"backup,omitempty"`
	Key    string `json:"key,omitempty"`
}

type Backup struct {
	APIVersion string       `json:"apiVersion,omitempty"`
	Kind       string       `json:"kind,omitempty"`
	Metadata   ObjectMeta   `json:"metadata"`
	Spec       BackupSpec   `json:"spec"`
	Status     BackupStatus `json:"status,omitempty"`
}

type BackupSpec struct {
	// Redis is the name of the Redis in the same namespace.
	Redis string `json:"redis"`
}

// The phases of a Backup.
const (
	backupRunning   = "Running"
	backupCompleted = "Completed"
	backupFailed    = "Failed"
)

type BackupStatus struct {
	Phase    string `json:"phase,omitempty"`
	Instance string `json:"instance,omitempty"`
	// Store and Key say where the snapshot is, so a Redis restores from
	// it even after its own spec.backup changes.
	Store       *BackupStore `json:"store,omitempty"`
	Key         string       `json:"key,omitempty"`
	Size        int64        `json:"size,omitempty"`
	Offset      int64        `json:"offset,omitempty"`
	StartedAt   string       `json:"startedAt,omitempty"`
	CompletedAt string       `json:"completedAt,omitempty"`
	Error       string       `json:"error,omitempty"`
}

type ScheduledBackup struct {
	APIVersion string                `json:"apiVersion,omitempty"`
	Kind       string                `json:"kind,omitempty"`
	Metadata   ObjectMeta            `json:"metadata"`
	Spec       ScheduledBackupSpec   `json:"spec"`
	Status     ScheduledBackupStatus `json:"status,omitempty"`
}

type ScheduledBackupSpec struct {
	Redis string `json:"redis"`
	// Schedule is a five-field cron schedule, in UTC.
	Schedule string `json:"schedule"`
}

type ScheduledBackupStatus struct {
	LastScheduleTime string `json:"lastScheduleTime,omitempty"`
	LastBackup       string `json:"lastBackup,omitempty"`
}

// backupFinalizer deletes a Backup's object from the store before the
// Backup goes.
const backupFinalizer = apiGroup + "/delete-object"
