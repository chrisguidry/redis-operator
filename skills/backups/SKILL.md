---
name: backups
description: "Back up a Redis to an S3-compatible object store or a shared volume with spec.backup, take a Backup on demand, schedule them with a ScheduledBackup, set retention, and restore into a new Redis with spec.bootstrap.recovery. Use when a Redis needs backups, when a Backup fails or stays Running, or when restoring data."
---

Before the first command, run `kubectl config current-context` and confirm that it names the cluster the person means.

# Back up and restore

A `Redis` names its backup store in `spec.backup`. A `Backup` is one
snapshot, and a `ScheduledBackup` makes `Backup`s on a cron schedule.
A new `Redis` restores from a snapshot through `spec.bootstrap.recovery`.

CloudNativePG also defines a kind named `Backup`. On a cluster with
both operators, `kubectl get backups` may list the other kind. Name
the resource in full:

```sh
kubectl -n example get backups.redis.guid.foo
kubectl -n example get scheduledbackups.redis.guid.foo
```

## Choose a store

A store is either an object store or a volume, never both.

### An S3-compatible object store

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cache-backup
  namespace: example
stringData:
  ACCESS_KEY_ID: <access key>
  SECRET_ACCESS_KEY: <secret key>
---
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache
  namespace: example
spec:
  instances: 3
  storage: {size: 4Gi}
  backup:
    destination: s3://redis-backups/prod   # bucket, then an optional prefix
    endpoint: https://s3.example.com       # optional
    region: us-east-1                      # optional, default us-east-1
    credentials:
      secret: cache-backup
    retention: 7d                          # optional
```

- The Secret must be in the `Redis`'s namespace and hold the keys
  `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY`.
- Without `endpoint`, the store is AWS S3 at
  `https://s3.<region>.amazonaws.com`. The client uses path-style URLs.
- An endpoint host of one label, such as `http://minio:9000`, names a
  Service in the `Redis`'s namespace.
- Objects land at `<prefix>/<namespace>/<redis>/<time>-<backup>.rdb`.

The instance managers read the credentials Secret by name, through
the Role the operator makes for the `Redis`, and upload the snapshot
themselves. The operator reads the Secret only to delete an object.

### A volume

```yaml
spec:
  backup:
    volume:
      claimName: redis-backups   # a ReadWriteMany claim in the Redis's namespace
    retention: 14d
```

or an NFS export:

```yaml
spec:
  backup:
    volume:
      nfs: {server: nas.example.com, path: /export/redis}
```

Every instance mounts the volume at `/backups`, so the claim must be
ReadWriteMany. The servers run as uid and gid 999, and an NFS export
must let that uid write. A snapshot lands at
`<namespace>/<redis>/<time>-<backup>.rdb` in the volume, written under
a temporary name and renamed when complete. Adding or changing a
volume store changes the pod template, so it rolls every pod.

## Take a backup

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: Backup
metadata:
  name: cache-before-upgrade
  namespace: example
spec:
  redis: cache
```

```sh
kubectl -n example get backups.redis.guid.foo cache-before-upgrade -w
```

A `Backup`'s `spec` cannot change. Its status:

| Field | Meaning |
|---|---|
| `phase` | empty until the operator picks an instance, then `Running`, `Completed`, or `Failed` |
| `instance` | the instance that took the snapshot |
| `store` | a copy of the `Redis`'s `spec.backup` at the start |
| `key` | where the snapshot is, in the store or the volume |
| `size` | bytes |
| `offset` | the instance's replication offset at the snapshot |
| `startedAt`, `completedAt` | times |
| `error` | why it failed |

The operator picks the replica with its replication link up and the
highest offset, so the fork happens on a replica, not the primary. With
no such replica, the primary takes it. The snapshot holds that
replica's data, which can lag the primary by the replication delay.
The instance runs `BGSAVE`, waits for it to finish, and uploads
`dump.rdb` from its data directory.

A `Backup` fails at once when its `Redis` does not exist or has no
`spec.backup`. A `Backup` with an empty phase waits while no instance
answers the operator. For a volume store it also waits until a pod
mounts the volume, so a `Backup` made while the roll after adding
`spec.backup.volume` runs starts once a replaced pod is ready.

## Schedule backups

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: ScheduledBackup
metadata:
  name: cache-nightly
  namespace: example
spec:
  redis: cache
  schedule: "0 3 * * *"   # five fields, in UTC
```

When the schedule's next time after `status.lastScheduleTime` comes,
the operator creates a `Backup` named `<ScheduledBackup>-<Unix time>`
with the label `redis.guid.foo/scheduled-backup: <ScheduledBackup>`.
For a new `ScheduledBackup` the first time counts from its creation, so
it takes no backup at once. Times missed while the operator was down
make one `Backup`, not one for each.

```sh
kubectl -n example get backups.redis.guid.foo -l redis.guid.foo/scheduled-backup=cache-nightly
```

## Retention

`spec.backup.retention` is a number of days, such as `7d`, or a Go
duration, such as `36h`. The operator deletes each `Completed` `Backup`
of the `Redis` that finished longer ago than that, and its snapshot,
but always keeps the newest `Completed` one. Retention never deletes a
`Failed` `Backup`; delete those by hand.

Deleting a `Backup` deletes its snapshot too, through the finalizer
`redis.guid.foo/delete-object`:

- For an object store, the operator deletes the object. If the store
  refuses, the `Backup` stays `Terminating` and the operator logs the
  error.
- For a volume, the primary's instance manager deletes the file. If
  the `Redis` no longer exists, nothing can, and the file stays on the
  volume.

## Restore into a new Redis

A restore bootstraps a new `Redis`. It never replaces the data of a
`Redis` that already ran.

From a `Completed` `Backup` in the same namespace:

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache-restored
  namespace: example
spec:
  instances: 3
  storage: {size: 4Gi}
  bootstrap:
    recovery:
      backup: cache-before-upgrade
```

From a key in a store, when the `Backup` object is gone, name the store
in the new `Redis`'s own `spec.backup` and the key as a `Backup`'s
`status.key` gave it:

```yaml
spec:
  backup:
    destination: s3://redis-backups/prod
    endpoint: https://s3.example.com
    credentials: {secret: cache-backup}
  bootstrap:
    recovery:
      key: prod/example/cache/20261102T030000Z-cache-nightly-1793588400.rdb
```

`recovery` names either `backup` or `key`, not both, and a `key` needs
`spec.backup`.

Instance 0 downloads the snapshot into its data directory before its
server starts, then takes the primary Lease, and the other instances
replicate from it. It downloads only while `status.currentPrimary` is
empty and its data directory holds no `dump.rdb` or `appendonlydir`,
so a restart never replaces newer data. A restore from a volume mounts
the volume at `/restore`, read-only, while `spec.bootstrap.recovery`
names it; leave `bootstrap` in the spec, because removing it changes
the pod template and rolls every pod.

The snapshot's RDB format must be one the new `Redis`'s engine and
version can read.

### A restore that fails

When the download fails, the instance manager of `cache-restored-0`
exits with the reason in its log, the pod restarts, and the `Redis`
stays not ready. It never starts empty in place of the restore. No
condition reports the restore; read the log:

```sh
kubectl -n example logs cache-restored-0 -c redis --previous
```

Common reasons: the `Backup` is not `Completed`, the credentials
Secret lacks a key, or the key does not exist in the store.

## A Backup that stays Running

- The operator marks a `Backup` `Failed` when it has run for an hour,
  and the instance manager gives up after 50 minutes.
- The operator marks it `Failed` at once when the instance's pod is
  gone, or was made again after the backup started, because a new pod
  may not have its data back yet.
- An instance takes a snapshot only once it holds data, so a replica
  that has not finished its first sync does not start one.

Read the instance's log for the upload:

```sh
kubectl -n example get backups.redis.guid.foo <name> -o jsonpath='{.status.instance}{"\n"}'
kubectl -n example logs <instance> -c redis
```

The Redis pods must reach the S3 endpoint; an egress NetworkPolicy
that blocks it makes every upload fail.
