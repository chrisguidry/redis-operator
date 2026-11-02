# Milestone 2: backups

A `Redis` that names a backup store keeps snapshots of its data there,
on a schedule and on request, and a new `Redis` can start from one. The
design follows CloudNativePG's: the store is declared on the resource
that owns the data, a `Backup` is one snapshot, a `ScheduledBackup`
makes `Backup`s on a cron schedule, and a restore is a way to bootstrap
a new resource.

## The resources

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache
spec:
  instances: 3
  backup:
    destination: s3://bucket/prefix   # objects land under <prefix>/<namespace>/<name>/
    endpoint: https://s3.example.com  # optional; https://s3.<region>.amazonaws.com otherwise
    region: us-east-1                 # optional, default us-east-1
    credentials:
      secret: cache-backup            # keys ACCESS_KEY_ID and SECRET_ACCESS_KEY
    retention: 7d                     # optional; Nd or a Go duration such as 36h
---
apiVersion: redis.guid.foo/v1alpha1
kind: Backup
metadata:
  name: cache-manual
spec:
  redis: cache
status:
  phase: Completed          # empty until the operator picks an instance, then Running | Completed | Failed
  instance: cache-1         # the instance that took the snapshot
  store: {...}              # a copy of the Redis's spec.backup when the Backup started
  key: prefix/example/cache/20261102T030000Z-cache-manual.rdb
  size: 1048576
  offset: 123456            # the instance's replication offset at the snapshot
  startedAt: ...
  completedAt: ...
  error: ""
---
apiVersion: redis.guid.foo/v1alpha1
kind: ScheduledBackup
metadata:
  name: cache-nightly
spec:
  redis: cache
  schedule: "0 3 * * *"     # five-field cron, in UTC
status:
  lastScheduleTime: ...
  lastBackup: cache-nightly-1793588400   # <ScheduledBackup name>-<Unix time>
```

A `Backup`'s `spec` cannot change. Its `status.store` keeps the store as
it was when the `Backup` started, so a `Redis` restores from it, and the
operator deletes its object, even after `spec.backup` changes. The CRD
requires exactly one of `destination` and `volume` in a store, requires
`credentials` with `destination`, and requires `spec.backup` when
`bootstrap.recovery` names a `key`.

The store can instead be a volume that every instance mounts at once,
a ReadWriteMany claim or an NFS export:

```yaml
spec:
  backup:
    volume:
      claimName: redis-backups      # or nfs: {server: nas.example.com, path: /backups}
    retention: 14d
```

Each instance mounts it at `/backups`, and a snapshot lands at
`<namespace>/<name>/<time>-<backup>.rdb` in it. A manager writes the
file under a temporary name and renames it, so a snapshot that stops
halfway never looks whole. The operator does not mount the volume, so
the primary's instance manager deletes the file of a deleted `Backup`,
then its finalizer. If the `Redis` is gone, nothing can, and the file
stays on the volume. Adding the volume to `spec.backup` changes the pod
template, so it rolls the pods.

A new `Redis` restores with:

```yaml
spec:
  bootstrap:
    recovery:
      backup: cache-manual   # a Completed Backup in the same namespace
```

or, from a store when the `Backup` object is gone, with `key` and its
own `backup` block naming the store. A Redis that restores from a
volume mounts it at `/restore`, read-only, and keeps it mounted while
`spec.bootstrap.recovery` stays in the spec, because removing it would
change the pod template and roll every pod.

## How a backup runs

The operator chooses; the instance manager does the work. The snapshot
never passes through the operator. The operator reads the store's
credentials only to delete an object, and the instance managers read
them by name, through a Role that names the Secrets they may read and no others.

1. A `Backup` with no phase appears. The operator adds the finalizer
   `redis.guid.foo/delete-object`. It fails the `Backup` when its
   `Redis` does not exist or has no `spec.backup`. Otherwise it picks
   the instance: the replica that answered the last poll, is not
   stopping, has its link up, and has the highest offset, or the
   primary when there is no such replica. With no instance answering, it
   waits for the next pass. It writes `phase: Running`, the instance,
   the store, the object key, and `startedAt`.
2. That instance's manager sees a running `Backup` that names it, once
   it holds data, so a replica that has not synced never uploads an
   empty snapshot. It runs `BGSAVE`, waits until `LASTSAVE` moves past
   the time it asked, and uploads `dump.rdb` from its data directory with a SigV4-signed
   `PUT`, or writes the file to the volume. It writes `phase: Completed`
   with the size and the offset it read before the snapshot, or
   `phase: Failed` with the error. It gives up after 50 minutes.
3. A `Backup` that stays `Running` for an hour is marked `Failed` by
   the operator. So is one whose instance's pod is gone, or was made
   again after the backup started, because a pod made again may not have
   its data back yet and its snapshot would miss writes.

Taking the snapshot on a replica keeps the fork off the primary. The
snapshot is the replica's data at its offset, which can lag the
primary by the replication delay.

## Scheduling

On each pass the operator creates a `Backup` named
`<ScheduledBackup name>-<Unix time>`, labeled
`redis.guid.foo/scheduled-backup`, when the schedule's next time after
`status.lastScheduleTime` has come. For a new `ScheduledBackup` that
time counts from its creation. A schedule that missed several times
while the operator was down makes one `Backup`, not one for each.

## Retention

The operator deletes each `Completed` `Backup` older than
`spec.backup.retention`, and its object, but always keeps the newest
completed one, so a store that stopped accepting backups never empties
itself. Deleting a `Backup` by hand deletes its object too, through a
finalizer. For an object store the operator deletes the object, and a
store that refuses keeps the `Backup`.

## Restore

The instance with ordinal 0 of a new `Redis` that names
`bootstrap.recovery` downloads the object into its data directory before
its server starts. Redis then loads it like any snapshot. The instance
holds data, so it takes the primary Lease as the first primary, and the
other instances replicate from it. The download happens only once: when
`status.currentPrimary` is empty and the volume holds no `dump.rdb` or
`appendonlydir`, so a restart never replaces newer data. The server
starts with its append-only file off, loads the snapshot, and turns the
append-only file on. A `Redis` whose bootstrap fails does not start: the
instance manager exits with the reason in its log, the pod restarts, and
the `Redis` stays not ready. It never starts empty in place of the
restore. No condition reports the restore yet.

## The S3 client

A small SigV4 client in this repository: `PUT` an object of known size,
`GET` an object, and `DELETE` an object. It signs with
`UNSIGNED-PAYLOAD`, so an upload streams from disk without hashing the
file first, which every S3-compatible store accepts over HTTPS. It uses
path-style URLs, which every store accepts.

An endpoint host of one label, such as `store`, names a Service in the
`Redis`'s namespace, because a pod's resolver searches its own
namespace first. The operator runs in another namespace, so it
qualifies the host before it connects: `store` in the namespace `example`
becomes `store.example.svc`.

## Tests

- The SigV4 signer against the example in AWS's documentation.
- The S3 client against a real SeaweedFS S3 server, started the way the
  tests start Redis.
- The operator's choices, against the fake API server.
- The instance manager's snapshot and upload, and the restore, against
  real servers and SeaweedFS.
- End to end: back up the Redis under the writer's load, restore it
  into a new `Redis`, and check that the restored counter is at least
  the highest value confirmed before the backup started.
- End to end, with a volume store: the same backup and restore against
  a ReadWriteMany claim, which checks that the file exists on the
  volume and that deleting the `Backup` removes it. A `ScheduledBackup`
  every minute under a retention of 90 seconds then checks that
  retention deletes a scheduled `Backup` and its file. The k3s nodes
  have no NFS mount helper, so the claim binds a PersistentVolume on a
  host directory that every node bind-mounts; the `nfs` source has unit
  tests only.
