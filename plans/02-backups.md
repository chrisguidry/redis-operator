# Milestone 2: backups

A `Redis` that names an object store keeps snapshots of its data there,
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
    endpoint: https://s3.example.com  # optional; the AWS endpoint for the region otherwise
    region: us-east-1                 # optional, default us-east-1
    credentials:
      secret: cache-backup            # keys ACCESS_KEY_ID and SECRET_ACCESS_KEY
    retention: 7d                     # optional; older completed Backups are deleted
---
apiVersion: redis.guid.foo/v1alpha1
kind: Backup
metadata:
  name: cache-manual
spec:
  redis: cache
status:
  phase: Completed          # Pending | Running | Completed | Failed
  instance: cache-1         # the instance that took the snapshot
  key: prefix/example/cache/20261102T030000Z.rdb
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
```

A new `Redis` restores with:

```yaml
spec:
  bootstrap:
    recovery:
      backup: cache-manual   # a Completed Backup in the same namespace
```

or, from a store when the `Backup` object is gone, with `key` and its
own `backup` block naming the store.

## How a backup runs

The operator chooses; the instance manager does the work. The operator
never handles the data or the store's credentials on the data path.

1. A `Backup` with no phase appears. The operator picks the instance:
   the ready replica with its link up and the highest offset, or the
   primary when there is no such replica. It writes `phase: Running`,
   the instance, the object key, and `startedAt`.
2. That instance's manager sees a running `Backup` that names it. It
   runs `BGSAVE`, waits until `LASTSAVE` moves past the time it asked,
   and uploads `dump.rdb` from its data directory with a SigV4-signed
   `PUT`. It writes `phase: Completed` with the size and the offset it
   read before the snapshot, or `phase: Failed` with the error.
3. A `Backup` that stays `Running` for an hour, or whose instance is
   gone, is marked `Failed` by the operator.

Taking the snapshot on a replica keeps the fork off the primary. The
snapshot is the replica's data at its offset, which can lag the
primary by the replication delay.

## Retention

The operator deletes each `Completed` `Backup` older than
`spec.backup.retention`, and its object, but always keeps the newest
completed one, so a store that stopped accepting backups never empties
itself. Deleting a `Backup` by hand deletes its object too, through a
finalizer.

## Restore

The instance with ordinal 0 of a new `Redis` that names
`bootstrap.recovery` downloads the object into its data directory before
its server starts. Redis then loads it like any snapshot. The instance
holds data, so it takes the primary Lease as the first primary, and the
other instances replicate from it. The condition `Restored` reports the
key and its size. A `Redis` whose bootstrap fails stays not ready, with
the reason, and never starts empty in place of the restore.

## The S3 client

A small SigV4 client in this repository: `PUT` an object of known size,
`GET` an object, and `DELETE` an object. It signs with
`UNSIGNED-PAYLOAD`, so an upload streams from disk without hashing the
file first, which every S3-compatible store accepts over HTTPS. It uses
path-style URLs, which every store accepts.

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
