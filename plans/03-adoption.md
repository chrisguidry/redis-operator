# Milestone 3: adoption

A new `Redis` can take over the data of a Redis or Valkey that runs
somewhere else: a Bitnami chart, a hand-built Sentinel set, a virtual
machine, or a managed service. It replicates from the source while the
source keeps serving, and on one edit it becomes the primary, with no
write the source accepted before the cutover lost. CloudNativePG calls
the same idea a replica cluster.

A source that refuses replication, such as a managed service that
blocks `SYNC`, is adopted from a snapshot instead: export an RDB to an
object store and restore it with `bootstrap.recovery.key`
([02-backups.md](02-backups.md)).

## The resource

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache
spec:
  instances: 3
  replica:
    enabled: true                # false promotes
    source:
      host: old-redis.example.svc   # host or sentinel, not both
      port: 6379                    # optional, default 6379
      password:                  # optional; the source's default user's password
        secret: old-redis
        key: password
      sentinel:                  # optional; follow whichever primary these name
        masterName: mymaster
        addresses: ["old-sentinel-0.example.svc:26379"]
        password:                # optional; the sentinels' own requirepass
          secret: old-sentinel
          key: password
status:
  replica:
    phase: Replicating           # Replicating | Promoting | Promoted
    source: 10.0.0.5:6379        # the primary the operator last reached
    sourceOffset: 123456
    lag: 0                       # bytes the first instance is behind the source
  conditions:
    - {type: SourceReachable, status: "True"}   # while replicating
    - {type: Adopted, status: "True", reason: Promoted}
```

A host of one label, such as `old-redis`, names a Service in the
`Redis`'s namespace, and the operator qualifies it to
`old-redis.<namespace>.svc` before it connects, because it runs in
another namespace. The same holds for each sentinel address.

## How it runs

While `status.replica.phase` is `Replicating`:

- The instance that holds the primary Lease follows the source, not
  itself: it runs `REPLICAOF` to the source's address, authenticating
  with the source's password, which it reads from the named Secret. A
  source named by hostname is followed by its IP address. With a
  Sentinel source, it asks the sentinels for the primary every step
  and follows each move. It authenticates to the sentinels with
  `sentinel.password` when the spec names one. Sentinel's `requirepass`
  is a setting of its own, separate from the data password.
- The other instances follow the Lease holder, as they always do, so
  they replicate from the source through it.
- The `-rw` Service has no endpoint that accepts writes: the holder is
  a replica. Clients keep writing to the source.
- Failover works as usual. If the first instance fails, the next
  primary takes the Lease and follows the source in its place.
- The operator reads the source's offset and reports the lag. When it
  cannot reach the source, it sets `SourceReachable` to `False`.
- Switching the holder hands the Lease over without a `FAILOVER`,
  because the holder is a replica of the source: the next holder
  follows the source in its place.

Replica mode starts only on a `Redis` that has never had a primary. On
one that has served as a primary, the operator sets `Adopted` to `False`
with the reason `AlreadyPrimary` and does nothing else.

Setting `enabled: false` promotes, in this order:

1. The operator pauses writes on the source with
   `CLIENT PAUSE 30000 WRITE`, so nothing changes there while the
   replica catches up, then writes `phase: Promoting`.
2. It waits until the holder's offset reaches the source's. If neither
   the operator nor the holder can reach the source, it promotes at
   once without waiting, which is the disaster-recovery case, and says
   so in the condition `Adopted`. If the operator cannot reach the
   source, or its sentinels, but the holder reports its replication
   link to the source up, the source is alive and may still take
   writes: a NetworkPolicy that admits the Redis pods and not the
   operator, or a name that resolves only from the `Redis`'s
   namespace, looks the same to the operator as a source that is down.
   The operator does not promote then. `Adopted` is `False` with the
   reason `SourceUnreachableFromOperator`, and the operator tries again
   after the switchover backoff. A source whose process stops closes
   the holder's link, which then reads down at once. A source whose
   machine is gone leaves the link up until the holder's own
   `repl-timeout` passes, 60 seconds by default, and the promotion
   follows on the next attempt after that.
   If the source is reachable but refuses `CLIENT PAUSE`, it
   does not promote: `Adopted` is `False` with the reason `PauseRefused`,
   and the operator tries again after the switchover backoff of 30
   seconds. The same holds, with the reason `SourceRefused`, for a
   source that answers the connection with an error, such as a wrong
   password, and, with the reason `PasswordUnreadable`, when the
   operator cannot read the source's password Secret or the sentinels'.
   Each of these messages ends with "To promote without pausing the
   source, remove spec.replica".
   If the holder has not reached the source's offset when the pause is
   nearly over (25 seconds in), the operator does not promote either. It
   writes `phase: Replicating`, sets `Adopted` to `False` with the
   reason `CatchUpTimedOut` and both offsets in the message, and pauses
   the source again after the same 30-second backoff. The source takes
   writes again in between, so no write it accepted is left behind by a
   promotion.
3. It writes `phase: Promoted`. The holder's manager promotes its server
   and goes back to its own password for replication.
4. The pause on the source ends by itself after 30 seconds. Clients
   that still write to the source get writes accepted there after the
   cutover, so they must move to `<name>-rw` before the pause ends.

`enabled: false` and removing `spec.replica` are different edits.
`enabled: false` promotes with the source paused and the holder caught
up, as above. Removing `spec.replica` while replicating promotes at
once, with no pause and no wait, because the spec no longer names the
source. `Adopted` is `True` with the message "spec.replica was
removed, so the operator promoted without pausing the source". Writes
the source accepted after the holder's offset are lost, and so are
writes that clients still send to the source afterward. This is the way
to force a promotion that keeps timing out, with the source's clients
stopped first.

## Limits

- The source's RDB format must be one the target engine reads: Valkey
  reads Redis 7.2 and earlier, and Redis 8 reads Valkey 7.2 and earlier.
- The source must accept a replica connection from the pods' network.

## Tests

- The instance manager following a real source server, with and without
  a password, and through a real Sentinel with and without its own
  `requirepass`.
- The operator's promotion sequence against the fake API server and a
  real source server: the pause, the catch-up, and the promotion.
- End to end: a Redis outside the operator under the writer's load,
  adopted into a `Redis`, the writer moved to `cache-rw`, and every
  value it confirmed against the source present after the cutover.
