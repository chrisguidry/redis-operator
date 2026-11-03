---
name: adopt-an-existing-redis
description: "Move the data of a Redis or Valkey that runs outside the operator into a new Redis resource with spec.replica: replicate from the old server, compare, stop the clients, promote with the source's writes paused, and move the clients to the -rw Service. Use when replacing a Bitnami chart, a Sentinel set, a VM, or any other Redis with one the operator runs, or when a promotion reports CatchUpTimedOut, PauseRefused, SourceRefused, SourceUnreachableFromOperator, or PasswordUnreadable."
---

Before the first command, run `kubectl config current-context` and confirm that it names the cluster the person means.

# Adopt an existing Redis

A new `Redis` with `spec.replica.enabled: true` replicates from a
source server outside the operator while the source keeps serving.
Setting `enabled: false` promotes it: the operator pauses writes on the
source, waits until the new `Redis` has every write the source
accepted, and makes it the primary. The recipe below is the order that
loses no write.

The examples adopt a server reached through the Service `old-redis` in
the namespace `example`, into a `Redis` named `cache`.

## Before you start

- The source must accept a replica connection from the Redis pods'
  network, and the operator's pods must reach it too: the operator
  reads the source's replication offset and sends it `CLIENT PAUSE`.
  A NetworkPolicy that blocks the operator from the source stops the
  promotion; see "When the operator cannot reach the source" below.
- The source's RDB format must be one the new engine reads. Valkey
  reads Redis 7.2 and earlier; Redis 8 reads Valkey 7.2 and earlier.
- A source that refuses `SYNC`, as some managed services do, cannot be
  adopted this way. Export an RDB to an object store and restore it
  with `bootstrap.recovery.key`; see the `backups` skill.
- The `Redis` must be new. Replica mode starts only on a `Redis` that
  has never had a primary. On one that has, the operator sets
  `Adopted` to `False` with the reason `AlreadyPrimary` and does
  nothing else.
- Size `memory` so that 70% of it holds the source's data set.

## 1. Declare the Redis in replica mode

If the source has a password, put it in a Secret in the `Redis`'s
namespace. It is the password of the source's default user.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: old-redis
  namespace: example
stringData:
  password: <the source's password>
---
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache
  namespace: example
spec:
  engine: redis
  instances: 3
  memory: 2Gi
  storage: {size: 8Gi}
  replica:
    enabled: true
    source:
      host: old-redis     # one label: a Service in this namespace
      port: 6379          # optional, default 6379
      password:           # optional
        secret: old-redis
        key: password
```

A host of one label, such as `old-redis`, names a Service in the
`Redis`'s namespace; the operator qualifies it to
`old-redis.example.svc`. A host in another namespace needs its full
name, such as `old-redis.legacy.svc`.

For a Sentinel set, name the sentinels instead of `host`; a source
names one or the other:

```yaml
    source:
      sentinel:
        masterName: mymaster
        addresses: ["old-sentinel.example.svc:26379"]
      password: {secret: old-redis, key: password}
```

Here `password` is the Redis servers' password. If the sentinels
have their own `requirepass`, name it in `sentinel.password`, which
takes the same `{secret, key}`:

```yaml
      sentinel:
        masterName: mymaster
        addresses: ["old-sentinel.example.svc:26379"]
        password: {secret: old-sentinel, key: password}
```

The holder of the new `Redis`'s primary Lease asks the sentinels for
the primary on every step and follows each move. The operator asks
them too, to read the source's offset and to pause it.

While replicating:

- The Lease holder replicates from the source, and the other instances
  replicate from the holder.
- `cache-rw` routes to the holder, which is a replica and answers
  writes with `READONLY`. Clients keep writing to the source.
- Failover works as usual: if the holder fails, the next holder
  follows the source in its place.

## 2. Wait for it to catch up, and compare

```sh
kubectl -n example get redis cache -o jsonpath='{.status.replica}{"\n"}'
kubectl -n example get redis cache -o jsonpath='{.status.readyInstances}{"\n"}'
kubectl -n example get redis cache -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.reason}: {.message}{"\n"}{end}'
```

Wait for:

- `status.replica.phase` `Replicating`;
- `status.readyInstances` equal to `spec.instances`;
- the condition `SourceReachable` `True`. When it is `False`, the
  message says why the operator cannot connect to the source;
- `status.replica.lag`, the bytes the holder is behind the source,
  near 0. `status.replica.source` is the address the operator last
  reached and `sourceOffset` the source's offset.

Compare the key counts. On the instance that holds the Lease, which
`status.currentPrimary` names:

```sh
holder=$(kubectl -n example get redis cache -o jsonpath='{.status.currentPrimary}')
kubectl -n example exec "$holder" -c redis -- sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli INFO keyspace'
```

and on the source, with its own client and password. With `engine:
valkey`, the tool is `valkey-cli`. The counts match within the writes
that arrive while you compare.

## 3. Stop the clients

Scale the source's clients to zero, or stop their writes. From here on,
a write that reaches the source after the promotion is not in the new
`Redis`.

## 4. Promote with enabled: false

```sh
kubectl -n example patch redis cache --type=merge -p '{"spec":{"replica":{"enabled":false}}}'
```

The operator:

1. Runs `CLIENT PAUSE 30000 WRITE` on the source, so the source
   accepts no write while the new `Redis` catches up, and writes
   `phase: Promoting`.
2. Waits until the holder's offset reaches the source's.
3. Writes `phase: Promoted`, and the condition `Adopted` `False` with
   the reason `TakingWrites`. On its next step the holder stops
   following the source, becomes the primary, and goes back to its own
   password for replication.
4. Sets `Adopted` `True` once the holder answers as a writable
   primary.

The pause on the source ends by itself after 30 seconds.

```sh
kubectl -n example get redis cache -o jsonpath='{.status.replica.phase}{"\n"}'
kubectl -n example get redis cache -o jsonpath='{.status.conditions[?(@.type=="Adopted")]}{"\n"}'
```

Success is `phase: Promoted` and the condition `Adopted` `True` with
the reason `Promoted` and the message:

```text
cache-0 takes writes as the primary
```

Wait for `Adopted` `True`, not only the phase: until then the holder
may still answer writes with `READONLY`.

## 5. Move the clients

Point the clients at `cache-rw` with the password from the Secret
`cache-app`, and start them. The `declare-a-redis` skill shows how a
client reads the Secret.

## 6. Remove the old server after a soak

Keep the source until the new `Redis` has served for long enough to
trust it, then remove it. After `Promoted`, `spec.replica` changes
nothing and can be removed. Replica mode does not start again on this
`Redis`, even with `enabled: true`.

## enabled: false is not the same as removing spec.replica

Setting `enabled: false` promotes with the source paused. Removing
`spec.replica` entirely while replicating promotes at once, without
pausing the source, because the spec no longer names the source.
`Adopted` is then `True` with the message:

```text
spec.replica was removed, so the operator promoted without pausing the source
```

Any write the source accepted after the holder's offset is lost, and
any write that still lands on the source afterward is not in the new
`Redis`. Remove `spec.replica` only to force a promotion that keeps
failing, with the source's clients stopped.

## When the operator cannot reach the source

When the operator cannot connect to the source, or to its sentinels,
it checks the holder's replication link to the source:

- **The holder's link is up.** The source is up and may still take
  writes; only the operator cannot reach it, as with a NetworkPolicy
  that admits the Redis pods and not the operator, or a host that
  resolves only in the `Redis`'s namespace. The operator does not
  promote. It leaves `phase: Replicating`, sets `Adopted` to `False`
  with the reason `SourceUnreachableFromOperator`, and tries again
  after 30 seconds. Admit the operator's pods to the source, and check
  `SourceReachable`, whose message gives the operator's error.
- **The holder's link is down too.** The source is taken for down,
  which is the disaster-recovery case, and the operator promotes at
  once without pausing or waiting. `Adopted` is `True` with the message
  `neither the operator nor the holder could reach the source
  (<error>), so the operator promoted without waiting`.

A source whose process stops closes the holder's link at once. A
source whose machine is gone leaves the link up until the holder's
`repl-timeout` passes, 60 seconds by default, so the promotion can wait
up to that long, through `SourceUnreachableFromOperator` attempts,
before it goes ahead.

Confirm `SourceReachable` is `True` before you set `enabled: false`.

## When the promotion does not happen

Each of these leaves `phase: Replicating`, sets `Adopted` to `False`
with the reason below, and tries again after 30 seconds. The message
ends with `To promote without pausing the source, remove spec.replica`,
except for `CatchUpTimedOut`.

| Reason | Meaning | What to do |
|---|---|---|
| `CatchUpTimedOut` | The holder had not reached the source's offset 25 seconds into the 30-second pause. The message gives both offsets, or says the holder had no link to the source, or that the source's offset could not be read. The source takes writes again between attempts. | Usually the clients are still writing, or the link is slow. Stop the clients, check `status.replica.lag`, and let the next attempt run. |
| `PauseRefused` | The source answered, and refused `CLIENT PAUSE`, for example because the command is renamed, the user lacks the permission, or the source requires a password and `spec.replica.source` names none (`NOAUTH`). The source is up and still takes writes. | Allow `CLIENT PAUSE` on the source, or stop its clients and remove `spec.replica`. |
| `SourceRefused` | The source answered the connection with an error, such as a wrong password. It is up and may still take writes. | Fix the password in the Secret, or the source's settings. |
| `SourceUnreachableFromOperator` | The operator cannot reach the source or its sentinels, and the holder's link to the source is up. | Admit the operator's pods to the source; see the section above. |
| `PasswordUnreadable` | The operator could not read the source's password Secret, or the sentinels', or its key. This says nothing about the source. | Create the Secret in the `Redis`'s namespace with the key that `spec.replica.source.password` or `spec.replica.source.sentinel.password` names. |

The operator's log has a line for each attempt:

```sh
kubectl -n redis-operator logs <leader pod> | grep 'redis example/cache'
```

The `install` skill shows how to find the leader pod.
