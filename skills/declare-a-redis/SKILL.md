---
name: declare-a-redis
description: "Declare a Redis resource for Redis or Valkey: instances, engine, memory and the maxmemory share, storage or none, scheduling, the metrics exporter beside each instance and the keys it checks; connect clients through the -rw and -ro Services and the -app Secret; read the status and its conditions; scale up and down. Use when an application needs a Redis or Valkey, when a client cannot connect, or when changing the number of instances."
---

Before the first command, run `kubectl config current-context` and confirm that it names the cluster the person means.

# Declare a Redis

A `Redis` (API group `redis.guid.foo`, version `v1alpha1`) is one
primary and zero or more replicas of Redis or Valkey. The operator must
be installed first; see the `install` skill.

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache
  namespace: example
spec:
  engine: valkey
  instances: 3
  memory: 1Gi
  storage:
    size: 4Gi
```

## The fields

| Field | Default | What it does |
|---|---|---|
| `engine` | `redis` | `redis` or `valkey`. Cannot change after creation. |
| `image` | the engine's image at the version the operator pins | The server image. In release 2026.11.01-001 the pins are `redis:8.10.3` and `valkey/valkey:9.0.7`. Changing it starts a canary rollout; see the `upgrade` skill. |
| `instances` | `1` | 1 to 9. One is the primary; the rest are replicas. |
| `memory` | `256Mi` | The container's memory request and limit. A whole number with an optional suffix `Ki`, `Mi`, `Gi`, `Ti`, `k`, `M`, `G`, or `T`. |
| `maxmemoryPolicy` | `noeviction` | The server's `maxmemory-policy`: `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, or `volatile-ttl`. |
| `storage.size`, `storage.storageClassName` | none | A volume claim per instance. Cannot change after creation, so it cannot be added or removed later. |
| `minReplicasToWrite` | `1` when `instances` is more than 1, else `0` | The server's `min-replicas-to-write`: the primary refuses writes with `NOREPLICAS` while fewer replicas have acknowledged it within 10 seconds. Must be less than `instances`. The value the primary runs with is in `status.minReplicasToWrite`; see "Scale". |
| `cpu` | none | The container's CPU request. There is no CPU limit. |
| `nodeSelector`, `tolerations`, `priorityClassName` | none | Copied into the pod spec. |
| `metrics.enabled` | `true` | Runs redis_exporter beside each instance; see "Metrics". |
| `metrics.image` | redis_exporter at the version the operator pins | The exporter image. The operator pins `oliver006/redis_exporter:v1.96.0`. |
| `metrics.checkSingleKeys` | none | Keys whose value or length the exporter reports, such as `celery`. A key is in database 0; write `db1=jobs` for another database. No key may hold a comma. |

`spec.backup`, `spec.bootstrap`, and `spec.replica` are covered by the
`backups` and `adopt-an-existing-redis` skills.

### Memory

The server's `maxmemory` is 70% of `memory`. A snapshot, a full resync
to a new replica, and an append-only file rewrite each fork the server,
and the kernel copies every page that changes during the fork. The
remaining 30% is room for those copies, so the kernel does not kill the
container. With `memory: 1Gi`, the data set may grow to about 717 MiB.
At `maxmemory`, `noeviction` refuses writes with an `OOM` error, and
the other policies evict keys.

### Storage

With `storage`, each instance has a volume claim from the
StatefulSet's claim template `data`, the append-only file is on with
`appendfsync everysec`, and a restarted pod comes back with its data.

Without `storage`, the data lives in an `emptyDir` and in the
replicas, and snapshots and the append-only file are off. A restarted
instance comes back empty and copies the data from the primary. If
every instance restarts at once, the data is gone, the operator starts
an empty data set on instance 0, and it sets the condition `DataLost`
with the reason `AllInstancesEmpty`. A `Redis` of one instance without
storage loses its data each time its pod restarts, including on an
image or operator upgrade, and comes back by itself as an empty primary
with `DataLost` set. A `Redis` that is still replicating from an
adoption source sets no `DataLost` when its instances restart empty:
instance 0 copies the source again.

### Metrics

Each pod runs [redis_exporter](https://github.com/oliver006/redis_exporter)
in a container named `metrics`. It reads its own server over 127.0.0.1
with the password from `cache-app`, and serves on port 9121, the
container port named `metrics`, with redis_exporter's series names:
`redis_up`, `redis_memory_used_bytes`, `redis_memory_max_bytes`,
`redis_connected_clients`, `redis_connected_slaves`,
`redis_master_repl_offset`, and the rest. Every instance reports
itself, so a replica's link and lag are visible, not only the
primary's. redis_exporter reads Valkey too.

To alert on a queue's length, name the key:

```yaml
spec:
  metrics:
    checkSingleKeys: [celery]
```

The exporter then serves `redis_key_size{db="db0",key="celery"}`, on
every instance. A replica's value can trail the primary's, so select the
primary in a rule, such as
`redis_key_size{key="celery", redis_role="primary"}` with the
monitoring component's labels (see the `install` skill).

A change to `spec.metrics`, and the release that first adds the
exporter, changes the pod template, so the operator rolls every pod
once: the replicas one at a time, a switchover, then the old primary,
as it rolls any change. The exporter counts toward the pod's
readiness, so an exporter image that cannot be pulled holds the roll
at its first replica, and an exporter that crashes takes its pod out
of its Service until it restarts. `metrics.enabled: false` removes the
container, which rolls the pods the same way.

### Placement

Every pod has a required anti-affinity on `kubernetes.io/hostname`, so
no two instances of one `Redis` share a node. A `Redis` of three
instances needs three nodes that its `nodeSelector` and `tolerations`
admit. A pod that cannot be placed stays `Pending`, and the `Redis`
reports `InstancesNotReady`.

## What the operator makes

Every object is owned by the `Redis`, so deleting the `Redis` deletes
them.

| Object | Name | Purpose |
|---|---|---|
| StatefulSet | `cache` | the pods `cache-0`, `cache-1`, ..., and their claims |
| Service | `cache-rw` | the primary, port 6379 |
| Service | `cache-ro` | the replicas, port 6379 |
| Service | `cache-nodes` | headless, one name per pod |
| Secret | `cache-app` | connection details |
| ServiceAccount, Role, RoleBinding | `cache` | what the instance managers may read and write |
| PodDisruptionBudget | `cache-primary` | `minAvailable: 1` on the primary, when `instances` is more than 1 |
| PodDisruptionBudget | `cache-replicas` | one replica may be evicted at a time |
| Lease | `cache-primary` | held by the primary's instance manager |

Each pod carries the label `redis.guid.foo/cluster: cache`. The
operator writes `redis.guid.foo/role: primary` on the pod that holds
the Lease and `redis.guid.foo/role: replica` on the others, and the
Services and budgets select on that label.

## Connect a client

The Secret `cache-app` holds:

| Key | Value |
|---|---|
| `password` | the default user's password, 48 hex characters |
| `host` | `cache-rw.example.svc` |
| `port` | `6379` |
| `uri` | `redis://:<password>@cache-rw.example.svc:6379` |
| `ro-uri` | `redis://:<password>@cache-ro.example.svc:6379` |

The operator creates the Secret once and never rewrites it. Do not
delete it: each server reads its password from it when its container
starts, a container cannot start while it is missing, and a Secret the
operator makes again holds a new password that the running servers do
not have.

The password is hex, so it needs no escaping in a URL. A client in the
same namespace can take the whole URI:

```yaml
env:
  - name: REDIS_URL
    valueFrom:
      secretKeyRef: {name: cache-app, key: uri}
```

Or build one with a database number, with Kubernetes's `$(VAR)`
expansion. The variable must come earlier in the list:

```yaml
env:
  - name: REDIS_PASSWORD
    valueFrom:
      secretKeyRef: {name: cache-app, key: password}
  - name: REDIS_URL
    value: redis://:$(REDIS_PASSWORD)@cache-rw:6379/0
```

A client in another namespace uses `cache-rw.example.svc`, and needs a
copy of the password, because a pod cannot read a Secret in another
namespace. In a namespace that denies ingress by default, the Redis
pods need a NetworkPolicy that admits the client on 6379.

Send writes to `cache-rw` and reads that may lag to `cache-ro`.
`cache-ro` has no endpoints when `instances` is 1.

A pod carries the label `redis.guid.foo/role: primary` only while it
holds the primary Lease and is `status.targetPrimary`. When a
switchover or failover starts, the operator takes the label off the old
primary at once, so `cache-rw` has no endpoint and new connections to
it are refused until the new primary takes the Lease. In a planned
switchover, the old primary pauses its clients' writes while it is
still primary, waits until its pod has lost the label and 5 seconds
more for the route to settle, closes its normal and pub/sub client
connections, hands the role over, and closes them again as it ends
the pause. It holds the writes for at most 30 seconds. A write held in
the pause ends with a closed connection, not with `READONLY`.

A client must reconnect after a closed connection, and retry a refused
connection until the new primary takes over. A switchover that has not
finished in 30 seconds is given up, and the old primary takes the label
back. Most clients reconnect on their own; check that the client's
retries cover 30 seconds.

## Read the status

```sh
kubectl -n example get redis cache
kubectl -n example get redis cache -o wide
kubectl -n example get redis cache -o yaml
```

The columns are `ENGINE`, `INSTANCES`, `READY` (`status.readyInstances`),
`PRIMARY` (`status.currentPrimary`), `STATUS` (the `Ready` condition),
and `AGE`; `-o wide` adds `IMAGE` (`status.image`), `TARGET`
(`status.targetPrimary`), and `REASON` (the `Ready` condition's reason).

In the status:

- `targetPrimary` is the operator's decision: the instance that should
  be primary, or `pending` while a failover chooses one.
- `currentPrimary` is the instance that holds the Lease.
- `instances` lists each pod with its `role` (`primary`, `replica`,
  `detached`, or `starting`), `ready`, and replication `offset`.
- `switchover` is set while a planned switchover runs, with `from`,
  `to`, `reason`, and `startedAt`.

The `Ready` condition's reasons:

| Status | Reason | Meaning |
|---|---|---|
| True | `AllInstancesReady` | a primary holds the Lease and every instance is ready |
| True | `SwitchoverInProgress` | a planned switchover is moving the primary |
| True | `ScalingDown` | the StatefulSet still has pods beyond `instances`, which are being removed |
| False | `InstancesNotReady` | fewer instances are ready than `instances` |
| False | `NoPrimary` | no instance holds the primary Lease |
| False | `FailoverInProgress` | the primary failed and the operator is choosing the next one |

A primary is ready when it holds the Lease. A replica is ready when
its replication link to the primary is up.

Wait for a new `Redis`:

```sh
kubectl -n example wait redis/cache --for=condition=Ready --timeout=5m
```

## Scale

Change `instances`:

```sh
kubectl -n example patch redis cache --type=merge -p '{"spec":{"instances":3}}'
```

Scaling up adds pods with the next ordinals, and each new replica
copies the data set from the primary with a full resync, which forks
the primary.

The primary runs with `status.minReplicasToWrite`, not the spec's value
directly. A new `Redis` starts with the spec's value. When the spec's
value rises, as the default does from 0 to 1 when `instances` goes from
1 to more, the operator raises the status value only as far as the
number of replicas the primary reports as synced, so a scale from one
instance keeps taking writes while the first replica syncs. Once
raised, the value stays when replicas go away: a primary with no
replica refuses writes it could lose. When the spec's value falls, the
status value falls at once.

```sh
kubectl -n example get redis cache -o jsonpath='{.status.minReplicasToWrite}{"\n"}'
```

Scaling down removes the pods with the highest ordinals. When the
primary is one of them, the operator keeps the StatefulSet at its old
size, switches over to the most caught-up replica that stays, with the
reason `ScaleDown`, and shrinks the StatefulSet once that replica holds
the Lease. The volume claims of removed instances are deleted with
them, so a later scale-up starts those instances empty, and each copies
the data from the primary. Scaling to one instance removes the
`cache-primary` PodDisruptionBudget, so a drain can evict the only pod.

`minReplicasToWrite` must stay less than `instances`. If it is set
explicitly, the API server refuses a scale to a count at or below it;
lower it in the same edit.

## Delete

```sh
kubectl -n example delete redis cache
```

This deletes the pods, Services, Secret, budgets, and Lease. The
volume claims from `storage`, named `data-cache-0`, `data-cache-1`,
and so on, stay with their data until you delete them:

```sh
kubectl -n example delete pvc -l redis.guid.foo/cluster=cache
```

`Backup` objects that name the `Redis` stay, with their snapshots.
