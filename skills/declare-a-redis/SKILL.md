---
name: declare-a-redis
description: "Declare a Redis resource for Redis or Valkey: instances, engine, memory and the maxmemory share, storage or none, scheduling, the metrics exporter beside each instance and the keys it checks; ACL users with passwords in Secrets; TLS from a cert-manager issuer or a Secret; connect clients through the -rw and -ro Services and the -app Secret; read the status and its conditions; scale up and down. Use when an application needs a Redis or Valkey, when an application needs its own restricted user or TLS, when a client cannot connect, or when changing the number of instances."
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
| `minReplicasToWrite` | `1` when `instances` is more than 1, else `0` | The server's `min-replicas-to-write`: the primary refuses writes with `NOREPLICAS` while fewer replicas have acknowledged it within 10 seconds. Must be less than `instances`. The value the primary runs with is in `status.minReplicasToWrite`; see "Scale". It also sets what a failover keeps: every write a client confirmed with `WAIT n`, for `n` at least this value and at least 1. Raising it lets a failover go ahead with fewer instances answering. |
| `cpu` | none | The container's CPU request. There is no CPU limit. |
| `nodeSelector`, `tolerations`, `priorityClassName` | none | Copied into the pod spec. |
| `metrics.enabled` | `true` | Runs redis_exporter beside each instance; see "Metrics". |
| `metrics.image` | redis_exporter at the version the operator pins | The exporter image. The operator pins `oliver006/redis_exporter:v1.96.0`. |
| `metrics.checkSingleKeys` | none | Keys whose value or length the exporter reports, such as `celery`. A key is in database 0; write `db1=jobs` for another database. No key may hold a comma. |
| `users` | none | ACL users beside the default user; see "Users". |
| `tls.issuerRef`, `tls.secret`, `tls.plaintext` | none | TLS on port 6380; see "TLS". |

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
instance comes back empty, even when a backup or a replica's sync left
a snapshot in the `emptyDir`, and copies the data from the primary. If
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
| Certificate | `cache-tls` | with `tls.issuerRef` only: the certificate cert-manager issues into the Secret `cache-tls` |
| ServiceAccount, Role, RoleBinding | `cache` | what the instance managers may read and write |
| PodDisruptionBudget | `cache-primary` | `minAvailable: 1` on the primary, when the StatefulSet has more than one pod |
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
| `tls-port`, `tls-uri`, `tls-ro-uri` | with `tls` only: `6380`, and the URIs with `rediss://` and port 6380 |

With `tls` and `plaintext: false`, `port`, `uri`, and `ro-uri` hold the
TLS values too, because port 6379 is closed.

The operator creates the Secret once and keeps its password. It
rewrites the other keys when `tls` changes them. Do not delete it: each server reads its password from it when its container
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

## Users

`users` declares ACL users beside the default user. Each takes its
password from a key of a Secret in the same namespace:

```sh
kubectl -n example create secret generic app-user --from-literal=password="$(openssl rand -hex 24)"
```

```yaml
spec:
  users:
    - name: app
      password: {secret: app-user, key: password}
      rules: "~app:* &app:* +@all -@dangerous"
```

`rules` are ACL rules as `ACL SETUSER` takes them, separated by
spaces: `~pattern` for keys, `&pattern` for pub/sub channels,
`+@category` and `-command` for commands, and selectors in
parentheses. The CRD refuses rules that set a password (`>`, `<`, `#`,
`!`, `nopass`, `resetpass`) and `on`, `off`, and `reset`, because the
operator sets those. A name is letters, digits, `.`, `_`, and `-`, and
cannot be `default`.

Every instance manager sets the users on its own server, replicas
included, so a failover keeps them. A change to `users` or to a
password's Secret takes effect within about a second, with no restart
and no roll. Connections that already authenticated keep their
session; new ones need the new password. Removing a user deletes it
from every server. A user made by hand with `ACL SETUSER` is left
alone.

A user whose Secret or key is missing, whose password is empty, or
whose rules the server refuses is not set, and the instance manager
logs why; see the `troubleshoot` skill. The default user and its
password in `cache-app` keep every permission, and the operator, the
exporter, and replication use it.

A client connects with the user's name and password:

```yaml
env:
  - name: REDIS_PASSWORD
    valueFrom:
      secretKeyRef: {name: app-user, key: password}
  - name: REDIS_URL
    value: redis://app:$(REDIS_PASSWORD)@cache-rw:6379/0
```

The operator makes no connection Secret for each user.

## TLS

`tls` serves TLS on port 6380, named `redis-tls`, on every pod and
Service. The certificate comes from a cert-manager issuer or from a
Secret.

With an issuer, the operator makes the Certificate `cache-tls`, and
cert-manager writes its Secret `cache-tls`. The issuer must sign names
inside the cluster, as a CA issuer does; an ACME issuer cannot.
cert-manager must be installed; see the `install` skill.

```yaml
spec:
  tls:
    issuerRef: {name: internal-ca, kind: ClusterIssuer}   # kind defaults to Issuer
```

The Certificate covers `cache-rw` and `cache-ro` as a short name, with
the namespace, with `.svc`, and with `.svc.cluster.local`, every pod
under `*.cache-nodes.example.svc`, and `127.0.0.1`.

With a Secret you keep, it must hold `tls.crt`, `tls.key`, and
`ca.crt`, and the certificate must name the hosts your clients dial:

```yaml
spec:
  tls:
    secret: cache-tls
```

`plaintext` defaults to `false`: each instance closes port 6379 once
every instance of the `Redis` serves TLS. With `plaintext: true`, 6379
stays open beside 6380. Clients authenticate with passwords; the
servers ask for no client certificate.

Replicas follow the primary over TLS, and verify its certificate
against `ca.crt`. A new certificate in the Secret, such as
cert-manager's renewal, loads into every server within about a second,
with no restart; open connections keep the certificate they started
with.

A client needs the CA that signed the certificate. With a CA issuer,
that is the `ca.crt` of the issuer's own Secret; copy it into a
ConfigMap or Secret the client mounts, or distribute it with
trust-manager. Do not mount `cache-tls` in a client: it holds the
servers' private key.

```yaml
env:
  - name: REDIS_URL
    valueFrom:
      secretKeyRef: {name: cache-app, key: tls-uri}
```

### Turn TLS on for an existing Redis

Adding `tls` changes the pod template, so the operator rolls every pod
once. Replication keeps working during the roll: an instance closes
6379 only once every pod serves TLS, and an instance replicates over
plaintext from a pod that does not. If `plaintext` is `false`,
`cache-app`'s `uri` moves to `rediss://` at once, and clients that do
not trust the CA fail once 6379 closes. To move clients over without
an outage:

1. Set `tls` with `plaintext: true`, and wait until `kubectl get redis`
   shows every instance ready.
2. Point the clients at `tls-uri`, with the CA.
3. Set `plaintext: false`. Each instance closes 6379 within about a
   second, and no pod restarts.

Removing `tls` rolls the pods back to plaintext, and each instance
reopens 6379 as soon as it sees a pod without TLS. Closing 6379 at run
time needs Redis 7 or Valkey 7.2 or later, which every pinned image
is.

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
| False | `QuorumNotReached` | a failover waits for enough instances to detach with their data that the next primary holds every confirmed write; see the `troubleshoot` skill |

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

The StatefulSet does not shrink while a failover is pending, while no
instance holds the Lease, while the holder's pod is not ready or its
manager does not answer as primary, or while a switchover is moving the
primary,
because one of the pods a scale-down removes may hold a write a client
confirmed with `WAIT`. The failover can still name any of those pods.
Once the next primary holds the Lease, the scale-down goes on as above,
with a `ScaleDown` switchover first if the new primary is one of the
pods it removes. While it waits, the `Ready` message ends with `The
StatefulSet keeps its 3 pods until the next primary holds the lease,
then scales down to 1`, and the budgets still let a drain evict only
one replica at a time.

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
