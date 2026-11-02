# Milestone 1: high availability

A `Redis` with three instances keeps one writable primary through a
primary crash, an operator crash, a node drain, a frozen process, and a
spec change, and loses no write that a client confirmed with `WAIT 1`.
[00-design.md](00-design.md) gives the failover design. This document
gives the resource, the objects the operator makes, the timings, and the
tests.

The milestone ships in two releases. The second adds the failover
quorum, the partition test in which the primary's clients still reach
it, ACL users, and TLS.

## The resource

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache
spec:
  engine: valkey          # redis (default) | valkey
  image: ""               # default: the engine's image at the version the operator pins
  instances: 3            # default 1
  memory: 1Gi             # the container's memory limit; maxmemory is derived from it
  maxmemoryPolicy: noeviction
  storage:                # optional; without it, data lives in an emptyDir
    size: 4Gi
    storageClassName: ""
  minReplicasToWrite: 1   # default 1 when instances > 1, else 0; see "Scaling"
  cpu: 100m               # the container's CPU request; there is no CPU limit
  nodeSelector: {}
  tolerations: []
  priorityClassName: ""
  metrics:                # see "Metrics"
    enabled: true         # default true; false removes the exporter
    image: ""             # default: redis_exporter at the version the operator pins
    checkSingleKeys: []   # keys whose value or length the exporter reports
  users:                  # ACL users beside the default user; see "Users"
    - {name: app, password: {secret: app-user, key: password}, rules: "~app:* +@all"}
  tls:                    # see "TLS"
    issuerRef: {name: ca, kind: ClusterIssuer}   # or secret: <name>
    plaintext: false
status:
  observedGeneration: 3
  targetPrimary: cache-0
  currentPrimary: cache-0
  readyInstances: 3
  minReplicasToWrite: 1
  instances:
    - {name: cache-0, role: primary, ready: true, offset: 1234}
    - {name: cache-1, role: replica, ready: true, offset: 1234}
  switchover: {from: cache-0, to: cache-1, reason: NodeCordoned, startedAt: ...}
  conditions:
    - {type: Ready, status: "True", reason: AllInstancesReady}
```

The resource also takes `backup`, `bootstrap`, and `replica`
([02-backups.md](02-backups.md), [03-adoption.md](03-adoption.md)), and
its status takes `image`, `rollout`, and `replica`
([04-upgrades.md](04-upgrades.md)). The conditions this milestone sets
are `Ready` (reasons `AllInstancesReady`, `InstancesNotReady`,
`SwitchoverInProgress`, `FailoverInProgress`, `QuorumNotReached`,
`NoPrimary`, and `ScalingDown`),
`SwitchoverFailed`, and `DataLost`.

`maxmemory` is 70% of `memory`. A snapshot or a full resync forks the
server, and the pages that change during the fork are copied, so the
container needs headroom above `maxmemory` or the kernel kills it.

`engine` and `storage` are immutable, enforced by the CRD. Changing the
engine would mean loading one engine's data files into the other, and a
StatefulSet's volume claims are fixed when it is made. `minReplicasToWrite`
must be less than `instances`, or the primary could never accept a write.

With `storage`, every instance runs with its append-only file on and
`appendfsync everysec`. Without it, snapshots and the append-only file
are off, and the data lives in memory and in the replicas.

## The objects

Every object is owned by the `Redis`, so deleting it deletes them, and
every name starts with the resource's name.

| Object | Name | Purpose |
|---|---|---|
| StatefulSet | `<name>` | the pods and their volume claims |
| Service | `<name>-rw` | the primary, for writes |
| Service | `<name>-ro` | the replicas, for reads |
| Service | `<name>-nodes` | headless, the StatefulSet's governing Service |
| Secret | `<name>-app` | `password`, `host`, `port`, `uri`, `ro-uri`, and with TLS `tls-port`, `tls-uri`, `tls-ro-uri` |
| ServiceAccount, Role, RoleBinding | `<name>` | what the instance managers may read and write |
| PodDisruptionBudget | `<name>-primary` | `minAvailable: 1` on the primary, when instances > 1; the operator deletes it when instances drops to 1, because it would block every drain of the last pod's node |
| PodDisruptionBudget | `<name>-replicas` | `minAvailable: instances - 2` on the replicas, never below 0, so one may go at a time; made for every `Redis`, including one instance |
| Lease | `<name>-primary` | held by the primary's instance manager |
| Certificate | `<name>-tls` | with `tls.issuerRef` only, for cert-manager; see "TLS" |

Each pod carries the label `redis.guid.foo/cluster` with the resource's
name. The operator writes the label `redis.guid.foo/role` on each pod,
from the Lease and `targetPrimary`: `primary` on the holder while it is
also the target, `replica` on the others. During a switchover or a
failover no pod is `primary`, so `<name>-rw` has no endpoint and a
client's connect fails until the new primary holds the Lease. The
Services and the budgets select on it. The operator writes the labels
after the status, so a decision that did not land moves no label.

The server listens on the pod's IP and on 127.0.0.1, so `redis-cli` in
the container reaches it without `-h`.

Pods carry a required anti-affinity on `kubernetes.io/hostname`, so no
two instances share a machine.

## Timings

| Setting | Value | Why |
|---|---|---|
| Lease duration | 15 s | CNPG's and client-go's default |
| Renew deadline | 10 s | a primary fences itself before its Lease can expire |
| Renew period | 2 s | |
| Operator poll of `/status` | 2 s | a watch event also starts a pass |
| Failure threshold | 6 s | three missed polls before a failover starts |
| Leaderless threshold | 21 s | the Lease has had no holder for this long after a primary held it, which is longer than a lease duration because a new target waits out the old Lease |
| Detach wait | 5 s | how long a failover waits for every replica to detach before it names the next primary |
| Switchover timeout | 10 s | the `FAILOVER ... TIMEOUT` the primary runs |
| Switchover deadline | 30 s | the operator gives up a switchover after this, and tries again after a 30 s backoff |
| Manager step | 1 s | how often an instance manager reads the decision and the Lease |
| `min-replicas-max-lag` | 10 s | |
| Termination grace | 30 s | a deleted primary waits up to 20 s for its switchover |
| Route settle | 5 s | how long a primary that steps down waits, after its pod lost the primary label, for kube-proxy to stop sending it new connections |
| Demotion hold | 30 s | the longest a primary that steps down holds writes waiting for the label to leave its pod; the server's own pause limit is twice this plus the switchover timeout, for a manager that dies mid-step |

## Failover, step by step

1. A failover starts when the operator cannot reach the target
   primary's `/status` for the failure threshold, when the Lease has had
   no holder for the leaderless threshold, or when the target answers
   that it restarted with no data and cannot take the Lease.
2. It writes `targetPrimary: pending`. Every instance manager that sees
   it detaches its Redis, so no replica keeps following an old primary
   that might still accept writes, and their offsets stop moving.
3. After every reachable instance reports that it detached, or after 5
   seconds, and once enough instances detached with their data to hold
   every confirmed write ("The failover quorum" below), the operator
   writes the reachable instance that holds data and is not stopping,
   and has the highest offset, as `targetPrimary`. Ties go to the
   lowest ordinal. If every instance answers and none
   holds data, it writes instance 0 and sets the condition `DataLost`.
4. That replica's instance manager takes the Lease once it is empty, or
   once its renew time has not changed for the lease duration, then runs
   `REPLICAOF NO ONE`.
5. The operator moves the role labels to the new holder. The other
   instances follow the holder.

A `Redis` of one instance has no other instance to fail over to. Its
silence, as while its pod restarts, starts no failover, and the
instance comes back as the target and takes its Lease again if it kept
its data on a volume. An instance with no volume restarts empty,
releases the Lease, and reports that it cannot take it, which starts a
failover that names the same instance again with `DataLost` set, as
when every instance of a larger `Redis` restarts empty. A `Redis` that
replicates loses nothing that way, because its instance 0 copies the
source again, so it sets no `DataLost`. A single instance whose pod is
deleted stops at once, because no switchover can come.

A primary that is alive when the failover starts steps down as in
"Closing a demoted primary's clients" below, then releases the Lease.
Its replicas detached first, so it refuses writes with `NOREPLICAS`
while it waits. If the operator names the next primary before the old
one has stepped down, the old one still detaches rather than run
`FAILOVER`, because its replicas detached and may not reach it at all,
as when a partition cuts it off from its peers but not from the API
server. The instance manager tells the two apart by `status.switchover`,
which only a switchover sets.

## The failover quorum

A failover promotes the instance with the highest offset among the
instances it reached. Redis replicates asynchronously, so an instance
it did not reach may hold a write that the promoted one lacks. The
failover therefore names the next primary only once it has reached
enough instances that one of them must hold every confirmed write.

Let `N` be the number of instances: `spec.instances`, or the number of
pods while a scale-down has not yet removed the extra ones, because
those pods could have acknowledged a write too. Let `n` be the larger
of 1 and `status.minReplicasToWrite`, the value the primary runs with.
A write that a client confirmed with `WAIT n` is on the old primary and
on at least `n` of the other `N - 1` instances, so at most `N - 1 - n`
of the others lack it. Any `N - n` instances therefore include one that
holds the write, and the highest offset among them is at least the
write's offset.

| `instances` | `minReplicasToWrite` | `n` | instances the failover must reach |
|---|---|---|---|
| 2 | 1 | 1 | 1 |
| 3 | 1 | 1 | 2 |
| 3 | 2 | 2 | 1 |
| 5 | 1 | 1 | 4 |
| 5 | 2 | 2 | 3 |

An instance counts toward the `N - n` only when its manager answered
within the last two polls, it reports `detached`, it holds data, and it is not
stopping. A detached server no longer follows the old primary, so the
offset it reports is final. A replica that still follows could take
more writes after it reported. An instance that restarted with no data
lost what it acknowledged, and a stopping instance cannot be named.
The old primary counts like any other instance once it reports
`detached`.

The guarantee covers a write confirmed with `WAIT n` for an `n` of at
least `minReplicasToWrite`, and at least 1. A write confirmed with a
smaller `n`, or never confirmed, has no such guarantee.
`min-replicas-to-write` makes the primary refuse a write while too few
replicas are connected, but a write it accepted can still be on no
replica when the primary fails.

### Waiting for the quorum

The failover waits for the detach first, as step 3 says. If fewer than
`N - n` instances count once that wait is over, it keeps
`targetPrimary: pending`, sets `Ready` to False with the reason
`QuorumNotReached`, logs the fault once, and checks again on every
pass. The message says how many instances counted, how many the
failover needs, and how to force it. The condition's transition posts
a Warning Event. An instance that comes back, the old primary
included, counts once it reports `detached` with its data, and the
failover then names the highest offset among them.

The quorum costs availability. With three instances and
`minReplicasToWrite: 1`, losing the primary and one replica at once
leaves the Redis with no primary until one of them returns. Without the
quorum, the operator would promote the remaining replica, which may
lack writes that clients were told are safe, and the old primary would
discard those writes when it rejoins as a replica of the new one.

### Forcing a failover

When a person knows that the instances the failover cannot reach will
not come back with their data, such as a node that is gone with its
volume, they let the failover go on without the quorum:

```sh
kubectl annotate redis cache redis.guid.foo/failover-without-quorum=true
```

The failover then names the reachable instance with data and the
highest offset, posts the Warning Event `FailoverForced` with how many
instances it reached, and removes the annotation once its decision
lands. A write that only the missing instances held is lost. Only the
value `true` forces a failover.

This is an annotation and not a spec field because forcing is a
decision about one failover, made during an incident by a person who
judged the missing instances lost. A spec field would make every later
failover unsafe, and in a repository that applies the spec, it would
stay set after the incident. The operator removes the annotation, so it
cannot outlast the failover it was set for, and the operator needs
`patch` on `redis` to remove it. An annotation set before a failover
starts applies to the next failover.

### A partitioned primary that its clients still reach

A partition can cut the primary off from its replicas, the operator,
and the API server, and leave its clients' path to it open. Two
mechanisms stop it from accepting writes:

- Its server: with `min-replicas-to-write`, it refuses writes with
  `NOREPLICAS` once fewer replicas than that have acknowledged within
  `min-replicas-max-lag`. A replica acknowledges every second, and the
  server recounts its replicas every second, so it refuses no later
  than `min-replicas-max-lag` and two seconds after the cut. A replica that detaches for the failover closes its
  link, and the primary stops counting it at once.
- Its instance manager: once it has not renewed the Lease for the renew
  deadline and a peer is unreachable or names another target, it fences
  the server with `REPLICAOF` an unreachable address.

So a partitioned primary accepts writes for at most
`min-replicas-max-lag` and two seconds after it loses its replicas, and no
replica acknowledges a write in that window, so `WAIT n` returns less
than `n` for each one. A client that confirms its writes counts them as
unconfirmed, and they are lost when the old primary rejoins as a
replica. The next primary takes the Lease only after the old holder's
renew time has stood still for the lease duration, which is longer than
that window, so the two never accept writes at the same time. With
`minReplicasToWrite: 0`, only the fence stops the old primary, within
the renew deadline and a step.

The operator takes the role label off the old primary when the failover
starts, so a new connection through `<name>-rw` stops reaching it. A
connection that a client opened before keeps reaching it until the
server refuses its writes or the fence closes it.

## Switchover, step by step

A switchover starts when the primary's node is cordoned, when a pod
asks for one because it is being deleted, or when the operator rolls a
spec change.

1. The operator picks a ready replica on a schedulable node that
   follows the primary with its link up, preferring the highest offset
   and then the lowest ordinal, and writes it as `targetPrimary`, with
   `status.switchover` saying why: `NodeCordoned`, `PodDeleted`, or
   `Rollout`. A rollout switches only to a replica on the updated
   revision. With no such replica, the operator logs the fault and tries
   again on the next pass.
2. The operator takes the `primary` label off the old primary in the
   same pass. The primary's instance manager pauses its writes, waits for
   the label to leave its pod and for the route settle, closes its
   clients, runs `FAILOVER TO <ip> 6379 TIMEOUT 10000`, waits until its
   Redis reports the replica role, closes its clients again, and
   releases the Lease. "Closing a demoted primary's clients" gives why.
3. The new primary takes the empty Lease. It is already a master,
   because `FAILOVER` promoted it. The old primary is already its
   replica, and keeps that link while the Lease is empty instead of
   detaching, because the new primary counts it toward
   `min-replicas-to-write` and would refuse writes with `NOREPLICAS`
   without it.
4. If the Lease holder has not changed 30 seconds after the switchover
   started, the operator writes the old primary back as `targetPrimary`
   and sets the condition `SwitchoverFailed`. It tries again after a
   30-second backoff.

## Closing a demoted primary's clients

A primary that becomes a replica keeps its clients' connections, and
the next write on one fails with `READONLY`. Clients such as redis-py
raise that as an application error instead of reconnecting, so a
switchover would crash their workers. Redis Sentinel closes the
clients of an instance it demotes, with `CLIENT KILL TYPE normal` and
`TYPE pubsub`, and the instance manager does the same. Its clients
reconnect through `<name>-rw`, which follows the role label, so the
order of the steps decides where they land:

1. The operator takes the `primary` label off the holder as soon as
   `targetPrimary` names another instance or `pending`.
2. The holder's instance manager sends `CLIENT PAUSE <ms> WRITE`. A
   primary holds a paused write instead of refusing it. A replica
   refuses every write with `READONLY`, paused or not, which is why the
   pause has to start while the server is still primary.
3. It waits until it reads its own pod without the `primary` label,
   and then for the route settle, the time kube-proxy takes to stop
   sending new connections to the pod. It waits no longer than the
   demotion hold.
4. It closes every normal and pub/sub client. A held write ends with
   a closed connection, which clients retry, and the reconnect finds no
   endpoint until the new primary holds the Lease.
5. It changes the role, with `FAILOVER` or `REPLICAOF`, then closes the
   clients again in the same `MULTI` as `CLIENT UNPAUSE`, so no held
   write runs on the replica between the two.

If `FAILOVER` aborts, the server is still primary, and the manager ends
the pause. The replication links are clients of other types, and the
manager skips its own connection. A replica's clients, which reach it
through `<name>-ro`, are never closed in steady state.

A primary that fences itself, or that finds another instance holding
the Lease, as one does when it comes back from a freeze or a partition,
takes the same steps before its `REPLICAOF`. Its server stays a paused
primary until routing has moved, which is as safe as a replica: it
takes no write while paused, and once its replicas are gone,
`min-replicas-to-write` refuses a write with `NOREPLICAS` before the
pause holds it. A manager that cannot read its pod, such as one that
fenced itself because the API server is unreachable, cannot tell when
the label moves, so it waits the demotion hold before it closes the
clients and changes the role. No client that reaches a primary as it
steps down is answered `READONLY`.

A primary whose pod is deleted steps down in the same way and then
stops its server, which closes the remaining connections.

## Scaling

The StatefulSet removes the highest ordinals first, so lowering
`instances` would delete a primary whose ordinal is at or beyond the new
count. The operator keeps the StatefulSet's replicas at the largest of
`instances`, the holder's ordinal plus one, and the target's ordinal
plus one. When the holder's ordinal is at or beyond `instances`, it
starts a switchover with reason `ScaleDown` to the most caught-up
replica whose ordinal stays. A switchover for any reason picks only
such a replica. Once that replica holds the Lease, the StatefulSet
shrinks. While there are more pods than `instances`, `Ready` has
reason `ScalingDown`. Shrinking to one instance deletes
`<name>-primary`.

The StatefulSet's `persistentVolumeClaimRetentionPolicy` is
`whenScaled: Delete` and `whenDeleted: Retain`. A removed instance
takes its volume with it, as CloudNativePG does. A later scale-up
starts that instance empty, and it copies the primary's data in a full
sync, which it would do from stale data anyway. Deleting the `Redis`
leaves every volume.

The default `minReplicasToWrite` is 0 for one instance and 1 for more,
so scaling a single instance up would make its primary refuse every
write with `NOREPLICAS` until the first new replica synced. The operator
writes the value the primary runs with to `status.minReplicasToWrite`,
and the instance managers read it there, or the spec's before the
operator has written one. A new `Redis` starts at the spec's value.
When the spec's value rises, the operator raises the status value only
as far as the primary's own count of replicas that finished their sync,
which its instance manager reports as `syncedReplicas` from `INFO
replication`. A replica's report would not do: an instance that a
scale-down removed leaves a report behind for a moment, and a scale-up
right after brings its name back. Once raised, it stays when
replicas go away, because then the refusal is the guarantee. When the
spec's value falls, the status value falls at once.

## Rolling a change

The StatefulSet uses `OnDelete`, so a change to the pod template changes
no pod by itself. The operator deletes the replicas that run an old
revision one at a time, highest ordinal first, each after every pod is
ready again. A replica that is down on an old revision is deleted at
once, without waiting. Then it switches over to an updated replica, and
then it deletes the old primary. A `Redis` of one instance has no
replica to switch to, so the operator deletes its only pod. Nothing
rolls while a failover or switchover runs, while the Lease has no
holder, or while the StatefulSet has fewer pods than `instances`.

## Events

The operator posts a Kubernetes Event for each decision a person wants
to read in `kubectl describe redis`, beside the log line it already
writes. A condition says what is true now, an Event what just
happened, and the log keeps every attempt and detail.

On a `Redis`, it posts one Event for each condition transition: a
condition that is new, or whose status or reason changed. A condition
whose message alone changed, such as the count of ready instances, did
not transition. The Event's reason is the condition's reason, and its
message is `<Type> is <Status>: <message>`. It also posts one Event for
each action: `FailoverStarted` with the instance and why,
`PrimaryChosen` with its offset, `FailoverForced`, `AllInstancesEmpty`,
`SwitchoverStarted` with its cause (`PodDeleted`, `NodeCordoned`,
`ScaleDown`, or `Rollout`) in words, `SwitchoverDone`,
`RolloutStarted`, `RollingInstance` for each pod it deletes,
`CanaryPromoted`, `CanaryRolledBack`, `RolloutCompleted`,
`RolloutReverted`, `ReplicaModeStarted`, `PromotionStarted`, and
`BackupDeleted` for each Backup retention deletes. On a `Backup` it
posts `Started`, `Completed`, and `Failed`, and on a `ScheduledBackup`
`BackupCreated`.

A transition to a condition's bad state is a Warning: `Ready` False,
`DataLost`, `SwitchoverFailed`, and `RolloutFailed` True, and
`SourceReachable` and `Adopted` False. `Ready` False with
`InstancesNotReady` is Normal, because each pod of a rollout restarts
that way, and so is `NoPrimary` before the first primary took the
Lease, because each new `Redis` starts that way. A failover, the
instances all restarting empty, and a canary rolled back are Warnings.

The decisions of a pass post their Events only after the status write
lands. A write that a newer version refused lands nothing, and the next
pass decides again, so an Event posted earlier could tell of a decision
that never took effect. The pod deletes of a rollout post at once,
because the delete happened whatever the status write does.

An instance manager writes a Backup's end, so the operator learns of it
from the phase, and remembers the phase it last saw of each Backup. A
Backup it first sees after it starts is remembered without an Event, so
a restarted operator does not post again each Backup that finished
before it. This memory decides only Events.

The Events are core/v1, as liken's `kubernetes/events` package writes
them, with `reportingComponent` `redis-operator` and the pod's name as
`reportingInstance`, and no `eventTime`, so the API server does not
apply the stricter checks of events.k8s.io/v1. A post never slows or
fails a pass: it puts the Event on a bounded queue and returns, and one
goroutine writes the queue, each request under a timeout. A full queue
drops the Event, and the first drop is logged. A write that fails is
sent again, and after the last attempt the operator logs one line until
a write succeeds again. An Event in a namespace that is gone or being
deleted is dropped with no log line. A repeat of the same Event on the
same object within 10 minutes patches `count` and `lastTimestamp` on
the Event already written, as client-go's recorder does, so a
condition that flaps prints one line, such as `(x12 over 8m)`, and
writes no new object. The API server deletes an Event an hour after its
last write, so the conditions and the status, not the Events, hold
what must last. The operator needs `create` and `patch` on `events`.

## Metrics

Each instance pod runs a second container, `metrics`, with
[redis_exporter](https://github.com/oliver006/redis_exporter). It reads
its own server at `redis://127.0.0.1:6379`, with the password from
`<name>-app` in the variable `REDIS_PASSWORD`, and serves on port 9121,
the container port named `metrics`. Before this, a user ran one
redis_exporter Deployment per `Redis` pointed at `<name>-rw`, which saw
only the primary. Beside each instance, every replica reports its own
link, offset, and memory, and the series keep redis_exporter's names,
so the dashboards and alert rules written for it work unchanged.
redis_exporter reads Valkey as it reads Redis, and the e2e scenario
runs it against both engines.

The operator pins the exporter's image, which `spec.metrics.image`
overrides. The container requests a little CPU and memory and has a
memory limit, so an exporter that leaks cannot take the node's memory
from the server. `spec.metrics.enabled` defaults to true, and false
leaves the container out. `spec.metrics.checkSingleKeys` passes keys to
redis_exporter's `-check-single-keys`, joined with commas, so a user can
alert on a queue's length, such as a Celery `celery` list. The CRD
refuses a key with a comma, which would split into two.

The exporter counts toward the pod's readiness, as every container
does, so an exporter that crashes takes its pod out of the Services
until it restarts, and an exporter image that cannot be pulled holds a
roll at the first replica. That is the cost of reading each server
over the loopback from its own pod, and `enabled: false` removes it.
When a pod waits on a container other than `redis`, the canary's
failure reason names that container.

Adding the exporter, and any later change to `spec.metrics`, changes
the pod template, so every existing `Redis` rolls once through
"Rolling a change" when the operator that adds it first runs.

The instance manager adds no role label to the series. The PodMonitor
in `deploy/monitoring` copies the pod's `redis.guid.foo/role` into the
target label `redis_role`, and redis_exporter's `redis_instance_info`
already carries the server's own view as `role` (`master` or `slave`).
A query that wants the server's view on another series joins on it:

```promql
redis_memory_used_bytes * on (namespace, pod) group_left (role) redis_instance_info
```

The target label is `redis_role`, not `role`, because a target label
named `role` would rename `redis_instance_info`'s own to
`exported_role` and break the queries written for redis_exporter.

### The monitoring component

`deploy/monitoring` is a kustomize Component that a cluster with the
Prometheus Operator takes beside the base. It holds:

- a PodMonitor for the operator's port `metrics`, whose `jobLabel`
  takes the job from the pods' `app` label, so the rules find
  `job="redis-operator"` in any namespace;
- a PodMonitor for every pod with the label `redis.guid.foo/cluster`
  in every namespace, on the port `metrics`, which adds `redis` from
  that label and `redis_role` from the role label;
- a PrometheusRule with the alerts below;
- a Grafana dashboard, as a ConfigMap labeled `grafana_dashboard` in
  the namespace `monitoring`: the role of each instance, commands per
  second, memory against `maxmemory`, clients, replication offsets and
  lag, the single keys, failovers and switchovers by reason, and the
  operator's leader and reconcile errors.

| Alert | Expression | For | Why the wait |
|---|---|---|---|
| `RedisOperatorNoLeader` | `sum(redis_operator_leader) < 1`, or no series | 5m | a standby takes the Lease within a lease duration |
| `RedisRepeatedFailovers` | three or more failovers in an hour | 0 | the count is the wait |
| `RedisDown` | `redis_up == 0` or the exporter's `up == 0` | 5m | a restarting server answers within its liveness window, and a deleted pod's target leaves instead of going down |
| `RedisNoPrimary` | a `Redis` has targets and none labeled `primary` | 3m | no pod is `primary` during a switchover or a failover, for seconds |
| `RedisReplicationLinkDown` | `redis_master_link_up == 0` | 5m | every replica detaches during a failover, and a new replica's link is down until its first sync ends |
| `RedisReplicaLagging` | `redis_connected_slave_lag_seconds > 30` | 5m | a replica acknowledges every second |
| `RedisMemoryNearMaxmemory` | used over 90% of `maxmemory`, `noeviction` only | 15m | with an eviction policy, a full server is a cache doing its job |

`RedisNoPrimary` reads the role label from `up`, not `redis_up`, so a
primary whose exporter is down still counts as a primary, and
`RedisDown` reports the exporter. `RedisRepeatedFailovers` counts
across every `Redis`, because the operator's counter has no label for
the `Redis`.

## Users

`spec.users` declares ACL users beside the default user:

```yaml
spec:
  users:
    - name: app
      password: {secret: app-user, key: password}
      rules: "~app:* &app:* +@all -@dangerous"
```

`name` is a word of letters, digits, `.`, `_`, and `-`, and is not
`default`. `rules` is a string of ACL rules as `ACL SETUSER` and
`ACL LIST` write them, separated by spaces, with a selector in
parentheses kept whole. The CRD refuses a rule that sets or clears a
password (`>`, `<`, `#`, `!`, `nopass`, `resetpass`) and `on`, `off`,
and `reset`, because the operator sets those itself. A rule the CRD
cannot check, such as an unknown command, fails when the server reads
it.

The default user keeps working as before: every command, its password
in `<name>-app`, and every client that uses that Secret unchanged.

### How the users reach each server

ACL users are not replicated: each server keeps its own. Every instance
manager therefore sets the users on its own server, on replicas too,
so a replica that a failover promotes already has them. On each step
it reads each user's Secret through the API server and runs

    ACL SETUSER <name> reset on #<sha256 of the password> <rules...>

for each user whose password or rules differ from what it last set,
and `ACL DELUSER` for each user it set that the spec no longer names.
The command replaces the whole user in one step, so a client never
sees a half-changed user. A user that a person made with `ACL SETUSER`
by hand is left alone. The manager remembers what it set for as long
as the server runs, and a server that restarts starts with a new
manager, which sets every user again before the server takes a client.

`ACL LOAD` from a file the manager writes was the other way. It
replaces every user at once, the default user included, so the file
would have to hold the default user's password and any user a person
made by hand, and one rule the server refuses blocks every other
user's change. `ACL SETUSER` changes one user, and a bad rule fails
only that user.

A change applies within a step of the manager's next read, with no
restart and no roll: the users are not part of the pod template. The
manager reads the Secrets on every step, so changing a password in its
Secret takes effect the same way. Connections that already
authenticated keep their session, and new connections need the new
password. A Secret or key that is missing, an empty password, or a
rule the server refuses leaves that user as the manager last set it,
or absent, and the manager logs the reason once until it changes.

The instance managers' Role grants `get` on each user's Secret by
name, as it does for an object store's credentials.

### The operator's own connections

The instance manager, replication, the exporter, and backups keep
using the default user. They run these commands:

| Who | Commands |
|---|---|
| instance manager | `AUTH`, `INFO`, `REPLICAOF`, `CONFIG SET` (`masterauth`, `min-replicas-to-write`, `appendonly`, `port`, `tls-replication`, and the certificate files), `CLIENT PAUSE`, `CLIENT UNPAUSE`, `CLIENT KILL`, `MULTI`, `EXEC`, `DISCARD`, `FAILOVER`, `SHUTDOWN`, `ACL SETUSER`, `ACL DELUSER` |
| backups | `INFO`, `BGSAVE`, `LASTSAVE` |
| replication (`masterauth`) | `PSYNC`, `REPLCONF`, `PING` |
| redis_exporter | `INFO`, `CONFIG GET`, `CLIENT LIST`, `SLOWLOG`, `LATENCY`, `SELECT`, `SCAN`, `TYPE`, the length and value reads of `-check-single-keys`, and `EVAL` for its scripts |

A dedicated user with exactly those commands would not narrow what a
compromised pod can do. The manager writes `requirepass`, so every pod
holds the default user's password already. Most of these commands are
`@admin` or `@dangerous` anyway. And during the roll that would bring
the dedicated user in, a replica on the new revision would
authenticate as a user that the primary on the old revision does not
have, and its link would stay down.

### No connection Secret for each user

The operator makes no Secret for each user. It would have to copy a
password it does not own into a Secret of its own and keep the copy
current, which takes a watch on Secrets and write access to them. A
client reads its password from the user's own Secret, and the host and
port from `<name>-app`:
`redis://app:<password>@<name>-rw.<namespace>.svc:6379`.

## TLS

```yaml
spec:
  tls:
    issuerRef: {name: ca, kind: ClusterIssuer}   # or secret: cache-tls
    plaintext: false                              # default false
```

`tls` names either a cert-manager issuer or a Secret the user keeps,
with the keys `tls.crt`, `tls.key`, and `ca.crt`, not both.

### Certificates

With `issuerRef`, the operator applies a cert-manager Certificate
`<name>-tls`, owned by the `Redis`, whose Secret is `<name>-tls`. It
covers `<name>-rw` and `<name>-ro` as a short name, with the
namespace, with `.svc`, and with `.svc.cluster.local`, every pod's
headless name through `*.<name>-nodes` in the same forms, and
`127.0.0.1`, for `redis-cli` in the container. `kind` is `Issuer` (the
default) or `ClusterIssuer`. The issuer has to sign names inside the
cluster, as a CA or a Vault issuer does; an ACME issuer cannot.
Removing `tls`, or moving to `secret`, deletes the Certificate.

cert-manager is not needed to install the operator or to run a
`Redis` without `issuerRef`. A `Redis` that names one where
cert-manager is missing gets no Certificate. The operator logs the
failed apply with a hint, and the rest of the pass goes on, so a
running `Redis` keeps failing over while cert-manager is down. Its new
pods wait for the Secret. The operator's ClusterRole has `create`,
`patch`, and `delete` on `certificates.cert-manager.io`, which grants
nothing where the group does not exist.

A user's own Secret must cover the names its clients dial. Neither
engine checks a peer's name when it replicates, so the instances only
need the chain to verify against `ca.crt`.

### Ports

TLS is served on port 6380, named `redis-tls`, on the pods and on every
Service, beside the plaintext port 6379. `tls-auth-clients` is `no`:
clients authenticate with a password, not a client certificate.

With `plaintext: false`, the instance manager closes port 6379 with
`CONFIG SET port 0`, but only while every pod of the `Redis` serves
TLS. While any pod does not, as during the roll that turns TLS on or
off, it serves 6379 too. A replica on the old revision has no
certificate and no CA, and must still reach the primary, and an old
primary's `FAILOVER` must still reach a new replica. The manager reads
which pods serve TLS from the annotation `redis.guid.foo/tls` on each
pod, which the pod template sets, so a pod's annotation always matches
how its manager configured its server. `plaintext` is read from the
spec at run time, so changing it rolls nothing. Closing a port at run
time needs Redis 7 or Valkey 7.2 or later.

### Replication and the operator's connections

An instance manager replicates over TLS from a peer when its own pod
and the peer's both serve TLS, and over plaintext otherwise. It sets
`tls-replication` to match and names the peer's TLS port, before every
`REPLICAOF` and `FAILOVER TO`. A holder in replica mode follows its
source over plaintext; a source that requires TLS is not supported.

The instance manager connects to its own server over TLS when its pod
serves TLS, and verifies the chain against `ca.crt` without checking
the name, as the servers do: it dials its own pod's IP, which no
certificate from cert-manager names. The exporter reads
`rediss://127.0.0.1:6380` with `-skip-tls-verification`: the
connection never leaves the pod's network namespace, and verifying it
would only add a way for the exporter to fail. The operator connects
to no instance's server.

### Certificate rotation

The instance manager reads the Secret through the API server, writes
`tls.crt`, `tls.key`, and `ca.crt` to `/manager/tls/`, each by a
rename, and starts the server pointed at them. On each step it reads
the Secret again, and when it changed, it writes the files and runs
`CONFIG SET tls-cert-file ... tls-key-file ... tls-ca-cert-file ...`,
which makes both engines load the files again, even at the same paths.
New connections get the new certificate, and open ones keep the one
they started with. The manager also trusts the new `ca.crt` from then
on. Nothing restarts.

The Secret is read through the API rather than mounted, because the
kubelet updates a mounted Secret only after up to a minute, and
because a pod waiting for a missing volume says nothing in its
`Redis`'s logs. A manager whose Secret is missing logs that it waits
for it, and starts its server once it appears.

### The connection Secret

With TLS, `<name>-app` also holds `tls-port`, `tls-uri`, and
`tls-ro-uri`, with `rediss://` and port 6380. While `plaintext` is
true, `port`, `uri`, and `ro-uri` stay on 6379. With `plaintext:
false` they equal the TLS keys, since 6379 is closed. The operator
rewrites these keys whenever `tls` changes, and keeps the password.
`<name>-app` holds no CA: a client trusts the issuer's CA, and must
not mount the server's Secret, which holds its private key.

### Turning TLS on for an existing Redis

Adding `tls` changes the pod template, so every pod rolls once, and
replication keeps working throughout by the rule in "Ports". Clients
that read `uri` move to `rediss://` at once if `plaintext` is false. To
move them over without an outage:

1. Set `tls` with `plaintext: true`, and wait for the roll.
2. Point the clients at `tls-uri` with the CA.
3. Set `plaintext: false`, which closes 6379 within a step and rolls
   nothing.

Removing `tls` rolls the pods back, and each one reopens 6379 as soon
as it sees a pod without TLS.

## Tests

- **Unit, with real servers:** the RESP client and the instance manager
  against real `redis-server` and `valkey-server` processes, including
  `FAILOVER` between two of them, ACL users set on every instance and
  kept across a switchover, a password and rules changed and a user
  removed with no restart, replication and `FAILOVER` over TLS with a
  CA and certificates the tests make, the plaintext port closing and
  opening with the pods' TLS, an instance without TLS beside ones with
  it, and a certificate and CA rotated with no restart.
- **Unit, with fakes:** the operator against the fake API server, with
  `httptest` servers in place of the instance managers. It covers each
  step of failover, switchover, and rolling, and each fault in them,
  and the failover quorum for each count of instances and
  `minReplicasToWrite` in its table, with the annotation that forces it.
  It also covers the Certificate for an issuer, its deletion when `tls`
  moves to a Secret, a missing cert-manager failing only the
  Certificate, the Role granting the users' and the certificate's
  Secrets, and the connection Secret's addresses following `tls` while
  its password stays.
- **End to end, in CI:** a k3s cluster in Docker with one server and
  three agents, both engines, and a writer that confirms each `INCR`
  with `WAIT 1` and records what it confirmed. The scenarios: delete the
  primary pod; drain the primary's node; stop the operator and delete
  the primary; freeze the primary's processes for 30 seconds; drop all
  traffic to and from the primary's pod until a new primary takes over
  and for 15 seconds more; drop the same traffic except the writer's,
  so its clients still reach the old primary, which must refuse a
  write sent to its pod IP with `NOREPLICAS` or `READONLY` within 25
  seconds of the cut; change `memory` and let the operator roll it;
  scale from three instances to one while the primary is not instance
  0, and back to three; delete the pod of a one-instance `Redis` with no
  volume, which must come back empty with `DataLost`, and of one with a
  volume, which must keep its data; and read each instance's exporter
  through the API server's pod proxy, which must report `redis_up 1`
  and the role the pod's label names, before and after
  `spec.metrics.checkSingleKeys` rolls the pods, after which the
  primary's exporter must report the writer's key; declare an ACL user
  from a Secret, which must write its own keys and be refused another's
  and `CONFIG` on every instance, before and after its primary's pod is
  deleted, take a rotated password with no pod restart, and be gone
  once the spec drops it; and run a second `Redis` of two instances
  with TLS from a Secret the test makes, whose plaintext port must
  close, whose replica must follow over TLS before and after its
  primary's pod is deleted, whose exporters must read their servers
  over TLS, and which must load a rotated certificate with no pod
  restart. cert-manager does not run in the test cluster, so the
  Certificate from an issuer is tested against the fake API server
  only. A second writer
  keeps one connection through `<name>-rw` for as long as the server
  keeps it, as an application's client does. After each scenario, the test checks that the counter is
  at least the highest confirmed value, that the writer confirms new
  writes, and that the held connection was never answered `READONLY`.
  Scaling up must also draw no `NOREPLICAS`. It does not check that the
  Lease never had two holders.
