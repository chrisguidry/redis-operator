# Milestone 1: high availability

A `Redis` with three instances keeps one writable primary through a
primary crash, an operator crash, a node drain, a frozen process, and a
spec change, and loses no write that a client confirmed with `WAIT 1`.
[00-design.md](00-design.md) gives the failover design. This document
gives the resource, the objects the operator makes, the timings, and the
tests.

The milestone ships in two releases. The first release has everything
below except the items under "After the first release".

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
`SwitchoverInProgress`, `FailoverInProgress`, `NoPrimary`, and
`ScalingDown`),
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
| Secret | `<name>-app` | `password`, `host`, `port`, `uri`, `ro-uri` |
| ServiceAccount, Role, RoleBinding | `<name>` | what the instance managers may read and write |
| PodDisruptionBudget | `<name>-primary` | `minAvailable: 1` on the primary, when instances > 1; the operator deletes it when instances drops to 1, because it would block every drain of the last pod's node |
| PodDisruptionBudget | `<name>-replicas` | `minAvailable: instances - 2` on the replicas, never below 0, so one may go at a time; made for every `Redis`, including one instance |
| Lease | `<name>-primary` | held by the primary's instance manager |

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
   seconds, the operator writes the reachable instance that holds data
   and is not stopping, and has the highest offset, as `targetPrimary`.
   Ties go to the lowest ordinal. If every instance answers and none
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
   because `FAILOVER` promoted it.
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
the Lease, cannot wait for routing. It pauses writes across the
`REPLICAOF`, closes its clients in the transaction that ends the pause,
and closes them again once it reads its pod without the label and once
more after the route settle. A manager that cannot read its pod closes
them once more after the demotion hold. Clients that reconnect to it
before then see `READONLY`.

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
`PrimaryChosen` with its offset, `AllInstancesEmpty`,
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

## Tests

- **Unit, with real servers:** the RESP client and the instance manager
  against real `redis-server` and `valkey-server` processes, including
  `FAILOVER` between two of them.
- **Unit, with fakes:** the operator against the fake API server, with
  `httptest` servers in place of the instance managers. It covers each
  step of failover, switchover, and rolling, and each fault in them.
- **End to end, in CI:** a k3s cluster in Docker with one server and
  three agents, both engines, and a writer that confirms each `INCR`
  with `WAIT 1` and records what it confirmed. The scenarios: delete the
  primary pod; drain the primary's node; stop the operator and delete
  the primary; freeze the primary's processes for 30 seconds; drop all
  traffic to and from the primary's pod until a new primary takes over
  and for 15 seconds more; change `memory` and let the operator roll it;
  scale from three instances to one while the primary is not instance
  0, and back to three; delete the pod of a one-instance `Redis` with no
  volume, which must come back empty with `DataLost`, and of one with a
  volume, which must keep its data; and read each instance's exporter
  through the API server's pod proxy, which must report `redis_up 1`
  and the role the pod's label names, before and after
  `spec.metrics.checkSingleKeys` rolls the pods, after which the
  primary's exporter must report the writer's key. A second writer
  keeps one connection through `<name>-rw` for as long as the server
  keeps it, as an application's client does. After each scenario, the test checks that the counter is
  at least the highest confirmed value, that the writer confirms new
  writes, and that the held connection was never answered `READONLY`.
  Scaling up must also draw no `NOREPLICAS`. It does not check that the
  Lease never had two holders.

## After the first release

These stay in this milestone and ship in its second release:

- ACL users declared from Secrets, and TLS with certificates from
  cert-manager.
- The failover quorum: promote only after reaching enough replicas to
  see every write confirmed with `WAIT n`.
- A network partition test in which the primary's clients can still
  reach it. The partition scenario in `e2e/scenarios.sh` drops all of
  the pod's traffic, clients included.
