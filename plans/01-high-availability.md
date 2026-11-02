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
  minReplicasToWrite: 1   # default 1 when instances > 1, else 0
  cpu: 100m               # the container's CPU request; there is no CPU limit
  nodeSelector: {}
  tolerations: []
  priorityClassName: ""
status:
  observedGeneration: 3
  targetPrimary: cache-0
  currentPrimary: cache-0
  readyInstances: 3
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
`SwitchoverInProgress`, `FailoverInProgress`, and `NoPrimary`),
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
| PodDisruptionBudget | `<name>-primary` | `minAvailable: 1` on the primary, when instances > 1 |
| PodDisruptionBudget | `<name>-replicas` | `minAvailable: instances - 2` on the replicas, never below 0, so one may go at a time; made for every `Redis`, including one instance |
| Lease | `<name>-primary` | held by the primary's instance manager |

Each pod carries the label `redis.guid.foo/cluster` with the resource's
name. The operator writes the label `redis.guid.foo/role` on each pod,
from the Lease: `primary` on the holder, `replica` on the others. The
Services and the budgets select on it.

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
2. The primary's instance manager runs
   `FAILOVER TO <ip> 6379 TIMEOUT 10000`, waits until its Redis reports
   the replica role, and releases the Lease.
3. The new primary takes the empty Lease. It is already a master,
   because `FAILOVER` promoted it.
4. If the Lease holder has not changed 30 seconds after the switchover
   started, the operator writes the old primary back as `targetPrimary`
   and sets the condition `SwitchoverFailed`. It tries again after a
   30-second backoff.

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
  and for 15 seconds more; change `memory` and let the operator roll it.
  After each, the test checks that the counter is at least the highest
  confirmed value and that the writer confirms new writes. It does not
  check that the Lease never had two holders.

## After the first release

These stay in this milestone and ship in its second release:

- ACL users declared from Secrets, and TLS with certificates from
  cert-manager.
- A Redis metrics exporter beside each instance. The operator's own
  metrics are built (`redis_operator_` series on port 9200).
- The failover quorum: promote only after reaching enough replicas to
  see every write confirmed with `WAIT n`.
- A network partition test in which the primary's clients can still
  reach it. The partition scenario in `e2e/scenarios.sh` drops all of
  the pod's traffic, clients included.
- Scaling down when the primary's ordinal is beyond the new count,
  through a switchover first.
- An Event for each condition transition, as liken's `events` package
  posts them. The first release logs each decision instead.
