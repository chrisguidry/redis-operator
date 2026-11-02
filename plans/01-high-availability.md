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
  resources:
    requests: {cpu: 100m}
  nodeSelector: {}
  tolerations: []
  priorityClassName: ""
status:
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

`maxmemory` is 70% of `memory`. A snapshot or a full resync forks the
server, and the pages that change during the fork are copied, so the
container needs headroom above `maxmemory` or the kernel kills it.

`engine` is immutable. Changing it would mean loading one engine's data
files into the other.

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
| PodDisruptionBudget | `<name>-replicas` | `maxUnavailable: 1` on the replicas |
| Lease | `<name>-primary` | held by the primary's instance manager |

The operator writes the label `redis.guid.foo/role` on each pod, from the
Lease: `primary` on the holder, `replica` on the others. The Services
and the budgets select on it.

Pods carry a required anti-affinity on `kubernetes.io/hostname`, so no
two instances share a machine.

## Timings

| Setting | Value | Why |
|---|---|---|
| Lease duration | 15 s | CNPG's and client-go's default |
| Renew deadline | 10 s | a primary fences itself before its Lease can expire |
| Renew period | 2 s | |
| Operator poll of `/status` | 2 s | |
| Failure threshold | 6 s | three missed polls before a failover starts |
| Switchover timeout | 10 s | the `FAILOVER ... TIMEOUT` the primary runs |
| `min-replicas-max-lag` | 10 s | |
| Termination grace | 30 s | a deleted primary waits up to 20 s for its switchover |

## Failover, step by step

1. The operator cannot reach the primary's `/status` for the failure
   threshold, or the primary pod is gone or not running.
2. It writes `targetPrimary: pending`. Every instance manager that sees
   it detaches its Redis, so no replica keeps following an old primary
   that might still accept writes, and their offsets stop moving.
3. After every reachable replica reports that it detached, or after 5
   seconds, the operator writes the reachable replica with the highest
   offset as `targetPrimary`. Ties go to the lowest ordinal.
4. That replica's instance manager takes the Lease once it is empty, or
   once its renew time has not changed for the lease duration, then runs
   `REPLICAOF NO ONE`.
5. The operator moves the role labels to the new holder. The other
   instances follow the holder.

## Switchover, step by step

A switchover starts when the primary's node is cordoned, when a pod
asks for one because it is being deleted, or when the operator rolls a
spec change.

1. The operator picks a ready replica on a schedulable node whose
   replication link is up, preferring the smallest lag, and writes it as
   `targetPrimary`, with `status.switchover` saying why.
2. The primary's instance manager runs
   `FAILOVER TO <ip> 6379 TIMEOUT 10000`, waits until its Redis reports
   the replica role, and releases the Lease.
3. The new primary takes the empty Lease. It is already a master,
   because `FAILOVER` promoted it.
4. If the Lease holder has not changed 30 seconds after the switchover
   started, the operator writes the old primary back as `targetPrimary`
   and sets the condition `SwitchoverFailed`. It tries again after a
   backoff.

## Rolling a change

The StatefulSet uses `OnDelete`, so a change to the pod template changes
no pod by itself. The operator deletes the replicas that run an old
revision one at a time, each after the one before is ready and caught
up. Then it switches over to an updated replica, and then it deletes the
old primary.

## Tests

- **Unit, with real servers:** the RESP client and the instance manager
  against real `redis-server` and `valkey-server` processes, including
  `FAILOVER` between two of them.
- **Unit, with fakes:** the operator against the fake API server, with
  `httptest` servers in place of the instance managers. It covers each
  step of failover, switchover, and rolling, and each fault in them.
- **End to end, in CI:** a kind cluster with three workers, both
  engines, and a writer that confirms each `INCR` with `WAIT 1` and
  records what it confirmed. The scenarios: delete the primary pod;
  drain the primary's node; stop the operator and delete the primary;
  freeze the primary's processes for 30 seconds; change `memory` and let
  the operator roll it. After each, the test checks that every confirmed
  value is present and that the Lease never had two holders.

## After the first release

These stay in this milestone and ship in its second release:

- ACL users declared from Secrets, and TLS with certificates from
  cert-manager.
- A Redis metrics exporter beside each instance, and the operator's own
  metrics.
- The failover quorum: promote only after reaching enough replicas to
  see every write confirmed with `WAIT n`.
- A network partition test, which isolates the primary's node from the
  API server and its peers while its clients can still reach it.
- Scaling down when the primary's ordinal is beyond the new count,
  through a switchover first.
