---
name: troubleshoot
description: "Diagnose a Redis resource from its conditions, status.instances, the operator's log, and the instance managers' logs; run redis-cli inside a pod; move the primary off a pod with the redis.guid.foo/switchover-requested annotation; and work through drains, PodDisruptionBudgets, failovers, and a Backup stuck Running. Use when a Redis is not Ready, a client gets READONLY or NOREPLICAS, a drain hangs, or the primary will not move."
---

Before the first command, run `kubectl config current-context` and confirm that it names the cluster the person means.

# Troubleshoot a Redis

Read in this order: the `Ready` condition and the other conditions,
then `status.instances`, then the log of the operator's leader, then
the log of the instance manager in the pod the status points at.

The examples use the `Redis` `cache` in the namespace `example`.

```sh
kubectl -n example get redis cache -o wide
kubectl -n example get redis cache -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.reason}: {.message}{"\n"}{end}'
kubectl -n example get redis cache -o jsonpath='{range .status.instances[*]}{.name} {.role} ready={.ready} offset={.offset}{"\n"}{end}'
kubectl -n example get pods -l redis.guid.foo/cluster=cache -L redis.guid.foo/role -o wide
```

## The conditions

| Type | Status | Reason | Meaning |
|---|---|---|---|
| `Ready` | True | `AllInstancesReady` | a primary holds the Lease and every instance is ready |
| `Ready` | True | `SwitchoverInProgress` | a planned switchover is moving the primary; `status.switchover` says from, to, and why |
| `Ready` | True | `ScalingDown` | pods beyond `spec.instances` are being removed |
| `Ready` | False | `InstancesNotReady` | fewer instances are ready than `spec.instances` |
| `Ready` | False | `NoPrimary` | no instance holds the Lease `cache-primary` |
| `Ready` | False | `FailoverInProgress` | `status.targetPrimary` is `pending`: the primary failed and the operator is choosing the next one |
| `SwitchoverFailed` | True | `Timeout` | the target did not take the primary role within 30 seconds; the old primary stays, and the operator tries again after 30 seconds |
| `SwitchoverFailed` | False | `Started` | the last switchover started; this is the normal state after one |
| `DataLost` | True | `AllInstancesEmpty` | every instance restarted with no data, so the data set started empty on instance 0 |
| `RolloutFailed` | True | `CanaryFailed` | a new image failed on the canary and was rolled back; see the `upgrade` skill |
| `RolloutFailed` | False | `Completed`, `Reverted` | the last rollout finished, or `spec.image` went back |
| `SourceReachable` | True/False | `Reachable`, `Unreachable` | replica mode only: whether the operator reaches the source |
| `Adopted` | True/False | `Promoted`, `AlreadyPrimary`, `CatchUpTimedOut`, `PauseRefused`, `SourceRefused`, `SourceUnreachableFromOperator`, `PasswordUnreadable` | replica mode; see the `adopt-an-existing-redis` skill |

`DataLost` stays `True` once set; it records that the data set started
again, not that it is still missing.

## status.instances

Each entry is what that pod's instance manager last reported:

| `role` | Meaning |
|---|---|
| `primary` | the server is a master; it is ready only while it holds the Lease |
| `replica` | the server follows another; it is ready only while its replication link is up |
| `detached` | the server is a replica of nothing, refuses writes, and keeps its data: during a failover, while no instance holds the Lease, or after it fenced itself |
| `starting` | the instance manager has not answered yet, or cannot reach its server |

`offset` is the replication offset. A replica far behind the primary's
offset is still syncing or has a broken link.

## Logs

The operator's leader holds the Lease `redis-operator` in the
operator's namespace; the part of the holder before the underscore is
the pod name.

```sh
leader=$(kubectl -n redis-operator get lease redis-operator -o jsonpath='{.spec.holderIdentity}' | cut -d_ -f1)
kubectl -n redis-operator logs "$leader" | grep 'redis example/cache'
```

The operator logs each decision: failing over and why, the next
primary and its offset, each switchover and its reason, each pod it
rolls, and each backup. A fault that repeats on every pass is logged
once.

Each pod's instance manager logs to the `redis` container, beside the
server's own log:

```sh
kubectl -n example logs cache-0 -c redis
kubectl -n example logs cache-0 -c redis --previous   # after a restart
```

It logs each role change: `took the primary lease`, `primary`,
`following cache-1`, `holding writes until the primary label is off
this pod`, `handing the primary role to cache-1`, `stepping down, a
failover is pending`, `stepping down, a failover named cache-1`, and
`fencing`. `disconnecting the clients of the demoted primary: <error>`
means closing the old primary's clients failed.

## Run redis-cli in a pod

The `redis` container has the server's own tools and the password in
the variable `REDIS_PASSWORD`. Single-quote the command so the
variable expands inside the pod:

```sh
kubectl -n example exec cache-0 -c redis -- sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli INFO replication'
```

The container sets `REDIS_PASSWORD`, not `REDISCLI_AUTH`, so the
command sets `REDISCLI_AUTH` from it. With `engine: valkey`, use
`valkey-cli`; it reads `REDISCLI_AUTH` too. The server listens on
127.0.0.1 and on the pod's IP. Only release 2026.11.01-001 listens on
the pod's IP alone; there, add `-h "$POD_IP"`.

`INFO replication` shows `role`, `master_host`, `master_link_status`,
and `master_repl_offset`. A replica of `192.0.2.1` is detached: that
address is reserved for documentation and nothing answers there.

## A client gets a closed or refused connection, or READONLY

A pod carries the label `redis.guid.foo/role: primary` only while it
holds the Lease and is `status.targetPrimary`. When the operator names
another primary, or starts a failover, it takes the label off the old
primary's pod at once, so `cache-rw` has no endpoint, and new
connections are refused, until the new primary takes the Lease.

In a planned switchover, the old primary pauses its clients' writes
while it is still primary, waits until its pod has lost the label and
5 seconds more for the route to settle, closes its normal and pub/sub
client connections, runs `FAILOVER`, and closes them again as it ends
the pause. It holds the writes for at most 30 seconds. A held write
ends with a closed connection, not `READONLY`.

A primary that fenced itself, or found another instance holding the
Lease, cannot wait: it changes its role at once and closes its clients,
then closes them again once its pod has lost the label and after the
route settle. A client that reconnects to it in between gets
`READONLY`.

So a client must reconnect after a closed connection and retry refused
connections for the length of a switchover. A client that connects to
a pod by its own address, not through `cache-rw`, gets `READONLY` from
a replica. Connect through `cache-rw`.

During adoption, `cache-rw` routes to a replica of the source, so
every write gets `READONLY` until the promotion.

## A client gets NOREPLICAS

With more than one instance, the primary runs with
`min-replicas-to-write`, 1 by default, and refuses writes when fewer
replicas have acknowledged it within 10 seconds. That stops a primary
cut off from its replicas from taking writes, even when its instance
manager is stalled. Check that the replicas are `ready` in
`status.instances`. The value the primary runs with is
`status.minReplicasToWrite`; after a scale-up the operator raises it
only as replicas sync, and it does not lower it when replicas go away.
During a failover the replicas detach first, so the old primary
refuses writes with `NOREPLICAS` while it steps down.

## Move the primary off a pod

Annotate the primary's pod. The operator switches over to the best
ready replica on a schedulable node, with the reason `PodDeleted`:

```sh
kubectl -n example annotate pod cache-0 redis.guid.foo/switchover-requested="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

The operator clears the annotation once the pod is a replica, so a
later term as primary does not start another switchover. The instance
manager sets the same annotation itself when the primary's pod is
deleted, and keeps serving until the operator has moved the Lease, or
for 20 seconds.

The other switchover reasons are `NodeCordoned`, `Rollout`, and
`ScaleDown`. The operator logs `a switchover off cache-0 is needed
(...), and no replica is ready to take it` when no replica qualifies. A replica
qualifies when it is ready, not being deleted, on a node that is not
cordoned, not annotated itself, following the primary with its link
up, within `spec.instances` after a scale-down, and, for a rollout,
on the new revision.

## Drains and PodDisruptionBudgets

- `cache-primary` keeps `minAvailable: 1` on the pod with the primary
  label, when `instances` is more than 1. An eviction cannot take the
  primary.
- `cache-replicas` lets one replica go at a time.

When the primary's node is cordoned, the operator switches over to a
replica on another node, with the reason `NodeCordoned`. The old
primary then carries the replica label, and the drain evicts it.

A drain that waits on `cache-primary` means the switchover has not
happened. Look for the switchover in the operator's log and for
`SwitchoverFailed`. Common causes: every replica is on a cordoned node
or not ready, or the operator is not running. With one instance there
is no `cache-primary` budget; a drain evicts the only pod, and the
`Redis` serves nothing until it is back.

A replica that cannot be placed after a drain stays `Pending`, because
no two instances share a node. The `Redis` then reports
`InstancesNotReady` until a node is free.

## A failover

The operator fails over when the primary's instance manager has not
answered for 6 seconds, when no instance has held the Lease for 21
seconds, or when the target restarted with no data. It writes
`targetPrimary: pending`, every instance detaches, and after every
reachable instance reports detached, or 5 seconds, it names the
reachable instance with data and the highest offset. That instance
takes the Lease once the old holder releases it, or once it has gone
15 seconds without a renewal. A
failover takes about 20 seconds.

Writes that no replica acknowledged before the failure can be lost;
replication is asynchronous. Writes a client confirmed with `WAIT 1`
survive. `redis_operator_failovers_total` counts failovers.

A failover that does not finish (`FailoverInProgress` for long) means
no reachable instance holds data. Check that the pods run and that the
operator reaches them on port 9123: a NetworkPolicy that blocks the
operator makes every instance look silent. See the `install` skill.

## A primary that fenced itself

A primary that cannot renew its Lease for 10 seconds asks its peers on
port 9123 which instance should be primary. If a peer cannot be
reached or names another, it detaches its server and closes its
clients; its log says `fencing`. If every peer agrees, the API server
alone is down, and it keeps serving. A primary that fences while the
API server is reachable usually means its peers cannot reach it on
9123.

## A Backup stuck Running

The operator marks a `Backup` `Failed` after an hour, or at once when
the instance that takes it has lost its pod or its pod was made again.
Read the instance's log for the upload. The `backups` skill covers the
store, credentials, and restore failures.

## The operator restarts or does nothing

```sh
kubectl -n redis-operator get pods -l app=redis-operator
kubectl -n redis-operator logs "$leader" --tail=50
```

The operator keeps no state of its own; a new leader starts from what
each `Redis`'s status holds. `redis_operator_reconcile_errors_total`
rising means a pass over some `Redis` fails; the log says why, once.
A missing permission shows as a `403` in the log; the ClusterRole in
`deploy/rbac.yaml` lists what the operator needs.
