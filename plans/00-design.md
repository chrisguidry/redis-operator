# redis-operator design

redis-operator runs Redis and Valkey on Kubernetes from one resource that
declares how big and how available the data should be. It is modeled on
[CloudNativePG](https://cloudnative-pg.io) (CNPG): the operator decides
which instance is the primary, records the decision through the
Kubernetes API, and an instance manager inside each pod carries the
decision out and fences its own Redis when it cannot confirm it.

This document gives the decisions that every milestone builds on and the
evidence behind each one. The milestones are numbered after it.

## Resources

The API group is `redis.guid.foo`, version `v1alpha1`.

- `Redis` is one primary and zero or more replicas. `instances: 1` is a
  single Redis, and `instances: 3` is a primary with two replicas.
  Scaling from one to three is an edit, not a migration.
- `RedisCluster` is sharded Redis Cluster. It is not built yet. It is a
  kind of its own because sharding changes the client contract: a client
  must follow `MOVED` and `ASK` redirects, and a command across slots
  fails with `CROSSSLOT`. No router hides that from clients, so a
  `Redis` cannot become a `RedisCluster` in place, and each topology
  fails over through different code.
- `Backup` is one snapshot of a `Redis`, and `ScheduledBackup` makes
  `Backup`s on a cron schedule ([02-backups.md](02-backups.md)). Both
  name their `Redis` in `spec.redis`.
- Both data kinds take `engine: redis | valkey`. Redis 8 and Valkey
  speak the same protocol, and the operator uses only commands that both
  have.

Two kinds risk the drift that the four kinds of
[OT-CONTAINER-KIT/redis-operator](https://github.com/OT-CONTAINER-KIT/redis-operator)
show, where a feature reaches one kind and not another (issues #1462,
#1542). The defenses: both kinds embed the same Go structs for the
engine, storage, auth, TLS, and memory; a test checks that both CRDs
expose them; children of each kind get names that cannot collide, the
problem valkey-operator met in issue #531; and a `Backup` will name its
source as `{kind, name}` once a second kind exists. Today it names a
`Redis` by `spec.redis`, because only one kind exists.

The decision to revisit: if sharded mode also runs under the operator's
authority (`cluster-replica-no-failover yes` plus
`CLUSTER FAILOVER TAKEOVER`), both kinds share one failover engine, and
one kind with an optional `cluster` block becomes the better shape.

## Who decides failover

The operator decides. Sentinel does not run.

Five adversarial reviews argued the question, three on failover and two
on the kinds. The reviews for operator-decided failover and for the
best hybrid converged on CNPG's design. The review for Sentinel
identified the two costs of that design, which this document accepts
below.

The case against Sentinel, from a production incident and from
`src/sentinel.c`:

- Sentinel resolves hostnames inside its single event loop
  (`createSentinelAddr` calls `anetResolve`, a blocking `getaddrinfo`).
  A stalled lookup delays the loop past the TILT trigger, and a
  sentinel in TILT runs no part of a failover for 30 seconds
  (`sentinelCheckTiltCondition`). In the incident, the sentinel that
  led a failover promoted a replica, then entered TILT before it
  published the new epoch.
- Agreement is gossip plus epochs. A second sentinel that had not seen
  the new epoch saw a replica claiming to be master while the old master
  still looked sane, and it ran `+convert-to-slave` on the new master.
  There was no master for about a minute.
- Nothing fences an old master. Sentinel promotes with a bare
  `REPLICAOF NO ONE` and never pauses the old one. Jepsen measured
  Sentinel losing acknowledged writes under partition
  ([Call me maybe: Redis](https://aphyr.com/posts/283-call-me-maybe-redis)).
- Sentinel's coordinated failover, which closes the epoch gap for a
  planned switchover, exists only in Valkey 9.0 and later. Redis 8 does
  not have it.

The design instead:

1. **The decision is a record in the API.** `status.targetPrimary` on
   the `Redis` names the instance that should be primary. The operator
   changes it in two steps when the primary fails: first to `pending`,
   so every instance stops following the old primary, then to the
   replica with the highest replication offset. Writes to the status are
   guarded by `resourceVersion`, so a stale operator's write fails.
2. **The primary holds a Lease.** An instance may run as primary only
   while it holds the `<name>-primary` Lease. An instance that is the
   target takes the Lease when it is empty, or after it has watched the
   Lease's renew time stay unchanged for the lease duration on its own
   monotonic clock, so clock skew between machines does not shorten the
   wait. This is CNPG's primary lease
   (`internal/cmd/manager/instance/run/lease/`).
3. **The primary fences itself.** A primary that cannot renew its Lease
   for the renew deadline asks its peers. If a peer cannot be reached or
   names a different target, it detaches: it points `REPLICAOF` at an
   unreachable address, which makes its Redis a read-only replica that
   keeps its data, and it closes its client connections. If every peer
   agrees with it, it keeps serving, so an API server outage alone stops
   no healthy primary.
4. **Redis fences itself too.** With replicas, every instance runs with
   `min-replicas-to-write` and `min-replicas-max-lag`, so a primary that
   loses its replicas refuses writes even if its instance manager is
   stalled (`checkGoodReplicasStatus` in `src/replication.c`).
5. **No instance starts as primary unless it holds the Lease.** An
   instance that starts without it starts as a replica of the Lease
   holder, or detached when there is none. An instance that restarts
   with no data on disk, because it has no persistent storage or lost
   its volume, releases the Lease it holds instead of serving an empty
   dataset as primary. When every instance restarts with no data, the
   operator starts an empty dataset on instance 0 and sets the
   condition `DataLost`.
6. **A planned switchover uses Redis's own `FAILOVER`.** The operator
   moves `targetPrimary` to a replica. The primary's instance manager
   runs `FAILOVER TO <ip> <port> TIMEOUT <ms>`, which pauses writes,
   waits until the replica has acknowledged the primary's whole offset,
   and only then swaps the roles. If the timeout passes, Redis aborts
   and nothing changes. Both engines have this command.
7. **A drain triggers the switchover before eviction.** A
   PodDisruptionBudget with `minAvailable: 1` covers the primary, so an
   eviction cannot take it. The operator watches Nodes, and when the
   primary's node is cordoned it switches over to a replica on a node
   that is not. The old primary is then a replica, and the eviction
   proceeds. A primary pod deleted directly asks for the same
   switchover: its instance manager gets SIGTERM, annotates its pod with
   `redis.guid.foo/switchover-requested`, and keeps serving until the
   operator has moved the Lease or 20 seconds have passed.
8. **Clients connect through a Service.** `<name>-rw` selects the pod
   that holds the Lease while it is also the target, by a role label the
   operator writes, and `<name>-ro` selects the replicas. Any Redis
   client works. A demoted Redis answers writes with `READONLY`, which
   clients take for an application error, so a primary that steps down
   closes its clients' connections, as Sentinel does, and they
   reconnect through the Service. It does so only after the label has
   left its pod, or they would reconnect to it.
   [01-high-availability.md](01-high-availability.md) gives the steps.

### What this design costs

- **Detection is slower than Sentinel's.** The operator polls each
  instance manager, and an unplanned failover waits for the failed
  primary's Lease to expire. Failover takes about 20 seconds. Sentinel
  takes 5 to 8. The operator never waits for Kubernetes to mark a node
  NotReady, which takes 40 to 55 seconds.
- **No failover while the API server is down.** A primary that its
  peers agree on keeps serving, but a primary that dies during the
  outage stays dead until the API server returns. CNPG makes the same
  choice.
- **Our code decides.** The Lease protocol follows CNPG's, which CNPG
  checks with a TLA+ model (`tla/primary-lease`). Its safety holds only
  for bounded clock skew between machines.

### What users can rely on

- A planned switchover, for a drain or a spec change, loses no write.
- After an unplanned failover, every write that the client confirmed
  with `WAIT n`, for `n` at least `minReplicasToWrite` and at least 1,
  survives. The failover names the next primary only once it has
  reached enough instances that one of them holds every such write,
  unless a person forces it
  ([01-high-availability.md](01-high-availability.md), "The failover
  quorum"). Writes that no replica acknowledged can be lost: Redis
  replicates asynchronously, and no design changes that.
- Two instances never accept writes at once for longer than about the
  lease duration.
- If the primary's instance manager and a replica's instance manager
  stall at the same time, the stalled replica does not count toward the
  quorum. When the instances that answer might lack a confirmed write
  without it, as with three instances and `WAIT 1`, the failover waits,
  and the Redis has no primary until a stalled manager answers again.

## The instance manager

The operator's image holds one binary. An init container copies it into
an `emptyDir`, and the Redis container runs it as its entrypoint in
front of the stock `redis-server` or `valkey-server` from the engine's
own image. Users keep the images their engine publishes.

The instance manager:

- reads the `Redis` and the Lease from the API, writes the Redis
  configuration, and starts and supervises the server;
- carries out the role that `targetPrimary` and the Lease give it, as
  described above;
- sets the `Redis`'s ACL users on its own server, and keeps its TLS
  certificate loaded, both read from Secrets on every step
  ([01-high-availability.md](01-high-availability.md));
- serves its state on port 9123: `/status` for the operator, `/ready`
  for the readiness probe, `/healthz` for the liveness probe, and
  serves neither `/status` nor a ready `/ready` from a report taken
  before its process was paused
  ([01-high-availability.md](01-high-availability.md));
- takes the snapshot of a running `Backup` that names it, restores a new
  `Redis` from a snapshot before its server starts, and makes the Lease
  holder follow the source in replica mode
  ([02-backups.md](02-backups.md), [03-adoption.md](03-adoption.md)).

It talks to Redis over RESP with a small client in this repository, and
to the API server with the same hand-written client as the operator.

## The operator

The operator runs two replicas that elect a leader through a Lease, as
healthchecks-operator does. It talks to the API server over HTTP with
no client-go and no controller framework. It reconciles from watches of
`Redis`, Pods, Leases, StatefulSets, Nodes, `Backup`s, and
`ScheduledBackup`s, plus a timer every 2 seconds that polls each
instance manager's `/status`. It serves Prometheus metrics, prefixed
`redis_operator_`, on port 9200, or on `METRICS_PORT`. `CANARY_SOAK` and
`CANARY_TIMEOUT` override the rollout timings of
[04-upgrades.md](04-upgrades.md).

For each `Redis` it applies a StatefulSet with `podManagementPolicy:
Parallel` and `updateStrategy: OnDelete`. The StatefulSet gives stable
pod names and volume claims. The operator decides which pod restarts and
when, so a change rolls the replicas first, then switches over, then
rolls the old primary.

Conditions follow the Kubernetes rule that liken's `conditions.Set`
keeps: `lastTransitionTime` moves only when the status changes. The
operator logs each decision. It posts no Events yet.

## Testing

Tests run against real servers and fakes of real servers, never mocks:
real `redis-server` and `valkey-server` processes for the RESP client
and the instance manager, a fake Kubernetes API server for the operator,
and `httptest` servers that serve an instance manager's `/status`.

The end-to-end suite (`make e2e`) runs a k3s cluster of four nodes in
Docker, in CI, against each engine. Its chaos tests kill the primary,
stop the operator and kill the primary, drain the primary's node, freeze
the primary's processes for longer than the lease duration, and
partition the primary from the API server and its peers, while a writer
confirms each write with `WAIT 1`. Each test checks that the counter is
at least the highest confirmed value and that the writer confirms new
writes again. No test yet checks that two instances never accepted
writes at once.

## Milestones

1. [High availability](01-high-availability.md).
2. [Backups](02-backups.md) to object storage or a volume, and restore
   into a new `Redis`.
3. [Adoption](03-adoption.md): replicate from an existing Redis or
   Valkey, then promote.
4. [Upgrades and downgrades](04-upgrades.md), staged so they lose no
   data.
