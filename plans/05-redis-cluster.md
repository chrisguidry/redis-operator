# Milestone 5: RedisCluster

A `RedisCluster` runs Redis Cluster or Valkey Cluster: the keyspace's
16384 hash slots split across several shards, each shard a primary and
its replicas. It is for data that does not fit in one server's memory,
or write load that one primary cannot carry. This document is a design
for review. Nothing in it is built, and the questions at the end need
the maintainer's call before the build starts.

[00-design.md](00-design.md) made `RedisCluster` a kind of its own,
because sharding changes the client contract: a client must follow
`MOVED` and `ASK` redirects, and a command across slots fails with
`CROSSSLOT`. It left one decision open: if sharded mode also failed
over under the operator's authority, through
`cluster-replica-no-failover yes` and `CLUSTER FAILOVER TAKEOVER`, the
two kinds would share one failover engine, and one kind with an
optional `cluster` block would be the better shape. This plan
recommends that the cluster's own election decide an unplanned
failover. The two kinds then fail over through different code, and the
two-kind decision stands.

## Terms

The repository already uses "instance" for one pod's server and "node"
for a Kubernetes node. This document keeps both meanings. A cluster
member's identity is its **node ID**, the 40-character hex name that
the engine generates on first start and keeps in its cluster
configuration file, `nodes.conf`. A **shard** is the set of instances
that serve one range of slots: a primary and its replicas.

## How the engines behave

These are the facts the design depends on. The sources are listed at
the end.

- **The cluster bus.** Every instance listens on a second port, the
  client port plus 10000 unless `cluster-port` says otherwise. The
  instances exchange heartbeats and gossip over it. An instance joins
  another's view with `CLUSTER MEET <ip> <port>`, and gossip spreads
  membership from there.
- **Node IDs survive address changes.** An instance keeps its node ID
  for as long as its `nodes.conf` survives. The cluster spec says an
  instance can change its IP address without changing its node ID, and
  gossip updates the address. When an instance receives a `MEET` to a
  new address and the reply comes from a node ID it already knows, it
  updates that node's address instead of adding a node (the handshake
  branch of `clusterProcessPacket` in `src/cluster_legacy.c`).
- **Epochs.** `currentEpoch` is a cluster-wide logical clock.
  `configEpoch` versions one primary's claim to its slots. When two
  claims to a slot conflict, the higher `configEpoch` wins: the spec's
  "last failover wins" rule. Both epochs are written and fsynced to
  `nodes.conf` before an instance acts on them.
- **Unplanned failover is a vote.** An instance marks a peer `PFAIL`
  when it has not answered for `cluster-node-timeout`, and `FAIL` when a
  majority of primaries report it. A replica of a `FAIL`ed primary waits
  500 ms, plus up to 500 ms at random, plus 1000 ms for each replica of
  the same primary with a higher replication offset than its own, then
  asks every primary for a vote in a new epoch. A primary votes once per
  epoch, records the vote on disk, and votes only for a replica whose
  primary it sees as `FAIL`. A replica that wins a majority takes its
  primary's slots under the new epoch.
- **The minority stops writing.** A primary that cannot reach a majority
  of primaries for `cluster-node-timeout` refuses writes. A failover
  needs the old primary to be unreachable from the majority for at
  least that long, so a partition shorter than the timeout loses no
  write.
- **Planned failover.** `CLUSTER FAILOVER`, run on a replica, makes its
  primary stop client writes and report its offset, waits until the
  replica has that offset, then wins an epoch from a majority of
  primaries and takes the slots. The old primary then answers its
  clients with `MOVED` to the new one. If the replica does not finish
  in time (5 seconds, which Valkey 8.1 and later read from
  `cluster-manual-failover-timeout`), the primary resumes and nothing
  changes. `FORCE` skips the handshake with the primary but still needs
  the majority. `TAKEOVER` needs no majority: the replica raises its
  own epoch and claims the slots, which the docs say breaks "last
  failover wins" and should be used with care.
- **Replica migration.** By default a replica moves to a primary that
  has none left, and a primary that loses its last slot becomes a
  replica of the instance that took them. `cluster-allow-replica-migration
  no` turns off both.
- **Coverage.** With `cluster-require-full-coverage yes`, the default,
  every instance refuses queries while any slot has no serving primary.
- **Announcing an address.** `cluster-announce-ip`, `-port`,
  `-tls-port`, and `-bus-port` set the address an instance publishes in
  its bus packets. `cluster-announce-hostname` publishes a hostname for
  clients, and `cluster-preferred-endpoint-type` (`ip`, `hostname`, or
  `unknown-endpoint`) chooses which one `MOVED` and `CLUSTER SLOTS`
  return. The bus itself connects by IP.
- **Moving slots, the old way.** `CLUSTER SETSLOT <slot> IMPORTING` on
  the target and `MIGRATING` on the source, then `MIGRATE` key by key,
  then `CLUSTER SETSLOT <slot> NODE` on every instance. Clients receive
  `ASK` for a key that has moved while its slot is in flight. A crash
  partway through leaves slots in the `MIGRATING` and `IMPORTING`
  states for a tool to repair.
- **Moving slots, atomically.** Both engines now move a slot range as
  one operation: a forked snapshot of the slots streams to the target,
  then the writes made since, then the source pauses writes briefly and
  hands the slots over. Clients see `MOVED` only after the handover. The
  commands differ. Valkey 9.0 runs `CLUSTER MIGRATESLOTS` on the source
  and reports progress through `CLUSTER GETSLOTMIGRATIONS`. Redis 8.4
  runs `CLUSTER MIGRATION IMPORT` on the target and reports through
  `CLUSTER MIGRATION STATUS`; the source pauses writes for at most
  `cluster-slot-migration-write-pause-timeout`, then gives up and
  resumes.
- **Forgetting a node.** `CLUSTER FORGET <id>` removes a node from one
  instance's table and bans its ID there for 60 seconds, so every
  instance has to forget it within that time or gossip brings it back.
- **Loading data.** A primary that starts with keys in slots no one owns
  claims those slots, and deletes keys in slots that another node owns
  (`verifyClusterConfigWithData` in Redis's `src/cluster.c`).
- **Databases.** Redis Cluster serves database 0 only. Valkey 9 has
  `cluster-databases`, which defaults to 1.
- **An open bus is refused.** The bus has no password. Recent Redis
  patch releases (8.2.11, 8.10.2, and later) refuse to start a cluster
  instance unless `tls-cluster yes` authenticates the bus or
  `cluster-bus-port-protected-mode no` waives the check. Valkey 9.0.7
  has no such check. An older Redis refuses to start with a directive
  it does not know, so the instance manager has to know the server's
  version before it writes that line.

## Who decides failover

**The cluster decides an unplanned failover. The operator decides
everything else.**

The `Redis` kind lets the operator decide because Sentinel agrees
through gossip and epochs with no majority vote, and never fences an
old primary ([00-design.md](00-design.md)). Cluster mode has both
things Sentinel lacks. Each failover needs a majority of primaries to
vote in a new epoch, and each vote is persisted before it is given. A
primary cut off from the majority refuses writes after the node
timeout, which is the fence. These are the properties the `Redis` kind
builds from a Lease and a detach step.

Running cluster mode under the operator's authority would mean
`cluster-replica-no-failover yes` on every replica and `CLUSTER
FAILOVER TAKEOVER` from the operator's chosen replica. That costs:

- `TAKEOVER` skips the vote, so the operator would have to rebuild the
  fence the vote gives: a Lease for each shard, the detach step, and
  the failover quorum, all on top of the cluster's own failure
  detection, which keeps marking nodes `FAIL` and stopping minority
  primaries whatever the operator does. Two systems would then decide
  overlapping things.
- No shard could fail over while the API server or the operator is
  down. The `Redis` kind accepts that cost because Sentinel's
  alternative is worse. The cluster's election is not worse, and it
  works without the API server.
- Failover would take about 20 seconds, the `Redis` kind's figure,
  where the cluster's own takes the node timeout plus one to three
  seconds.

What deferring costs:

- **The confirmed-write guarantee is weaker for two or more replicas.**
  The rank delay makes the most caught-up replica stand first, but it
  only biases the race. If that replica cannot reach a majority in
  time, a replica that lacks a write can win. A write confirmed with
  `WAIT n` survives an unplanned failover for certain only when `n`
  equals the shard's replica count. With the default of one replica
  per shard, `WAIT 1` is that guarantee.
- **The operator learns of a failover after it happened.** It moves
  the role labels and the PodDisruptionBudgets on the next poll, within
  2 seconds. Cluster clients do not route by those labels, so only an
  eviction in that window acts on a stale label.

The operator keeps authority over every planned change. It decides
when a shard switches over, in what order pods roll, which slots move
where, which nodes join and which are forgotten, and it records each
decision in the status before an instance manager acts on it, as the
`Redis` kind does. When the cluster cannot elect, because a majority of
primaries is gone, a person can force the operator's choice:

```sh
kubectl annotate rediscluster sessions redis.guid.foo/failover-without-quorum=true
```

For each shard whose primary is `FAIL`, the operator then names the
reachable replica with the highest offset, and that replica's manager
runs `CLUSTER FAILOVER TAKEOVER`. It posts the Warning Event
`FailoverForced` and removes the annotation once each named replica
holds its slots. This mirrors the `Redis` kind's forced failover: one
annotation for one incident, removed by the operator. It also covers a
replica whose link to its primary was down for longer than
`cluster-replica-validity-factor` allows, which no election will
promote.

### Settings this choice fixes

| Setting | Value | Why |
|---|---|---|
| `cluster-replica-no-failover` | `no` | the cluster elects |
| `cluster-allow-replica-migration` | `no` | a replica stays in the StatefulSet of its shard, and a primary that a scale-down empties stays a primary until the operator removes it |
| `cluster-node-timeout` | 15 s, the engines' default | longer than `min-replicas-max-lag` plus the two seconds the server takes to count its replicas, so an old primary cut off from its replicas refuses writes before any replica can be elected |
| `min-replicas-to-write` | per shard, as `minReplicasToWrite` | the same fence as in the `Redis` kind, for a primary cut off from its replicas but not from the other primaries |
| `cluster-require-full-coverage` | `yes`, `spec.requireFullCoverage` | see the open questions |

A `RedisCluster` has no Lease, no `targetPrimary`, and no `pending`
state. The instance managers need no access to Leases.

## The resource

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: RedisCluster
metadata:
  name: sessions
spec:
  engine: valkey            # redis (default) | valkey
  image: ""                 # default: the engine's pinned image
  shards: 3                 # default 3, at least 3
  replicasPerShard: 1       # default 1
  minReplicasToWrite: 1     # per shard; default 1 when replicasPerShard > 0
  requireFullCoverage: true # default true
  memory: 1Gi               # per instance
  maxmemoryPolicy: noeviction
  storage: {size: 4Gi, storageClassName: ""}
  cpu: 100m
  nodeSelector: {}
  tolerations: []
  priorityClassName: ""
  metrics: {enabled: true}
  users:
    - {name: app, password: {secret: app-user, key: password}, rules: "~app:* +@all"}
  tls: {issuerRef: {name: ca, kind: ClusterIssuer}}
  backup: {destination: s3://bucket/prefix, credentials: {secret: backup}}
  bootstrap: {recovery: {backup: sessions-nightly-1793588400}}
status:
  observedGeneration: 2
  formed: true
  readyInstances: 6
  image: valkey/valkey:9.0.7
  shards:
    - index: 0
      slots: ["0-5460"]
      primary: sessions-0-0
      minReplicasToWrite: 1
      instances:
        - {name: sessions-0-0, nodeID: 3f1c..., role: primary, ready: true, offset: 1234}
        - {name: sessions-0-1, nodeID: 9a07..., role: replica, ready: true, offset: 1234}
  reshard: {phase: Moving, moves: [{slots: ["5461-5800"], from: 1, to: 3}]}
  switchover: {shard: 1, from: sessions-1-0, to: sessions-1-1, reason: NodeCordoned, startedAt: ...}
  forget: []                # node IDs every instance must forget
  rollout: {...}            # as in 04-upgrades.md
  conditions:
    - {type: Ready, status: "True", reason: AllShardsReady}
```

The fields from `engine` to `tls` mean what they mean on a `Redis`,
per instance. In Go they move from `RedisSpec` into a struct both specs
embed, with the defaulting methods on it, so the operator and every
instance manager read one definition. `TestEverySchemaHoldsItsSpecFields`
grows to check that both CRDs expose every embedded field. This is the
defense against drift that [00-design.md](00-design.md) names.

`shards` is at least 3. An election needs a majority of primaries, so
with two shards the loss of one primary leaves one vote of two, and no
replica can be elected. `replicasPerShard: 0` is allowed, for a cache
whose shards may lose their data; such a shard cannot fail over.

`engine` and `storage` are immutable, as on a `Redis`. `tls` is
immutable too, for the reason in "TLS". `replica` (adoption) does not
exist on this kind, for the reason in "Adoption".

`status.shards[].slots` is the operator's decision of which slots each
shard owns. The managers make their instances match it. The instance
list, roles, and offsets are what the managers last reported.

## The objects

| Object | Name | Purpose |
|---|---|---|
| StatefulSet | `<name>-<shard>` | one per shard, `replicasPerShard + 1` pods |
| Service | `<name>` | every ready instance, the address a client starts from |
| Service | `<name>-nodes` | headless, the governing Service of every shard's StatefulSet |
| Secret | `<name>-app` | `password`, `host`, `port`, `uri`, and `nodes`, a comma-separated list of every pod's headless name; with TLS the `tls-` forms |
| ServiceAccount, Role, RoleBinding | `<name>` | what the managers may read and write |
| PodDisruptionBudget | `<name>-<shard>-primary` | `minAvailable: 1` on the shard's primary |
| PodDisruptionBudget | `<name>-replicas` | one replica of the whole cluster may go at a time |
| NetworkPolicy | `<name>-bus` | admits the bus port only from this cluster's own pods |
| Certificate | `<name>-tls` | with `tls.issuerRef` |

Pods carry `redis.guid.foo/cluster: <name>`, as a `Redis`'s pods do, so
the existing PodMonitor scrapes them, plus `redis.guid.foo/shard` and
the role label. A client needs one address that reaches any instance;
it reads the slot map from there with `CLUSTER SLOTS` or `CLUSTER
SHARDS`. There is no `-rw` or `-ro` Service, because no single primary
exists.

### One StatefulSet per shard

The pod `sessions-1-0` is instance 0 of shard 1. A StatefulSet per
shard gives each shard its own scale, so the `Redis` kind's rule for
removing replicas carries over unchanged, and adding or removing a
shard is creating or deleting a StatefulSet. One StatefulSet for the
whole cluster would map ordinals to shards by arithmetic, and a
scale-down would remove the highest ordinals whatever shard they
belong to.

Pods of one shard carry a required anti-affinity on
`kubernetes.io/hostname`, as a `Redis`'s pods do, so a machine holds at
most one copy of a shard. Pods of different shards carry a preferred
anti-affinity. Primaries matter more: if one machine holds the
primaries of two shards out of three and fails, one primary of three
remains, no majority exists, and neither shard can elect. After every
failover and every roll, the operator looks for two primaries on one
Kubernetes node, and moves one with a planned switchover to a replica
on a node that holds no primary. It does nothing when no such replica
exists.

### Addresses

The instance manager starts the server with:

- `cluster-announce-ip` set to the pod's IP, because the bus connects
  by IP;
- `cluster-announce-hostname` set to the pod's headless name,
  `<pod>.<name>-nodes.<namespace>.svc`;
- `cluster-preferred-endpoint-type ip` by default, so every client can
  follow `MOVED`. The open questions ask whether to offer `hostname`.

A pod's IP changes when it restarts. Gossip carries the new address to
every peer that the restarted instance reaches, and the restarted
instance reaches its peers at the addresses in its own `nodes.conf`.
That fails when every pod restarts at once, as after a power loss or a
cluster-wide upgrade of Kubernetes: each `nodes.conf` then holds only
old addresses. The manager closes that gap. On each step it reads the
pods of its `RedisCluster`, and for each peer that its server reports
as disconnected at an address other than the pod's current IP, it runs
`CLUSTER MEET <current ip> <port> <bus port>`. The engine matches the
reply to the known node ID and updates the address. The manager never
edits `nodes.conf`.

### Node IDs across restarts

With `storage`, `nodes.conf` lives on the data volume beside the
append-only file, so an instance keeps its node ID, its slots, and its
epochs across a restart. Without `storage`, a restarted pod is a new
node with a new ID and no data. The operator then:

1. waits for the cluster to fail over the old ID's slots, if it was a
   primary;
2. writes the old ID to `status.forget`, and every manager runs
   `CLUSTER FORGET` for each listed ID its server still knows, on every
   step, which completes well inside the 60-second ban;
3. has the new instance's manager run `CLUSTER REPLICATE` to its
   shard's primary.

It clears `status.forget` once no manager reports knowing the IDs.

A shard whose every instance restarted with no data has lost its
slots' data. The operator assigns the shard's slots to instance 0 and
sets `DataLost`, as the `Redis` kind does when every instance restarts
empty. Until it does, `cluster-require-full-coverage` stops the whole
cluster.

## Forming a new cluster

1. The operator applies every shard's StatefulSet. Each manager starts
   its server with `cluster-enabled yes` and an empty `nodes.conf`, and
   reports its node ID.
2. Once every instance has reported, the operator writes
   `status.shards`: the slot ranges, split evenly in shard order, and
   instance 0 of each shard as its primary.
3. Each primary's manager runs `CLUSTER SET-CONFIG-EPOCH <shard + 1>`
   while it knows no other node, then `CLUSTER ADDSLOTSRANGE` for its
   ranges, then `CLUSTER MEET` to every other instance. Distinct
   starting epochs are what `redis-cli --cluster create` sets, and they
   save the epoch collision rounds.
4. Each replica's manager runs `CLUSTER REPLICATE <primary's node ID>`
   once its server knows that node.
5. When every instance reports `cluster_state:ok` and the slot map the
   status names, the operator writes `formed: true`.

A manager whose `nodes.conf` already holds a cluster never forms
again. Every step is a comparison of the server's state with the
status, so a manager that restarts halfway resumes.

## Switchover

A switchover starts for the reasons a `Redis`'s does: the primary's
node is cordoned, its pod is being deleted, a roll reaches it, or two
primaries share a machine.

1. The operator picks a ready replica of the shard on a schedulable
   node with its link up, by the `Redis` kind's order, and writes
   `status.switchover` with the shard.
2. That replica's manager runs `CLUSTER FAILOVER`. The engine pauses
   the primary's writes, waits for the replica to reach its offset, and
   takes an epoch from a majority of primaries. No write is lost.
3. The operator sees the replica report the primary role and the
   shard's slots, moves the role labels, and clears the switchover.
4. If that has not happened within the switchover deadline, it sets
   `SwitchoverFailed` and tries again after the backoff. It never falls
   back to `FORCE` or `TAKEOVER` on its own.

The `Redis` kind's rule for closing a demoted primary's clients does
not apply. A demoted cluster primary answers a write to a slot it no
longer owns with `MOVED`, which every cluster client follows. It never
answers `READONLY`, unless the client asked for replica reads with
`READONLY`, and such a client handles that reply already.

A primary pod being deleted asks for its switchover as a `Redis`'s
does: its manager annotates the pod and keeps serving until the
operator has moved the slots or 20 seconds have passed.

## Scaling shards

### Up

1. The operator applies the StatefulSets of the new shards and waits
   until their instances report node IDs.
2. It adds the new shards to `status.shards` with no slots, and their
   managers join and replicate as in "Forming a new cluster", without
   the epoch step.
3. It writes `status.reshard` with the moves that give every shard an
   even share: from each old shard, the highest slots above its new
   share, to the new shards in order. Moving the fewest slots keeps
   each move's snapshot small.
4. It runs one move at a time. The manager that runs the move is the
   source's primary on Valkey and the target's primary on Redis. It
   starts the atomic migration and reports its progress from the
   engine's status command. The operator writes the next move when the
   engine reports this one complete and every instance's slot map
   agrees.
5. It clears `status.reshard` after the last move.

A move that fails, or a source or target that fails over during the
move, ends with the engine's status saying so. The operator writes
`ReshardFailed` with the engine's error, waits the backoff, and starts
the same move again from the slot map as it then stands. A move already
done is not repeated, because the move list is derived from the slot
map and the spec on each pass.

### Down

Shards are removed from the highest index, as StatefulSet ordinals are.

1. Before it starts, the operator checks that the remaining shards can
   hold the data: the used memory of every shard, from the managers'
   reports, must fit in 80% of the remaining shards' `maxmemory`. If it
   does not, it sets `Ready` False with the reason `ScaleDownWouldOverfill`
   and does nothing.
2. It moves every slot of the removed shards to the remaining shards,
   evenly, one move at a time as above.
3. When each removed shard's primary reports no slots, the operator
   deletes its StatefulSet, writes its node IDs to `status.forget`, and
   deletes its volume claims.
4. It removes the shards from `status.shards`.

`cluster-allow-replica-migration no` keeps the emptied primary from
turning into a replica of another shard, which would put a pod that
the operator is about to delete into a shard that stays. A shard that
still owns a slot is never deleted, so a scale-down interrupted at any
point leaves every key served. Lowering `shards` below 3 is refused by
the CRD.

### Replicas per shard

`replicasPerShard` scales each shard's StatefulSet by the `Redis`
kind's rules, shard by shard, and a new replica joins with `CLUSTER
MEET` and `CLUSTER REPLICATE`. A removed replica's node ID goes to
`status.forget`. The raise of `minReplicasToWrite` toward the spec
follows `reconcile_writes.go` for each shard's primary.

## Rolling a change

One shard at a time, in shard order, and within the shard by the
`Redis` kind's order: replicas from the highest ordinal, each after
every pod of the whole cluster is ready, then a switchover to an
updated replica, then the old primary. Nothing rolls while a shard has
no primary, while a reshard moves slots, or while the cluster state is
not `ok`. The staged image upgrade of [04-upgrades.md](04-upgrades.md)
applies with the canary in shard 0: its highest-ordinal replica proves
the image, and a failed canary rolls that one pod back. A shard with no
replicas rolls its only pod, and its slots are down until the pod is
back.

## Backups

A `Backup` of a `RedisCluster` is one snapshot from each shard, taken
on a replica when the shard has one, by the same `BGSAVE` and upload
that a `Redis` backup runs ([02-backups.md](02-backups.md)). Its status
holds one entry per shard, with the instance, the object key, the size,
the offset, and the slots that shard owned. It is `Completed` when
every shard's snapshot is, and `Failed` when any one fails.

The snapshots are taken at about the same moment, not at one instant.
Redis Cluster promises no atomicity across shards: a multi-key command
works only within one slot, and a slot lives in one shard. Each shard's
snapshot is consistent on its own, so every key, and every group of
keys a client could change together, is restored as it was at one
moment. Two keys in different shards can come from moments up to the
spread between the snapshots apart. A consistent cut across shards
would pause writes on every primary until each one forks, putting the
fork back on the primaries; the open questions ask whether to offer it.

`Backup` names its source as `{kind, name}` once a second kind exists,
as [00-design.md](00-design.md) says:

```yaml
spec:
  source: {kind: RedisCluster, name: sessions}
```

`spec.redis` stays as the form for a `Redis`, and the CRD requires one
of the two.

### Restore

A new `RedisCluster` restores from a `Backup` of a cluster with the
same number of shards. Instance 0 of shard `k` downloads shard `k`'s
snapshot before its server starts, as instance 0 of a `Redis` does.
Forming the cluster then assigns each shard the slots the backup
recorded for it instead of an even split. A restore into a different
shard count is a restore at the same count followed by a scale. Loading
a snapshot under a different slot map would make each primary delete
the keys in slots another shard owns.

`ScheduledBackup` and retention work unchanged on `Backup`s of either
kind.

## Adoption

The `Redis` kind adopts by replication: its Lease holder runs
`REPLICAOF` to the source. A cluster instance cannot replicate from a
server outside its own cluster, so that path does not exist here.

To adopt an existing Redis Cluster, the new instances would join it:
`CLUSTER MEET` into the source cluster, `CLUSTER REPLICATE` to each
source primary, a `CLUSTER FAILOVER` per shard to make the new
instances primaries, and `CLUSTER FORGET` for the old nodes once they
are stopped. That needs the source's instances and the pods to reach
each other's bus ports in both directions, the same engine family and
a compatible version, the same password for replication, and the same
shard count as the source. It is a separate milestone after this one.

A non-cluster Redis or Valkey cannot be adopted into a `RedisCluster`
by replication at all, because its keys must be split across shards
and its clients must change protocol. The way there is a `Redis`
adoption first, then a migration of the keys by the application or a
tool. Moving a `Redis` to a `RedisCluster` in place is out of scope,
as [00-design.md](00-design.md) already says.

## Users and TLS

ACL users are per instance in cluster mode too, so every manager sets
them on its own server, by the code a `Redis`'s managers run. The
instance managers, replication, and slot migration keep using the
default user. Valkey's atomic migration authenticates to the target as
a replica does, and its ACL rule needs access to every database
(`alldbs`); the default user has it.

With `tls`, every instance runs `tls-cluster yes` and
`tls-replication yes`, and the bus requires a certificate that verifies
against `ca.crt`. The Certificate covers `<name>`, every pod through
`*.<name>-nodes`, and `127.0.0.1`. A bus with TLS and a bus without it
cannot talk, so turning TLS on or off on a running cluster would split
it in two halfway through the roll. That is why `tls` is immutable on
this kind. Rotating the certificate works as on a `Redis`, with no
restart. Whether the plaintext client port closes follows `plaintext`,
as on a `Redis`. That an instance with `tls-cluster yes` names its
plaintext port in a `MOVED` sent to a plaintext client, and its TLS
port to a TLS client, is to be confirmed by the real-server tests of
step 3.

Without `tls`, the bus has no authentication: any pod that reaches the
bus port can `MEET` an instance and be trusted. The operator applies
the NetworkPolicy `<name>-bus`, which admits the bus port from this
`RedisCluster`'s pods only, and the manager writes
`cluster-bus-port-protected-mode no` for a Redis version that has the
directive. A cluster whose network plugin does not enforce
NetworkPolicy has an open bus, and the README says so.

## Metrics and Events

Every pod runs the exporter, reading its own server, as a `Redis`'s
pods do. redis_exporter runs `CLUSTER INFO` when the server reports
`cluster_enabled:1` and exports each field as a series, so the
existing PodMonitor yields `redis_cluster_state`,
`redis_cluster_slots_ok`, `redis_cluster_slots_fail`,
`redis_cluster_known_nodes`, and `redis_cluster_current_epoch` with no
change. The exporter does not need its `-is-cluster` flag, which makes
it discover the whole cluster from one address, because it reads only
its own instance.

The monitoring component gains:

| Alert | Expression | For | Why the wait |
|---|---|---|---|
| `RedisClusterDown` | `redis_cluster_state == 0` | 1m | an election takes the node timeout and a few seconds |
| `RedisClusterSlotsFailing` | `redis_cluster_slots_fail > 0` | 1m | the same |

The `Redis` kind's alerts apply per instance and keep working.
`RedisNoPrimary` groups by the `Redis`'s name and would fire for every
shard but one, so it gains `shard` in its grouping.

The operator posts Events through the same queue, with the same rules
for transitions, Warnings, and repeats. New actions: `FailoverObserved`
with the shard, the old and new primary, and the epoch; `SwitchoverStarted`
and `SwitchoverDone` with the shard; `ClusterFormed`; `ReshardStarted`,
`SlotsMoved` for each move, and `ReshardCompleted`; `NodeForgotten`;
and `FailoverForced`. `ReshardFailed`, `DataLost`, and `Ready` False
with `SlotsNotCovered` or `ClusterDown` are Warnings. The operator's
failover counter gains the reason `observed`.

## Tests

- **Unit, with real servers.** Six real `redis-server` and
  `valkey-server` processes in cluster mode on loopback ports, driven
  by the instance manager: forming a cluster from the status; a
  switchover with `CLUSTER FAILOVER`; an atomic slot move on each
  engine; an instance restarted on a new port that rejoins through the
  manager's `MEET`, which stands in for a new pod IP; every instance
  restarted on new ports at once, which must reform; `FORGET` across
  every instance within the ban; and a restore of per-shard snapshots
  under the recorded slot map.
- **Unit, with fakes.** The operator against the fake API server and
  `httptest` managers: forming, the switchover rules, the move list for
  each scale, the overfill refusal, the primaries-on-one-machine
  rebalance, the forced takeover, and each status write and Event.
- **End to end.** The four-node k3s cluster, both engines, a
  `RedisCluster` of three shards with one replica each, and a writer
  that increments one counter in each shard, through hash tags, and
  confirms each write with `WAIT 1` on the connection that wrote it,
  following `MOVED`. After each scenario, every counter must be at
  least the highest value confirmed, and the writer must confirm new
  writes in every shard. The scenarios:
  - delete a primary pod; its shard switches over and loses nothing;
  - kill a primary's processes with no warning; the cluster elects its
    replica, and no write confirmed with `WAIT 1` is lost, which holds
    because each shard has one replica;
  - drain a machine that holds a primary;
  - freeze a primary's processes for longer than the node timeout; the
    cluster elects its replica, and the old primary, once it runs
    again, must rejoin as a replica of the new one;
  - cut a primary off from every other pod but the writer; it must
    refuse a write sent to its pod IP within the node timeout of the
    cut, and its replica must be elected;
  - delete every pod at once; the cluster must reform at the new IPs
    with its data;
  - scale from three shards to four under the writer's load and back
    to three, with no confirmed write lost and no slot uncovered;
  - roll a change of `memory`, then an image upgrade with the canary;
  - back up under load and restore into a new `RedisCluster`, whose
    counters must each be at least the highest value confirmed before
    the backup started;
  - read each exporter, which must report `redis_cluster_state 1`.

  Four machines are enough for six pods: each shard needs two
  machines, and the primaries need three. The rebalance must spread
  the primaries before each scenario starts.

## Build sequence

Each step ends with its tests passing and the plan updated.

1. **Shared spec.** Move the shared fields of `RedisSpec` into the
   embedded struct, with its defaults, and extend the CRD test. No
   behavior changes for `Redis`.
2. **The kind and its objects.** `api_cluster.go`, the CRD, the
   children (StatefulSets per shard, Services, Secret, Role, budgets,
   NetworkPolicy), and the name-collision rule from the open questions.
3. **The manager in cluster mode.** The server's cluster settings, the
   address announcements, the version probe for the bus directive,
   users, TLS, and the step that matches the server to
   `status.shards`. Real-server tests for forming.
4. **Forming and status.** The operator's forming decision, the poll of
   each manager's cluster view, `status.shards`, role labels, and
   `Ready`.
5. **Membership.** Re-`MEET` at new addresses, `status.forget`, and
   rebuilding an instance that lost its data. The every-pod restart
   test belongs here.
6. **Switchover and failover.** Planned `CLUSTER FAILOVER` on cordon,
   pod deletion, and the primaries-on-one-machine rebalance; observed
   failover with its Events; the forced takeover annotation.
7. **End-to-end base.** The cluster writer and the failover, drain,
   freeze, and restart scenarios, on both engines. The kind is usable
   from here at a fixed shard count.
8. **Rolling and upgrades.** Shard-by-shard roll and the canary.
9. **Resharding.** Atomic slot moves for each engine, scale up, the
   overfill check, and scale down.
10. **Backups and restore.** `spec.source`, per-shard snapshots, and
    restore under the recorded slot map.
11. **Metrics.** The alerts, the `shard` grouping, and the dashboard's
    cluster panels.
12. **Documentation.** README, a `skills/declare-a-rediscluster` skill,
    and the troubleshooting skill's cluster states.

Adoption of an existing Redis Cluster is a later milestone.

## Open questions

1. **Defer unplanned failover to the cluster's election?**
   Recommendation: yes, as "Who decides failover" argues, with the
   forced `TAKEOVER` annotation for the case where no majority remains.
   This also closes the "decision to revisit" in 00-design in favor of
   two kinds. If you would rather the operator decide here too, the
   one-kind shape comes back into play and this plan needs a rewrite
   before anything is built.

2. **Atomic slot migration only, or the key-by-key path too?**
   Recommendation: atomic only, which requires Redis 8.4 or Valkey 9.0
   and later. The operator's default images are past both. The
   key-by-key path brings `ASK` handling, slots left half-moved by a
   crash, and repair logic that the atomic path does not need. The cost
   is that the two engines take different commands, which breaks the
   rule in 00-design that the operator uses only commands both have.
   The difference stays inside the one manager function that starts
   and polls a move. A `RedisCluster` on an older image would run but
   refuse to reshard, with a condition saying why.

3. **Minimum shards: 3, or allow 1 and 2?** Recommendation: 3. With
   fewer, a lost primary cannot be replaced by election, and the user
   gets a cluster that fails over in some shard counts and not others.
   A user who wants cluster mode on one server to test a client can
   run three small shards.

4. **Default `replicasPerShard` and the confirmed-write guarantee.**
   Recommendation: default 1, and state the guarantee as "`WAIT n`
   survives an unplanned failover when `n` equals `replicasPerShard`".
   The `Redis` kind's quorum gives a stronger guarantee for `WAIT 1`
   with two replicas. Matching it here would mean taking the election
   away from the cluster.

5. **Names that two kinds could both claim.** A `Redis` named
   `sessions` and a `RedisCluster` named `sessions` in one namespace
   would both want `sessions-nodes`, `sessions-app`, and pods labeled
   `redis.guid.foo/cluster: sessions`. Recommendation: the operator
   refuses the second one, with a condition `NameConflict` and no
   children, and every selector gains a label that names the kind, so
   a budget never selects the other kind's pods. The alternative is a
   different label key for the new kind, which breaks the shared
   PodMonitor and the alerts.

6. **Should the operator apply a NetworkPolicy?** It applies none for
   a `Redis`. Recommendation: yes for `RedisCluster`, because the bus
   without TLS trusts anyone who reaches it, and Redis now refuses to
   start that way unless told otherwise. The alternative is to require
   `tls` for every `RedisCluster`, which needs cert-manager or a
   user-managed CA for every cluster.

7. **`cluster-preferred-endpoint-type hostname` as an option?**
   Recommendation: offer `spec.endpoint: ip | hostname`, default `ip`.
   With `hostname`, `MOVED` names the pod's headless name, which lets a
   TLS client verify the certificate by name and survives a pod's new
   IP between a redirect and the reconnect. Older clients do not
   understand hostname endpoints, so it cannot be the default.

8. **A consistent cut across shards for backups?** Recommendation: not
   in this milestone. Per-shard snapshots match what the cluster
   guarantees clients. A `consistent: true` on `Backup` that pauses
   writes on every primary until each one forks can come later, if a
   user needs it.

9. **`requireFullCoverage` as a spec field?** Recommendation: yes,
   default true. A cache would rather serve the shards that are up
   than fail every request because one shard is down, and that is the
   one cluster setting users commonly change. The other `cluster-*`
   settings stay fixed, as `appendfsync` is on a `Redis`.

10. **Adoption of an existing cluster: in this milestone or later?**
    Recommendation: later, as its own milestone. It needs two-way bus
    reachability with a cluster the operator does not control, and
    every other part of this plan works without it.

## Sources

- Valkey, [Cluster specification](https://valkey.io/topics/cluster-spec/),
  and Redis, [Redis cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/):
  the bus, node IDs and address changes, epochs, failure detection,
  elections and rank, replica migration, `MOVED` and `ASK`, the
  key-by-key migration, `CLUSTER FORGET`, and write safety.
- Valkey, [CLUSTER FAILOVER](https://valkey.io/commands/cluster-failover/):
  the default, `FORCE`, and `TAKEOVER` modes, and
  `cluster-manual-failover-timeout`.
- Valkey, [Atomic slot migration](https://valkey.io/topics/atomic-slot-migration/)
  and [CLUSTER MIGRATESLOTS](https://valkey.io/commands/cluster-migrateslots/).
- Redis, [CLUSTER MIGRATION](https://redis.io/docs/latest/commands/cluster-migration/).
- `redis.conf` at Redis 8.10.3 and `valkey.conf` at Valkey 9.0.7: the
  defaults and comments for `cluster-replica-no-failover`,
  `cluster-allow-replica-migration`, `cluster-require-full-coverage`,
  the `cluster-announce-*` settings, `cluster-preferred-endpoint-type`,
  `cluster-databases`, the slot migration settings, `tls-cluster`, and
  `cluster-bus-port-protected-mode`. The protected mode appears in the
  `redis.conf` of 8.2.11, 8.4.8, 8.6.8, 8.8.4, 8.10.2, and 8.10.3, and
  not in 8.10.0 or 7.4.12.
- Redis 8.10.3 source: `clusterProcessPacket` in `src/cluster_legacy.c`
  for the address update on a handshake with a known node, and
  `verifyClusterConfigWithData` in `src/cluster.c` for keys in unowned
  slots. The Valkey equivalents are to be confirmed by the real-server
  tests in step 3.
- redis_exporter, `exporter/exporter.go` and `exporter/metrics.go`:
  `CLUSTER INFO` collected when `cluster_enabled:1`, every `cluster_`
  field exported.
