# Milestone 5: RedisCluster

A `RedisCluster` would run Redis Cluster or Valkey Cluster: the
keyspace's 16384 hash slots split across several shards, each shard a
primary and its replicas. It is for data that does not fit in one
server's memory, or write load that one primary cannot carry.

This is a stub. Nothing here is designed or built yet; the design
starts in conversation with the maintainer.

[00-design.md](00-design.md) made `RedisCluster` a kind of its own,
because sharding changes the client contract: a client must follow
`MOVED` and `ASK` redirects, and a command across slots fails with
`CROSSSLOT`. It left open whether sharded mode should fail over under
the operator's authority, which would argue for one kind with an
optional `cluster` block instead.

## Questions the design has to answer

- Who decides an unplanned failover: the cluster's own election among
  primaries, or the operator, as it does for `Redis`?
- What the resource looks like, and which of `Redis`'s fields it
  shares.
- How pods keep their node IDs and addresses across restarts.
- How shards scale up and down, and how slots move between them.
- How a change rolls out across shards.
- What a backup and a restore mean when each shard snapshots on its
  own.
- Whether an existing cluster can be adopted.
- What the e2e has to prove.

## Notes from early reading

- The two engines move slots with different commands: Valkey 9.0 has
  `CLUSTER MIGRATESLOTS`, and Redis 8.4 has `CLUSTER MIGRATION
  IMPORT`. The operator so far uses only commands both engines share.
- Recent Redis releases refuse to start a cluster node whose cluster
  bus is unauthenticated unless `cluster-bus-port-protected-mode` says
  otherwise, and older releases refuse that directive as unknown.
  Valkey 9.0 has no such check.
