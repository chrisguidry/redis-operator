# redis-operator

A Kubernetes operator for Redis and Valkey. It is not usable yet: the
design is in progress in [plans/](plans/).

The goal is an operator that runs Redis the way
[CloudNativePG](https://cloudnative-pg.io) runs Postgres:

- One resource declares a primary and its replicas, and the operator
  keeps clients on the primary through failover and node drains.
- A Service always points at the primary, so any Redis client works
  without Sentinel support.
- Backups to object storage, restore into a new deployment, adoption of
  an existing Redis or Valkey by replicating from it, and upgrades
  staged so they lose no data.
- Chaos tests in CI that kill, stall, and partition every part and
  check that no acknowledged write is lost.

The API group is `redis.guid.foo`. Images are published to
`ghcr.io/chrisguidry/redis-operator`.

## License

MIT
