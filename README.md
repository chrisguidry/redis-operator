# redis-operator

A Kubernetes operator for Redis and Valkey, in the spirit of
[CloudNativePG](https://cloudnative-pg.io). One `Redis` resource
declares a primary and its replicas, and the operator keeps one
writable primary through pod deletions, node drains, crashes, and
image upgrades.

- **Resources.** `Redis`, `Backup`, and `ScheduledBackup`, in the API
  group `redis.guid.foo`, version `v1alpha1`. `engine: redis` runs
  Redis 8, and `engine: valkey` runs Valkey 9, from the images each
  project publishes.
- **Failover decided by the operator.** Sentinel does not run. The
  operator records which instance should be primary in the `Redis`'s
  status, and an instance manager in each pod carries the decision out,
  holds the primary Lease, and fences its own server when it cannot
  confirm the decision. A planned switchover uses the server's own
  `FAILOVER` command and loses no write. A failover keeps every write
  a client confirmed with `WAIT n`, for `n` at least
  `minReplicasToWrite`: it promotes only once enough instances answered
  that one of them holds each such write.
- **Backups** to an S3-compatible object store or to a shared volume,
  on demand or on a cron schedule, with retention, and restore into a
  new `Redis`.
- **Adoption** of a Redis or Valkey that runs elsewhere: the new
  `Redis` replicates from it, then promotes with the source's writes
  paused, so no write the source accepted is lost.
- **Staged image upgrades.** A canary replica proves a new image before
  the rest roll, and a canary that fails is rolled back.
- **Metrics** from redis_exporter beside every instance, and a
  monitoring component with PodMonitors, alerts, and a Grafana
  dashboard.
- **ACL users and TLS.** ACL users declared with their passwords in
  Secrets, set on every instance, and TLS with a certificate from a
  cert-manager issuer or from a Secret. A rotated password or
  certificate takes effect without a restart.

The design of each feature, with the reasons and the test results, is
in [plans/](plans/): [the design](plans/00-design.md),
[high availability](plans/01-high-availability.md),
[backups](plans/02-backups.md), [adoption](plans/03-adoption.md), and
[upgrades](plans/04-upgrades.md).

## Install

The operator ships as a kustomize base in [deploy/](deploy/): the three
CRDs, a ClusterRole and its binding, and a Deployment of two replicas
that elect a leader. Pin the base and the image to the same release.
Releases are calendar versions, such as `2026.11.01-001`; the
[releases page](https://github.com/chrisguidry/redis-operator/releases)
lists them.

The base does not create a namespace. Create one, then write a
kustomization that takes the base:

```sh
kubectl create namespace redis-operator
```

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: redis-operator
resources:
  - https://github.com/chrisguidry/redis-operator//deploy?ref=2026.11.01-001
images:
  - name: ghcr.io/chrisguidry/redis-operator
    newTag: 2026.11.01-001
```

```sh
kubectl apply -k .
kubectl -n redis-operator rollout status deployment/redis-operator
```

The base's own image tag is `latest`, so a kustomization that leaves
out the `images` entry runs whatever release is newest.

## A minimal Redis

```yaml
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache
  namespace: example
spec:
  engine: valkey   # or redis, the default
  instances: 3     # one primary and two replicas
  memory: 512Mi    # the container's memory limit; maxmemory is 70% of it
  storage:
    size: 2Gi      # without storage, the data lives only in memory
```

```sh
kubectl get redis -n example
```

```text
NAME    ENGINE   INSTANCES   READY   PRIMARY   STATUS   AGE
cache   valkey   3           3       cache-0   True     1m
```

Each instance runs on a different node, so a `Redis` of three
instances needs three schedulable nodes.

`kubectl describe redis cache -n example` lists the operator's Events
for the last hour: each condition that changed, each failover and
switchover with its cause, and each step of an image rollout. A
`Backup` has Events for its start and its end. The `troubleshoot`
skill lists them.

## Connecting clients

For a `Redis` named `cache`, the operator makes:

| Object | Name | What it is |
|---|---|---|
| Service | `cache-rw` | the primary, for reads and writes, on port 6379 |
| Service | `cache-ro` | the replicas, for reads, on port 6379 |
| Service | `cache-nodes` | headless; one DNS name per pod |
| Secret | `cache-app` | the keys `password`, `host`, `port`, `uri`, and `ro-uri` |

`host` is `cache-rw.<namespace>.svc`, `port` is `6379`, and `uri` is
`redis://:<password>@cache-rw.<namespace>.svc:6379`. `ro-uri` is the
same for `cache-ro`. Any Redis client works; none needs Sentinel
support. During a switchover or failover, `cache-rw` has no endpoint
until the new primary takes over, so new connections are refused for
that time; the old primary then closes its client connections, and a
client that retries its connection through `cache-rw` reaches the new
primary.

With `spec.tls`, the Services also serve TLS on port 6380, named
`redis-tls`, and `cache-app` also holds `tls-port`, `tls-uri`, and
`tls-ro-uri`, which start with `rediss://`. With `plaintext: false`,
the default, port 6379 closes, and `port`, `uri`, and `ro-uri` name
TLS too. A client trusts the CA that signed the certificate. The
`declare-a-redis` skill shows both ways to get one.

A user declared in `spec.users` connects with its own name and the
password in its own Secret, such as
`redis://app:<password>@cache-rw.<namespace>.svc:6379`. The operator
makes no Secret for each user.

A client in the same namespace can build its URL from the password:

```yaml
env:
  - name: REDIS_PASSWORD
    valueFrom:
      secretKeyRef: {name: cache-app, key: password}
  - name: REDIS_URL
    value: redis://:$(REDIS_PASSWORD)@cache-rw:6379/0
```

## Monitoring

Every instance pod runs
[redis_exporter](https://github.com/oliver006/redis_exporter) in a
container named `metrics`, which reads its own server over 127.0.0.1
and serves on port 9121, the container port named `metrics`. The series
keep redis_exporter's names, such as `redis_up`,
`redis_memory_used_bytes`, and `redis_connected_slaves`, so dashboards
and alert rules written for redis_exporter work on them. Each replica
reports itself, not only the primary. `spec.metrics` turns the exporter
off, replaces its image, and names single keys for it to check, such as
a queue's list; the `declare-a-redis` skill has the fields. The
operator serves its own metrics, prefixed `redis_operator_`, on port
9200 of its pods.

A cluster that runs the Prometheus Operator takes the component in
[deploy/monitoring/](deploy/monitoring/) beside the base:

```yaml
resources:
  - https://github.com/chrisguidry/redis-operator//deploy?ref=2026.11.01-001
components:
  - https://github.com/chrisguidry/redis-operator//deploy/monitoring?ref=2026.11.01-001
```

It adds a PodMonitor for the operator, a PodMonitor for every Redis
instance in every namespace, a PrometheusRule of alerts, and a Grafana
dashboard as a ConfigMap labeled `grafana_dashboard` in the namespace
`monitoring`. Each instance series carries `namespace`, `pod`, `redis`
(the `Redis`'s name), and `redis_role` (`primary` or `replica`, the
label the operator wrote on the pod). The header of
[deploy/monitoring/kustomization.yaml](deploy/monitoring/kustomization.yaml)
says how to add the label a Prometheus selects monitors and rules by.
The `install` skill lists the alerts, and the `troubleshoot` skill says
what to read when each fires.

## Agent skills

[skills/](skills/) holds one guide for each task, written for coding
agents in the [Agent Skills](https://agentskills.io/) format:
[install](skills/install/SKILL.md),
[declare-a-redis](skills/declare-a-redis/SKILL.md),
[backups](skills/backups/SKILL.md),
[adopt-an-existing-redis](skills/adopt-an-existing-redis/SKILL.md),
[upgrade](skills/upgrade/SKILL.md), and
[troubleshoot](skills/troubleshoot/SKILL.md).

This repository is also a plugin marketplace. In Claude Code:

```sh
claude plugin marketplace add chrisguidry/redis-operator
claude plugin install redis-operator@redis-operator
```

The skills then run as `/redis-operator:install`,
`/redis-operator:backups`, and so on.

In Codex:

```sh
codex plugin marketplace add chrisguidry/redis-operator
codex plugin add redis-operator@redis-operator
```

Any other agent that reads `SKILL.md` files can take the `skills/`
directory directly, for example with
`npx skills add https://github.com/chrisguidry/redis-operator`.

## License

MIT
