---
name: install
description: "Install, upgrade, or remove redis-operator from its kustomize base pinned to a release, choose its namespace, admit it through NetworkPolicies, add the monitoring component of PodMonitors, alerts, and a Grafana dashboard, check what TLS from cert-manager needs, and check that it runs and serves metrics. Use when a cluster has no Redis CRD yet, when moving the operator to a new release, or when removing it."
---

Before the first command, run `kubectl config current-context` and confirm that it names the cluster the person means.

# Install the operator

redis-operator ships as a kustomize base in `deploy/` of
https://github.com/chrisguidry/redis-operator. The base holds:

- the CRDs `redis.redis.guid.foo`, `backups.redis.guid.foo`, and
  `scheduledbackups.redis.guid.foo`;
- the ServiceAccount, ClusterRole, and ClusterRoleBinding
  `redis-operator`, and a Role and RoleBinding `redis-operator` in the
  operator's namespace for its leader-election Lease;
- the Deployment `redis-operator`: two replicas, labeled
  `app: redis-operator`, of which one leads and the other waits.

You need `kubectl` with cluster-admin rights. The install creates
CRDs and a ClusterRole, and the ClusterRole lets the operator create
Roles that grant the instance managers access to Leases, pods, and
Secrets. Kubernetes allows that only to a subject that holds those
permissions itself.

## Choose the namespace

The base creates no namespace, so removing the operator never deletes
a namespace that other workloads share. The base defaults to
`redis-operator`; a kustomization that sets `namespace:` moves the
ServiceAccount, the Deployment, and the ClusterRoleBinding's subject
together.

```sh
kubectl create namespace redis-operator
```

`Redis` resources can live in any namespace. The operator watches all
of them.

## Pin a release

Releases are calendar versions such as `2026.11.01-001`. List them at
https://github.com/chrisguidry/redis-operator/releases. Pin the base
and the image to the same version, because the CRDs in the base must
match the operator that reads them:

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
```

Do not apply the base URL directly with `kubectl apply -k <url>`: the
base's image tag is `latest`, and without the `images` entry the
operator runs whatever release is newest.

With Flux, take the base from its OCI artifact instead. Each version
of the operator ships `deploy/` as
`oci://ghcr.io/chrisguidry/redis-operator-deploy`, with the operator's
image already set to the same version, so the manifests and the image
move in one revision and need no `images` entry. The artifacts start at
release 2026.11.04-002:

```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: redis-operator
  namespace: redis-operator
spec:
  interval: 12h
  url: oci://ghcr.io/chrisguidry/redis-operator-deploy
  ref:
    tag: 2026.11.04-002
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: redis-operator
  namespace: redis-operator
spec:
  interval: 1h
  prune: true
  sourceRef:
    kind: OCIRepository
    name: redis-operator
  path: ./
  targetNamespace: redis-operator
```

A Kustomization that takes the base from git and sets the image in
`images` can apply a new image with the old base: the image tag
changes the moment the Kustomization does, and the base only when the
source fetches the new tag. A release that needs a manifest change,
such as a new environment variable, then fails to start.

## Choose the default images

A `Redis` that names no image runs the images in three variables of the
operator's environment: `DEFAULT_REDIS_IMAGE`, `DEFAULT_VALKEY_IMAGE`,
and `DEFAULT_EXPORTER_IMAGE`. The base sets each to the version its
release was tested with, and the operator refuses to start when one is
empty. To run a newer server or exporter before the next release, patch
the value in your kustomization:

```yaml
patches:
  - target:
      kind: Deployment
      name: redis-operator
    patch: |
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: redis-operator
      spec:
        template:
          spec:
            containers:
              - name: operator
                env:
                  - name: DEFAULT_REDIS_IMAGE
                    value: redis:8.10.4
```

The patch restarts the operator, and every `Redis` that names no image
of its own then rolls to the new default, through a canary for a new
server image.

## Check that it runs

```sh
kubectl -n redis-operator rollout status deployment/redis-operator --timeout=120s
kubectl -n redis-operator get pods -l app=redis-operator
kubectl get crd redis.redis.guid.foo backups.redis.guid.foo scheduledbackups.redis.guid.foo
kubectl -n redis-operator exec deploy/redis-operator -- /redis-operator version
```

The image has no shell, so `exec` runs the binary directly.

Each replica logs one line when it starts:

```text
redis-operator 2026.11.01-001 started, metrics on [::]:9200, instance managers from ghcr.io/chrisguidry/redis-operator:2026.11.01-001
```

The image named there is the one every Redis pod's init container runs
to install the instance manager. The operator reads it from its own
pod's container named `operator`, so a patch that renames that
container stops the operator from starting.

The leader holds the Lease `redis-operator` in the operator's
namespace. Its holder is the leader's pod name, an underscore, and a
random suffix:

```sh
kubectl -n redis-operator get lease redis-operator -o jsonpath='{.spec.holderIdentity}{"\n"}'
```

Read the leader's log, not the standby's:

```sh
kubectl -n redis-operator logs <leader pod> -f
```

## Metrics

Each replica serves Prometheus metrics on port 9200, the container
port named `metrics`, at `/metrics`. The environment variable
`METRICS_PORT` moves it. The base makes no Service or monitor for it;
take the monitoring component below, or add a PodMonitor or a Service
that selects `app: redis-operator`. The series:

| Series | What it counts |
|---|---|
| `redis_operator_leader` | 1 on the replica that holds the Lease, 0 on the standby |
| `redis_operator_build_info{version}` | always 1; the label is the release |
| `redis_operator_reconcile_errors_total` | passes over a `Redis` that failed |
| `redis_operator_failovers_total` | failovers started, counted once the start is written to the `Redis`'s status |
| `redis_operator_switchovers_total{reason}` | planned switchovers started, by reason: `NodeCordoned`, `PodDeleted`, `Rollout`, or `ScaleDown`, counted once the start is written to the `Redis`'s status |
| `redis_operator_watch_restarts_total{resource}` | watches that ended and were opened again |
| `redis_operator_polls_total{outcome}` | polls of a `Redis`'s instance managers: `informative` when one answered, `unanswered` when none did, `canceled` when the operator stopped first |
| `redis_operator_poll_duration_seconds` | how long each poll took, as a histogram |
| `redis_operator_replica_lag_bytes{namespace,redis}` | bytes the holder of a `Redis` in replica mode is behind its source, as last measured |

Every Redis instance pod also runs redis_exporter in a container named
`metrics`, on port 9121, the container port named `metrics`. Its series
keep redis_exporter's names. The `declare-a-redis` skill covers
`spec.metrics`.

### The monitoring component

A cluster that runs the Prometheus Operator takes
`deploy/monitoring` beside the base, at the same release:

```yaml
resources:
  - https://github.com/chrisguidry/redis-operator//deploy?ref=2026.11.01-001
components:
  - https://github.com/chrisguidry/redis-operator//deploy/monitoring?ref=2026.11.01-001
```

It adds, in the namespace `redis-operator`:

- the PodMonitor `redis-operator`, which scrapes the operator under
  `job="redis-operator"`;
- the PodMonitor `redis`, which scrapes the port named `metrics` of
  every pod with the label `redis.guid.foo/cluster`, in every
  namespace, and adds the labels `redis` (the `Redis`'s name) and
  `redis_role` (the pod's `redis.guid.foo/role`) to `namespace` and
  `pod`;
- the PrometheusRule `redis-operator`, with the alerts below.

It also adds the ConfigMap `redis-operator-dashboard`, labeled
`grafana_dashboard: "1"`, in the namespace `monitoring`, where the
kube-prometheus-stack's Grafana sidecar loads dashboards from.

The role label is `redis_role`, not `role`, because redis_exporter's
`redis_instance_info` has a `role` label of its own: the server's view,
`master` or `slave`. To label another series with the server's view,
join on it:

```promql
redis_memory_used_bytes * on (namespace, pod) group_left (role) redis_instance_info
```

A Prometheus whose `podMonitorSelector` or `ruleSelector` matches a
label, such as the kube-prometheus-stack's `release` label, ignores
these objects until they carry it. Add it with a patch in the same
kustomization:

```yaml
patches:
  - target:
      group: monitoring.coreos.com
    patch: |
      - op: add
        path: /metadata/labels/release
        value: kube-prometheus-stack
```

A Prometheus with `ignoreNamespaceSelectors` reads pods only in the
PodMonitor's own namespace, so it scrapes no Redis. A kustomization
that sets `namespace:` moves the dashboard's ConfigMap too.

| Alert | Fires when | For |
|---|---|---|
| `RedisOperatorNoLeader` | no operator replica reports `redis_operator_leader` 1, or none is scraped | 5m |
| `RedisRepeatedFailovers` | three or more failovers in an hour, across every `Redis` | at once |
| `RedisDown` | an instance's `redis_up` is 0, or its exporter cannot be scraped | 5m |
| `RedisNoPrimary` | no pod of a `Redis` carries `redis.guid.foo/role=primary` | 3m |
| `RedisReplicationLinkDown` | an instance's `redis_master_link_up` is 0 | 5m |
| `RedisReplicaLagging` | a replica has not acknowledged its primary for 30 seconds | 5m |
| `RedisMemoryNearMaxmemory` | an instance with `maxmemoryPolicy: noeviction` uses more than 90% of `maxmemory` | 15m |

Each one waits longer than a switchover, a failover, or a rolling
update takes, so it fires on a fault that stays. The `troubleshoot`
skill says what to read for each.

## NetworkPolicies

The operator polls each instance manager's `/status` on port 9123 of
every Redis pod, every second. In a namespace whose policies deny
ingress by default, admit the operator's pods on that port. Without
it, the operator reaches no instance manager and logs
`no instance manager answered on status port 9123` once for each
`Redis`. It cannot tell whether the primary is up, so it starts no
failover when the primary dies, and a failover that starts because no
instance holds the Lease cannot name the next primary:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: redis-operator
  namespace: example
spec:
  podSelector:
    matchExpressions:
      - {key: redis.guid.foo/cluster, operator: Exists}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: redis-operator
          podSelector:
            matchLabels:
              app: redis-operator
      ports:
        - {port: 9123, protocol: TCP}
```

The Redis pods also reach each other on 6379, for replication, and on
9123, where a primary that cannot renew its Lease asks its peers which
instance should be primary. With `spec.tls` they replicate on 6380. A
policy that admits the namespace's own pods covers them all. Clients
need 6379, or 6380 with TLS.
Prometheus needs 9121, the exporter's port named `metrics`, on every
Redis pod.

The operator also connects out: to the Kubernetes API, to an S3
endpoint when it deletes a backup's object, and to the source Redis or
its sentinels while it adopts one. The Redis pods connect to the S3
endpoint and to an adoption source. An egress policy must allow those.

## cert-manager

A `Redis` whose `spec.tls.issuerRef` names a cert-manager issuer gets a
Certificate that the operator makes. cert-manager is not needed to
install the operator, nor for a `Redis` without `issuerRef`, such as
one whose `spec.tls.secret` names a Secret you keep. The base's
ClusterRole has `create`, `patch`, and `delete` on
`certificates.cert-manager.io`, which grants nothing where cert-manager
is not installed. Install cert-manager, and an issuer that signs names
inside the cluster such as a CA issuer, before a `Redis` names one.

Where cert-manager is missing, the operator logs, once until it
recovers:

```text
redis example/cache: applying certificates.cert-manager.io cache-tls: ...; is cert-manager installed?
```

The rest of the `Redis` keeps running and failing over, and its new
pods wait for the Secret `cache-tls`.

The ClusterRole also has `patch` on Secrets, to rewrite the addresses
in a `Redis`'s connection Secret when `spec.tls` changes. The operator
grants each `Redis`'s instance managers `get` on the Secrets they read,
by name: its users' passwords, its certificate, and its backup store's
credentials.

## Upgrade the operator

Move both the `ref` and the `newTag` to the new release in one commit
or one edit, and apply again. The CRDs change with the base.

The operator's Deployment rolls its two replicas, which restarts no
Redis pod. The new operator then writes its own image into each
StatefulSet's init container, which changes the pod template, and it
rolls each `Redis`'s pods: replicas first, one at a time, then a
switchover, then the old primary. A `Redis` of one instance has its
only pod deleted and restarted.

A `Redis` that leaves `spec.image` empty runs the operator's default
server image, and a release can move that default. The `upgrade` skill
covers both rolls. The exporter image beside each instance is a default
of the operator in the same way, and its move changes the pod template
in the same roll; `spec.metrics.image` holds it still.

## Remove the operator

Deleting the operator's Deployment leaves every Redis running: the
instance managers keep serving and renewing the primary Lease. Without
the operator, nothing fails over, switches over before a drain, takes
a backup, or labels a new primary for the `-rw` Service.

Deleting the CRDs deletes every `Redis`, `Backup`, and
`ScheduledBackup` in the cluster, and the garbage collector then
deletes each `Redis`'s StatefulSet, pods, Services, and Secret. The
volume claims of a `Redis` with `storage` stay, with their data, until
you delete them.

To remove the operator and its data on purpose:

1. Delete each `Backup` you no longer want while the operator still
   runs. Each carries the finalizer `redis.guid.foo/delete-object`, and
   only the operator removes it, after it deletes the snapshot from the
   store. With the operator gone, a deleted `Backup` stays
   `Terminating`.
2. Delete each `Redis`.
3. `kubectl delete -k .` with the kustomization you installed from.

A `Backup` left `Terminating` after the operator is gone keeps its
CRD from being deleted. Clear its finalizers by hand only if the
object in the store may stay:

```sh
kubectl -n example patch backups.redis.guid.foo <name> --type=merge -p '{"metadata":{"finalizers":null}}'
```
