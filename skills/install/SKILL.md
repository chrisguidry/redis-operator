---
name: install
description: "Install, upgrade, or remove redis-operator from its kustomize base pinned to a release, choose its namespace, admit it through NetworkPolicies, and check that it runs and serves metrics. Use when a cluster has no Redis CRD yet, when moving the operator to a new release, or when removing it."
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
add a PodMonitor or a Service that selects `app: redis-operator` if
you scrape it. The series:

| Series | What it counts |
|---|---|
| `redis_operator_leader` | 1 on the replica that holds the Lease, 0 on the standby |
| `redis_operator_build_info{version}` | always 1; the label is the release |
| `redis_operator_reconcile_errors_total` | passes over a `Redis` that failed |
| `redis_operator_failovers_total` | failovers started because the primary could not be reached |
| `redis_operator_switchovers_total{reason}` | planned switchovers, by reason: `NodeCordoned`, `PodDeleted`, `Rollout`, or `ScaleDown` |
| `redis_operator_watch_restarts_total{resource}` | watches that ended and were opened again |

## NetworkPolicies

The operator polls each instance manager's `/status` on port 9123 of
every Redis pod, every 2 seconds. In a namespace whose policies deny
ingress by default, admit the operator's pods on that port, or the
operator sees every instance as silent and fails the primary over:

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
instance should be primary. A policy that admits the namespace's own
pods covers both. Clients need 6379.

The operator also connects out: to the Kubernetes API, to an S3
endpoint when it deletes a backup's object, and to the source Redis or
its sentinels while it adopts one. The Redis pods connect to the S3
endpoint and to an adoption source. An egress policy must allow those.

## Upgrade the operator

Move both the `ref` and the `newTag` to the new release in one commit
or one edit, and apply again. The CRDs change with the base.

The operator's Deployment rolls its two replicas, which restarts no
Redis pod. The new operator then writes its own image into each
StatefulSet's init container, which changes the pod template, and it
rolls each `Redis`'s pods: replicas first, one at a time, then a
switchover, then the old primary. A `Redis` of one instance has its
only pod deleted and restarted.

A `Redis` that leaves `spec.image` empty runs the engine's image at the
version the operator pins, and a release can move that pin. The
`upgrade` skill covers both rolls.

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
