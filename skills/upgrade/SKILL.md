---
name: upgrade
description: "Move a Redis to another server image through the operator's canary rollout, read status.rollout and the RolloutFailed condition, tune CANARY_SOAK and CANARY_TIMEOUT, roll back a failed image, and upgrade the operator itself, which rolls every Redis's pods. Use when changing spec.image, when a rollout stays in Canary or ends Failed, or when moving the operator to a new release."
---

Before the first command, run `kubectl config current-context` and confirm that it names the cluster the person means.

# Upgrade

Two things change a `Redis`'s pods: a new server image, through
`spec.image`, and a new operator release, which runs a new instance
manager in every pod. Each rolls the pods one at a time: the replicas
first, highest ordinal first, then a switchover to an updated replica,
then the old primary. A planned switchover loses no write.

Take a `Backup` before a server upgrade (see the `backups` skill). A
downgrade that fails, or a failed upgrade, is undone by restoring it
into a new `Redis` on the old version.

## Change the server image

```sh
kubectl -n example patch redis cache --type=merge -p '{"spec":{"image":"redis:8.10.4"}}'
```

`status.image` is the image every instance runs. When `spec.image`
differs from it, the operator starts a rollout and records it in
`status.rollout`:

| Field | Meaning |
|---|---|
| `image` | the image the rollout moves to |
| `phase` | `Canary`, `Rolling`, or `Failed` |
| `canary` | the replica that proves the image |
| `startedAt` | when the rollout started |
| `reason` | why the canary failed |

1. **Canary.** The canary is the replica with the highest ordinal. The
   operator applies the new image to the StatefulSet and deletes only
   the canary's pod.
2. **Proof.** The canary passes when its pod runs the new pod template,
   is ready, has its replication link up, and is within 1 MiB of the
   primary's offset, and stays so for the canary soak, 30 seconds by
   default. A check that fails restarts the soak.
3. **Rolling.** The other replicas roll one at a time, each after every
   pod is ready again, then the switchover, then the old primary.
4. **Done.** When every pod runs the new template and is ready, the
   operator writes the new image to `status.image`, clears
   `status.rollout`, and sets `RolloutFailed` to `False` with the reason
   `Completed`.

A `Redis` of one instance has no replica to prove the image on. Its
rollout starts in `Rolling`, and its only pod is deleted, so it serves
nothing until the new pod is ready. Without `storage`, that restart
loses its data.

Watch it:

```sh
kubectl -n example get redis cache -o wide -w
kubectl -n example get redis cache -o jsonpath='{.status.rollout}{"\n"}'
kubectl -n example get pods -l redis.guid.foo/cluster=cache -L controller-revision-hash,redis.guid.foo/role
```

Nothing rolls while a failover or switchover runs, while no instance
holds the primary Lease, or while the StatefulSet has fewer pods than
`instances`.

### A canary that fails

If the canary is not proven within the canary timeout, 5 minutes from
the start of the rollout by default, the operator:

- writes `phase: Failed` and a `reason`: the container's waiting
  reason, such as `ImagePullBackOff` or `CrashLoopBackOff`, if it has
  one; otherwise the error the instance manager reported; otherwise
  that the canary could not replicate from the primary, which is what a
  server that cannot read the primary's RDB format shows;
- puts `status.image` back on the StatefulSet and deletes the canary
  again, so it returns on the image that works;
- sets `RolloutFailed` to `True` with the reason `CanaryFailed` and a
  message naming the canary, the image, and the reason.

The primary is never touched by a failed rollout. The operator does
not try the same image again. To end the failed rollout, set
`spec.image` back to `status.image`; the operator clears
`status.rollout` and sets `RolloutFailed` to `False` with the reason
`Reverted`. To try another image, set `spec.image` to it; a change
during a rollout starts a new rollout with a new canary.

### Downgrades

A downgrade runs the same stages. One the old version can read, such
as a patch release with the same RDB format, completes like an upgrade.
One it cannot read fails at the canary, because a replica on an older
version refuses a newer primary's snapshot. The data is whole on the
newer version, and the way back to the older one is a restore of a
`Backup` taken before the upgrade.

### Canary timings

The operator reads two environment variables, each a Go duration such
as `90s`:

| Variable | Default | What it sets |
|---|---|---|
| `CANARY_SOAK` | `30s` | how long the canary must stay proven |
| `CANARY_TIMEOUT` | `5m` | how long from the start of a rollout before the canary fails |

Set them on the operator's Deployment through the kustomization you
install from, so the next apply does not remove them:

```yaml
patches:
  - target: {kind: Deployment, name: redis-operator}
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/env/-
        value: {name: CANARY_SOAK, value: 2m}
```

A large data set takes longer to sync to the canary; raise
`CANARY_TIMEOUT` above the time a full resync takes.

### Other spec changes

A change of `memory`, `cpu`, `nodeSelector`, `tolerations`,
`priorityClassName`, or a backup volume changes the pod template and
rolls the pods in the same order, without a canary.

## Upgrade the operator

Move the kustomization's `ref` and `newTag` to the new release together
and apply it; the `install` skill shows the kustomization.

1. The Deployment `redis-operator` rolls its two replicas. That
   restarts no Redis pod, and the instance managers keep serving.
2. The new operator writes its own image into each StatefulSet's init
   container `install-manager`, which installs the instance manager in
   each pod. That changes the pod template, so the operator rolls each
   `Redis`'s pods: replicas first, then a switchover with the reason
   `Rollout`, then the old primary. There is no canary for this roll.

Check which instance manager each `Redis` runs:

```sh
kubectl -n example get statefulset cache -o jsonpath='{.spec.template.spec.initContainers[0].image}{"\n"}'
kubectl -n example get pods -l redis.guid.foo/cluster=cache -o custom-columns=NAME:.metadata.name,MANAGER:.spec.initContainers[0].image
```

### The default server image is the operator's

A `Redis` that leaves `spec.image` empty runs the image in the
operator's `DEFAULT_REDIS_IMAGE` or `DEFAULT_VALKEY_IMAGE`, such as
`redis:8.10.3` or `valkey/valkey:9.0.7`. Each release sets them in
`deploy/operator.yaml`. A release or a patch that changes one starts a
canary rollout of the new server image on every such `Redis` as soon
as the restarted operator reconciles it. When a release moves the
default, the new server image and the new instance manager roll
together, through the canary. To choose when the server version
changes, set `spec.image` explicitly on each `Redis`.
