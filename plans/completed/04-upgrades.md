# Milestone 4: staged upgrades and downgrades

A change of `spec.image` moves a Redis to another server version one
instance at a time, and never puts the data at risk: if the new version
cannot take the data, the operator stops, puts the instance it tried
back on the version that works, and says why.

## The problem

A rolling change already rolls the replicas first, switches over, and
rolls the old primary last ([01-high-availability.md](01-high-availability.md)).
That is enough for a change the server does not notice, such as more
memory. A version change can fail in a way a memory change cannot: a
replica copies its primary's data as an RDB snapshot, and a server
refuses a snapshot in a format newer than its own. Redis replicates
from an older primary to a newer replica, and not the other way, so
most downgrades fail at the first replica, and an image that does not
start fails every instance it reaches.

Without a stage, the roll stops at the first replica, which stays down
on the new image, and the Redis runs one replica short until a person
notices and reverts the spec.

## The design

`status.image` is the image every instance runs. A Redis that is not
rolling runs it, whatever `spec.image` says.

1. **Canary.** When `spec.image` differs from `status.image`, the
   operator writes `status.rollout` with the target image, the canary's
   name, and the phase `Canary`, applies the new image to the
   StatefulSet, and deletes the canary, the replica with the highest
   ordinal. Until the canary passes, it is the only pod that rolls. A
   `Redis` of one instance has no replica to prove the image on, so its
   rollout starts in the phase `Rolling`.
2. **Proof.** The canary passes when its pod runs the new pod template
   and is ready, and its replication link is up with its offset within
   1 MiB of the primary's. It must stay so for the canary soak, so a
   server that syncs and then fails is caught. A check that fails
   restarts the soak.
3. **Rolling.** After the canary passes, the phase is `Rolling`, and the
   other replicas roll one at a time, then the switchover, then the old
   primary, as in milestone 1.
4. **Done.** When every pod runs the new pod template and is ready,
   the operator writes the new image as `status.image`, clears
   `status.rollout`, and sets the condition `RolloutFailed` to `False`
   with the reason `Completed`.

If the canary is not proven within the canary timeout, counted from the
start of the rollout, the operator writes the phase `Failed`, with the
reason, applies `status.image` back to the StatefulSet, and the roll
deletes the canary at once, because it is down, so it comes back on the
image that works. The reason is the container's waiting reason if it has
one, otherwise the error its manager reported, otherwise that the
replication link is down. It does not try the same image again until
`spec.image` changes. The condition `RolloutFailed` is `True` with the
reason `CanaryFailed`, and names the image and the reason. When
`spec.image` returns to `status.image`, the operator clears the failed
rollout and sets the condition to `False` with the reason `Reverted`.
A `spec.image` that changes during a rollout starts a new one with a
new canary.

The primary changes version only after a replica on the new version has
held the data, so a failed roll never touches the primary.

## Downgrades

A downgrade runs the same stages. One the old version can take, such as
a patch release with the same RDB format, completes like an upgrade.
One it cannot take fails at the canary, with the Redis whole on the
newer version. The way back from there is a restore: take a `Backup`
on the old version before the upgrade, and restore it into a new
`Redis` on that version ([02-backups.md](02-backups.md)).

## Settings

| Setting | Value | Environment variable of the operator |
|---|---|---|
| Canary soak | 30 s | `CANARY_SOAK` |
| Canary timeout | 5 min | `CANARY_TIMEOUT` |
| Canary lag | 1 MiB | |

Each variable takes a Go duration such as `90s`. The end-to-end test
sets them short so a failed rollout shows within a run.

## Tests

- The operator's stages against the fake API server: a canary that
  passes and rolls on, one that never becomes ready and is rolled back,
  and a second spec change after a failure.
- End to end: an upgrade from an older Redis release under the writer's
  load with no confirmed write lost, and an image that cannot run, which
  the operator rolls back with the Redis whole.
