#!/usr/bin/env bash
# The operator's end-to-end test: a k3s cluster of four nodes in Docker,
# the operator built from this checkout, a Redis of three instances of
# ENGINE (redis or valkey), and a writer that confirms each write with
# WAIT 1. scenarios.sh lists what breaks, and redis.sh says what each
# scenario must keep.
#
# Every container and network name carries RUN_ID, so a second run on
# the same machine does not collide with this one. cleanup runs on any
# exit, and a failure prints the operator's log and the cluster's state.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
E2E_DIR="$REPO_ROOT/e2e"
ENGINE=${ENGINE:-redis}
NS=e2e
# The scenarios fall into shards. Each shard stands up its own cluster,
# so CI runs the shards as parallel jobs. A shard's scenarios run in the
# order listed, and each starts from the state the one before it left.
declare -A SHARDS=(
  [failover]="delete_primary drain_primary_node operator_down"
  [partition]="freeze_primary partition_primary partition_primary_from_peers"
  [access]="metrics acl_users tls backup_restore adoption"
  [backup_volume]="backup_restore_volume rollout"
  [scale]="scale scale_during_failover scale_while_the_primary_dies single_instance"
  [upgrade]="upgrade broken_image"
)
SHARD_ORDER="failover partition access backup_volume scale upgrade"
# SHARD runs one shard. SCENARIOS names the scenarios to run, in order,
# such as SCENARIOS=adoption to run one of them alone. With neither,
# every shard runs, one after another, on one cluster.
if [ -n "${SHARD:-}" ]; then
  [ -n "${SHARDS[$SHARD]:-}" ] || { echo "there is no shard named $SHARD; the shards are $SHARD_ORDER" >&2; exit 1; }
  SCENARIOS=${SHARDS[$SHARD]}
fi
if [ -z "${SCENARIOS:-}" ]; then
  for shard in $SHARD_ORDER; do
    SCENARIOS="${SCENARIOS:-} ${SHARDS[$shard]}"
  done
fi

# shellcheck source=common.sh disable=SC1091
source "$E2E_DIR/common.sh"
# shellcheck source=cluster.sh disable=SC1091
source "$E2E_DIR/cluster.sh"
# shellcheck source=redis.sh disable=SC1091
source "$E2E_DIR/redis.sh"
# shellcheck source=scenarios.sh disable=SC1091
source "$E2E_DIR/scenarios.sh"
# shellcheck source=backup.sh disable=SC1091
source "$E2E_DIR/backup.sh"
# shellcheck source=adoption.sh disable=SC1091
source "$E2E_DIR/adoption.sh"
# shellcheck source=upgrade.sh disable=SC1091
source "$E2E_DIR/upgrade.sh"
# shellcheck source=single.sh disable=SC1091
source "$E2E_DIR/single.sh"
# shellcheck source=metrics.sh disable=SC1091
source "$E2E_DIR/metrics.sh"
# shellcheck source=access.sh disable=SC1091
source "$E2E_DIR/access.sh"

for scenario in $SCENARIOS; do
  declare -F "scenario_$scenario" >/dev/null || die "there is no scenario named $scenario"
done

case "$ENGINE" in
  redis) SERVER_IMAGE=$(sed -n 's/^[[:space:]]*engineRedis: *"\(.*\)",$/\1/p' "$REPO_ROOT/api_redis.go") ;;
  valkey) SERVER_IMAGE=$(sed -n 's/^[[:space:]]*engineValkey: *"\(.*\)",$/\1/p' "$REPO_ROOT/api_redis.go") ;;
  *) die "ENGINE is $ENGINE; it must be redis or valkey" ;;
esac
EXPORTER_IMAGE=$(sed -n 's/^const defaultExporterImage = "\(.*\)"$/\1/p' "$REPO_ROOT/api_metrics.go")

RUN_ID=$(random_id)
NETWORK="redis-e2e-$RUN_ID"
SERVER="redis-e2e-$RUN_ID"
# The writer runs on the server node, which no scenario drains.
WRITER_NODE=$SERVER
OPERATOR_IMAGE="ghcr.io/chrisguidry/redis-operator:e2e-$RUN_ID"

# .e2e/ is under the repository, not the system temp directory, which
# can be a tmpfs.
mkdir -p "$REPO_ROOT/.e2e"
WORKDIR=$(mktemp -d "$REPO_ROOT/.e2e/$RUN_ID.XXXXXX")
KUBECONFIG="$WORKDIR/kubeconfig"
export KUBECONFIG

cleanup() {
  # The instance managers own the files on the backup volume, so they
  # are removed from inside a node, as root.
  docker exec "$SERVER" sh -c "rm -rf $BACKUP_VOLUME_PATH/*" >/dev/null 2>&1 || true
  # shellcheck disable=SC2046
  docker rm -f $(node_containers) >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  docker rmi "$OPERATOR_IMAGE" >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}

print_diagnostics() {
  echo "===== operator log =====" >&2
  kubectl -n redis-operator logs -l app=redis-operator --prefix --tail=300 >&2 2>&1 || true
  echo "===== instance managers =====" >&2
  kubectl -n "$NS" logs -l redis.guid.foo/cluster=cache --prefix --tail=100 -c redis >&2 2>&1 || true
  echo "===== held writer =====" >&2
  kubectl -n "$NS" logs writer -c held --tail=60 >&2 2>&1 || true
  echo "===== state =====" >&2
  kubectl -n "$NS" get redis,pods,leases,pdb -o wide >&2 2>&1 || true
  kubectl -n "$NS" get redis cache -o yaml >&2 2>&1 || true
}

on_exit() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    print_diagnostics
  fi
  cleanup
  exit "$status"
}
trap on_exit EXIT

log "run $RUN_ID: engine $ENGINE ($SERVER_IMAGE)"
docker network create "$NETWORK" >/dev/null
prepare_images >"$WORKDIR/prepare-images.log" 2>&1 &
preparing=$!
start_cluster
wait "$preparing" || { cat "$WORKDIR/prepare-images.log" >&2; die "preparing the images failed"; }
cat "$WORKDIR/prepare-images.log"
load_images
deploy_operator
create_redis
start_writer

for scenario in $SCENARIOS; do
  "scenario_$scenario"
done

log "scenarios passed on $ENGINE: $SCENARIOS"
