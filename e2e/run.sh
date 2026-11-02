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

# shellcheck source=common.sh disable=SC1091
source "$E2E_DIR/common.sh"
# shellcheck source=cluster.sh disable=SC1091
source "$E2E_DIR/cluster.sh"
# shellcheck source=redis.sh disable=SC1091
source "$E2E_DIR/redis.sh"
# shellcheck source=scenarios.sh disable=SC1091
source "$E2E_DIR/scenarios.sh"

case "$ENGINE" in
  redis) SERVER_IMAGE=$(sed -n 's/^[[:space:]]*engineRedis: *"\(.*\)",$/\1/p' "$REPO_ROOT/api_redis.go") ;;
  valkey) SERVER_IMAGE=$(sed -n 's/^[[:space:]]*engineValkey: *"\(.*\)",$/\1/p' "$REPO_ROOT/api_redis.go") ;;
  *) die "ENGINE is $ENGINE; it must be redis or valkey" ;;
esac

RUN_ID=$(random_id)
NETWORK="redis-e2e-$RUN_ID"
SERVER="redis-e2e-$RUN_ID"
OPERATOR_IMAGE="ghcr.io/chrisguidry/redis-operator:e2e-$RUN_ID"

# .e2e/ is under the repository, not the system temp directory, which
# can be a tmpfs.
mkdir -p "$REPO_ROOT/.e2e"
WORKDIR=$(mktemp -d "$REPO_ROOT/.e2e/$RUN_ID.XXXXXX")
KUBECONFIG="$WORKDIR/kubeconfig"
export KUBECONFIG

cleanup() {
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
start_cluster
load_images
deploy_operator
create_redis
start_writer

scenario_delete_primary
scenario_drain_primary_node
scenario_operator_down
scenario_freeze_primary
scenario_partition_primary
scenario_rollout

log "all scenarios passed on $ENGINE"
