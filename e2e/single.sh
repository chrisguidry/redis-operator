#!/usr/bin/env bash
# The single-instance scenario. A Redis of one instance has no replica
# to fail over to, so a pod that restarts must come back as the
# primary: with its data when it has a volume, and as an empty dataset
# with the condition DataLost when it has none.

scenario_single_instance() {
  single_restart single-memory false
  single_restart single-disk true
}

# single_restart makes a Redis of one instance, writes a key, deletes
# its pod, and checks what the Redis holds once it is ready again.
single_restart() {
  local name=$1 persistent=$2 storage=""
  [ "$persistent" = true ] && storage="storage: {size: 1Gi}"
  log "scenario: delete the pod of $name, a Redis of one instance (volume: $persistent)"
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: $name
spec:
  engine: $ENGINE
  image: $SERVER_IMAGE
  instances: 1
  memory: 64Mi
  $storage
YAML
  retry "$name being ready" 180 single_ready "$name"
  retry "$name taking a write" 60 single_set "$name"
  kubectl -n "$NS" delete pod "$name-0" --wait=true --timeout=90s >/dev/null
  retry "$name being ready again" 180 single_ready "$name"
  retry "$name answering" 60 single_answers "$name"

  local value lost
  value=$(single_cli "$name" GET kept)
  lost=$(kubectl -n "$NS" get redis "$name" -o jsonpath='{.status.conditions[?(@.type=="DataLost")].status}')
  log "$name: kept=${value:-(none)} DataLost=${lost:-(none)}"
  if [ "$persistent" = true ]; then
    [ "$value" = yes ] || die "$name lost its data across a restart, though it has a volume"
    [ "$lost" != True ] || die "$name reports DataLost, though it kept its data"
  else
    [ -z "$value" ] || die "$name kept data with no volume, which the test does not expect"
    [ "$lost" = True ] || die "$name restarted empty and does not report DataLost"
  fi
  kubectl -n "$NS" delete redis "$name" --wait=false >/dev/null
}

single_ready() {
  local ready status
  ready=$(kubectl -n "$NS" get redis "$1" -o jsonpath='{.status.readyInstances}')
  status=$(kubectl -n "$NS" get redis "$1" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
  echo "readyInstances=$ready Ready=$status"
  [ "$ready" = 1 ] && [ "$status" = True ]
}

# single_cli runs a command against the Redis named $1 through its -rw
# Service, from the writer pod.
single_cli() {
  local name=$1 cli=redis-cli password
  shift
  [ "$ENGINE" = valkey ] && cli=valkey-cli
  password=$(kubectl -n "$NS" get secret "$name-app" -o jsonpath='{.data.password}' | base64 -d)
  kubectl -n "$NS" exec writer -- env REDISCLI_AUTH="$password" "$cli" -t 2 -h "$name-rw" "$@" 2>/dev/null
}

single_set() {
  [ "$(single_cli "$1" SET kept yes)" = OK ]
}

single_answers() {
  [ "$(single_cli "$1" PING)" = PONG ]
}
