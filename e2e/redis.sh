#!/usr/bin/env bash
# The Redis under test, the writer that loads it, and the checks the
# scenarios share.

create_redis() {
  log "creating a Redis of $ENGINE with three instances"
  kubectl create namespace "$NS" >/dev/null
  kubectl apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: cache
  namespace: $NS
spec:
  engine: $ENGINE
  image: $SERVER_IMAGE
  instances: 3
  memory: 128Mi
YAML
  wait_ready 180
}

# wait_ready waits until the Redis reports Ready with every instance
# ready.
wait_ready() {
  retry "the Redis being ready" "$1" redis_ready
}

redis_ready() {
  local ready status
  ready=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.readyInstances}')
  status=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
  echo "readyInstances=$ready Ready=$status"
  [ "$ready" = 3 ] && [ "$status" = True ]
}

primary() {
  kubectl -n "$NS" get redis cache -o jsonpath='{.status.currentPrimary}'
}

# start_writer runs a pod that increments a counter in a loop through
# cache-rw, and confirms each increment with WAIT 1 on the same
# connection. It prints each value a replica acknowledged, so the
# values in its log are the writes the client was told are safe.
start_writer() {
  log "starting the writer"
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: writer
spec:
  nodeName: $SERVER
  restartPolicy: Never
  containers:
    - name: writer
      image: $SERVER_IMAGE
      env:
        - name: REDISCLI_AUTH
          valueFrom: {secretKeyRef: {name: cache-app, key: password}}
      command:
        - sh
        - -c
        - |
          cli=redis-cli
          command -v valkey-cli >/dev/null && cli=valkey-cli
          while true; do
            reply=\$(printf 'INCR counter\nWAIT 1 1000\n' | timeout 3 \$cli -h cache-rw 2>/dev/null)
            value=\$(echo "\$reply" | sed -n 1p)
            acked=\$(echo "\$reply" | sed -n 2p)
            case "\$value\$acked" in
              *[!0-9]*|"") ;;
              *) [ "\$acked" -ge 1 ] && echo "\$value" ;;
            esac
            sleep 0.05
          done
YAML
  kubectl -n "$NS" wait --for=condition=Ready pod/writer --timeout=60s >/dev/null
  retry "the writer confirming writes" 60 writer_confirmed_some
}

writer_confirmed_some() {
  [ -n "$(kubectl -n "$NS" logs writer --tail=1 2>/dev/null)" ]
}

max_confirmed() {
  kubectl -n "$NS" logs writer 2>/dev/null | sort -n | tail -1
}

# counter reads the counter on the primary.
counter() {
  local cli=redis-cli
  [ "$ENGINE" = valkey ] && cli=valkey-cli
  kubectl -n "$NS" exec writer -- sh -c "$cli -h cache-rw GET counter" 2>/dev/null
}

# assert_no_confirmed_write_lost checks the invariant every scenario
# keeps: the counter is at least every value a replica confirmed, so no
# confirmed increment was lost, and writes are being confirmed again.
assert_no_confirmed_write_lost() {
  local before
  before=$(max_confirmed)
  retry "the writer confirming new writes after $1" 90 confirmed_past "$before"
  local confirmed current
  confirmed=$(max_confirmed)
  current=$(counter)
  log "$1: counter $current, highest confirmed $confirmed"
  [ "$current" -ge "$confirmed" ] || die "$1 lost confirmed writes: the counter is $current, and $confirmed was confirmed"
}

confirmed_past() {
  [ "$(max_confirmed)" -gt "$1" ]
}
