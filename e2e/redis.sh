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

# wait_ready waits until the Redis reports Ready with as many ready
# instances as its spec names, and every replica holds the primary's
# data. The next scenario breaks one instance, and the operator's
# failover quorum tolerates one failure only when every other instance
# holds the data: started on a replica that has not synced since its
# container restarted, a scenario breaks two, and the failover rightly
# waits for the old primary.
wait_ready() {
  retry "the Redis being ready" "$1" redis_whole
}

# redis_whole checks the Ready condition and the instance managers'
# own reports at once. The Ready condition alone can pass on a status
# written just before a container restarted.
redis_whole() {
  redis_ready && instances_synced
}

redis_ready() {
  local want ready status
  want=$(kubectl -n "$NS" get redis cache -o jsonpath='{.spec.instances}')
  ready=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.readyInstances}')
  status=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
  echo "instances=$want readyInstances=$ready Ready=$status"
  [ "$ready" = "$want" ] && [ "$status" = True ]
}

# instances_synced reads each instance manager's /status through the
# API server's proxy, and passes when the current primary holds the
# Lease and streams to every other pod, and every other pod follows it
# with its link up and is not stopping.
instances_synced() {
  local holder ip pods others report pod
  holder=$(primary)
  ip=$(kubectl -n "$NS" get pod "$holder" -o jsonpath='{.status.podIP}')
  pods=$(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=cache -o name | cut -d/ -f2)
  others=$(($(grep -c . <<<"$pods") - 1))
  for pod in $pods; do
    report=$(kubectl get --raw "/api/v1/namespaces/$NS/pods/$pod:9123/proxy/status") || return 1
    echo "$pod: $report"
    grep -q '"stopping":true' <<<"$report" && return 1
    if [ "$pod" = "$holder" ]; then
      grep -q '"role":"primary","holdsLease":true' <<<"$report" || return 1
      [ "$others" = 0 ] || grep -q "\"syncedReplicas\":$others[,}]" <<<"$report" || return 1
    else
      grep -q '"role":"replica"' <<<"$report" &&
        grep -q '"linkUp":true' <<<"$report" &&
        grep -q "\"following\":\"$ip\"" <<<"$report" || return 1
    fi
  done
}

primary() {
  kubectl -n "$NS" get redis cache -o jsonpath='{.status.currentPrimary}'
}

# start_writer runs a pod that increments a counter in a loop through
# cache-rw, and confirms each increment with WAIT 1 on the same
# connection. It prints each value a replica acknowledged, so the
# values in its log are the writes the client was told are safe.
#
# Its second container, held, writes through one connection that it
# keeps for as long as the server does, as an application's client
# does, and logs every reply. Like an application's client, it gives
# up a connect after a timeout, so a connect that kube-proxy sends to a
# pod that is gone does not hang it. A server that stops being primary
# must close that connection rather than answer READONLY, so its log is
# how the scenarios check what a client sees across a role change.
start_writer() {
  log "starting the writer"
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: writer
  annotations:
    kubectl.kubernetes.io/default-container: writer
spec:
  ${WRITER_NODE:+nodeName: $WRITER_NODE}
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
    - name: held
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
            while true; do echo "INCR held"; sleep 0.05; done | \$cli -t 2 -h cache-rw 2>&1
            sleep 1
          done
YAML
  kubectl -n "$NS" wait --for=condition=Ready pod/writer --timeout=60s >/dev/null
  retry "the writer confirming writes" 60 writer_confirmed_some
}

writer_confirmed_some() {
  [ -n "$(kubectl -n "$NS" logs writer --tail=1 2>/dev/null)" ]
}

# held_replies counts the replies in the held writer's log that match
# a pattern.
held_replies() {
  kubectl -n "$NS" logs writer -c held 2>/dev/null | grep -c "$1" || true
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
  assert_counter_kept "$1"
}

# assert_counter_kept checks that the counter is at least every value a
# replica confirmed, and that no write on the held connection was
# answered READONLY.
assert_counter_kept() {
  local confirmed current readonly
  confirmed=$(max_confirmed)
  current=$(counter)
  readonly=$(held_replies READONLY)
  log "$1: counter $current, highest confirmed $confirmed, READONLY replies $readonly"
  [ "$current" -ge "$confirmed" ] || die "$1 lost confirmed writes: the counter is $current, and $confirmed was confirmed"
  [ "$readonly" = 0 ] || die "$1: a client that kept its connection was answered READONLY $readonly times"
}

confirmed_past() {
  [ "$(max_confirmed)" -gt "$1" ]
}
