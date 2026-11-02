#!/usr/bin/env bash
# The adoption scenario: a Redis that the operator does not run, under
# a writer of its own, adopted into a Redis that replicates from it and
# is then promoted. Every write the source confirmed with WAIT 1 before
# the cutover must be in the adopted Redis.

scenario_adoption() {
  log "scenario: adopt a Redis the operator does not run"
  local password
  password=$(od -An -tx1 -N16 /dev/urandom | tr -d ' \n')
  kubectl -n "$NS" create secret generic legacy --from-literal=password="$password" >/dev/null
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: legacy
spec:
  selector: {matchLabels: {app: legacy}}
  template:
    metadata: {labels: {app: legacy}}
    spec:
      ${WRITER_NODE:+nodeName: $WRITER_NODE}
      containers:
        - name: server
          image: $SERVER_IMAGE
          env:
            - name: PASSWORD
              valueFrom: {secretKeyRef: {name: legacy, key: password}}
          command: [sh, -c, 'exec \$(command -v valkey-server || command -v redis-server) --requirepass "\$PASSWORD" --save "" --appendonly no']
---
apiVersion: v1
kind: Service
metadata:
  name: legacy
spec:
  selector: {app: legacy}
  ports: [{port: 6379}]
---
apiVersion: v1
kind: Pod
metadata:
  name: legacy-writer
spec:
  ${WRITER_NODE:+nodeName: $WRITER_NODE}
  restartPolicy: Never
  containers:
    - name: writer
      image: $SERVER_IMAGE
      env:
        - name: REDISCLI_AUTH
          valueFrom: {secretKeyRef: {name: legacy, key: password}}
      command:
        - sh
        - -c
        - |
          cli=redis-cli
          command -v valkey-cli >/dev/null && cli=valkey-cli
          while true; do
            reply=\$(printf 'INCR counter\nWAIT 1 1000\n' | timeout 3 \$cli -h legacy 2>/dev/null)
            value=\$(echo "\$reply" | sed -n 1p)
            acked=\$(echo "\$reply" | sed -n 2p)
            case "\$value\$acked" in
              *[!0-9]*|"") ;;
              *) [ "\$acked" -ge 1 ] && echo "\$value" ;;
            esac
            sleep 0.05
          done
YAML
  kubectl -n "$NS" rollout status deployment/legacy --timeout=120s >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready pod/legacy-writer --timeout=60s >/dev/null
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: adopted
spec:
  engine: $ENGINE
  image: $SERVER_IMAGE
  instances: 2
  memory: 128Mi
  replica:
    enabled: true
    source:
      host: legacy
      password: {secret: legacy, key: password}
YAML
  retry "the adopted Redis replicating with every instance ready" 240 adopted_replicating
  retry "the source confirming writes through its new replica" 60 legacy_confirmed_some
  sleep 10
  log "promoting the adopted Redis"
  kubectl -n "$NS" patch redis adopted --type=merge -p '{"spec":{"replica":{"enabled":false}}}' >/dev/null
  retry "the adopted Redis being promoted" 120 adopted_promoted
  # Writes the source accepts after the pause ends are not part of the
  # adoption, so the check reads what it confirmed before the cutover.
  local confirmed adopted
  confirmed=$(legacy_confirmed_before_promotion)
  retry "the adopted Redis taking writes" 60 adopted_writable
  adopted=$(adopted_get GET counter)
  log "adopting: the adopted counter is $adopted, and the source confirmed $confirmed before the cutover"
  [ "$adopted" -ge "$confirmed" ] || die "the adoption lost confirmed writes: $adopted is below $confirmed"
  kubectl -n "$NS" delete pod legacy-writer --wait=false >/dev/null
}

adopted_replicating() {
  local phase ready
  phase=$(kubectl -n "$NS" get redis adopted -o jsonpath='{.status.replica.phase}')
  ready=$(kubectl -n "$NS" get redis adopted -o jsonpath='{.status.readyInstances}')
  echo "phase=$phase ready=$ready"
  [ "$phase" = Replicating ] && [ "$ready" = 2 ]
}

adopted_promoted() {
  local phase
  phase=$(kubectl -n "$NS" get redis adopted -o jsonpath='{.status.replica.phase}')
  echo "phase=$phase"
  [ "$phase" = Promoted ]
}

legacy_confirmed_some() {
  [ -n "$(kubectl -n "$NS" logs legacy-writer --tail=1 2>/dev/null)" ]
}

# legacy_confirmed_before_promotion is the highest value the source
# confirmed at the moment the promotion is seen done: the pause holds
# every write the source accepts from the promotion's start, so a
# value confirmed later than that started after the cutover.
legacy_confirmed_before_promotion() {
  kubectl -n "$NS" logs legacy-writer 2>/dev/null | sort -n | tail -1
}

adopted_get() {
  local cli=redis-cli password
  [ "$ENGINE" = valkey ] && cli=valkey-cli
  password=$(kubectl -n "$NS" get secret adopted-app -o jsonpath='{.data.password}' | base64 -d)
  kubectl -n "$NS" exec writer -- env REDISCLI_AUTH="$password" "$cli" -h adopted-rw "$@" 2>/dev/null
}

adopted_writable() {
  [ "$(adopted_get SET adopted yes)" = OK ]
}
