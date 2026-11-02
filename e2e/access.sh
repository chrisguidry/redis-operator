#!/usr/bin/env bash
# The access scenarios: ACL users declared from a Secret, and TLS with a
# certificate from a Secret the user keeps. cert-manager is not
# installed in the test cluster, so a Certificate from an issuer is
# tested only against the fake API server in the unit tests.

cli_name() {
  if [ "$ENGINE" = valkey ]; then echo valkey-cli; else echo redis-cli; fi
}

# as_app runs a command as the user app with password $1, against host
# $2, from the writer pod, and prints the reply or the error.
as_app() {
  local password=$1 host=$2
  shift 2
  kubectl -n "$NS" exec writer -- "$(cli_name)" --no-auth-warning --user app --pass "$password" -h "$host" "$@" 2>&1
}

# app_rules_hold checks what the app user may do through the host $2:
# write its own keys, and neither write another's nor read the
# configuration.
app_rules_hold() {
  local password=$1 host=$2 reply
  reply=$(as_app "$password" "$host" SET app:checked yes)
  echo "SET app:checked: $reply"
  [ "$reply" = OK ] || return 1
  reply=$(as_app "$password" "$host" SET other:checked yes)
  echo "SET other:checked: $reply"
  grep -q NOPERM <<<"$reply" || return 1
  reply=$(as_app "$password" "$host" CONFIG GET maxmemory)
  echo "CONFIG GET: $reply"
  grep -q NOPERM <<<"$reply"
}

# app_on_every_instance checks that every instance's server has the user,
# with password $1, by reading one of its keys on each.
app_on_every_instance() {
  local pod reply
  for pod in $(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=cache -o name | cut -d/ -f2); do
    reply=$(as_app "$1" "$pod.cache-nodes" GET app:checked)
    echo "$pod: $reply"
    if grep -Eq 'WRONGPASS|NOAUTH|NOPERM|ERR' <<<"$reply"; then return 1; fi
  done
}

app_refused() {
  local reply
  reply=$(as_app "$1" cache-rw PING)
  echo "PING: $reply"
  grep -q WRONGPASS <<<"$reply"
}

scenario_acl_users() {
  log "scenario: declare an ACL user from a Secret, and check its rules on every instance across a failover"
  kubectl -n "$NS" create secret generic app-user --save-config --from-literal=password=first-password >/dev/null
  kubectl -n "$NS" patch redis cache --type=merge -p \
    '{"spec":{"users":[{"name":"app","password":{"secret":"app-user","key":"password"},"rules":"~app:* +@all -@dangerous"}]}}' >/dev/null
  retry "app's rules holding on the primary" 60 app_rules_hold first-password cache-rw
  retry "app reaching every instance" 60 app_on_every_instance first-password

  local old
  old=$(primary)
  log "scenario: delete the primary $old, and check app's rules on the next one"
  kubectl -n "$NS" delete pod "$old" --wait=false >/dev/null
  retry "a new primary" 120 primary_is_not "$old"
  wait_ready 180
  retry "app's rules holding on the new primary" 60 app_rules_hold first-password cache-rw
  retry "app reaching every instance after the failover" 60 app_on_every_instance first-password

  log "scenario: rotate app's password in its Secret"
  local before
  before=$(pod_identities cache)
  kubectl -n "$NS" create secret generic app-user --from-literal=password=second-password \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  retry "app's new password working on every instance" 60 app_on_every_instance second-password
  retry "app's old password refused" 30 app_refused first-password
  [ "$(pod_identities cache)" = "$before" ] || die "rotating a password restarted a pod"

  log "scenario: remove app from the spec"
  kubectl -n "$NS" patch redis cache --type=json -p '[{"op":"remove","path":"/spec/users"}]' >/dev/null
  retry "app being gone" 60 app_refused second-password
  assert_no_confirmed_write_lost "declaring a user and failing over"
}

# pod_identities prints each pod of the Redis $1 with its uid and its
# server's restart count, so a change shows a pod that restarted.
pod_identities() {
  kubectl -n "$NS" get pods -l "redis.guid.foo/cluster=$1" \
    -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.uid} {.status.containerStatuses[?(@.name=="redis")].restartCount}{"\n"}{end}'
}

# make_certificate writes a CA to $1/ca.crt the first time, and a server
# certificate for the Redis named secure, signed by it, to
# $1/tls.crt and $1/tls.key, named $2.
make_certificate() {
  local dir=$1 name=$2
  if [ ! -f "$dir/ca.crt" ]; then
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
      -subj "/CN=e2e ca" -keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null
  fi
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$name" -keyout "$dir/tls.key" -out "$dir/tls.csr" 2>/dev/null
  openssl x509 -req -in "$dir/tls.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial -days 1 \
    -out "$dir/tls.crt" -extfile <(printf 'subjectAltName=%s\n' \
      "DNS:secure-rw,DNS:secure-rw.$NS.svc,DNS:secure-ro,DNS:secure-ro.$NS.svc,DNS:*.secure-nodes.$NS.svc,IP:127.0.0.1") 2>/dev/null
}

put_tls_secret() {
  kubectl -n "$NS" create secret generic secure-tls \
    --from-file=tls.crt="$1/tls.crt" --from-file=tls.key="$1/tls.key" --from-file=ca.crt="$1/ca.crt" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

# tls_cli runs a command against the Redis named secure over TLS, from
# a pod that trusts the CA, and prints the reply or the error.
tls_cli() {
  kubectl -n "$NS" exec tls-client -- "$(cli_name)" --tls --cacert /ca/ca.crt -p 6380 "$@" 2>&1
}

scenario_tls() {
  log "scenario: a Redis of $ENGINE with TLS from a Secret, and plaintext closed"
  local certs="$WORKDIR/tls"
  mkdir -p "$certs"
  make_certificate "$certs" first
  put_tls_secret "$certs"
  kubectl -n "$NS" create secret generic secure-ca --from-file=ca.crt="$certs/ca.crt" >/dev/null
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: secure
spec:
  engine: $ENGINE
  image: $SERVER_IMAGE
  instances: 2
  memory: 64Mi
  tls: {secret: secure-tls}
---
apiVersion: v1
kind: Pod
metadata:
  name: tls-client
spec:
  ${WRITER_NODE:+nodeName: $WRITER_NODE}
  restartPolicy: Never
  containers:
    - name: client
      image: $SERVER_IMAGE
      command: [sleep, infinity]
      volumeMounts: [{name: ca, mountPath: /ca}]
  volumes: [{name: ca, secret: {secretName: secure-ca}}]
YAML
  kubectl -n "$NS" wait --for=condition=Ready pod/tls-client --timeout=60s >/dev/null
  retry "secure being ready" 180 single_ready_count secure 2
  local password
  password=$(kubectl -n "$NS" get secret secure-app -o jsonpath='{.data.password}' | base64 -d)
  [ "$(kubectl -n "$NS" get secret secure-app -o jsonpath='{.data.uri}' | base64 -d)" = "rediss://:$password@secure-rw.$NS.svc:6380" ] ||
    die "secure-app's uri does not name TLS"
  retry "a write over TLS" 60 tls_set "$password" kept yes
  retry "the replica following over TLS" 60 replica_follows_over_tls "$password"
  retry "the plaintext port closing" 60 plaintext_refused "$password"
  retry "each exporter reading its server over TLS" 60 exporters_up secure

  local old
  old=$(kubectl -n "$NS" get redis secure -o jsonpath='{.status.currentPrimary}')
  log "scenario: delete secure's primary $old"
  kubectl -n "$NS" delete pod "$old" --wait=false >/dev/null
  retry "secure's new primary" 120 secure_primary_is_not "$old"
  retry "secure being ready again" 180 single_ready_count secure 2
  retry "secure keeping its data over TLS" 60 tls_get_is "$password" kept yes
  retry "the replica following over TLS after the failover" 60 replica_follows_over_tls "$password"

  log "scenario: rotate secure's certificate"
  local before
  before=$(pod_identities secure)
  make_certificate "$certs" second
  put_tls_secret "$certs"
  retry "every instance loading the new certificate" 60 every_instance_reloaded
  retry "a write over TLS after the rotation" 60 tls_set "$password" rotated yes
  [ "$(pod_identities secure)" = "$before" ] || die "rotating the certificate restarted a pod"

  kubectl -n "$NS" delete redis secure --wait=false >/dev/null
  kubectl -n "$NS" delete pod tls-client --wait=false >/dev/null
}

single_ready_count() {
  local ready status
  ready=$(kubectl -n "$NS" get redis "$1" -o jsonpath='{.status.readyInstances}')
  status=$(kubectl -n "$NS" get redis "$1" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
  echo "readyInstances=$ready Ready=$status"
  [ "$ready" = "$2" ] && [ "$status" = True ]
}

tls_set() {
  local reply
  reply=$(tls_cli -h secure-rw -a "$1" --no-auth-warning SET "$2" "$3")
  echo "$reply"
  [ "$reply" = OK ]
}

tls_get_is() {
  local reply
  reply=$(tls_cli -h secure-rw -a "$1" --no-auth-warning GET "$2")
  echo "$reply"
  [ "$reply" = "$3" ]
}

secure_primary_is_not() {
  local now
  now=$(kubectl -n "$NS" get redis secure -o jsonpath='{.status.currentPrimary}')
  echo "primary: $now"
  [ -n "$now" ] && [ "$now" != "$1" ]
}

# replica_follows_over_tls checks that the replica's link to the
# primary is up, on the primary's TLS port.
replica_follows_over_tls() {
  local info
  info=$(tls_cli -h secure-ro -a "$1" --no-auth-warning INFO replication)
  echo "$info"
  grep -q '^master_port:6380' <<<"$info" && grep -q '^master_link_status:up' <<<"$info"
}

plaintext_refused() {
  local reply
  reply=$(kubectl -n "$NS" exec tls-client -- "$(cli_name)" -h secure-rw -p 6379 -a "$1" --no-auth-warning PING 2>&1)
  echo "$reply"
  [ "$reply" != PONG ]
}

# exporters_up checks that every pod of the Redis $1 has an exporter
# that reaches its server.
exporters_up() {
  local pod
  for pod in $(kubectl -n "$NS" get pods -l "redis.guid.foo/cluster=$1" -o name | cut -d/ -f2); do
    instance_metrics "$pod" | grep -qx 'redis_up 1' || { echo "$pod: redis_up is not 1"; return 1; }
  done
}

every_instance_reloaded() {
  local pod
  for pod in $(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=secure -o name | cut -d/ -f2); do
    kubectl -n "$NS" logs "$pod" -c redis | grep -q 'loaded a new TLS certificate' ||
      { echo "$pod has not loaded a new certificate"; return 1; }
  done
}
