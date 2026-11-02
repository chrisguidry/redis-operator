#!/usr/bin/env bash
# The chaos scenarios. Each one breaks something while the writer
# runs, waits for the Redis to be whole again, and checks that no write
# the writer was told is safe went missing.

scenario_delete_primary() {
  local old
  old=$(primary)
  log "scenario: delete the primary pod, $old"
  kubectl -n "$NS" delete pod "$old" --wait=false >/dev/null
  retry "a new primary" 120 primary_is_not "$old"
  wait_ready 180
  assert_no_confirmed_write_lost "deleting the primary"
}

scenario_drain_primary_node() {
  local old node
  old=$(primary)
  node=$(node_of "$old")
  log "scenario: drain $node, which runs the primary $old"
  kubectl cordon "$node" >/dev/null
  retry "the switchover off the cordoned node" 90 primary_is_not "$old"
  kubectl drain "$node" --pod-selector=redis.guid.foo/cluster=cache --ignore-daemonsets \
    --delete-emptydir-data --timeout=120s >/dev/null
  kubectl uncordon "$node" >/dev/null
  wait_ready 180
  assert_no_confirmed_write_lost "draining the primary's node"
}

scenario_operator_down() {
  local old
  old=$(primary)
  log "scenario: stop the operator, delete the primary $old, then start the operator"
  kubectl -n redis-operator scale deployment/redis-operator --replicas=0 >/dev/null
  kubectl -n redis-operator wait --for=delete pod -l app=redis-operator --timeout=60s >/dev/null
  kubectl -n "$NS" delete pod "$old" --wait=true --timeout=90s >/dev/null
  kubectl -n redis-operator scale deployment/redis-operator --replicas=2 >/dev/null
  retry "a new primary" 120 primary_is_not "$old"
  wait_ready 180
  assert_no_confirmed_write_lost "deleting the primary while the operator was down"
}

scenario_freeze_primary() {
  local old node
  old=$(primary)
  node=$(node_of "$old")
  log "scenario: freeze the primary $old on $node for 30 seconds"
  signal_instance "$node" STOP
  sleep 30
  signal_instance "$node" CONT
  retry "a new primary" 60 primary_is_not "$old"
  wait_ready 180
  assert_no_confirmed_write_lost "freezing the primary"
}

# scenario_partition_primary cuts the primary off from the API server,
# its peers, the operator, and its clients with a NetworkPolicy that
# admits nothing in or out, the way a network split leaves a machine.
# k3s enforces NetworkPolicy, and the kubelet's probes still reach the
# pod, as they do through a real split's node-local path.
scenario_partition_primary() {
  local old
  old=$(primary)
  log "scenario: partition the primary $old for 45 seconds"
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: partition
spec:
  podSelector:
    matchLabels:
      statefulset.kubernetes.io/pod-name: $old
  policyTypes: [Ingress, Egress]
YAML
  retry "a new primary" 90 primary_is_not "$old"
  sleep 15
  kubectl -n "$NS" delete networkpolicy partition >/dev/null
  wait_ready 180
  assert_no_confirmed_write_lost "partitioning the primary"
}

scenario_rollout() {
  log "scenario: change memory, so the operator rolls every pod"
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"memory":"160Mi"}}' >/dev/null
  retry "every pod running the new memory limit" 300 all_pods_rolled
  wait_ready 180
  assert_no_confirmed_write_lost "rolling a change"
}

# signal_instance sends a signal to the instance manager and the server
# on node. Each node runs one instance, by the pods' anti-affinity. The
# k3s image has no pkill, so this reads each process's command line.
signal_instance() {
  local count
  count=$(docker exec "$1" sh -c '
    n=0
    for dir in /proc/[0-9]*; do
      line=$(tr "\0" " " < "$dir/cmdline" 2>/dev/null) || continue
      case "$line" in
        "/manager/redis-operator manager"*|redis-server*|valkey-server*)
          kill -'"$2"' "${dir#/proc/}" && n=$((n + 1)) ;;
      esac
    done
    echo $n')
  [ "$count" -ge 2 ] || die "signal $2 reached $count processes on $1, not the manager and the server"
}

primary_is_not() {
  local now
  now=$(primary)
  echo "primary: $now"
  [ -n "$now" ] && [ "$now" != "$1" ]
}

all_pods_rolled() {
  local limits
  limits=$(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=cache \
    -o jsonpath='{range .items[*]}{.spec.containers[0].resources.limits.memory}{"\n"}{end}')
  echo "$limits"
  [ "$(echo "$limits" | grep -c '^160Mi$')" = 3 ]
}
