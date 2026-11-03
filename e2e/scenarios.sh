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
  retry "an Event that the primary moved off $old" 30 primary_move_event "$old"
}

# primary_move_event checks that the Redis has an Event for the
# switchover or the failover that moved the primary off $1, as
# `kubectl get events` lists it. Deleting the primary's pod usually
# starts a switchover (PodDeleted); a pod that stops before its manager
# asks for one starts a failover instead.
primary_move_event() {
  local events
  events=$(kubectl -n "$NS" get events \
    --field-selector involvedObject.kind=Redis,involvedObject.name=cache \
    -o jsonpath='{range .items[*]}{.reason} {.message}{"\n"}{end}')
  echo "$events"
  grep -Eq "^(SwitchoverStarted moving the primary from $1 |FailoverStarted failing over from $1:)" <<<"$events"
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

# scenario_partition_primary_from_peers cuts the primary off from the
# API server, its peers, and the operator, as scenario_partition_primary
# does, but keeps the writer's path to it open, as a split that leaves
# a machine with its clients does. The writer's connections through
# cache-rw stop reaching it once the operator moves the role label, and
# its probe by pod IP shows the old primary refusing writes once
# min-replicas-to-write finds no replica, or once its instance manager
# fences it. No replica acknowledges a write it took after the cut, so
# the writer counts each one as unconfirmed, and the counter check
# shows that none it confirmed was lost.
scenario_partition_primary_from_peers() {
  local old node ip writer_ip started
  old=$(primary)
  node=$(node_of "$old")
  ip=$(kubectl -n "$NS" get pod "$old" -o jsonpath='{.status.podIP}')
  writer_ip=$(kubectl -n "$NS" get pod writer -o jsonpath='{.status.podIP}')
  log "scenario: partition the primary $old ($ip on $node) from everything but the writer ($writer_ip)"
  partition "$node" "$ip" -I
  admit "$node" "$ip" "$writer_ip" -I
  started=$SECONDS
  retry "the old primary refusing writes from the client that still reaches it" 25 old_primary_refuses "$ip"
  log "the old primary refused writes $((SECONDS - started))s after the cut"
  retry "a new primary" 90 primary_is_not "$old"
  sleep 15
  admit "$node" "$ip" "$writer_ip" -D
  partition "$node" "$ip" -D
  wait_ready 180
  assert_no_confirmed_write_lost "partitioning the primary from its peers"
}

# admit adds (-I) or removes (-D) the rules that let the traffic between
# the pod at $2 and the pod at $3 through the drops that partition
# puts on the node. -I puts them above the drops.
admit() {
  docker exec "$1" sh -c "
    PATH=\$PATH:/bin/aux
    iptables -t mangle $4 FORWARD -s $2 -d $3 -j ACCEPT
    iptables -t mangle $4 FORWARD -s $3 -d $2 -j ACCEPT"
}

# old_primary_refuses writes a probe key to the server at $1 by its pod
# IP, from the writer, and passes when the server refuses it.
old_primary_refuses() {
  local cli=redis-cli reply
  [ "$ENGINE" = valkey ] && cli=valkey-cli
  reply=$(kubectl -n "$NS" exec writer -- sh -c "timeout 3 $cli -h $1 INCR partition-probe" 2>&1)
  echo "$reply"
  grep -Eq 'NOREPLICAS|READONLY' <<<"$reply"
}

# scenario_partition_primary cuts the primary off from the API server,
# its peers, the operator, and its clients: the node it runs on drops
# every packet the pod sends or receives through the pod network, the
# way a network split leaves a machine. The kubelet's probes reach the
# pod from the node itself, so they still answer, as they do through a
# real split's node-local path.
scenario_partition_primary() {
  local old node ip
  old=$(primary)
  node=$(node_of "$old")
  ip=$(kubectl -n "$NS" get pod "$old" -o jsonpath='{.status.podIP}')
  log "scenario: partition the primary $old ($ip on $node) until another instance is primary, then 15 seconds more"
  partition "$node" "$ip" -I
  retry "a new primary" 90 primary_is_not "$old"
  sleep 15
  partition "$node" "$ip" -D
  wait_ready 180
  assert_no_confirmed_write_lost "partitioning the primary"
}

# partition adds (-I) or removes (-D) the rules that drop the pod's
# forwarded traffic on its node. They go in the mangle table, which runs
# before the filter table: k3s's network policy controller keeps its own
# jump at the top of the filter table's FORWARD chain, and that chain
# accepts the pod's traffic before a drop below it is reached.
partition() {
  docker exec "$1" sh -c "
    PATH=\$PATH:/bin/aux
    iptables -t mangle $3 FORWARD -s $2 -j DROP
    iptables -t mangle $3 FORWARD -d $2 -j DROP"
}

scenario_rollout() {
  log "scenario: change memory, so the operator rolls every pod"
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"memory":"160Mi"}}' >/dev/null
  retry "every pod running the new memory limit" 300 all_pods_rolled
  wait_ready 180
  assert_no_confirmed_write_lost "rolling a change"
}

# scenario_scale scales the Redis from three instances to one while the
# primary is not cache-0, so the operator must switch over to cache-0
# before the StatefulSet shrinks, and then back to three, which must
# not refuse a write while the new replicas sync.
scenario_scale() {
  if [ "$(primary)" = cache-0 ]; then
    log "scenario: delete the primary cache-0, so that scaling down has to move the primary"
    kubectl -n "$NS" delete pod cache-0 --wait=false >/dev/null
    retry "a new primary" 120 primary_is_not cache-0
    wait_ready 180
  fi
  log "scenario: scale from three instances to one while $(primary) is primary"
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"instances":1}}' >/dev/null
  retry "the primary moving to cache-0" 120 primary_is cache-0
  retry "one pod left" 120 pod_count_is 1
  wait_ready 180
  retry "the held writer writing to cache-0" 60 held_writes_after "$(held_replies '^[0-9]')"
  assert_counter_kept "scaling to one instance"

  local refused
  refused=$(held_replies NOREPLICAS)
  log "scenario: scale from one instance to three"
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"instances":3}}' >/dev/null
  wait_ready 180
  assert_no_confirmed_write_lost "scaling to three instances"
  [ "$(held_replies NOREPLICAS)" = "$refused" ] ||
    die "scaling up refused writes: the held writer saw $(($(held_replies NOREPLICAS) - refused)) NOREPLICAS replies"
}

# scenario_scale_during_failover freezes the primary and one replica, so
# the failover that starts waits for its quorum, and lowers instances
# to one while it waits. The StatefulSet must keep its three pods, since
# the frozen replica may hold the only other copy of a confirmed write.
# The freeze ends before the kubelet's liveness probe restarts either
# pod. Then the failover names the next primary, the scale-down moves it
# to cache-0 if it is not there already, and the StatefulSet shrinks.
scenario_scale_during_failover() {
  local old replica old_node replica_node
  old=$(primary)
  replica=$(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=cache -o name | cut -d/ -f2 | grep -vx "$old" | head -1)
  old_node=$(node_of "$old")
  replica_node=$(node_of "$replica")
  log "scenario: freeze the primary $old and the replica $replica, then scale to one instance while the failover waits"
  signal_instance "$old_node" STOP
  signal_instance "$replica_node" STOP
  retry "a pending failover" 20 target_is pending
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"instances":1}}' >/dev/null
  retry "Ready saying the scale-down waits" 10 ready_message_has "keeps its 3 pods until the next primary holds the lease, then scales down to 1"
  statefulset_replicas_are 3 || die "the StatefulSet shrank while the failover was pending"
  pod_count_is 3 >/dev/null || die "a pod went away while the failover was pending"
  signal_instance "$replica_node" CONT
  signal_instance "$old_node" CONT
  retry "the primary moving to cache-0" 180 primary_is cache-0
  retry "one pod left" 120 pod_count_is 1
  wait_ready 180
  # One instance has no replica to acknowledge WAIT 1, so the writer
  # confirms nothing new; the held writer shows that writes go on.
  retry "the held writer writing to cache-0" 60 held_writes_after "$(held_replies '^[0-9]')"
  assert_counter_kept "scaling to one instance during a failover"

  log "scenario: scale back to three instances"
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"instances":3}}' >/dev/null
  wait_ready 180
  assert_no_confirmed_write_lost "scaling back to three instances"
}

target_is() {
  local now
  now=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.targetPrimary}')
  echo "targetPrimary: $now"
  [ "$now" = "$1" ]
}

ready_message_has() {
  local message
  message=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}')
  echo "Ready: $message"
  grep -qF "$1" <<<"$message"
}

statefulset_replicas_are() {
  local replicas
  replicas=$(kubectl -n "$NS" get statefulset cache -o jsonpath='{.spec.replicas}')
  echo "StatefulSet replicas: $replicas"
  [ "$replicas" = "$1" ]
}

primary_is() {
  local now
  now=$(primary)
  echo "primary: $now"
  [ "$now" = "$1" ]
}

pod_count_is() {
  local pods
  pods=$(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=cache -o name)
  echo "$pods"
  [ "$(echo "$pods" | grep -c .)" = "$1" ]
}

held_writes_after() {
  [ "$(held_replies '^[0-9]')" -gt "$1" ]
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
