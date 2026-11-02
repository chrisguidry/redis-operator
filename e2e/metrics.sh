#!/usr/bin/env bash
# The exporter beside each instance: each pod's port named metrics
# serves its own server's series, and a change to spec.metrics rolls the
# pods like any other change to the template.

scenario_metrics() {
  log "scenario: every instance's exporter reports its server up, in the role the operator gave its pod"
  retry "each exporter reporting its server and role" 60 exporters_report_roles
  log "scenario: name a single key for the exporters to check, which rolls every pod"
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"metrics":{"checkSingleKeys":["counter"]}}}' >/dev/null
  retry "every pod checking the single key" 300 all_pods_check_counter
  wait_ready 180
  retry "each exporter reporting its server and role after the roll" 60 exporters_report_roles
  retry "the primary's exporter reporting the counter" 60 primary_reports_counter
  assert_no_confirmed_write_lost "rolling in the exporter's single keys"
}

# instance_metrics prints what the exporter in pod $1 serves, read
# through the API server's proxy to the pod's port.
instance_metrics() {
  kubectl get --raw "/api/v1/namespaces/$NS/pods/$1:9121/proxy/metrics"
}

# exporters_report_roles checks that every pod's exporter reaches its
# server, and that the server's own role matches the role label the
# operator wrote on the pod.
exporters_report_roles() {
  local pod label want metrics
  for pod in $(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=cache -o name | cut -d/ -f2); do
    label=$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.metadata.labels.redis\.guid\.foo/role}')
    case "$label" in
      primary) want=master ;;
      replica) want=slave ;;
      *) echo "$pod has no role label yet"; return 1 ;;
    esac
    metrics=$(instance_metrics "$pod") || { echo "$pod: no metrics"; return 1; }
    echo "$metrics" | grep -qx 'redis_up 1' || { echo "$pod: redis_up is not 1"; return 1; }
    echo "$metrics" | grep '^redis_instance_info{' | grep -q "role=\"$want\"" ||
      { echo "$pod is labeled $label, but its server does not report role $want"; return 1; }
  done
}

all_pods_check_counter() {
  local args
  args=$(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=cache \
    -o jsonpath='{range .items[*]}{.spec.containers[?(@.name=="metrics")].args}{"\n"}{end}')
  echo "$args"
  [ "$(echo "$args" | grep -c 'check-single-keys=counter')" = 3 ]
}

primary_reports_counter() {
  instance_metrics "$(primary)" | grep -E '^redis_key_size\{db="db0",key="counter"\} [1-9]'
}
