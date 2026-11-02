#!/usr/bin/env bash
# A k3s cluster in Docker: one server and three agents on one network,
# so every Redis instance has a machine of its own and one can be
# drained. The operator's image is built from this checkout and loaded
# into each node's containerd, and the server images are loaded the
# same way, so no node pulls anything during a scenario.

K3S_IMAGE=rancher/k3s:v1.37.0-k3s1
AGENTS=3

start_cluster() {
  log "starting k3s: $SERVER and $AGENTS agents"
  docker run -d --name "$SERVER" --hostname "$SERVER" --network "$NETWORK" --privileged \
    -p 127.0.0.1::6443 -e K3S_TOKEN="$RUN_ID" \
    "$K3S_IMAGE" server --disable=traefik,metrics-server --tls-san 127.0.0.1 >/dev/null
  for i in $(seq 1 "$AGENTS"); do
    docker run -d --name "$SERVER-agent-$i" --hostname "$SERVER-agent-$i" --network "$NETWORK" --privileged \
      -e K3S_TOKEN="$RUN_ID" -e K3S_URL="https://$SERVER:6443" \
      "$K3S_IMAGE" agent >/dev/null
  done
  retry "k3s's kubeconfig appearing" 90 docker exec "$SERVER" test -s /etc/rancher/k3s/k3s.yaml
  local port
  port=$(docker port "$SERVER" 6443/tcp | head -1 | cut -d: -f2)
  docker exec "$SERVER" cat /etc/rancher/k3s/k3s.yaml |
    sed "s#server: https://127.0.0.1:6443#server: https://127.0.0.1:$port#" >"$KUBECONFIG"
  retry "every node registering" 120 nodes_registered
  kubectl wait --for=condition=Ready node --all --timeout=120s >/dev/null
}

nodes_registered() {
  [ "$(kubectl get nodes --no-headers 2>/dev/null | wc -l)" -eq $((AGENTS + 1)) ]
}

node_containers() {
  echo "$SERVER"
  for i in $(seq 1 "$AGENTS"); do echo "$SERVER-agent-$i"; done
}

# load_images builds the operator image and imports it, and the server
# image of this run's engine, into every node.
load_images() {
  log "building the operator image ($OPERATOR_IMAGE)"
  docker build -q -t "$OPERATOR_IMAGE" --build-arg VERSION=e2e "$REPO_ROOT" >/dev/null
  docker pull -q "$SERVER_IMAGE" >/dev/null
  docker pull -q "$SEAWEEDFS_IMAGE" >/dev/null
  log "loading $OPERATOR_IMAGE, $SERVER_IMAGE, and $SEAWEEDFS_IMAGE into every node"
  for node in $(node_containers); do
    docker save "$OPERATOR_IMAGE" "$SERVER_IMAGE" "$SEAWEEDFS_IMAGE" | docker exec -i "$node" ctr images import - >/dev/null
  done
}

deploy_operator() {
  log "applying the CRD and the operator"
  kubectl apply -f "$REPO_ROOT/deploy/redis-crd.yaml" >/dev/null
  kubectl wait --for=condition=Established crd/redis.redis.guid.foo --timeout=30s >/dev/null
  kubectl create namespace redis-operator >/dev/null
  kubectl kustomize "$REPO_ROOT/deploy" |
    sed "s#ghcr.io/chrisguidry/redis-operator:latest#$OPERATOR_IMAGE#" |
    kubectl apply -f - >/dev/null
  # Short canary timings, so a failed rollout shows within the run.
  kubectl -n redis-operator set env deployment/redis-operator CANARY_SOAK=20s CANARY_TIMEOUT=90s >/dev/null
  kubectl -n redis-operator rollout status deployment/redis-operator --timeout=120s >/dev/null
}

# node_of prints the name of the node a pod runs on, which is also the
# name of its container.
node_of() {
  kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}'
}
