#!/usr/bin/env bash
# A k3s cluster in Docker: one server and three agents on one network,
# so every Redis instance has a machine of its own and one can be
# drained. The operator's image is built from this checkout and loaded
# into each node's containerd, and the server images are loaded the
# same way, so no node pulls anything during a scenario. The images are
# built and pulled while the cluster starts.
#
# Every node also bind-mounts one host directory at BACKUP_VOLUME_PATH.
# That stands in for a ReadWriteMany volume: the k3s image has no NFS
# mount helper, and k3s has no ReadWriteMany storage class, but a
# PersistentVolume on this path shows every node the same files.

K3S_IMAGE=rancher/k3s:v1.37.0-k3s1
AGENTS=3
BACKUP_VOLUME_PATH=/e2e-backups

start_cluster() {
  log "starting k3s: $SERVER and $AGENTS agents"
  mkdir -p "$WORKDIR/backups"
  # The instance managers run as another user than this script.
  chmod 777 "$WORKDIR/backups"
  # A k3s agent that starts while the server is still coming up can
  # lose its connection through its load balancer as the balancer moves
  # from the server's name to its address, and it shuts down. The
  # restart policy starts it again, and it joins.
  docker run -d --name "$SERVER" --hostname "$SERVER" --network "$NETWORK" --privileged --restart unless-stopped \
    -v "$WORKDIR/backups:$BACKUP_VOLUME_PATH" -p 127.0.0.1::6443 -e K3S_TOKEN="$RUN_ID" \
    "$K3S_IMAGE" server --disable=traefik,metrics-server --tls-san 127.0.0.1 >/dev/null
  for i in $(seq 1 "$AGENTS"); do
    docker run -d --name "$SERVER-agent-$i" --hostname "$SERVER-agent-$i" --network "$NETWORK" --privileged --restart unless-stopped \
      -v "$WORKDIR/backups:$BACKUP_VOLUME_PATH" -e K3S_TOKEN="$RUN_ID" -e K3S_URL="https://$SERVER:6443" \
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

# e2e_images lists the images every node gets: the operator's, the
# server image of this run's engine, and the exporter's. SeaweedFS's,
# the largest, joins them only when backup_restore runs, since that is
# the one scenario that starts a store.
e2e_images() {
  echo "$OPERATOR_IMAGE $SERVER_IMAGE $EXPORTER_IMAGE"
  case " $SCENARIOS " in *" backup_restore "*) echo "$SEAWEEDFS_IMAGE" ;; esac
}

# prepare_images builds the operator image and pulls the others, then
# saves them all to one archive. It runs while the cluster starts. The
# binary is built on this machine, as the Dockerfile builds it, so it
# reuses this machine's Go build cache, and the image holds only the
# binary.
prepare_images() {
  log "building the operator image ($OPERATOR_IMAGE)"
  mkdir -p "$WORKDIR/image"
  (cd "$REPO_ROOT" && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=e2e" -o "$WORKDIR/image/redis-operator" .)
  docker build -q -t "$OPERATOR_IMAGE" -f - "$WORKDIR/image" >/dev/null <<'DOCKERFILE'
FROM scratch
COPY redis-operator /redis-operator
USER 65532:65532
ENTRYPOINT ["/redis-operator"]
DOCKERFILE
  local image pids=()
  # shellcheck disable=SC2046
  for image in $(e2e_images); do
    [ "$image" = "$OPERATOR_IMAGE" ] && continue
    docker pull -q "$image" >/dev/null &
    pids+=($!)
  done
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
  # shellcheck disable=SC2046
  docker save -o "$WORKDIR/images.tar" $(e2e_images)
}

# load_images imports the archive prepare_images saved into every node
# at once.
load_images() {
  log "loading $(e2e_images | xargs) into every node"
  local node pids=()
  for node in $(node_containers); do
    docker exec -i "$node" ctr images import - <"$WORKDIR/images.tar" >/dev/null &
    pids+=($!)
  done
  for pid in "${pids[@]}"; do
    wait "$pid" || die "importing the images into a node failed"
  done
}

deploy_operator() {
  log "applying the CRD and the operator"
  kubectl apply -f "$REPO_ROOT/deploy/redis-crd.yaml" >/dev/null
  # kubectl wait fails at once on a CRD with no status yet, so this
  # waits for the condition itself.
  retry "the Redis CRD being established" 30 crd_established redis.redis.guid.foo
  kubectl create namespace redis-operator >/dev/null
  kubectl kustomize "$REPO_ROOT/deploy" |
    sed "s#ghcr.io/chrisguidry/redis-operator:latest#$OPERATOR_IMAGE#" |
    kubectl apply -f - >/dev/null
  # Short canary timings, so a failed rollout shows within the run.
  kubectl -n redis-operator set env deployment/redis-operator CANARY_SOAK=10s CANARY_TIMEOUT=40s >/dev/null
  kubectl -n redis-operator rollout status deployment/redis-operator --timeout=120s >/dev/null
}

crd_established() {
  [ "$(kubectl get crd "$1" -o jsonpath='{.status.conditions[?(@.type=="Established")].status}')" = True ]
}

# node_of prints the name of the node a pod runs on, which is also the
# name of its container.
node_of() {
  kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}'
}
