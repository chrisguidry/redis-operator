#!/usr/bin/env bash
# The backup scenario: a SeaweedFS S3 server in the test namespace, a
# Backup of the Redis under the writer's load, and a restore into a new
# Redis. SEAWEEDFS_IMAGE is the store's image.

SEAWEEDFS_IMAGE=chrislusf/seaweedfs:4.51

deploy_store() {
  log "deploying a SeaweedFS S3 store in $NS"
  local access secret
  access=e2e-access
  secret=$(od -An -tx1 -N16 /dev/urandom | tr -d ' \n')
  kubectl -n "$NS" create secret generic store \
    --from-literal=ACCESS_KEY_ID="$access" --from-literal=SECRET_ACCESS_KEY="$secret" >/dev/null
  kubectl -n "$NS" create secret generic store-config --from-literal=config.json="$(printf \
    '{"identities":[{"name":"e2e","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write","List"]}]}' \
    "$access" "$secret")" >/dev/null
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: store
spec:
  selector: {matchLabels: {app: store}}
  template:
    metadata: {labels: {app: store}}
    spec:
      ${WRITER_NODE:+nodeName: $WRITER_NODE}
      containers:
        - name: weed
          image: $SEAWEEDFS_IMAGE
          args: [server, -dir=/data, -s3, -s3.config=/etc/store/config.json, -volume.max=5, -master.volumeSizeLimitMB=64]
          volumeMounts:
            - {name: data, mountPath: /data}
            - {name: config, mountPath: /etc/store}
      volumes:
        - {name: data, emptyDir: {}}
        - {name: config, secret: {secretName: store-config}}
---
apiVersion: v1
kind: Service
metadata:
  name: store
spec:
  selector: {app: store}
  ports: [{name: s3, port: 8333}]
YAML
  kubectl -n "$NS" rollout status deployment/store --timeout=180s >/dev/null
  retry "the store making its bucket" 120 make_bucket
}

# make_bucket creates the bucket through the store's own shell. The
# image's entrypoint binds the master to the pod's address, so the shell
# dials that and not localhost.
make_bucket() {
  local output
  output=$(kubectl -n "$NS" exec deploy/store -- sh -c \
    'echo "s3.bucket.create -name backups" | weed shell -master="$(hostname -i):9333"' 2>&1)
  echo "$output"
  echo "$output" | grep -qE 'created bucket|already exist'
}

backup_phase() {
  kubectl -n "$NS" get backup "$1" -o jsonpath='{.status.phase}'
}

backup_completed() {
  local phase
  phase=$(backup_phase "$1")
  echo "phase: $phase $(kubectl -n "$NS" get backup "$1" -o jsonpath='{.status.error}')"
  [ "$phase" = Completed ]
}

scenario_backup_restore() {
  log "scenario: back up under load, then restore into a new Redis"
  deploy_store
  kubectl -n "$NS" patch redis cache --type=merge -p \
    '{"spec":{"backup":{"destination":"s3://backups/e2e","endpoint":"http://store:8333","credentials":{"secret":"store"}}}}' >/dev/null
  wait_ready 180
  local before
  before=$(max_confirmed)
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: Backup
metadata:
  name: under-load
spec:
  redis: cache
YAML
  retry "the backup completing" 180 backup_completed under-load
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: restored
spec:
  engine: $ENGINE
  image: $SERVER_IMAGE
  instances: 2
  memory: 128Mi
  bootstrap:
    recovery:
      backup: under-load
YAML
  retry "the restored Redis being ready" 240 restored_ready
  local restored
  restored=$(restored_counter)
  log "restoring: the restored counter is $restored, and $before was confirmed before the backup"
  [ "$restored" -ge "$before" ] || die "the restore lost confirmed writes: $restored is below $before"
  kubectl -n "$NS" delete backup under-load --wait=true --timeout=60s >/dev/null
}

restored_ready() {
  [ "$(kubectl -n "$NS" get redis restored -o jsonpath='{.status.readyInstances}')" = 2 ]
}

restored_counter() {
  local cli=redis-cli password
  [ "$ENGINE" = valkey ] && cli=valkey-cli
  password=$(kubectl -n "$NS" get secret restored-app -o jsonpath='{.data.password}' | base64 -d)
  kubectl -n "$NS" exec writer -- env REDISCLI_AUTH="$password" "$cli" -h restored-rw GET counter 2>/dev/null
}

# scenario_backup_restore_volume repeats the backup and the restore with
# a volume store, a ReadWriteMany claim, then lets a ScheduledBackup and
# a short retention take backups and delete them.
scenario_backup_restore_volume() {
  log "scenario: back up to a volume under load, then restore into a new Redis"
  create_backup_volume
  kubectl -n "$NS" patch redis cache --type=merge -p \
    '{"spec":{"backup":{"destination":null,"endpoint":null,"credentials":null,"volume":{"claimName":"backups"}}}}' >/dev/null
  # Adding the volume rolls the pods. A Backup started halfway through
  # could pick a pod that has no /backups yet and fail.
  retry "every pod mounting the volume" 300 all_pods_mount_volume
  wait_ready 180
  local before key
  before=$(max_confirmed)
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: Backup
metadata:
  name: volume-under-load
spec:
  redis: cache
YAML
  retry "the backup completing" 180 backup_completed volume-under-load
  key=$(backup_key volume-under-load)
  [ "$(kubectl -n "$NS" get backup volume-under-load -o jsonpath='{.status.size}')" -gt 0 ] ||
    die "the backup to the volume completed with no size"
  retry "the backup's file on the volume" 30 volume_has_file "$key"
  kubectl -n "$NS" delete redis restored --wait=true --timeout=120s >/dev/null 2>&1 || true
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: Redis
metadata:
  name: restored
spec:
  engine: $ENGINE
  image: $SERVER_IMAGE
  instances: 2
  memory: 128Mi
  bootstrap:
    recovery:
      backup: volume-under-load
YAML
  retry "the restored Redis being ready" 240 restored_ready
  local restored
  restored=$(restored_counter)
  log "restoring from the volume: the restored counter is $restored, and $before was confirmed before the backup"
  [ "$restored" -ge "$before" ] || die "the restore from the volume lost confirmed writes: $restored is below $before"
  kubectl -n "$NS" delete redis restored --wait=true --timeout=120s >/dev/null
  kubectl -n "$NS" delete backup volume-under-load --wait=true --timeout=60s >/dev/null
  retry "the backup's file leaving the volume" 30 volume_lacks_file "$key"

  scheduled_volume_backups
  assert_no_confirmed_write_lost "backing up to a volume"
}

# scheduled_volume_backups lets a ScheduledBackup make a Backup every
# minute under a retention of 90 seconds, and waits for retention to
# delete the first one, which must take its file off the volume too.
scheduled_volume_backups() {
  log "scenario: a ScheduledBackup every minute under a retention of 90 seconds"
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"backup":{"retention":"90s"}}}' >/dev/null
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: redis.guid.foo/v1alpha1
kind: ScheduledBackup
metadata:
  name: every-minute
spec:
  redis: cache
  schedule: "* * * * *"
YAML
  retry "a scheduled backup" 150 scheduled_backup_exists
  local first key
  first=$(scheduled_backups | head -1)
  retry "the first scheduled backup completing" 180 backup_completed "$first"
  key=$(backup_key "$first")
  retry "the first scheduled backup's file on the volume" 30 volume_has_file "$key"
  retry "retention deleting the first scheduled backup" 300 backup_is_gone "$first"
  retry "the deleted backup's file leaving the volume" 30 volume_lacks_file "$key"
  kubectl -n "$NS" delete scheduledbackup every-minute --wait=true --timeout=60s >/dev/null
  kubectl -n "$NS" delete backup -l redis.guid.foo/scheduled-backup --wait=true --timeout=120s >/dev/null
}

create_backup_volume() {
  kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: PersistentVolume
metadata:
  name: backups-$RUN_ID
spec:
  capacity: {storage: 1Gi}
  accessModes: [ReadWriteMany]
  storageClassName: ""
  persistentVolumeReclaimPolicy: Retain
  hostPath: {path: $BACKUP_VOLUME_PATH, type: Directory}
  claimRef: {namespace: $NS, name: backups}
YAML
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: backups
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ""
  volumeName: backups-$RUN_ID
  resources: {requests: {storage: 1Gi}}
YAML
}

all_pods_mount_volume() {
  local mounts
  mounts=$(kubectl -n "$NS" get pods -l redis.guid.foo/cluster=cache \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.containers[0].volumeMounts[*].mountPath}{"\n"}{end}')
  echo "$mounts"
  [ "$(echo "$mounts" | grep -c ' .*/backups')" = 3 ]
}

backup_key() {
  kubectl -n "$NS" get backup "$1" -o jsonpath='{.status.key}'
}

scheduled_backups() {
  kubectl -n "$NS" get backup -l redis.guid.foo/scheduled-backup -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
}

scheduled_backup_exists() {
  local names
  names=$(scheduled_backups)
  echo "backups: $names"
  [ -n "$names" ]
}

backup_is_gone() {
  ! kubectl -n "$NS" get backup "$1" >/dev/null 2>&1
}

volume_has_file() {
  docker exec "$SERVER" test -s "$BACKUP_VOLUME_PATH/$1"
}

volume_lacks_file() {
  ! docker exec "$SERVER" test -e "$BACKUP_VOLUME_PATH/$1"
}
