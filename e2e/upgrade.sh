#!/usr/bin/env bash
# The upgrade scenarios: a staged rollout to an image that works, which
# proves the canary and rolls the rest, and one to an image that cannot
# run, which the operator rolls back with the Redis whole. The e2e
# operator shortens its canary timings, so the failure shows in
# minutes.

scenario_upgrade() {
  local image=docker.io/$SERVER_IMAGE
  case "$SERVER_IMAGE" in */*) ;; *) image=docker.io/library/$SERVER_IMAGE ;; esac
  log "scenario: a staged rollout to $image"
  kubectl -n "$NS" patch redis cache --type=merge -p "{\"spec\":{\"image\":\"$image\"}}" >/dev/null
  retry "the rollout to $image landing" 600 image_is "$image"
  wait_ready 180
  assert_no_confirmed_write_lost "a staged rollout"
}

scenario_broken_image() {
  local before
  before=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.image}')
  log "scenario: a rollout to an image that cannot run"
  kubectl -n "$NS" patch redis cache --type=merge -p '{"spec":{"image":"redis-operator.invalid/no-such-image:1"}}' >/dev/null
  retry "the canary failing" 600 rollout_phase_is Failed
  retry "the canary rolling back" 300 redis_ready
  [ "$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.image}')" = "$before" ] ||
    die "the image changed after a failed rollout"
  kubectl -n "$NS" patch redis cache --type=merge -p "{\"spec\":{\"image\":\"$before\"}}" >/dev/null
  retry "the failed rollout ending" 60 rollout_cleared
  assert_no_confirmed_write_lost "a failed rollout"
}

image_is() {
  local image phase
  image=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.image}')
  phase=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.rollout.phase}')
  echo "image=$image rollout=$phase"
  [ "$image" = "$1" ] && [ -z "$phase" ]
}

rollout_phase_is() {
  local phase
  phase=$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.rollout.phase}')
  echo "rollout=$phase $(kubectl -n "$NS" get redis cache -o jsonpath='{.status.rollout.reason}')"
  [ "$phase" = "$1" ]
}

rollout_cleared() {
  [ -z "$(kubectl -n "$NS" get redis cache -o jsonpath='{.status.rollout.phase}')" ]
}
