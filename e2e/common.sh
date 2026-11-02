#!/usr/bin/env bash
# Small helpers shared by every phase of the end-to-end test: logging,
# a name unique to this run, and a generic retry loop that stays quiet
# until it gives up, then prints what it last saw.

log() {
  printf '+ %s\n' "$*" >&2
}

die() {
  printf 'FATAL: %s\n' "$*" >&2
  exit 1
}

# random_id prints 8 hex characters, for container, network, and Job
# names that a second run of this script, on the same machine, must
# not collide with.
random_id() {
  od -An -tx1 -N4 /dev/urandom | tr -d ' \n'
}

# retry NAME TIMEOUT CHECK [ARGS...]
#
# Calls CHECK every 2 seconds until it exits zero, or TIMEOUT seconds
# have passed. CHECK reports what it saw on stdout or stderr; retry
# shows that only from the final, failing call, so a check that passes
# on its own writes nothing.
retry() {
  local name=$1 timeout=$2
  shift 2
  local deadline=$((SECONDS + timeout))
  local output
  while true; do
    if output=$("$@" 2>&1); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      printf 'timed out after %ss waiting for: %s\n' "$timeout" "$name" >&2
      printf '%s\n' "$output" >&2
      return 1
    fi
    sleep 2
  done
}
