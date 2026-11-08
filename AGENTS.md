# Working on redis-operator

This repository holds the `Redis`, `Backup`, and `ScheduledBackup`
resources and the operator that runs Redis and Valkey from them.
`plans/00-design.md` is the design and the source of each fact it
depends on. Later plans are numbered after it, one for each milestone:
high availability, backups, adoption, and staged upgrades. When a change
alters what a plan says, fix the plan in the same commit. Move a plan to
`plans/completed/` only when the maintainer says so.

## Layout

The operator is one flat `package main`, with files grouped by prefix.
It talks to the Kubernetes API over HTTP with no client-go and no
controller framework. The one binary is both the operator and the
instance manager that runs in every Redis pod. Its commands are the
operator (no argument), `manager`, `install DIR`, and `version`.

- `api_*.go`: the Go types of the resources it reads and writes, and
  the defaults of a spec (`api_redis.go`). `deploy/*-crd.yaml` are the
  schemas, written by hand, and `api_redis_crd_test.go` checks them
  against the types and against the API server's rules.
- `kube_*.go`, `leader.go`: the Kubernetes API client, the watches and
  their stores, and the operator's leader election.
- `reconcile*.go`, `children.go`: the operator's loop and decisions.
  `reconcile_redis.go` decides failover, switchover, and rolling;
  `reconcile_backup.go`, `reconcile_replica.go`, and
  `reconcile_rollout.go` decide backups, replica mode, and image
  rollouts. `children.go` builds the objects each `Redis` owns.
- `manager*.go`: the instance manager. `manager.go` is its step loop and
  the status endpoints, `manager_lease.go` the primary Lease,
  `manager_server.go` the server's configuration and role changes,
  `manager_backup.go` snapshots and restores, and `manager_replica.go`
  replica mode. `redis_conn.go` is its RESP client.
- `s3.go`, `backup_target.go`, `hosts.go`, `cron.go`: the S3 client, the
  volume store with the interface both share, the qualification of a
  host of one label into the `Redis`'s namespace, and the cron parser.
- `metrics.go`: the operator's Prometheus metrics, prefixed
  `redis_operator_`.
- `deploy/`: the kustomize base that a consumer takes: the CRDs, the
  operator's RBAC, and its Deployment. `deploy/examples/` holds a
  sample `Redis`.
- `e2e/`: the end-to-end scripts.
- `plans/`: one document for each milestone.
- `skills/`: one guide for each task a user does with the operator, as
  an Agent Skill (`skills/<name>/SKILL.md`). `.claude-plugin/` makes the
  repository a plugin marketplace that installs them, for Claude Code
  and for Codex.

## Conventions

- A change to what the operator does, such as a field, a default, a
  port, a condition reason, or a command, changes the text in `skills/`
  and `README.md` that describes it in the same commit, as it does the
  plan.
- The operator is level-triggered. It keeps no state that the API
  server does not hold, except what time measures (`redisState` in
  `reconcile.go`, and each Redis's poller in `reconcile_poll.go`). The
  instance manager also keeps the UIDs of the Backups it uploaded and
  the `min-replicas-to-write` its server runs; `plans/00-design.md`
  says why. Each decision is a write to a status field, made with
  the `resourceVersion` the pass read, so a stale pass fails.
- The operator makes children with server-side apply, and skips a write
  when the body equals what it applied last.
- Timings are fields of `timings` and `managerConfig`, so a test can
  shorten them. The two canary timings also come from the environment.
- A fault that repeats on every pass writes one log line, through
  `lineLog` in `reconcile_log.go`.
- A fact that two places must agree on has one definition. The defaults
  of a spec are methods on `RedisSpec`, and both the operator and the
  instance managers call them.
- Examples and tests use example.com and example.net for hosts.

## Tests

Write the failing test first. Tests run against fakes of the real
servers and against real Redis, Valkey, and SeaweedFS processes. Do not
add mocks. `kube_server_test.go` holds `fakeKube`, a fake API server
that keeps the rules the reconciler depends on, and
`reconcile_harness_test.go` holds the harness that runs a controller on
a clock the test moves. A test file sits beside the file it tests.

`make test` copies `redis-server`, `valkey-server`, and `weed` out of
the images the operator runs by default into `.servers/`, which needs
Docker, and puts them on the tests' PATH. To run `go test` by hand, run
`make servers` once and add `.servers` to PATH. `make test` runs
`gofmt`, a check that every package has a test file, `go vet`,
`go test -race`, a coverage gate, and `kubectl kustomize deploy`:
`.testcoverage.yml` sets the floor and the Makefile pins the toolchain
it measures on. CI runs the same gate as three parallel jobs, `make
lint`, `make test-race`, and `make test-coverage`.

`make e2e` is not part of `make test`. It needs Docker and kubectl,
builds the operator image from this checkout, starts a k3s cluster of
four nodes in Docker, and runs a Redis of three instances under a writer
that confirms each write with `WAIT 1`. The scenarios kill, drain,
freeze, partition, back up, adopt, and upgrade it, and each checks that
no confirmed write was lost. Set `ENGINE=redis` (the default) or
`ENGINE=valkey`. It takes up to 30 minutes, keeps its working files in
`.e2e/`, and removes its containers on exit. `e2e/run.sh` groups the
scenarios into shards, and `SHARD=scale make e2e` runs one of them on
its own cluster. CI runs one job for each shard and engine.

## Privacy

This repository is public. Use example.com and example.net in examples
and tests, and keep the names of real hosts and services out of it.

## Releases

Versions are calendar versions, `yyyy.mm.dd-nnn`, where `nnn` is a
three-digit serial within the day, starting at `001`. Run
`git tag -l "$(date +%Y.%m.%d)-*"` and take the next number. A tag is
the bare version, lightweight, not annotated.

A pushed tag is a release. `release.yaml` waits for the `ci.yaml` run
of the same commit to pass, then runs `docker buildx bake --push`, which
builds and pushes `ghcr.io/chrisguidry/redis-operator` under the version and
under `:latest`, with provenance and an SBOM. `docker-bake.hcl` holds the
platforms, the tags, and the OCI labels and annotations of the image, so
`docker buildx bake --load` on a workstation builds the same image. A
change to a label or an annotation goes in that file, not in the
Dockerfile.

A push to `main` is a development build, versioned from the most recent
release tag plus a suffix: `2026.09.27-001-dev-003-abcdef01` is three
commits past `2026.09.27-001`, at commit `abcdef01`. A development build
never moves `:latest`.
