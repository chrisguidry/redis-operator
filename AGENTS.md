# Working on redis-operator

This repository holds the `Redis` resource and the operator that runs
Redis and Valkey from it. `plans/00-design.md` is the design and the
source of each fact it depends on. Later plans are numbered after it.

`make test` runs every check CI runs.

## Layout

The operator is one flat `package main`, with files grouped by prefix.
It talks to the Kubernetes API over HTTP with no client-go and no
controller framework.

## Tests

Write the failing test first. Tests run against fakes of the real
servers and against real Redis and Valkey processes. Do not add mocks.

## Privacy

This repository is public. Use example.com and example.net in examples
and tests, and keep the names of real hosts and services out of it.

## Releases

Versions are calendar versions, `yyyy.mm.dd-nnn`, where `nnn` is a
three-digit serial within the day, starting at `001`. Run
`git tag -l "$(date +%Y.%m.%d)-*"` and take the next number. A tag is
the bare version, lightweight, not annotated.

A pushed tag is a release. `release.yaml` builds the image beside the
`ci.yaml` run of the same commit, waits for that run to pass, and pushes
`ghcr.io/chrisguidry/redis-operator` under the version and under
`:latest`.

A push to `main` is a development build, versioned from the most recent
release tag plus a suffix: `2026.09.27-001-dev-003-abcdef01` is three
commits past `2026.09.27-001`, at commit `abcdef01`. A development build
never moves `:latest`.
