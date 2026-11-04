# `make test` runs every check that CI runs, with the same commands, so
# a change that passes here passes there.

.PHONY: test
test: test-go

# The coverage gate measures on its own run, on a pinned toolchain. Go
# 1.27 splits a basic block into one profile row for each run of code
# inside it, and repeats the block's statement count on every row. Every
# reader sums those rows, `go tool cover` included, so a block counts
# once more for each comment inside it. Go 1.26 counts each block once,
# which is what the floor in .testcoverage.yml is set against. Move this
# pin to the newest toolchain that counts each block once.
#
# go-test-coverage is a tool dependency in go.mod, so the gate needs
# nothing installed except the Go toolchain.
COVERAGE_TOOLCHAIN := go1.26.7

# A package with no test file writes no rows to the profile, so the gate
# does not measure it and cannot fail it. This lists such packages, and
# test-go fails on the first one.
UNTESTED_PACKAGES := go list -f '{{if not (or .TestGoFiles .XTestGoFiles)}}{{.ImportPath}}{{end}}' ./...

# The tests run real redis-server and valkey-server processes. This
# copies both binaries out of the images the operator runs by default,
# so the tests run the same servers a cluster does. It needs Docker.
SERVERS := $(CURDIR)/.servers
DEFAULT_IMAGES := $(shell awk '/name: DEFAULT_(REDIS|VALKEY)_IMAGE$$/ { getline; print $$2 }' deploy/operator.yaml)
# The backup tests store their objects in a real SeaweedFS S3 server.
SEAWEEDFS_IMAGE := chrislusf/seaweedfs:4.51
export PATH := $(SERVERS):$(PATH)

$(SERVERS)/.images: deploy/operator.yaml Makefile
	mkdir -p $(SERVERS)
	for image in $(DEFAULT_IMAGES); do \
	  case $$image in valkey*) binary=valkey-server ;; *) binary=redis-server ;; esac; \
	  container=$$(docker create $$image) && \
	  docker cp -L $$container:/usr/local/bin/$$binary $(SERVERS)/$$binary && \
	  docker rm $$container >/dev/null || exit 1; \
	done
	container=$$(docker create $(SEAWEEDFS_IMAGE)) && \
	  docker cp -L $$container:/usr/bin/weed $(SERVERS)/weed && \
	  docker rm $$container >/dev/null
	echo $(DEFAULT_IMAGES) $(SEAWEEDFS_IMAGE) > $@

.PHONY: servers
servers: $(SERVERS)/.images

# The packages' own directories, so gofmt never walks .e2e/, where a
# running e2e leaves files only root can read.
GO_DIRS = $$(go list -f '{{.Dir}}' ./...)

# test-go is the whole gate. CI runs its three parts as parallel jobs.
.PHONY: test-go
test-go: lint test-race test-coverage

.PHONY: lint
lint:
	test -z "$$(gofmt -l $(GO_DIRS))" || { gofmt -l $(GO_DIRS); exit 1; }
	test -z "$$($(UNTESTED_PACKAGES))" || { echo 'packages with no test file:'; $(UNTESTED_PACKAGES); exit 1; }
	go vet ./...
	kubectl kustomize deploy > /dev/null

# The tests run in parallel, and most of their time is spent waiting on
# real servers and the managers' timings rather than on the CPU, so
# they run more at once than there are cores.
TEST_PARALLEL := 16

.PHONY: test-race
test-race: servers
	go test -race -parallel $(TEST_PARALLEL) -timeout 15m ./...

.PHONY: test-coverage
test-coverage: servers
	GOTOOLCHAIN=$(COVERAGE_TOOLCHAIN) go test -parallel $(TEST_PARALLEL) -timeout 15m -coverprofile=coverage.out ./...
	GOTOOLCHAIN=$(COVERAGE_TOOLCHAIN) go tool go-test-coverage --config=.testcoverage.yml

# The report draws the profile that the gate measured as one HTML page.
# `test` does not depend on it, because the gate decides whether a
# change passes, and the report only shows what the tests reached.
.PHONY: coverage-report
coverage-report: coverage.out
	GOTOOLCHAIN=$(COVERAGE_TOOLCHAIN) go tool cover -html=coverage.out -o coverage.html

coverage.out:
	$(MAKE) test-coverage

# e2e is not part of test: it needs Docker and kubectl, and it stands up
# a four-node k3s cluster in containers. It runs as its own jobs in CI,
# one per engine. See e2e/run.sh for what it proves.
.PHONY: e2e
e2e:
	timeout 60m bash e2e/run.sh
