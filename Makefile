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

.PHONY: test-go
test-go:
	test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	test -z "$$($(UNTESTED_PACKAGES))" || { echo 'packages with no test file:'; $(UNTESTED_PACKAGES); exit 1; }
	go vet ./...
	go test -race ./...
	GOTOOLCHAIN=$(COVERAGE_TOOLCHAIN) go test -coverprofile=coverage.out ./...
	GOTOOLCHAIN=$(COVERAGE_TOOLCHAIN) go tool go-test-coverage --config=.testcoverage.yml

# The report draws the profile that the gate measured as one HTML page.
# `test` does not depend on it, because the gate decides whether a
# change passes, and the report only shows what the tests reached.
.PHONY: coverage-report
coverage-report: coverage.out
	GOTOOLCHAIN=$(COVERAGE_TOOLCHAIN) go tool cover -html=coverage.out -o coverage.html

coverage.out:
	$(MAKE) test-go
