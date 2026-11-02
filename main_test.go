package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func noEnv(string) string { return "" }

func TestVersionReportsTheBuild(t *testing.T) {
	var log syncBuffer

	err := run(context.Background(), []string{"version"}, noEnv, &log)

	mustSucceed(t, err)
	mustMatch(t, log.lines()[0], "redis-operator dev")
}

func TestAnUnknownCommandIsRefused(t *testing.T) {
	err := run(context.Background(), []string{"bogus"}, noEnv, &syncBuffer{})

	mustMatch(t, err.Error(), `unknown command "bogus": the commands are operator, manager, install, and version`)
}

func TestTheOperatorNeedsItsPod(t *testing.T) {
	err := run(context.Background(), nil, noEnv, &syncBuffer{})

	mustMatch(t, err.Error(), "POD_NAMESPACE and POD_NAME must be set; the operator reads its own pod to learn its image")
}

func TestTheManagerNeedsItsSettings(t *testing.T) {
	err := run(context.Background(), []string{"manager"}, noEnv, &syncBuffer{})

	mustMatch(t, err != nil, true)
}

func TestInstallCopiesTheBinary(t *testing.T) {
	dir := t.TempDir()

	err := run(context.Background(), []string{"install", dir}, noEnv, &syncBuffer{})

	mustSucceed(t, err)
	info, err := os.Stat(filepath.Join(dir, "redis-operator"))
	mustSucceed(t, err)
	mustMatch(t, info.Mode().Perm(), os.FileMode(0o755))
}

func TestTheOperatorRefusesABadCanaryTimeout(t *testing.T) {
	env := map[string]string{"POD_NAMESPACE": "redis-operator", "POD_NAME": "redis-operator-0", "CANARY_TIMEOUT": "soon"}

	err := run(context.Background(), nil, func(name string) string { return env[name] }, &syncBuffer{})

	mustMatch(t, err.Error(), `CANARY_TIMEOUT is "soon"; it must be a duration such as 90s`)
}
