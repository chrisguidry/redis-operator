package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func noEnv(string) string { return "" }

func TestVersionReportsTheBuild(t *testing.T) {
	t.Parallel()
	var log syncBuffer

	err := run(context.Background(), []string{"version"}, noEnv, &log)

	mustSucceed(t, err)
	mustMatch(t, log.lines()[0], "redis-operator dev")
}

func TestAnUnknownCommandIsRefused(t *testing.T) {
	t.Parallel()
	err := run(context.Background(), []string{"bogus"}, noEnv, &syncBuffer{})

	mustMatch(t, err.Error(), `unknown command "bogus": the commands are operator, manager, install, and version`)
}

func TestTheOperatorNeedsItsPod(t *testing.T) {
	t.Parallel()
	err := run(context.Background(), nil, noEnv, &syncBuffer{})

	mustMatch(t, err.Error(), "POD_NAMESPACE and POD_NAME must be set; the operator reads its own pod to learn its image")
}

func TestTheManagerNeedsItsSettings(t *testing.T) {
	t.Parallel()
	err := run(context.Background(), []string{"manager"}, noEnv, &syncBuffer{})

	mustMatch(t, err != nil, true)
}

func TestInstallCopiesTheBinary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	err := run(context.Background(), []string{"install", dir}, noEnv, &syncBuffer{})

	mustSucceed(t, err)
	info, err := os.Stat(filepath.Join(dir, "redis-operator"))
	mustSucceed(t, err)
	mustMatch(t, info.Mode().Perm(), os.FileMode(0o755))
}

// operatorEnv is the environment deploy/operator.yaml gives the
// operator, with the changes in overrides. An empty value unsets one.
func operatorEnv(overrides map[string]string) func(string) string {
	env := map[string]string{
		"POD_NAMESPACE":          "redis-operator",
		"POD_NAME":               "redis-operator-0",
		"DEFAULT_REDIS_IMAGE":    "registry.example.com/redis:default",
		"DEFAULT_VALKEY_IMAGE":   "registry.example.com/valkey:default",
		"DEFAULT_EXPORTER_IMAGE": "registry.example.com/redis_exporter:default",
	}
	for name, value := range overrides {
		env[name] = value
	}
	return func(name string) string { return env[name] }
}

func TestTheOperatorNeedsEachDefaultImage(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"DEFAULT_REDIS_IMAGE", "DEFAULT_VALKEY_IMAGE", "DEFAULT_EXPORTER_IMAGE"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := run(context.Background(), nil, operatorEnv(map[string]string{name: ""}), &syncBuffer{})

			mustMatch(t, err.Error(), name+" must be set; it names the image a Redis runs when its spec names none")
		})
	}
}

func TestTheOperatorRefusesABadCanaryTimeout(t *testing.T) {
	t.Parallel()
	err := run(context.Background(), nil, operatorEnv(map[string]string{"CANARY_TIMEOUT": "soon"}), &syncBuffer{})

	mustMatch(t, err.Error(), `CANARY_TIMEOUT is "soon"; it must be a duration such as 90s`)
}
