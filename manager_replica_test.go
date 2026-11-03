package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const sourcePassword = "source-password"

// startSource starts a server outside the operator, with a password of
// its own, as a source to adopt. It starts a replica's sync at once,
// because a chain of source, holder, and replica otherwise waits out
// the default diskless sync delay twice, at the source and at the
// holder, and only the holder's delay is the operator's.
func startSource(t *testing.T, engine string) *testServer {
	t.Helper()
	source := startTestServer(t, engine, "repl-diskless-sync-delay 0")
	_, err := source.client(t).do(context.Background(), "CONFIG", "SET", "requirepass", sourcePassword)
	mustSucceed(t, err)
	return source
}

func sourceClient(t *testing.T, source *testServer) *redisConn {
	t.Helper()
	conn, err := dialRedis(context.Background(), source.addr, sourcePassword)
	mustSucceed(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// adopting declares replica mode for source on the harness's Redis, with
// the operator's decision that it replicates.
func (h *managerHarness) adopting(source ReplicaSource) {
	h.t.Helper()
	h.api.create(secretsResource, map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "source", "namespace": testNamespace},
		"data":     map[string]any{"password": []byte(sourcePassword)},
	})
	source.Password = &SecretKeyRef{Secret: "source", Key: "password"}
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["replica"] = toObject(h.t, ReplicaSpec{Enabled: true, Source: source})
	})
	h.setReplicaPhase(replicaReplicating)
}

func (h *managerHarness) setReplicaPhase(phase string) {
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		status, _ := object["status"].(map[string]any)
		if status == nil {
			status = map[string]any{}
		}
		status["replica"] = map[string]any{"phase": phase}
		object["status"] = status
	})
}

func sourceHostPort(source *testServer) (string, int) {
	var host string
	var port int
	_, _ = fmt.Sscanf(hostPort(source.addr), "%s %d", &host, &port)
	return host, port
}

func TestInReplicaModeTheHolderFollowsTheSourceAndTheRestFollowTheHolder(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			source := startSource(t, engine)
			_, err := sourceClient(t, source).do(context.Background(), "SET", "adopted", "yes")
			mustSucceed(t, err)
			h := startManagerHarness(t, engine, 2)
			host, port := sourceHostPort(source)
			h.adopting(ReplicaSource{Host: host, Port: port})
			h.setTarget("cache-0")

			h.start("cache-0", "")
			h.start("cache-1", "")

			h.waitFor("cache-0 follows the source and cache-1 follows cache-0", func() bool {
				return h.report("cache-0").Following == host && h.report("cache-0").LinkUp &&
					h.report("cache-1").Following == h.ip("cache-0") && h.report("cache-1").LinkUp
			})
			got, err := h.client("cache-1").do(context.Background(), "GET", "adopted")
			mustSucceed(t, err)
			mustMatch(t, got, any("yes"))
			mustMatch(t, h.holder(), "cache-0")
		})
	}
}

func TestPromotionMakesTheHolderTheWritablePrimary(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	h := startManagerHarness(t, "redis", 2)
	host, port := sourceHostPort(source)
	h.adopting(ReplicaSource{Host: host, Port: port})
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.start("cache-1", "")
	h.waitFor("cache-1 replicates through cache-0", func() bool { return h.report("cache-1").LinkUp && h.report("cache-0").LinkUp })
	h.setCurrentPrimary("cache-0")

	h.setReplicaPhase(replicaPromoted)

	// REPLICAOF NO ONE drops cache-1's link to cache-0, and cache-0
	// refuses writes with NOREPLICAS until cache-1 resyncs, so the wait
	// is for cache-0's count of the replicas it streams to.
	h.waitFor("cache-0 is a ready primary that streams to cache-1", func() bool {
		report := h.report("cache-0")
		return report.Role == reportPrimary && report.Ready && report.SyncedReplicas == 1
	})
	_, err := h.client("cache-0").do(context.Background(), "SET", "after", "promotion")
	mustSucceed(t, err)
	h.waitFor("cache-1 has the write", func() bool {
		got, err := h.client("cache-1").do(context.Background(), "GET", "after")
		return err == nil && got == "promotion"
	})
}

const sentinelPassword = "sentinel-password"

// startSentinel runs a real sentinel that monitors source, and that
// asks its own clients for requirepass when it is not empty.
func startSentinel(t *testing.T, source *testServer, requirepass string) string {
	t.Helper()
	dir := t.TempDir()
	port := freePort(t)
	host, sourcePort := sourceHostPort(source)
	config := fmt.Sprintf("port %d\nbind 127.0.0.1\ndir %s\nsentinel monitor mymaster %s %d 1\nsentinel auth-pass mymaster %s\n",
		port, dir, host, sourcePort, sourcePassword)
	if requirepass != "" {
		config += "requirepass " + requirepass + "\n"
	}
	path := filepath.Join(dir, "sentinel.conf")
	mustSucceed(t, os.WriteFile(path, []byte(config), 0o600))
	cmd := exec.Command(serverBinary(source.engine), path, "--sentinel")
	mustSucceed(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	address := fmt.Sprintf("127.0.0.1:%d", port)
	eventually(t, "the sentinel names the primary", func() bool {
		host, _, err := sourceAddress(context.Background(), ReplicaSource{Sentinel: &SentinelSource{MasterName: "mymaster", Addresses: []string{address}}}, testNamespace, requirepass)
		return err == nil && host != ""
	})
	return address
}

func TestSourceAddressAsksASentinelWithItsOwnPassword(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		requirepass string
		password    string
		answers     bool
	}{
		{"an open sentinel", "", "", true},
		{"a sentinel with a password, given it", sentinelPassword, sentinelPassword, true},
		{"a sentinel with a password, not given it", sentinelPassword, "", false},
	}
	source := startSource(t, "redis")
	open := startSentinel(t, source, "")
	closed := startSentinel(t, source, sentinelPassword)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			address := open
			if c.requirepass != "" {
				address = closed
			}

			host, _, err := sourceAddress(context.Background(), ReplicaSource{Sentinel: &SentinelSource{MasterName: "mymaster", Addresses: []string{address}}}, testNamespace, c.password)

			mustMatch(t, err == nil && host != "", c.answers)
		})
	}
}

func TestTheHolderFollowsThePrimaryASentinelNames(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	sentinel := startSentinel(t, source, "")
	h := startManagerHarness(t, "redis", 1)
	h.adopting(ReplicaSource{Sentinel: &SentinelSource{MasterName: "mymaster", Addresses: []string{"127.0.0.1:1", sentinel}}})
	h.setTarget("cache-0")

	h.start("cache-0", "")

	host, _ := sourceHostPort(source)
	h.waitFor("cache-0 follows the source the sentinel names", func() bool {
		return h.report("cache-0").Following == host && h.report("cache-0").LinkUp
	})
}

func TestTheHolderFollowsThePrimaryASentinelWithAPasswordNames(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	sentinel := startSentinel(t, source, sentinelPassword)
	h := startManagerHarness(t, "redis", 1)
	h.api.create(secretsResource, map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "sentinel", "namespace": testNamespace},
		"data":     map[string]any{"password": []byte(sentinelPassword)},
	})
	h.adopting(ReplicaSource{Sentinel: &SentinelSource{
		MasterName: "mymaster", Addresses: []string{sentinel},
		Password: &SecretKeyRef{Secret: "sentinel", Key: "password"},
	}})
	h.setTarget("cache-0")

	h.start("cache-0", "")

	host, _ := sourceHostPort(source)
	h.waitFor("cache-0 follows the source the sentinel names", func() bool {
		return h.report("cache-0").Following == host && h.report("cache-0").LinkUp
	})
}
