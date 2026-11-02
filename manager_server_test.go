package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// startManagedServer starts a server the way the instance manager
// does, on a loopback address of its own, so several servers in one
// test share a port the way pods do.
func startManagedServer(t *testing.T, engine, ip string, port int) *server {
	t.Helper()
	return startManagedServerWith(t, engine, ip, port, false)
}

// startManagedServerWith starts a server that also listens on loopback
// when loopback is true. Only one server on a port may.
func startManagedServerWith(t *testing.T, engine, ip string, port int, loopback bool) *server {
	t.Helper()
	dir := t.TempDir()
	settings := serverSettings{
		engine: engine, bindIP: ip, loopback: loopback, port: port, dataDir: dir,
		configPath: filepath.Join(dir, "server.conf"), password: testPassword,
		maxmemory: 64 << 20, maxmemoryPolicy: "noeviction",
	}
	if _, err := exec.LookPath(serverBinary(engine)); err != nil {
		t.Fatalf("%s is not on PATH; AGENTS.md says how to install it", serverBinary(engine))
	}
	cmd, err := startServer(settings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	s := &server{addr: fmt.Sprintf("%s:%d", ip, port), password: testPassword}
	t.Cleanup(s.close)
	eventually(t, "the server answers", func() bool {
		_, err := s.do(context.Background(), "PING")
		return err == nil
	})
	return s
}

func replication(t *testing.T, s *server) replicationInfo {
	t.Helper()
	info, err := s.replication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestAServerStartsDetachedAndRefusesWrites(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			s := startManagedServer(t, engine, "127.0.0.2", freePort(t))

			_, err := s.do(context.Background(), "SET", "key", "value")

			mustMatch(t, replication(t, s).masterHost, detachedHost)
			var reply replyError
			mustMatch(t, errors.As(err, &reply) && reply.code() == "READONLY", true)
		})
	}
}

func TestAClientInThePodReachesTheServerOnLoopback(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			port := freePort(t)
			startManagedServerWith(t, engine, "127.0.0.2", port, true)
			local := &server{addr: fmt.Sprintf("127.0.0.1:%d", port), password: testPassword}
			t.Cleanup(local.close)

			_, err := local.do(context.Background(), "PING")

			mustSucceed(t, err)
		})
	}
}

func TestFailoverToHandsThePrimaryRoleOverWithItsData(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			port := freePort(t)
			a := startManagedServer(t, engine, "127.0.0.2", port)
			b := startManagedServer(t, engine, "127.0.0.3", port)
			mustSucceed(t, a.promote(ctx, replication(t, a)))
			mustSucceed(t, b.follow(ctx, replication(t, b), "127.0.0.2", port))
			eventually(t, "b's link is up", func() bool { return replication(t, b).linkUp })
			_, err := a.do(ctx, "SET", "key", "value")
			mustSucceed(t, err)

			err = a.failoverTo(ctx, "127.0.0.3", port, 5*time.Second)

			mustSucceed(t, err)
			mustMatch(t, replication(t, a).role, "slave")
			// The old primary turns replica before the new one takes the
			// PSYNC that promotes it.
			eventually(t, "b is the primary", func() bool { return replication(t, b).role == "master" })
			got, err := b.do(ctx, "GET", "key")
			mustSucceed(t, err)
			mustMatch(t, got, any("value"))
		})
	}
}

func TestFollowAndPromoteLeaveAServerThatIsAlreadyThere(t *testing.T) {
	ctx := context.Background()
	port := freePort(t)
	a := startManagedServer(t, "redis", "127.0.0.2", port)
	b := startManagedServer(t, "redis", "127.0.0.3", port)
	mustSucceed(t, a.promote(ctx, replication(t, a)))
	mustSucceed(t, b.follow(ctx, replication(t, b), "127.0.0.2", port))
	eventually(t, "b's link is up", func() bool { return replication(t, b).linkUp })

	mustSucceed(t, a.promote(ctx, replication(t, a)))
	mustSucceed(t, b.follow(ctx, replication(t, b), "127.0.0.2", port))

	mustMatch(t, replication(t, b).linkUp, true)
}
