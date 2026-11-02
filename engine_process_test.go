package main

// The tests run real redis-server and valkey-server processes, because
// replication and FAILOVER are behaviour no fake reproduces. Each
// process listens on a free local port with its data in the test's
// temporary directory, and stops when the test ends.
//
// The binaries come from PATH. CI installs them from the distribution's
// packages; AGENTS.md says how to get them on a workstation.

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// engines lists the engines each server test runs against.
var engines = []string{"redis", "valkey"}

const testPassword = "test-password"

// testServer is one running server process.
type testServer struct {
	engine string
	addr   string
	dir    string
	cmd    *exec.Cmd
}

// startTestServer starts a server of engine with a password, and with
// extra lines added to its configuration file.
func startTestServer(t *testing.T, engine string, extra ...string) *testServer {
	t.Helper()
	binary, err := exec.LookPath(serverBinary(engine))
	if err != nil {
		t.Fatalf("%s is not on PATH; AGENTS.md says how to install it: %v", serverBinary(engine), err)
	}
	port := freePort(t)
	dir := t.TempDir()
	config := strings.Join(append([]string{
		fmt.Sprintf("port %d", port),
		"bind 127.0.0.1",
		"dir " + dir,
		"requirepass " + testPassword,
		"masterauth " + testPassword,
		"save \"\"",
		"appendonly no",
	}, extra...), "\n") + "\n"
	path := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, path)
	cmd.Dir = dir
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	server := &testServer{engine: engine, addr: fmt.Sprintf("127.0.0.1:%d", port), dir: dir, cmd: cmd}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	server.waitUntilAnswering(t)
	return server
}

func (s *testServer) waitUntilAnswering(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := dialRedis(context.Background(), s.addr, testPassword); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s on %s did not answer within 10 seconds", s.engine, s.addr)
}

func (s *testServer) client(t *testing.T) *redisConn {
	t.Helper()
	c, err := dialRedis(context.Background(), s.addr, testPassword)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// handedOut holds every port freePort returned. The kernel can hand a
// port it just freed to the next request, so two calls in a row, such
// as the ports of one SeaweedFS server, could otherwise get the same
// port, and the second listener fails.
var handedOut = struct {
	sync.Mutex
	ports map[int]bool
}{ports: map[int]bool{}}

// freePort returns a port nothing listens on, and that no earlier call
// in this process returned.
func freePort(t *testing.T) int {
	t.Helper()
	handedOut.Lock()
	defer handedOut.Unlock()
	for {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
		if !handedOut.ports[port] {
			handedOut.ports[port] = true
			return port
		}
	}
}

// hostPort turns "127.0.0.1:6379" into "127.0.0.1 6379", the form that
// REPLICAOF and FAILOVER TO take.
func hostPort(addr string) string {
	host, port, _ := net.SplitHostPort(addr)
	return host + " " + port
}

// eventually waits up to ten seconds for done to hold.
func eventually(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting until %s", what)
}
