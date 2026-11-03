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
	"errors"
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

// handedOut is the next port takePort tries. The ports come from below
// the kernel's ephemeral range, which starts at 32768 by default: the
// kernel gives the client end of every connection a port from that
// range, so a port taken from it can already be the client end of a
// connection on the address a test binds, and the test's listener
// fails. Counting up also never returns a port twice.
var handedOut = struct {
	sync.Mutex
	next int
}{next: 20000}

// freePort returns a port nothing listens on, and that no earlier call
// in this process returned.
func freePort(t *testing.T) int {
	t.Helper()
	port, err := takePort()
	if err != nil {
		t.Fatal(err)
	}
	return port
}

// takePort is freePort for a caller outside any one test.
func takePort() (int, error) {
	handedOut.Lock()
	defer handedOut.Unlock()
	for ; handedOut.next < 32768; handedOut.next++ {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", handedOut.next))
		if err != nil {
			continue
		}
		_ = listener.Close()
		handedOut.next++
		return handedOut.next - 1, nil
	}
	return 0, errors.New("no free port is left below the ephemeral range")
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
