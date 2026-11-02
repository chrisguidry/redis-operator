package main

// The instance manager's hold on the server beside it: the
// configuration file it writes, the process it starts, and the commands
// that change the server's role.
//
// The server always starts detached: a replica of an address that
// nothing answers on. A replica refuses writes and keeps its data, so a
// server that starts before its manager knows the primary cannot accept
// a write it should not, and the manager promotes it only once it holds
// the primary Lease.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// detachedHost is in TEST-NET-1, which RFC 5737 sets aside for
// documentation, so no server ever answers there.
const detachedHost = "192.0.2.1"

// serverSettings is what the configuration file says, from the spec
// and the pod.
type serverSettings struct {
	engine             string
	bindIP             string
	port               int
	dataDir            string
	configPath         string
	password           string
	maxmemory          int64
	maxmemoryPolicy    string
	persistent         bool
	minReplicasToWrite int
}

// minReplicasMaxLag is how long a replica may go without acknowledging
// the primary before the primary stops counting it toward
// min-replicas-to-write.
const minReplicasMaxLag = 10

func (s serverSettings) config() string {
	lines := []string{
		"port " + strconv.Itoa(s.port),
		"bind " + s.bindIP,
		"protected-mode no",
		"dir " + s.dataDir,
		"requirepass " + quoteConfig(s.password),
		"masterauth " + quoteConfig(s.password),
		fmt.Sprintf("maxmemory %d", s.maxmemory),
		"maxmemory-policy " + s.maxmemoryPolicy,
		// The primary sees a replica's connection come from its pod IP,
		// but stating it keeps the replica's address in INFO and in
		// FAILOVER TO the same one the operator knows.
		"replica-announce-ip " + s.bindIP,
		"replica-read-only yes",
		fmt.Sprintf("min-replicas-to-write %d", s.minReplicasToWrite),
		fmt.Sprintf("min-replicas-max-lag %d", minReplicasMaxLag),
		fmt.Sprintf("replicaof %s %d", detachedHost, s.port),
	}
	if s.persistent {
		lines = append(lines, "appendonly yes", "appendfsync everysec")
	} else {
		// With no volume, the data lives only in memory and in the
		// replicas, and writing it to the container's disk would cost
		// the fork for nothing.
		lines = append(lines, "appendonly no", `save ""`)
	}
	return strings.Join(lines, "\n") + "\n"
}

// quoteConfig quotes a value for the configuration file, which splits
// words on spaces and reads backslash escapes inside double quotes.
func quoteConfig(value string) string {
	return strconv.Quote(value)
}

// startServer writes the configuration and starts the server. The
// caller waits for the process with cmd.Wait.
func startServer(settings serverSettings) (*exec.Cmd, error) {
	if err := os.MkdirAll(filepath.Dir(settings.configPath), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(settings.configPath, []byte(settings.config()), 0o600); err != nil {
		return nil, fmt.Errorf("writing the server configuration: %w", err)
	}
	cmd := exec.Command(serverBinary(settings.engine), settings.configPath)
	cmd.Dir = settings.dataDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting %s: %w", serverBinary(settings.engine), err)
	}
	return cmd, nil
}

// server is the manager's connection to the server beside it, opened
// again whenever a command finds it broken.
type server struct {
	addr     string
	password string
	conn     *redisConn
}

func (s *server) do(ctx context.Context, args ...string) (any, error) {
	if s.conn == nil {
		conn, err := dialRedis(ctx, s.addr, s.password)
		if err != nil {
			return nil, err
		}
		s.conn = conn
	}
	reply, err := s.conn.do(ctx, args...)
	if err != nil && !errors.As(err, new(replyError)) {
		// A network error leaves the connection in an unknown state, so
		// the next command opens a new one.
		_ = s.conn.Close()
		s.conn = nil
	}
	return reply, err
}

func (s *server) close() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

func (s *server) replication(ctx context.Context) (replicationInfo, error) {
	reply, err := s.do(ctx, "INFO", "replication")
	if err != nil {
		return replicationInfo{}, err
	}
	text, ok := reply.(string)
	if !ok {
		return replicationInfo{}, fmt.Errorf("INFO answered %T, not text", reply)
	}
	return parseReplication(text), nil
}

// follow makes the server a replica of host. A server that already
// follows host is left alone, because REPLICAOF to the same primary
// would not resync but still logs and resets the link.
func (s *server) follow(ctx context.Context, info replicationInfo, host string, port int) error {
	if info.role == "slave" && info.masterHost == host && info.masterPort == port {
		return nil
	}
	_, err := s.do(ctx, "REPLICAOF", host, strconv.Itoa(port))
	return err
}

// detach makes the server a replica of nothing. It keeps its data and
// refuses writes, and its offset stops moving.
func (s *server) detach(ctx context.Context, info replicationInfo, port int) error {
	return s.follow(ctx, info, detachedHost, port)
}

// promote makes the server a primary. It keeps its data, and a primary
// that is promoted already is left alone.
func (s *server) promote(ctx context.Context, info replicationInfo) error {
	if info.role == "master" {
		return nil
	}
	_, err := s.do(ctx, "REPLICAOF", "NO", "ONE")
	return err
}

// killClients closes every client connection except the manager's own,
// so a client that kept a connection to a demoted primary reconnects
// through the Service and finds the new one.
func (s *server) killClients(ctx context.Context) error {
	_, err := s.do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes")
	return err
}

func (s *server) setMinReplicasToWrite(ctx context.Context, n int) error {
	_, err := s.do(ctx, "CONFIG", "SET", "min-replicas-to-write", strconv.Itoa(n))
	return err
}

// errFailoverAborted is a FAILOVER that the server gave up on, because
// the replica did not reach the primary's offset in time. The primary
// keeps its role and nothing is lost.
var errFailoverAborted = errors.New("the server aborted the failover: the replica did not catch up in time")

// failoverTo hands the primary role to the replica at host with
// Redis's own FAILOVER: the server pauses writes, waits until the
// replica has acknowledged its whole offset, then makes the replica the
// primary and itself its replica. It returns once the server is a
// replica, or with errFailoverAborted when it gave up.
func (s *server) failoverTo(ctx context.Context, host string, port int, timeout time.Duration) error {
	if _, err := s.do(ctx, "FAILOVER", "TO", host, strconv.Itoa(port), "TIMEOUT", strconv.FormatInt(timeout.Milliseconds(), 10)); err != nil {
		return err
	}
	// The server answers OK at once and works in the background, so the
	// manager polls until the role changes or the failover ends.
	deadline := time.Now().Add(timeout + 5*time.Second)
	for time.Now().Before(deadline) {
		info, err := s.replication(ctx)
		if err == nil {
			if info.role == "slave" {
				return nil
			}
			if info.failoverState == "no-failover" {
				return errFailoverAborted
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("the failover to %s did not finish within %s", net.JoinHostPort(host, strconv.Itoa(port)), timeout)
}

// shutdown stops the server. A persistent server writes its data
// first, and a server with no volume has nothing worth writing.
func (s *server) shutdown(ctx context.Context, persistent bool) {
	mode := "NOSAVE"
	if persistent {
		mode = "SAVE"
	}
	_, _ = s.do(ctx, "SHUTDOWN", mode)
	s.close()
}
