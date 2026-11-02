package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// labelRole writes the role label on the pod of name, as the operator
// does from the Lease.
func (h *managerHarness) labelRole(name, role string) {
	h.api.update(podsResource, testNamespace, name, func(object map[string]any) {
		metadata := object["metadata"].(map[string]any)
		labels, _ := metadata["labels"].(map[string]any)
		if labels == nil {
			labels = map[string]any{}
		}
		labels[roleLabel] = role
		metadata["labels"] = labels
	})
}

// connect opens one connection to the server of name that never
// reconnects, as an application's client holds through a Service.
func (h *managerHarness) connect(name string) *redisConn {
	h.t.Helper()
	conn, err := dialRedis(context.Background(), fmt.Sprintf("%s:%d", h.ip(name), h.port), testPassword)
	mustSucceed(h.t, err)
	h.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// incrLater sends a write on conn and delivers its outcome.
func incrLater(conn *redisConn) <-chan error {
	outcome := make(chan error, 1)
	go func() {
		_, err := conn.do(context.Background(), "INCR", "counter")
		outcome <- err
	}()
	return outcome
}

func (h *managerHarness) outcome(of <-chan error) error {
	h.t.Helper()
	select {
	case err := <-of:
		return err
	case <-time.After(10 * time.Second):
		h.t.Fatal("the write has no outcome")
		return nil
	}
}

// writesHeld says whether a write to the server of name waits instead
// of running.
func (h *managerHarness) writesHeld(name string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := h.client(name).do(ctx, "SET", "probe", "held")
	return errors.Is(err, os.ErrDeadlineExceeded)
}

// mustBeDisconnected fails unless err is a dropped connection, which a
// client retries, and not a reply such as READONLY.
func mustBeDisconnected(t *testing.T, err error) {
	t.Helper()
	if err == nil || errors.As(err, new(replyError)) {
		t.Fatalf("the command ended with %v, not a closed connection", err)
	}
}

func TestASwitchoverHoldsWritesUntilThePodLosesThePrimaryLabelThenDisconnectsTheClients(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			h := twoInstances(t, engine)
			h.labelRole("cache-0", rolePrimary)
			early := h.connect("cache-0")

			h.switchTo("cache-1")
			h.waitFor("cache-0 holds writes", func() bool { return h.writesHeld("cache-0") })
			held := incrLater(early)
			select {
			case err := <-held:
				t.Fatalf("a write to the primary that is stepping down ended with %v while its pod was still labeled primary", err)
			case <-time.After(500 * time.Millisecond):
			}
			mustMatch(t, h.report("cache-0").Role, reportPrimary)
			h.labelRole("cache-0", roleReplica)
			late := incrLater(h.connect("cache-0"))

			mustBeDisconnected(t, h.outcome(held))
			mustBeDisconnected(t, h.outcome(late))
			h.waitFor("cache-1 holds the lease as a ready primary", func() bool {
				return h.holder() == "cache-1" && h.report("cache-1").Ready
			})
		})
	}
}

// A pending failover detaches the replicas at once, so the primary
// refuses writes with NOREPLICAS while it steps down. Its clients stay
// connected until its pod loses the primary label.
func TestAPrimaryThatStepsDownForAFailoverDisconnectsItsClientsOnceThePodLosesThePrimaryLabel(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			h := twoInstances(t, engine)
			h.labelRole("cache-0", rolePrimary)
			conn := h.connect("cache-0")

			h.setTarget(targetPending)
			h.waitFor("cache-1 detaches", func() bool { return h.report("cache-1").Role == reportDetached })
			time.Sleep(500 * time.Millisecond)
			_, err := conn.do(context.Background(), "PING")
			mustSucceed(t, err)
			mustMatch(t, h.report("cache-0").Role, reportPrimary)
			h.labelRole("cache-0", roleReplica)

			h.waitFor("cache-0 steps down and gives up the lease", func() bool {
				return h.report("cache-0").Role == reportDetached && h.holder() == ""
			})
			_, err = conn.do(context.Background(), "PING")
			mustBeDisconnected(t, err)
		})
	}
}

func TestAFencedPrimaryThatCannotReadItsLabelDisconnectsItsClients(t *testing.T) {
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			h := twoInstances(t, engine)
			h.labelRole("cache-0", rolePrimary)
			conn := h.connect("cache-0")

			h.crash("cache-1")
			h.outage()
			h.waitFor("cache-0 detaches", func() bool { return h.report("cache-0").Role == reportDetached })

			mustBeDisconnected(t, h.outcome(incrLater(conn)))
		})
	}
}

func TestADemotedInstanceThatIsPrimaryAgainAcceptsWrites(t *testing.T) {
	h := twoInstances(t, "redis")
	h.labelRole("cache-0", rolePrimary)
	h.setTarget(targetPending)
	h.waitFor("cache-0 steps down", func() bool { return h.report("cache-0").Role == reportDetached && h.holder() == "" })

	h.setTarget("cache-0")
	h.waitFor("cache-0 holds the lease as a ready primary", func() bool {
		return h.holder() == "cache-0" && h.report("cache-0").Ready
	})
	h.waitFor("cache-1 replicates cache-0 again", func() bool {
		report := h.report("cache-1")
		return report.LinkUp && report.Following == h.ip("cache-0")
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := h.client("cache-0").do(ctx, "SET", "after", "promotion")
	mustSucceed(t, err)
}

func TestAReplicaKeepsItsClientsConnected(t *testing.T) {
	h := twoInstances(t, "redis")
	h.labelRole("cache-0", rolePrimary)
	h.labelRole("cache-1", roleReplica)
	conn := h.connect("cache-1")

	time.Sleep(time.Second)

	got, err := conn.do(context.Background(), "GET", "key")
	mustSucceed(t, err)
	mustMatch(t, got, any("value"))
}

// A failover can name the next primary while the old holder, which the
// API server still reaches, is stepping down. Its replicas detached, so
// a FAILOVER to the next primary would fail; it detaches and releases
// the Lease instead.
func TestAHolderThatAFailoverPassedOverDetachesAndReleasesTheLease(t *testing.T) {
	h := twoInstances(t, "redis")
	h.labelRole("cache-0", rolePrimary)
	h.setTarget(targetPending)
	h.waitFor("cache-1 detaches while cache-0 waits for its label to move", func() bool {
		return h.report("cache-1").Role == reportDetached && h.report("cache-0").Role == reportPrimary
	})
	// cache-1 can no longer reach cache-0 as a replica, as when the
	// network cuts cache-0 off from its peers but not from the API
	// server.
	_, err := h.client("cache-1").do(context.Background(), "CONFIG", "SET", "masterauth", "wrong")
	mustSucceed(t, err)

	h.setTarget("cache-1")
	h.labelRole("cache-0", roleReplica)

	h.waitFor("cache-1 holds the lease as a ready primary", func() bool {
		return h.holder() == "cache-1" && h.report("cache-1").Ready
	})
}
