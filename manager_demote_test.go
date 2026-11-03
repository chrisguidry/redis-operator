package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
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
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
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

// A fenced primary that cannot read its pod cannot tell when routing
// moves, so it holds writes for the demotion hold before it closes its
// clients. A write meanwhile is refused for want of replicas, never
// with READONLY.
func TestAFencedPrimaryThatCannotReadItsLabelDisconnectsItsClients(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			h := twoInstances(t, engine)
			h.labelRole("cache-0", rolePrimary)
			conn := h.connect("cache-0")

			h.crash("cache-1")
			h.outage()
			h.fastForward("cache-0", "cache-0 detaches", time.Second, func() bool { return h.report("cache-0").Role == reportDetached })

			_, err := conn.do(context.Background(), "PING")
			mustBeDisconnected(t, err)
		})
	}
}

// reconnectingWriter writes to the server of name in a loop and
// connects again whenever its connection closes, as a client does
// through a Service that still routes to that pod, until stop. It
// delivers every error the server answered.
func (h *managerHarness) reconnectingWriter(name string, stop <-chan struct{}) <-chan []string {
	answered := make(chan []string, 1)
	address := fmt.Sprintf("%s:%d", h.ip(name), h.port)
	go func() {
		var replies []string
		var conn *redisConn
		for {
			select {
			case <-stop:
				if conn != nil {
					_ = conn.Close()
				}
				answered <- replies
				return
			case <-time.After(20 * time.Millisecond):
			}
			if conn == nil {
				dialed, err := dialRedis(context.Background(), address, testPassword)
				if err != nil {
					continue
				}
				conn = dialed
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := conn.do(ctx, "INCR", "counter")
			cancel()
			if reply, ok := errors.AsType[replyError](err); ok {
				replies = append(replies, reply.code())
			} else if err != nil {
				_ = conn.Close()
				conn = nil
			}
		}
	}()
	return answered
}

// A failover can move the Lease while the primary is cut off or frozen.
// When it comes back, it finds another holder, and its clients still
// reach it until routing moves. It holds their writes as a primary until
// then, and none is answered READONLY.
func TestAPrimaryThatFindsAnotherHolderAnswersNoWriteWithReadonly(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			h := twoInstances(t, engine)
			h.labelRole("cache-0", rolePrimary)
			stop := make(chan struct{})
			answered := h.reconnectingWriter("cache-0", stop)

			h.setTarget("cache-1")
			h.api.update(leasesResource, testNamespace, primaryLeaseName(h.name), func(object map[string]any) {
				object["spec"].(map[string]any)["holderIdentity"] = "cache-1"
			})
			h.waitFor("cache-1 is a ready primary", func() bool { return h.report("cache-1").Ready })
			time.Sleep(time.Second)
			close(stop)
			replies := <-answered
			h.labelRole("cache-0", roleReplica)

			h.waitFor("cache-0 is no longer primary", func() bool { return h.report("cache-0").Role != reportPrimary })
			mustMatch(t, slices.Contains(replies, "READONLY"), false)
		})
	}
}

func TestADemotedInstanceThatIsPrimaryAgainAcceptsWrites(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")
	h.labelRole("cache-0", rolePrimary)
	h.setTarget(targetPending)
	h.fastForward("cache-0", "cache-0 steps down", time.Second, func() bool { return h.report("cache-0").Role == reportDetached && h.holder() == "" })

	h.setTarget("cache-0")
	h.waitFor("cache-0 holds the lease as a ready primary", func() bool {
		return h.holder() == "cache-0" && h.report("cache-0").Ready
	})
	// The replica's link comes up before the primary counts it toward
	// min-replicas-to-write, so the wait is also for the primary's count.
	h.waitFor("cache-1 replicates cache-0 again", func() bool {
		report := h.report("cache-1")
		return report.LinkUp && report.Following == h.ip("cache-0") && h.report("cache-0").SyncedReplicas == 1
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := h.client("cache-0").do(ctx, "SET", "after", "promotion")
	mustSucceed(t, err)
}

func TestAReplicaKeepsItsClientsConnected(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

	h.fastForward("cache-0", "cache-1 holds the lease as a ready primary", time.Second, func() bool {
		return h.holder() == "cache-1" && h.report("cache-1").Ready
	})
}
