package main

import (
	"context"
	"errors"
	"testing"
)

func TestRedisConnAnswersEachReplyType(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			c := startTestServer(t, engine).client(t)
			ctx := context.Background()

			if got, err := c.do(ctx, "PING"); err != nil || got != "PONG" {
				t.Fatalf("PING = %v, %v", got, err)
			}
			if _, err := c.do(ctx, "SET", "key", "value"); err != nil {
				t.Fatal(err)
			}
			if got, err := c.do(ctx, "GET", "key"); err != nil || got != "value" {
				t.Fatalf("GET = %v, %v", got, err)
			}
			if got, err := c.do(ctx, "GET", "absent"); err != nil || got != nil {
				t.Fatalf("GET of an absent key = %v, %v", got, err)
			}
			if got, err := c.do(ctx, "INCR", "counter"); err != nil || got != int64(1) {
				t.Fatalf("INCR = %v, %v", got, err)
			}
			role, err := c.do(ctx, "ROLE")
			if err != nil {
				t.Fatal(err)
			}
			if items, ok := role.([]any); !ok || items[0] != "master" {
				t.Fatalf("ROLE = %#v", role)
			}
		})
	}
}

func TestRedisConnReturnsTheServersErrorAsAReplyError(t *testing.T) {
	t.Parallel()
	c := startTestServer(t, "redis").client(t)

	_, err := c.do(context.Background(), "NOSUCHCOMMAND")

	var reply replyError
	if !errors.As(err, &reply) {
		t.Fatalf("err = %v, want a replyError", err)
	}
	if reply.code() != "ERR" {
		t.Fatalf("code = %q, want ERR", reply.code())
	}
}

func TestDialRedisRefusesAWrongPassword(t *testing.T) {
	t.Parallel()
	server := startTestServer(t, "redis")

	_, err := dialRedis(context.Background(), server.addr, "wrong")

	if err == nil {
		t.Fatal("dial with a wrong password succeeded")
	}
}

func TestReplicationInfoReadsTheReplicationSection(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			primary := startTestServer(t, engine)
			replica := startTestServer(t, engine, "replicaof "+hostPort(primary.addr))
			eventually(t, "the replica's link is up", func() bool {
				info, err := readReplication(context.Background(), replica.client(t))
				return err == nil && info.linkUp
			})

			info, err := readReplication(context.Background(), replica.client(t))

			if err != nil {
				t.Fatal(err)
			}
			if info.role != "slave" || info.masterHost != "127.0.0.1" {
				t.Fatalf("info = %+v", info)
			}
		})
	}
}

func TestReplicationInfoCountsOnlyTheReplicasThatFinishedTheirSync(t *testing.T) {
	t.Parallel()
	text := "# Replication\r\nrole:master\r\nconnected_slaves:2\r\n" +
		"slave0:ip=10.0.0.2,port=6379,state=online,offset=100,lag=0\r\n" +
		"slave1:ip=10.0.0.3,port=6379,state=wait_bgsave,offset=0,lag=0\r\n"

	info := parseReplication(text)

	mustMatch(t, info.replicas, 2)
	mustMatch(t, info.onlineReplicas, 1)
}
