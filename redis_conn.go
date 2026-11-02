package main

// A small client for RESP, the protocol Redis and Valkey speak. The
// instance manager sends a handful of commands to the server beside it,
// and the operator sends none, so a client library would bring far more
// than either uses.
//
// A command is an array of bulk strings. A reply is one of five types,
// told apart by its first byte: + a simple string, - an error, : an
// integer, $ a bulk string or nil, and * an array of replies.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// replyError is an error the server answered, such as
// "READONLY You can't write against a read only replica.". The first
// word is its code.
type replyError string

func (e replyError) Error() string { return string(e) }

func (e replyError) code() string {
	code, _, _ := strings.Cut(string(e), " ")
	return code
}

type redisConn struct {
	conn   net.Conn
	reader *bufio.Reader
}

// dialRedis connects to addr and authenticates as the default user.
// The context bounds the connect and the AUTH, and each later command
// takes its own context.
func dialRedis(ctx context.Context, addr, password string) (*redisConn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &redisConn{conn: conn, reader: bufio.NewReader(conn)}
	if password != "" {
		if _, err := c.do(ctx, "AUTH", password); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("authenticating to %s: %w", addr, err)
		}
	}
	return c, nil
}

func (c *redisConn) Close() error { return c.conn.Close() }

// do sends one command and reads its reply. A reply of the error type
// is returned as a replyError. The context's deadline, or ten seconds
// when it has none, bounds the exchange, so a server that stops
// answering cannot hold the caller.
func (c *redisConn) do(ctx context.Context, args ...string) (any, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(10 * time.Second)
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	var command strings.Builder
	fmt.Fprintf(&command, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&command, "$%d\r\n%s\r\n", len(arg), arg)
	}
	if _, err := io.WriteString(c.conn, command.String()); err != nil {
		return nil, err
	}
	return c.readReply()
}

func (c *redisConn) readReply() (any, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return nil, errors.New("the server sent an empty reply line")
	}
	kind, rest := line[0], line[1:]
	switch kind {
	case '+':
		return rest, nil
	case '-':
		return nil, replyError(rest)
	case ':':
		return strconv.ParseInt(rest, 10, 64)
	case '$':
		size, err := strconv.Atoi(rest)
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, nil
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(c.reader, body); err != nil {
			return nil, err
		}
		return string(body[:size]), nil
	case '*':
		count, err := strconv.Atoi(rest)
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, nil
		}
		items := make([]any, count)
		for i := range items {
			item, err := c.readReply()
			if err != nil {
				// An error inside an array is part of the reply, such as
				// one command's failure in a transaction, not the end of
				// the read.
				if reply, ok := errors.AsType[replyError](err); ok {
					items[i] = reply
					continue
				}
				return nil, err
			}
			items[i] = item
		}
		return items, nil
	}
	return nil, fmt.Errorf("the server sent a reply of unknown type %q", kind)
}

// replicationInfo is what INFO replication says about the server's
// role. Redis and Valkey both still call a replica "slave" and its
// primary "master" in this section.
type replicationInfo struct {
	role       string
	masterHost string
	masterPort int
	linkUp     bool
	offset     int64
	replid     string
	replicas   int
	// failoverState is "no-failover" except while a FAILOVER runs.
	failoverState string
}

func readReplication(ctx context.Context, c *redisConn) (replicationInfo, error) {
	reply, err := c.do(ctx, "INFO", "replication")
	if err != nil {
		return replicationInfo{}, err
	}
	text, ok := reply.(string)
	if !ok {
		return replicationInfo{}, fmt.Errorf("INFO answered %T, not text", reply)
	}
	return parseReplication(text), nil
}

func parseReplication(text string) replicationInfo {
	var info replicationInfo
	for line := range strings.SplitSeq(text, "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), ":")
		if !found {
			continue
		}
		switch key {
		case "role":
			info.role = value
		case "master_host":
			info.masterHost = value
		case "master_port":
			info.masterPort, _ = strconv.Atoi(value)
		case "master_link_status":
			info.linkUp = value == "up"
		case "master_repl_offset":
			info.offset, _ = strconv.ParseInt(value, 10, 64)
		case "master_replid":
			info.replid = value
		case "master_failover_state":
			info.failoverState = value
		case "connected_slaves":
			info.replicas, _ = strconv.Atoi(value)
		}
	}
	return info
}
