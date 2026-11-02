// redis-operator runs Redis and Valkey on Kubernetes from Redis
// resources: one primary and its replicas, failover, switchover before a
// node drains, and the Services and Secrets that clients connect
// through. plans/00-design.md is the design.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// version is the release this binary was built from. The Dockerfile
// sets it with -ldflags "-X main.version=...". A build without the
// flag, such as go test, reports dev.
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run starts the operator and returns when ctx ends.
func run(ctx context.Context, out io.Writer) error {
	fmt.Fprintf(out, "redis-operator %s\n", version)
	<-ctx.Done()
	return nil
}
