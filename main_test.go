package main

import (
	"bytes"
	"context"
	"testing"
)

func TestRunReportsItsVersionAndStopsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer

	err := run(ctx, &out)

	if err != nil {
		t.Fatalf("run returned %v", err)
	}
	if got, want := out.String(), "redis-operator dev\n"; got != want {
		t.Fatalf("run wrote %q, want %q", got, want)
	}
}
