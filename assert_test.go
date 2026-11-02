package main

// The assertion helpers that the test files share. Each one fails the
// test with the value it read and the value it wanted.

import (
	"maps"
	"testing"
	"time"
)

// testTimeout bounds every wait in the tests. It is long enough for a
// loaded machine, and short enough that a broken program fails in
// seconds.
const testTimeout = 2 * time.Second

func mustSucceed(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("wanted no error, got %v", err)
	}
}

func mustMatch[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func mustMatchMap[K, V comparable](t *testing.T, got, want map[K]V) {
	t.Helper()
	if !maps.Equal(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
