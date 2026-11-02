package main

import (
	"bytes"
	"strings"
	"sync"
	"time"
)

// testClock is a clock that moves only when a test moves it.
type testClock struct {
	mutex sync.Mutex
	at    time.Time
}

func (c *testClock) now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.at
}

func (c *testClock) advance(by time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.at = c.at.Add(by)
}

// syncBuffer is a log that the controller's goroutines write and the
// test reads.
type syncBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

// lines returns every line written so far.
func (b *syncBuffer) lines() []string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return strings.Split(strings.TrimSpace(b.buffer.String()), "\n")
}
