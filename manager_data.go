package main

// The data a server starts with. A persistent server loads what its
// data directory holds, and the manager counts that as data. A server
// with no volume starts empty, and an instance that restarts empty
// gives up a primary Lease it still holds.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// serverDataFiles are what a server loads from its data directory when
// it starts: its append-only directory and its snapshot.
var serverDataFiles = []string{"appendonlydir", snapshotFile}

// dataOnDisk says whether a persistent server will load data when it
// starts: an append-only directory or a snapshot is in its data
// directory.
func dataOnDisk(dir string) bool {
	for _, name := range serverDataFiles {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return true
		}
	}
	return false
}

// clearData removes what a server with no volume left in its data
// directory before its container restarted. Such a server never saves
// on its own, but a backup's BGSAVE and a replica's sync from disk both
// write a snapshot there, and both need the snapshot file, so the
// server cannot be configured to stop writing it. Loading it at the next
// start would serve keys from before the restart while the manager
// reports the instance empty: as a primary that the operator let start
// with no data, or as a replica before it resyncs.
func clearData(dir string) error {
	for _, name := range serverDataFiles {
		if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("clearing the data directory: %w", err)
		}
	}
	return nil
}

// releaseLeaseIfEmpty releases a Lease that this pod still holds from
// before its container restarted, when the server will start with no
// data. Serving as primary then would hand every replica an empty
// dataset, so the operator fails over to a replica instead.
func (m *manager) releaseLeaseIfEmpty(ctx context.Context) error {
	if m.hasData {
		return nil
	}
	now := m.now()
	if err := m.lease.read(ctx, now); err != nil {
		return fmt.Errorf("reading the primary lease: %w", err)
	}
	if !m.lease.held() {
		return nil
	}
	m.say(m.config.podName + ": restarted with no data while holding the primary lease; releasing it")
	return m.lease.release(ctx, now)
}
