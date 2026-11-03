package main

import (
	"context"
	"testing"
)

func TestAnInstanceThatRestartsEmptyGivesUpThePrimaryLease(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.crash("cache-0")
	h.start("cache-0", "")

	h.waitFor("nobody holds the lease, and cache-0 stays detached", func() bool {
		return h.holder() == "" && h.report("cache-0").Role == reportDetached
	})
	mustMatch(t, h.report("cache-0").HasData, false)
}

// A server with no volume can still leave a snapshot in its data
// directory: a backup's BGSAVE or a replica's sync writes one there. The
// manager reports such an instance as empty after a restart, so its
// server must not load that snapshot and serve the keys in it.
func TestAnInstanceWithNoStorageRestartsWithNoKeys(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			h := twoInstances(t, engine)
			primary := h.client("cache-0")
			// SAVE refuses while a background save runs, such as the one
			// a replica's sync starts.
			h.waitFor("cache-0 saved a snapshot", func() bool {
				_, err := primary.do(context.Background(), "SAVE")
				return err == nil
			})
			dir := h.instances["cache-0"].dir

			h.crash("cache-0")
			h.start("cache-0", dir)

			h.waitFor("cache-0 answers", func() bool { return h.report("cache-0").Role != reportStarting })
			keys, err := h.client("cache-0").do(context.Background(), "DBSIZE")
			mustSucceed(t, err)
			mustMatch(t, keys, any(int64(0)))
			mustMatch(t, h.report("cache-0").HasData, false)
		})
	}
}
