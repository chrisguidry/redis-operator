package main

import "testing"

func TestThePrimaryReportsTheReplicasItStreamsTo(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.waitFor("cache-0 reports one synced replica", func() bool { return h.report("cache-0").SyncedReplicas == 1 })
	mustMatch(t, h.report("cache-1").SyncedReplicas, 0)
}

func TestOnlyThePrimaryReportsTheMinReplicasToWriteItRuns(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.waitFor("cache-0 reports the min-replicas-to-write it runs", func() bool {
		applied := h.report("cache-0").MinReplicasToWrite
		return applied != nil && *applied == 1
	})
	mustMatch(t, h.report("cache-1").MinReplicasToWrite, (*int)(nil))
}
