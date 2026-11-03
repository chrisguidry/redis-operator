package main

import "testing"

func TestThePrimaryReportsTheReplicasItStreamsTo(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")

	h.waitFor("cache-0 reports one synced replica", func() bool { return h.report("cache-0").SyncedReplicas == 1 })
	mustMatch(t, h.report("cache-1").SyncedReplicas, 0)
}
