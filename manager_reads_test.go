package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// waitForRenewals waits until the holder has renewed the primary Lease
// count more times, so the managers have run several steps.
func (h *managerHarness) waitForRenewals(count int) {
	h.t.Helper()
	lease := leasesResource.path(testNamespace, primaryLeaseName(h.name))
	start := h.api.requestCount("PATCH", lease, false)
	h.waitFor("the holder to renew", func() bool { return h.api.requestCount("PATCH", lease, false) >= start+count })
}

// Once the managers settle, the holder renews from the answer to its own
// last write, and an instance that is not the target reads the Lease
// only on its own clock, so steady steps send the Lease no GET.
func TestSteadyStepsReadTheLeaseOnlyWhenDue(t *testing.T) {
	t.Parallel()
	h := startManagerHarness(t, "redis", 2)
	h.leaseReadPeriod = time.Hour
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })
	h.start("cache-1", "")
	h.waitFor("cache-1 follows cache-0", func() bool { return h.report("cache-1").LinkUp })
	lease := leasesResource.path(testNamespace, primaryLeaseName(h.name))
	reads := h.api.requestCount("GET", lease, false)

	h.waitForRenewals(3)

	mustMatch(t, h.api.requestCount("GET", lease, false), reads)
}

// The pods are listed on the slow clock, not on every step.
func TestThePodsAreListedOnTheSlowClock(t *testing.T) {
	t.Parallel()
	h := startManagerHarness(t, "redis", 1)
	h.slowReadPeriod = time.Hour
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })
	// Taking the Lease changes the holder, which lists the pods once.
	h.waitForRenewals(1)
	lists := h.api.requestCount("GET", podsResource.path(testNamespace, ""), false)

	h.waitForRenewals(3)

	mustMatch(t, h.api.requestCount("GET", podsResource.path(testNamespace, ""), false), lists)
}

// A holder that the API server refuses as stale reads the Lease again,
// and learns that another instance took it.
func TestAHolderWhoseRenewalIsStaleReadsTheLeaseAgain(t *testing.T) {
	t.Parallel()
	h := startManagerHarness(t, "redis", 1)
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })

	h.api.update(leasesResource, testNamespace, primaryLeaseName(h.name), func(object map[string]any) {
		object["spec"].(map[string]any)["holderIdentity"] = "cache-9"
	})

	h.waitFor("cache-0 to see it no longer holds the Lease", func() bool { return !h.report("cache-0").HoldsLease })
}

// A step's API requests as the holder end by the renew deadline, and
// past it each ends within a retry period, so a request that hangs
// never keeps the holder from its isolation check.
func TestTheHoldersRequestsEndByTheRenewDeadline(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		renewed time.Duration
		within  time.Duration
	}{
		{"inside the deadline", -9 * time.Second, time.Second},
		{"past the deadline", -time.Minute, 3 * time.Second},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			m := newManager(managerConfig{podName: "cache-0", renewDeadline: 10 * time.Second, retryPeriod: 3 * time.Second}, nil, &syncBuffer{})
			m.lease.observed.HolderIdentity = "cache-0"
			m.lastRenew = time.Now().Add(one.renewed)

			ctx, cancel := m.apiContext(context.Background())
			defer cancel()

			deadline, _ := ctx.Deadline()
			mustMatch(t, time.Until(deadline) <= one.within, true)
			mustMatch(t, time.Until(deadline) > one.within-time.Second, true)
		})
	}
}

// Peers agree only when every other instance answered, so a list of
// pods that lacks one never makes the holder keep serving alone.
func TestPeersAgreeOnlyWhenEveryOtherInstanceWasAsked(t *testing.T) {
	t.Parallel()
	m := newManager(managerConfig{podName: "cache-0"}, nil, &syncBuffer{})
	m.instances = 2
	m.podIPs = map[string]string{"cache-0": "127.0.0.2"}

	mustMatch(t, m.peersAgree(context.Background()), false)
}

// A steady primary sends CONFIG SET of min-replicas-to-write only when
// the value it runs should change.
func TestASteadyPrimarySetsMinReplicasToWriteOnce(t *testing.T) {
	t.Parallel()
	h := twoInstances(t, "redis")
	sets := func() string {
		info, err := h.client("cache-0").do(context.Background(), "INFO", "commandstats")
		mustSucceed(t, err)
		for line := range strings.Lines(info.(string)) {
			if strings.HasPrefix(line, "cmdstat_config|set:") {
				calls, _, _ := strings.Cut(strings.TrimPrefix(line, "cmdstat_config|set:"), ",")
				return calls
			}
		}
		return ""
	}
	before := sets()

	h.waitForRenewals(3)

	mustMatch(t, sets(), before)
}

// A new target makes every instance list the pods again, so the address
// a replica follows or a holder hands over to is current.
func TestANewTargetListsThePodsAgain(t *testing.T) {
	t.Parallel()
	h := startManagerHarness(t, "redis", 1)
	h.slowReadPeriod = time.Hour
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })
	lists := h.api.requestCount("GET", podsResource.path(testNamespace, ""), false)

	h.setTarget(targetPending)

	h.waitFor("cache-0 to list the pods", func() bool {
		return h.api.requestCount("GET", podsResource.path(testNamespace, ""), false) > lists
	})
}
