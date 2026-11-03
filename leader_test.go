package main

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// testElection is a candidate whose lease lasts milliseconds. The renew
// deadline is a third of the lease duration, so a test can tell a
// leader that stopped in time from one that did not.
func testElection(api *fakeKube, identity string) (*election, *syncBuffer) {
	log := &syncBuffer{}
	e := newElection(api.client, operatorNamespace, identity, newMetrics("dev"), log)
	e.duration, e.deadline, e.retry = 900*time.Millisecond, 300*time.Millisecond, 20*time.Millisecond
	return e, log
}

// candidate is one election running in the background. Its work closes
// started and runs until its context ends, then closes stopped. done
// closes when lead returns, and err is what it returned.
type candidate struct {
	started chan struct{}
	stopped chan struct{}
	done    chan struct{}
	err     error
	stop    context.CancelFunc
}

func startCandidate(t *testing.T, e *election) *candidate {
	t.Helper()
	ctx, stop := context.WithCancel(t.Context())
	c := &candidate{started: make(chan struct{}), stopped: make(chan struct{}), done: make(chan struct{}), stop: stop}
	go func() {
		defer close(c.done)
		c.err = e.lead(ctx, func(ctx context.Context) error {
			close(c.started)
			<-ctx.Done()
			close(c.stopped)
			return nil
		})
	}()
	t.Cleanup(func() {
		stop()
		<-c.done
	})
	return c
}

func waitFor(t *testing.T, what string, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitForResult(t *testing.T, c *candidate) error {
	t.Helper()
	waitFor(t, "lead to return", c.done)
	return c.err
}

func readLease(t *testing.T, api *fakeKube) Lease {
	t.Helper()
	var lease Lease
	if !api.read(leasesResource, operatorNamespace, leaseName, &lease) {
		t.Fatal("the Lease does not exist")
	}
	return lease
}

func TestACandidateCreatesTheLeaseAndLeads(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	e, _ := testElection(api, "a")

	c := startCandidate(t, e)

	waitFor(t, "a to lead", c.started)
	lease := readLease(t, api)
	mustMatch(t, lease.Spec.HolderIdentity, "a")
	mustMatch(t, lease.Spec.LeaseDurationSeconds, 1)
	mustMatch(t, slices.Contains(scrape(t, e.readings), "redis_operator_leader 1"), true)
}

func TestALeaderRenewsTheLease(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	e, _ := testElection(api, "a")
	c := startCandidate(t, e)
	waitFor(t, "a to lead", c.started)
	first := readLease(t, api).Spec.RenewTime

	time.Sleep(5 * e.retry)

	renewed := readLease(t, api)
	mustMatch(t, renewed.Spec.RenewTime != first, true)
	mustMatch(t, renewed.Spec.AcquireTime, first)
}

func TestAStandbyWaitsWhileTheLeaderRenews(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	waitFor(t, "a to lead", startCandidate(t, a).started)
	b, log := testElection(api, "b")

	standby := startCandidate(t, b)
	time.Sleep(2 * b.duration)

	mustMatch(t, readLease(t, api).Spec.HolderIdentity, "a")
	select {
	case <-standby.started:
		t.Fatal("b led while a renewed the lease")
	default:
	}
	mustMatch(t, strings.Join(log.lines(), "\n"), "b: waiting for lease redis-operator/redis-operator, which a holds")
	mustMatch(t, slices.Contains(scrape(t, b.readings), "redis_operator_leader 0"), true)
}

// A leader that stops releases the lease, so a standby takes it on its
// next try instead of after a whole lease duration.
func TestAStandbyTakesALeaseItsLeaderReleased(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	leader := startCandidate(t, a)
	waitFor(t, "a to lead", leader.started)
	b, _ := testElection(api, "b")
	standby := startCandidate(t, b)

	leader.stop()
	mustSucceed(t, waitForResult(t, leader))
	released := time.Now()
	waitFor(t, "b to lead", standby.started)

	mustMatch(t, time.Since(released) < b.duration, true)
	lease := readLease(t, api)
	mustMatch(t, lease.Spec.HolderIdentity, "b")
	mustMatch(t, lease.Spec.LeaseTransitions, 1)
}

// A Lease whose holder died without releasing it, as a pod on a node
// that lost power does, is taken once it has not changed for a whole
// lease duration, as the standby's own clock measures it.
func TestAStandbyTakesALeaseItsHolderStoppedRenewing(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(leasesResource, Lease{
		Metadata: ObjectMeta{Namespace: operatorNamespace, Name: leaseName},
		Spec:     LeaseSpec{HolderIdentity: "gone", LeaseDurationSeconds: 1, RenewTime: "2026-10-28T12:00:00.000000Z"},
	})
	b, _ := testElection(api, "b")
	began := time.Now()

	standby := startCandidate(t, b)

	waitFor(t, "b to lead", standby.started)
	mustMatch(t, time.Since(began) >= b.duration, true)
	mustMatch(t, readLease(t, api).Spec.HolderIdentity, "b")
}

// A leader that cannot renew stops its work before the lease could
// expire, so a standby that takes the lease never runs beside it.
func TestALeaderThatCannotRenewStopsBeforeTheLeaseExpires(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, log := testElection(api, "a")
	leader := startCandidate(t, a)
	waitFor(t, "a to lead", leader.started)

	api.refuse("PATCH", leasesResource.path(operatorNamespace, leaseName), 503)
	refused := time.Now()
	waitFor(t, "a's work to stop", leader.stopped)

	mustMatch(t, time.Since(refused) < a.duration, true)
	mustMatch(t, errors.Is(waitForResult(t, leader), errLostLease), true)
	mustMatch(t, strings.HasPrefix(log.lines()[1], "a: lost lease redis-operator/redis-operator: it was not renewed for 300ms: "), true)
	mustMatch(t, slices.Contains(scrape(t, a.readings), "redis_operator_leader 0"), true)
}

func TestALeaderStopsAtOnceWhenAnotherTakesTheLease(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	a.deadline = testTimeout
	leader := startCandidate(t, a)
	waitFor(t, "a to lead", leader.started)

	api.update(leasesResource, operatorNamespace, leaseName, func(object map[string]any) {
		object["spec"].(map[string]any)["holderIdentity"] = "intruder"
	})

	err := waitForResult(t, leader)
	mustMatch(t, errors.Is(err, errLostLease), true)
	mustMatch(t, err.Error(), "lost lease redis-operator/redis-operator: intruder holds it")
	mustMatch(t, readLease(t, api).Spec.HolderIdentity, "intruder")
}

// A leader that loses the Lease waits as a standby, and leads again,
// with new work, once the Lease is free.
func TestALeaderThatLostTheLeaseLeadsAgainLater(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	terms := make(chan struct{}, 2)
	go func() {
		_ = a.serve(t.Context(), func(ctx context.Context) error {
			terms <- struct{}{}
			<-ctx.Done()
			return nil
		})
	}()
	waitFor(t, "a's first term", terms)

	api.update(leasesResource, operatorNamespace, leaseName, func(object map[string]any) {
		object["spec"].(map[string]any)["holderIdentity"] = "gone"
	})

	waitFor(t, "a's second term", terms)
	mustMatch(t, readLease(t, api).Spec.HolderIdentity, "a")
}

func TestServeReturnsWhenItsContextEnds(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	ctx, stop := context.WithCancel(t.Context())
	stop()

	mustSucceed(t, a.serve(ctx, func(context.Context) error { return nil }))
}

// Two candidates that create the Lease at once both see a Lease, and
// the one whose create lost says nothing about a conflict.
func TestACandidateThatLosesTheCreateWaits(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	b, log := testElection(api, "b")
	waitFor(t, "a to lead", startCandidate(t, a).started)

	holder, err := b.create(t.Context(), time.Now())

	mustSucceed(t, err)
	mustMatch(t, holder, "")
	mustMatch(t, len(log.lines()), 1)
}

func TestALeaderReleasesTheLeaseWhenItStops(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	leader := startCandidate(t, a)
	waitFor(t, "a to lead", leader.started)

	leader.stop()

	mustSucceed(t, waitForResult(t, leader))
	mustMatch(t, readLease(t, api).Spec.HolderIdentity, "")
}

func TestAnErrorFromTheWorkEndsTheLeadership(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	failed := errors.New("listing checks: forbidden")

	err := a.lead(t.Context(), func(context.Context) error { return failed })

	mustMatch(t, err, failed)
	mustMatch(t, readLease(t, api).Spec.HolderIdentity, "")
}

// A candidate stopped while it waits never leads, and leaves the Lease
// to its holder.
func TestACandidateStoppedWhileItWaitsDoesNotLead(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	a, _ := testElection(api, "a")
	waitFor(t, "a to lead", startCandidate(t, a).started)
	b, _ := testElection(api, "b")
	standby := startCandidate(t, b)

	standby.stop()

	mustSucceed(t, waitForResult(t, standby))
	mustMatch(t, readLease(t, api).Spec.HolderIdentity, "a")
}

// A candidate whose reads the API server refuses keeps trying, as one
// whose RBAC rule has not arrived yet does.
func TestACandidateKeepsTryingWhileTheAPIServerRefusesIt(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	path := leasesResource.path(operatorNamespace, leaseName)
	api.refuse("GET", path, 403)
	a, log := testElection(api, "a")
	c := startCandidate(t, a)
	time.Sleep(5 * a.retry)

	api.allow("GET", path)

	waitFor(t, "a to lead", c.started)
	mustMatch(t, strings.HasPrefix(log.lines()[0], "a: cannot read lease redis-operator/redis-operator: GET "+path+": 403 Forbidden: "), true)
}

func TestCandidateIdentitiesAreUnique(t *testing.T) {
	t.Parallel()
	first, second := candidateIdentity(os.Hostname), candidateIdentity(os.Hostname)

	mustMatch(t, first != second, true)
}

func TestACandidateWithNoHostnameNamesThisOperator(t *testing.T) {
	t.Parallel()
	identity := candidateIdentity(func() (string, error) { return "", errors.New("no hostname") })

	mustMatch(t, strings.HasPrefix(identity, "redis-operator_"), true)
}

const operatorNamespace = "redis-operator"
