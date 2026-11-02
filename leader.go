package main

// Two replicas of the operator run at once, and one of them, the
// leader, does the work. The leader holds a Lease in the operator's
// namespace and renews it every retry period. A standby tries for the
// Lease every retry period, and takes it when its holder released it,
// or when it has not changed for a whole lease duration. The standby
// measures that duration with its own clock from when it first read
// the Lease as it is, so a clock that disagrees with the leader's
// cannot make a Lease look expired early.
//
// A leader that cannot renew for the renew deadline stops its work and
// goes back to waiting as a standby. Its next term runs a new
// controller, so nothing from the old term's memory carries over. The
// process does not exit, because the kubelet would count each lost
// Lease as a crash and delay the restarts. The renew deadline is shorter than
// the lease duration, so the leader stops before a standby can take
// the Lease. A Lease is not a lock, though: a leader that the scheduler
// pauses past the deadline, or a probe that is in flight when the work
// stops, can still send a ping beside the new leader's. Every ping the
// operator sends is safe to send twice except a cronJob check's report
// of one Job, which the new leader can send again before the old one's
// status write lands.
//
// The defaults are client-go's, from k8s.io/client-go/tools/leaderelection.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

const (
	leaseName           = "redis-operator"
	leaseDuration       = 15 * time.Second
	leaseRenewDeadline  = 10 * time.Second
	leaseRetryPeriod    = 2 * time.Second
	leaseReleaseTimeout = 5 * time.Second
	// microTime is the format of a Lease's acquireTime and renewTime.
	microTime = "2006-01-02T15:04:05.000000Z07:00"
)

var leasesResource = kubeResource{"coordination.k8s.io", "v1", "leases"}

var errLostLease = errors.New("lost lease")

// Lease is a coordination.k8s.io/v1 Lease, with the fields that leader
// election reads and writes.
type Lease struct {
	APIVersion string     `json:"apiVersion,omitempty"`
	Kind       string     `json:"kind,omitempty"`
	Metadata   ObjectMeta `json:"metadata"`
	Spec       LeaseSpec  `json:"spec"`
}

// LeaseSpec is comparable, so a candidate can tell whether the Lease
// changed since it last read it.
type LeaseSpec struct {
	HolderIdentity       string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds int    `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          string `json:"acquireTime,omitempty"`
	RenewTime            string `json:"renewTime,omitempty"`
	LeaseTransitions     int    `json:"leaseTransitions,omitempty"`
}

// candidateIdentity names this process in the Lease: the pod's
// hostname, which is its name, and a random suffix, so two pods that
// share a hostname, or a container that restarts in the same pod, are
// never taken for the same holder. A process with no hostname uses the
// operator's name.
func candidateIdentity(hostname func() (string, error)) string {
	host, err := hostname()
	if err != nil {
		host = "redis-operator"
	}
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	return host + "_" + hex.EncodeToString(suffix)
}

type election struct {
	client    *kubeClient
	namespace string
	identity  string
	readings  *metrics
	log       io.Writer
	// duration, deadline, and retry are fields so a test waits
	// milliseconds.
	duration time.Duration
	deadline time.Duration
	retry    time.Duration

	// observed is the Lease's spec as last read or written, and
	// observedAt is when this candidate first saw it so. version is the
	// resourceVersion of this candidate's last write, which release
	// states, so a release never clears another holder's Lease.
	observed   LeaseSpec
	observedAt time.Time
	version    string
	// said holds the lines this candidate wrote since it last led, so a
	// wait that lasts, or that alternates between two causes, writes
	// each line once.
	said map[string]bool
}

func newElection(client *kubeClient, namespace, identity string, readings *metrics, log io.Writer) *election {
	return &election{
		client:    client,
		namespace: namespace,
		identity:  identity,
		readings:  readings,
		log:       log,
		duration:  leaseDuration,
		deadline:  leaseRenewDeadline,
		retry:     leaseRetryPeriod,
		said:      map[string]bool{},
	}
}

func (e *election) subject() string {
	return "lease " + e.namespace + "/" + leaseName
}

// say writes one line, once until this candidate next leads.
func (e *election) say(line string) {
	if !e.said[line] {
		e.said[line] = true
		_, _ = fmt.Fprintf(e.log, "%s: %s\n", e.identity, line)
	}
}

// serve leads whenever this candidate holds the Lease, and waits as a
// standby whenever it does not, until ctx ends or work fails.
func (e *election) serve(ctx context.Context, work func(context.Context) error) error {
	for {
		if err := e.lead(ctx, work); !errors.Is(err, errLostLease) {
			return err
		}
	}
}

// lead waits until this candidate holds the Lease, then runs work
// under a context that ends when ctx ends or the Lease is lost, and
// releases the Lease when work returns. It returns work's error, or
// errLostLease. It returns nil when ctx ends before this candidate
// leads.
func (e *election) lead(ctx context.Context, work func(context.Context) error) error {
	if !e.acquire(ctx) {
		return nil
	}
	clear(e.said)
	_, _ = fmt.Fprintf(e.log, "%s: leading, as the holder of %s\n", e.identity, e.subject())
	e.readings.setLeader(true)
	defer e.readings.setLeader(false)
	defer e.release()

	working, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	done := make(chan error, 1)
	go func() { done <- work(working) }()

	renewed := time.Now()
	ticker := time.NewTicker(e.retry)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			if lost := context.Cause(working); errors.Is(lost, errLostLease) {
				return lost
			}
			return err
		case <-ticker.C:
		}
		if context.Cause(working) != nil {
			continue
		}
		// An attempt ends at the deadline, so an API server that stops
		// answering cannot hold the leader past it. A renewal counts from
		// when its attempt started, because a standby may have read the
		// Lease as it was before the write, and counts from then.
		started := time.Now()
		attempt, cancel := context.WithDeadline(working, renewed.Add(e.deadline))
		holder, err := e.try(attempt)
		cancel()
		var lost error
		switch {
		case holder == e.identity:
			renewed = started
		case holder != "":
			lost = fmt.Errorf("%w %s/%s: %s holds it", errLostLease, e.namespace, leaseName, holder)
		case time.Since(renewed) >= e.deadline:
			lost = fmt.Errorf("%w %s/%s: it was not renewed for %s", errLostLease, e.namespace, leaseName, e.deadline)
			if err != nil {
				lost = fmt.Errorf("%w: %v", lost, err)
			}
		}
		if lost != nil {
			_, _ = fmt.Fprintf(e.log, "%s: %s\n", e.identity, lost)
			stop(lost)
		}
	}
}

// acquire tries for the Lease every retry period until this candidate
// holds it, and reports false when ctx ends first.
func (e *election) acquire(ctx context.Context) bool {
	for {
		holder, err := e.try(ctx)
		switch {
		case holder == e.identity:
			return true
		case err != nil:
			e.say(fmt.Sprintf("cannot read %s: %v", e.subject(), err))
		case holder != "":
			e.say(fmt.Sprintf("waiting for %s, which %s holds", e.subject(), holder))
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(e.retry):
		}
	}
}

// try reads the Lease, and takes or renews it when this candidate may.
// It returns the holder after the attempt: this candidate's identity
// when it holds the Lease, another's when another holds it and has not
// let it expire, and empty when the attempt failed.
func (e *election) try(ctx context.Context) (string, error) {
	var lease Lease
	err := e.client.get(ctx, leasesResource, e.namespace, leaseName, &lease)
	// The clock is read after the answer, so a slow read never makes a
	// Lease look older than it is.
	now := time.Now()
	if errors.Is(err, errNotFound) {
		return e.create(ctx, now)
	}
	if err != nil {
		return "", err
	}
	if lease.Spec != e.observed {
		e.observed, e.observedAt = lease.Spec, now
	}
	holder := lease.Spec.HolderIdentity
	if holder != "" && holder != e.identity && now.Before(e.observedAt.Add(e.duration)) {
		return holder, nil
	}

	spec := lease.Spec
	spec.HolderIdentity = e.identity
	spec.LeaseDurationSeconds = e.durationSeconds()
	spec.RenewTime = now.UTC().Format(microTime)
	if holder != e.identity {
		spec.AcquireTime = spec.RenewTime
		spec.LeaseTransitions++
	}
	var written Lease
	err = e.client.mergePatch(ctx, leasesResource, e.namespace, leaseName, Lease{
		Metadata: ObjectMeta{ResourceVersion: lease.Metadata.ResourceVersion},
		Spec:     spec,
	}, &written)
	if err != nil {
		return "", wroteFirst(err)
	}
	e.wrote(written, now)
	return e.identity, nil
}

// wroteFirst drops a conflict, which means another candidate wrote the
// Lease first. The next try reads what it wrote.
func wroteFirst(err error) error {
	if errors.Is(err, errConflict) {
		return nil
	}
	return err
}

func (e *election) create(ctx context.Context, now time.Time) (string, error) {
	stamp := now.UTC().Format(microTime)
	var written Lease
	err := e.client.create(ctx, leasesResource, e.namespace, Lease{
		APIVersion: "coordination.k8s.io/v1",
		Kind:       "Lease",
		Metadata:   ObjectMeta{Name: leaseName, Namespace: e.namespace},
		Spec: LeaseSpec{
			HolderIdentity:       e.identity,
			LeaseDurationSeconds: e.durationSeconds(),
			AcquireTime:          stamp,
			RenewTime:            stamp,
		},
	}, &written)
	if err != nil {
		return "", wroteFirst(err)
	}
	e.wrote(written, now)
	return e.identity, nil
}

func (e *election) wrote(lease Lease, now time.Time) {
	e.version = lease.Metadata.ResourceVersion
	e.observed, e.observedAt = lease.Spec, now
}

// durationSeconds is the lease duration that the Lease states for
// other tools to read. Candidates go by their own duration.
func (e *election) durationSeconds() int {
	return int(math.Ceil(e.duration.Seconds()))
}

// release clears the holder, so a standby takes the Lease on its next
// try instead of after a whole lease duration. The patch states the
// version this candidate last wrote, so it changes nothing when
// another candidate has written since.
func (e *election) release() {
	ctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
	defer cancel()
	err := e.client.mergePatch(ctx, leasesResource, e.namespace, leaseName, map[string]any{
		"metadata": map[string]any{"resourceVersion": e.version},
		"spec":     map[string]any{"holderIdentity": nil},
	}, nil)
	if err != nil && !errors.Is(err, errConflict) {
		_, _ = fmt.Fprintf(e.log, "%s: cannot release %s: %v\n", e.identity, e.subject(), err)
	}
}
