package main

// The primary Lease. An instance may run as primary only while it holds
// the Lease named <redis>-primary. The holder renews it every retry
// period. An instance that is the target takes it when it is empty, or
// when it has seen the Lease unchanged for a whole lease duration. It
// measures that with its own monotonic clock, from when it first read
// the Lease as it is, so a clock that disagrees with the holder's
// cannot make a Lease look expired early. This is the rule
// CloudNativePG's instance manager keeps for its primary.
//
// The identity in the Lease is the pod's name, so the operator can label
// the holder's pod, and a manager that restarts in the same pod finds
// the Lease still its own.

import (
	"context"
	"errors"
	"math"
	"time"
)

type primaryLease struct {
	client    *kubeClient
	namespace string
	name      string
	identity  string
	owner     OwnerReference
	duration  time.Duration

	// observed is the Lease as last read, and observedAt is when this
	// manager first read it that way.
	observed   LeaseSpec
	observedAt time.Time
	exists     bool
	version    string
}

// read reads the Lease, and tracks how long it has stayed as it is.
func (l *primaryLease) read(ctx context.Context, now time.Time) error {
	var lease Lease
	err := l.client.get(ctx, leasesResource, l.namespace, l.name, &lease)
	if errors.Is(err, errNotFound) {
		l.exists, l.version = false, ""
		l.see(LeaseSpec{}, now)
		return nil
	}
	if err != nil {
		return err
	}
	l.exists, l.version = true, lease.Metadata.ResourceVersion
	l.see(lease.Spec, now)
	return nil
}

func (l *primaryLease) see(spec LeaseSpec, now time.Time) {
	if spec != l.observed || l.observedAt.IsZero() {
		l.observed, l.observedAt = spec, now
	}
}

func (l *primaryLease) holder() string { return l.observed.HolderIdentity }

func (l *primaryLease) held() bool { return l.holder() == l.identity }

// takeable reports whether this instance may take the Lease now: it is
// empty, it is this instance's own, or its holder has not renewed it
// for a whole lease duration.
func (l *primaryLease) takeable(now time.Time) bool {
	holder := l.holder()
	return holder == "" || holder == l.identity || !now.Before(l.observedAt.Add(l.duration))
}

// take writes this instance as the holder. The write states the version
// read, so it fails when another instance wrote first.
func (l *primaryLease) take(ctx context.Context, now time.Time) error {
	stamp := now.UTC().Format(microTime)
	spec := l.observed
	spec.HolderIdentity = l.identity
	spec.LeaseDurationSeconds = int(math.Ceil(l.duration.Seconds()))
	spec.RenewTime = stamp
	if l.observed.HolderIdentity != l.identity {
		spec.AcquireTime = stamp
		spec.LeaseTransitions++
	}
	return l.write(ctx, spec, now)
}

// renew moves the renew time. It is take for an instance that holds the
// Lease already.
func (l *primaryLease) renew(ctx context.Context, now time.Time) error {
	return l.take(ctx, now)
}

// release clears the holder, so the next primary takes the Lease at
// once instead of after a whole lease duration.
func (l *primaryLease) release(ctx context.Context, now time.Time) error {
	if !l.exists || !l.held() {
		return nil
	}
	spec := l.observed
	spec.HolderIdentity = ""
	return l.write(ctx, spec, now)
}

func (l *primaryLease) write(ctx context.Context, spec LeaseSpec, now time.Time) error {
	var written Lease
	var err error
	if l.exists {
		err = l.client.mergePatch(ctx, leasesResource, l.namespace, l.name, map[string]any{
			"metadata": map[string]any{"resourceVersion": l.version},
			"spec":     leaseSpecPatch(spec),
		}, &written)
	} else {
		err = l.client.create(ctx, leasesResource, l.namespace, Lease{
			APIVersion: "coordination.k8s.io/v1",
			Kind:       "Lease",
			Metadata: ObjectMeta{
				Name: l.name, Namespace: l.namespace,
				Labels:          map[string]string{clusterLabel: l.owner.Name},
				OwnerReferences: []OwnerReference{l.owner},
			},
			Spec: spec,
		}, &written)
	}
	if err != nil {
		return err
	}
	l.exists, l.version = true, written.Metadata.ResourceVersion
	l.observed, l.observedAt = written.Spec, now
	return nil
}

// leaseSpecPatch states every field, with an empty holder as null, so
// a merge patch removes the holder rather than leaving it.
func leaseSpecPatch(spec LeaseSpec) map[string]any {
	patch := map[string]any{
		"holderIdentity":       nil,
		"leaseDurationSeconds": spec.LeaseDurationSeconds,
		"acquireTime":          spec.AcquireTime,
		"renewTime":            spec.RenewTime,
		"leaseTransitions":     spec.LeaseTransitions,
	}
	if spec.HolderIdentity != "" {
		patch["holderIdentity"] = spec.HolderIdentity
	}
	return patch
}
