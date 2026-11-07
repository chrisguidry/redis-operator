package main

// The operator's loop is level-triggered. A watch event, a poll that
// changed what a decision reads, or the backstop ticker wakes it, and
// each pass reads every Redis and the pods, Leases, StatefulSets, and
// Nodes it watches from the watches' stores, reads the latest poll of
// each Redis's instance managers (reconcile_poll.go), and reconciles
// each Redis. A restarted operator lists everything again, and starts
// correct: the decision it acts on is in each Redis's status, not in
// its memory.
//
// The memory holds only what time measures: when each instance manager
// last answered, since when it has missed the polls that other managers
// answered, when a failover started, and when a failed switchover may
// be tried again. A new leader starts that memory empty, and counts a
// manager's silence from the first such poll it misses.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

var (
	leasesWatched = kubeResource{"coordination.k8s.io", "v1", "leases"}
	// watchedCollections is the order in which run lists the resources.
	watchedCollections = []kubeResource{redisResource, podsResource, leasesWatched, statefulSetsResource, nodesResource, backupsResource, scheduledBackupsResource}
)

// selectors limits the watches of pods, Leases, and StatefulSets to the
// ones a Redis owns, so the stores do not hold every pod in the
// cluster.
var selectors = map[kubeResource]string{
	podsResource:         clusterLabel,
	leasesWatched:        clusterLabel,
	statefulSetsResource: clusterLabel,
}

var trims = map[kubeResource]func(json.RawMessage) (json.RawMessage, error){
	redisResource:            trimTo[Redis],
	podsResource:             trimTo[Pod],
	leasesWatched:            trimTo[leaseHolder],
	statefulSetsResource:     trimTo[StatefulSet],
	nodesResource:            trimTo[Node],
	backupsResource:          trimTo[Backup],
	scheduledBackupsResource: trimTo[ScheduledBackup],
}

// leaseHolder is the part of a primary Lease that a pass reads: its
// key and its holder. A renewal moves the renew time, and can move the
// generation, so its event leaves the trimmed copy as it was and wakes
// no pass.
type leaseHolder struct {
	Metadata struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Spec struct {
		HolderIdentity string `json:"holderIdentity,omitempty"`
	} `json:"spec"`
}

// The operator's timings. They are fields of the controller so a test
// can shorten them.
type timings struct {
	// backstop is how often a pass runs with nothing to prompt it. It
	// covers the decisions that time moves and no event announces: a
	// pending failover's wait for its replicas, a switchover's deadline,
	// a canary's soak, and a backup's schedule. pollPeriod is how often
	// each Redis's instance managers are polled.
	backstop   time.Duration
	pollPeriod time.Duration
	// failureThreshold is how long the primary's manager may miss
	// informative polls before a failover starts, counted from the first
	// one it missed (reconcile_poll.go).
	failureThreshold time.Duration
	// leaderlessThreshold is how long the Lease may have no holder before
	// a failover starts. It is longer than a lease duration, because a
	// new target waits out the old holder's Lease before it takes it.
	leaderlessThreshold time.Duration
	// pendingWait bounds how long a failover waits for every replica to
	// report that it detached before it names the next primary.
	pendingWait time.Duration
	// switchoverDeadline is how long a switchover may take before the
	// operator gives it up, and switchoverBackoff how long it waits
	// before it tries again. A promotion whose holder did not catch up
	// waits switchoverBackoff too.
	switchoverDeadline time.Duration
	switchoverBackoff  time.Duration
	pollTimeout        time.Duration
	canarySoak         time.Duration
	canaryTimeout      time.Duration
	// canaryLagBytes is how far behind the primary a canary may be and
	// still count as caught up.
	canaryLagBytes int64
	// eventTimeout bounds each write of an Event, and eventRetryWait is
	// the time between two sends of one write. An API server that
	// restarts refuses connections for some seconds, which the retries
	// cover.
	eventTimeout   time.Duration
	eventRetryWait time.Duration
}

var defaultTimings = timings{
	backstop:            2 * time.Second,
	pollPeriod:          time.Second,
	failureThreshold:    6 * time.Second,
	leaderlessThreshold: primaryLeaseDuration + 6*time.Second,
	pendingWait:         5 * time.Second,
	switchoverDeadline:  30 * time.Second,
	switchoverBackoff:   30 * time.Second,
	pollTimeout:         time.Second,
	canarySoak:          defaultCanarySoak,
	canaryTimeout:       defaultCanaryTimeout,
	canaryLagBytes:      1 << 20,
	eventTimeout:        5 * time.Second,
	eventRetryWait:      10 * time.Second,
}

type controller struct {
	client     *kubeClient
	readings   *metrics
	now        func() time.Time
	log        *lineLog
	events     *eventRecorder
	wake       chan struct{}
	pollWake   chan struct{}
	timings    timings
	images     images
	statusPort int
	poller     *http.Client
	watches    map[kubeResource]*collectionWatch

	// states, pollers, and backupPhases are read and written by the pass
	// only. polling runs the pollers inside run, and is nil outside it,
	// where each pass polls once itself.
	states       map[string]*redisState
	pollers      map[string]*instancePoller
	polling      *sync.WaitGroup
	backupPhases map[string]string
}

// redisState is what the operator remembers about one Redis.
type redisState struct {
	// pollView is the poll that this Redis's latest pass decided from.
	pollView
	// pendingSince is when the operator wrote targetPrimary: pending.
	pendingSince time.Time
	// leaderlessSince is when the Lease was last seen with no holder,
	// after a primary had held it.
	leaderlessSince time.Time
	retryAfter      time.Time
	// canarySince is when the rollout's canary was first seen proven.
	canarySince time.Time
	// promotionStarted is when a promotion paused the source, and
	// promotionRetryAfter when a promotion that timed out may pause it
	// again.
	promotionStarted    time.Time
	promotionRetryAfter time.Time
	// applied holds the body of each child as last applied, so a steady
	// pass writes nothing.
	applied map[string]string
	// appliedScale is the StatefulSet's scale as this operator last
	// applied it.
	appliedScale int
	// secretData is the connection Secret's data as the operator last
	// made it agree with the spec, less the password.
	secretData string
}

func newController(client *kubeClient, readings *metrics, imgs images, now func() time.Time, log io.Writer) *controller {
	c := &controller{
		client:       client,
		readings:     readings,
		now:          now,
		log:          newLineLog(log),
		wake:         make(chan struct{}, 1),
		pollWake:     make(chan struct{}, 1),
		timings:      defaultTimings,
		images:       imgs,
		statusPort:   statusPort,
		watches:      map[kubeResource]*collectionWatch{},
		states:       map[string]*redisState{},
		pollers:      map[string]*instancePoller{},
		backupPhases: map[string]string{},
	}
	c.events = newEventRecorder(client, now, c.log)
	c.poller = &http.Client{Timeout: c.timings.pollTimeout}
	for _, resource := range watchedCollections {
		c.watches[resource] = newCollectionWatch(client, resource, "", selectors[resource], trims[resource], c.wake, readings.watchRestarted(resource.Resource))
	}
	return c
}

// run fills each watch's store from a list, starts each watch from its
// list, and runs a pass at once and then on every wake and every tick,
// until ctx ends. Every list finishes before the first pass, so no pass
// acts on a store that is not full.
func (c *controller) run(ctx context.Context) error {
	versions, err := c.list(ctx)
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	var running sync.WaitGroup
	defer running.Wait()
	c.polling = &running
	c.watch(ctx, &running, versions)
	running.Go(func() { c.events.run(ctx, c.timings.eventRetryWait, c.timings.eventTimeout) })

	ticker := time.NewTicker(c.timings.backstop)
	defer ticker.Stop()
	for cause := passStart; ; {
		// select picks at random among the cases that are ready, so a
		// wake or a tick can win over a context that already ended, and
		// a pass on it would reach no instance manager.
		if ctx.Err() != nil {
			return nil
		}
		c.readings.passes.WithLabelValues(cause).Inc()
		c.pass(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-c.wake:
			cause = passEvent
		case <-c.pollWake:
			cause = passPoll
		case <-ticker.C:
			cause = passBackstop
		}
	}
}

// list fills the store of each watch from a list, and returns the
// resourceVersion each watch starts from.
func (c *controller) list(ctx context.Context) (map[kubeResource]string, error) {
	versions := map[kubeResource]string{}
	for _, resource := range watchedCollections {
		version, err := c.watches[resource].firstList(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing %s: %w", resource, err)
		}
		versions[resource] = version
	}
	return versions, nil
}

// watch starts each watch from the version its list returned.
func (c *controller) watch(ctx context.Context, running *sync.WaitGroup, versions map[kubeResource]string) {
	for resource, watch := range c.watches {
		running.Go(func() { watch.run(ctx, versions[resource]) })
	}
}

// world is one pass's read of the cluster.
type world struct {
	redis        []Redis
	pods         map[string][]Pod
	leases       map[string]Lease
	statefulSets map[string]StatefulSet
	nodes        map[string]Node

	backups          []Backup
	scheduledBackups []ScheduledBackup
}

func (c *controller) read() (*world, error) {
	w := &world{pods: map[string][]Pod{}, leases: map[string]Lease{}, statefulSets: map[string]StatefulSet{}, nodes: map[string]Node{}}
	var err error
	if w.redis, err = decodeSnapshot[Redis](c.watches[redisResource].store); err != nil {
		return nil, fmt.Errorf("decoding redis: %w", err)
	}
	pods, err := decodeSnapshot[Pod](c.watches[podsResource].store)
	if err != nil {
		return nil, fmt.Errorf("decoding pods: %w", err)
	}
	for _, pod := range pods {
		key := pod.Metadata.Namespace + "/" + pod.Metadata.Labels[clusterLabel]
		w.pods[key] = append(w.pods[key], pod)
	}
	leases, err := decodeSnapshot[Lease](c.watches[leasesWatched].store)
	if err != nil {
		return nil, fmt.Errorf("decoding leases: %w", err)
	}
	for _, lease := range leases {
		w.leases[objectKey(lease.Metadata)] = lease
	}
	sets, err := decodeSnapshot[StatefulSet](c.watches[statefulSetsResource].store)
	if err != nil {
		return nil, fmt.Errorf("decoding statefulsets: %w", err)
	}
	for _, set := range sets {
		w.statefulSets[objectKey(set.Metadata)] = set
	}
	nodes, err := decodeSnapshot[Node](c.watches[nodesResource].store)
	if err != nil {
		return nil, fmt.Errorf("decoding nodes: %w", err)
	}
	for _, node := range nodes {
		w.nodes[node.Metadata.Name] = node
	}
	if w.backups, err = decodeSnapshot[Backup](c.watches[backupsResource].store); err != nil {
		return nil, fmt.Errorf("decoding backups: %w", err)
	}
	if w.scheduledBackups, err = decodeSnapshot[ScheduledBackup](c.watches[scheduledBackupsResource].store); err != nil {
		return nil, fmt.Errorf("decoding scheduledbackups: %w", err)
	}
	return w, nil
}

func objectKey(meta ObjectMeta) string {
	return meta.Namespace + "/" + meta.Name
}

// pass reconciles each Redis, then the backups. A pass on a context
// that ended, as when the operator lost its Lease, stops at once: its
// requests fail because the operator stopped, not because a Redis is
// unwell, so it decides nothing and counts no error.
func (c *controller) pass(ctx context.Context) {
	began := time.Now()
	defer func() { c.readings.passDuration.Observe(time.Since(began).Seconds()) }()
	w, err := c.read()
	if err != nil {
		c.log.fault("reading the cluster", topicReconcile, errorText(err))
		return
	}
	c.log.clear("reading the cluster", topicReconcile)
	live := map[string]bool{}
	for index := range w.redis {
		if ctx.Err() != nil {
			return
		}
		r := &w.redis[index]
		live[r.Metadata.UID] = true
		subject := "redis " + objectKey(r.Metadata)
		if err := c.reconcile(ctx, r, w); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.readings.reconcileErrors.Inc()
			c.log.fault(subject, topicReconcile, errorText(err))
			continue
		}
		c.log.clear(subject, topicReconcile)
	}
	if ctx.Err() != nil {
		return
	}
	c.reconcileBackups(ctx, w)
	for uid := range c.states {
		if !live[uid] {
			delete(c.states, uid)
		}
	}
	c.forgetPollers(live)
}

func (c *controller) state(r *Redis) *redisState {
	st, ok := c.states[r.Metadata.UID]
	if !ok {
		st = &redisState{applied: map[string]string{}}
		c.states[r.Metadata.UID] = st
	}
	return st
}
