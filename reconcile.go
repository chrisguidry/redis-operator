package main

// The operator's loop is level-triggered. A watch event or the backstop
// ticker wakes it, and each pass reads every Redis and the pods, Leases,
// StatefulSets, and Nodes it watches from the watches' stores, polls
// each instance manager's /status, and reconciles each Redis. A
// restarted operator lists everything again, and starts correct: the
// decision it acts on is in each Redis's status, not in its memory.
//
// The memory holds only what time measures: when each instance manager
// last answered, when a failover started, and when a failed switchover
// may be tried again. A new leader starts that memory empty, and counts
// from its first poll.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

var (
	leasesWatched = kubeResource{"coordination.k8s.io", "v1", "leases"}
	// watchedCollections is the order in which run lists the resources.
	watchedCollections = []kubeResource{redisResource, podsResource, leasesWatched, statefulSetsResource, nodesResource}
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
	redisResource:        trimTo[Redis],
	podsResource:         trimTo[Pod],
	leasesWatched:        trimTo[Lease],
	statefulSetsResource: trimTo[StatefulSet],
	nodesResource:        trimTo[Node],
}

// The operator's timings. They are fields of the controller so a test
// can shorten them.
type timings struct {
	// backstop is how often a pass runs with nothing to prompt it. It is
	// also how often the instance managers are polled.
	backstop time.Duration
	// failureThreshold is how long the primary's manager may go
	// unanswered before a failover starts.
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
	// before it tries again.
	switchoverDeadline time.Duration
	switchoverBackoff  time.Duration
	pollTimeout        time.Duration
}

var defaultTimings = timings{
	backstop:            2 * time.Second,
	failureThreshold:    6 * time.Second,
	leaderlessThreshold: primaryLeaseDuration + 6*time.Second,
	pendingWait:         5 * time.Second,
	switchoverDeadline:  30 * time.Second,
	switchoverBackoff:   30 * time.Second,
	pollTimeout:         time.Second,
}

type controller struct {
	client        *kubeClient
	readings      *metrics
	now           func() time.Time
	log           *lineLog
	wake          chan struct{}
	timings       timings
	operatorImage string
	statusPort    int
	poller        *http.Client
	watches       map[kubeResource]*collectionWatch

	// states is read and written by the pass only.
	states map[string]*redisState
}

// redisState is what the operator remembers about one Redis.
type redisState struct {
	firstPoll time.Time
	lastSeen  map[string]time.Time
	reports   map[string]instanceReport
	// pendingSince is when the operator wrote targetPrimary: pending.
	pendingSince time.Time
	// leaderlessSince is when the Lease was last seen with no holder,
	// after a primary had held it.
	leaderlessSince time.Time
	retryAfter      time.Time
	// applied holds the body of each child as last applied, so a steady
	// pass writes nothing.
	applied   map[string]string
	hasSecret bool
}

func newController(client *kubeClient, readings *metrics, operatorImage string, now func() time.Time, log io.Writer) *controller {
	c := &controller{
		client:        client,
		readings:      readings,
		now:           now,
		log:           newLineLog(log),
		wake:          make(chan struct{}, 1),
		timings:       defaultTimings,
		operatorImage: operatorImage,
		statusPort:    statusPort,
		watches:       map[kubeResource]*collectionWatch{},
		states:        map[string]*redisState{},
	}
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
	c.watch(ctx, &running, versions)

	ticker := time.NewTicker(c.timings.backstop)
	defer ticker.Stop()
	for {
		c.pass(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-c.wake:
		case <-ticker.C:
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
	return w, nil
}

func objectKey(meta ObjectMeta) string {
	return meta.Namespace + "/" + meta.Name
}

func (c *controller) pass(ctx context.Context) {
	w, err := c.read()
	if err != nil {
		c.log.fault("reading the cluster", topicReconcile, errorText(err))
		return
	}
	c.log.clear("reading the cluster", topicReconcile)
	live := map[string]bool{}
	for index := range w.redis {
		r := &w.redis[index]
		live[r.Metadata.UID] = true
		subject := "redis " + objectKey(r.Metadata)
		if err := c.reconcile(ctx, r, w); err != nil {
			c.readings.reconcileErrors.Inc()
			c.log.fault(subject, topicReconcile, errorText(err))
			continue
		}
		c.log.clear(subject, topicReconcile)
	}
	for uid := range c.states {
		if !live[uid] {
			delete(c.states, uid)
		}
	}
}

func (c *controller) state(r *Redis) *redisState {
	st, ok := c.states[r.Metadata.UID]
	if !ok {
		st = &redisState{lastSeen: map[string]time.Time{}, reports: map[string]instanceReport{}, applied: map[string]string{}}
		c.states[r.Metadata.UID] = st
	}
	return st
}

// ensureChildren applies each child whose body changed since the
// operator last applied it, and creates the Secret once.
func (c *controller) ensureChildren(ctx context.Context, r *Redis, st *redisState) error {
	if !st.hasSecret {
		var existing map[string]any
		err := c.client.get(ctx, secretsResource, r.Metadata.Namespace, secretName(r.Metadata.Name), &existing)
		switch {
		case err == nil:
		case errors.Is(err, errNotFound):
			if err := c.client.create(ctx, secretsResource, r.Metadata.Namespace, secret(r, newPassword()), nil); err != nil && !errors.Is(err, errConflict) {
				return fmt.Errorf("creating secret %s: %w", secretName(r.Metadata.Name), err)
			}
			c.log.printf("redis %s: created secret %s", objectKey(r.Metadata), secretName(r.Metadata.Name))
		default:
			return fmt.Errorf("reading secret %s: %w", secretName(r.Metadata.Name), err)
		}
		st.hasSecret = true
	}
	for _, each := range children(r, c.operatorImage) {
		body, err := json.Marshal(each.body)
		if err != nil {
			return err
		}
		key := each.resource.String() + "/" + each.name
		if st.applied[key] == string(body) {
			continue
		}
		if err := c.client.apply(ctx, each.resource, r.Metadata.Namespace, each.name, each.body, nil); err != nil {
			return fmt.Errorf("applying %s %s: %w", each.resource, each.name, err)
		}
		st.applied[key] = string(body)
	}
	return nil
}

func newPassword() string {
	bytes := make([]byte, 24)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// poll reads every instance manager's /status at once.
func (c *controller) poll(ctx context.Context, st *redisState, pods []Pod) {
	now := c.now()
	if st.firstPoll.IsZero() {
		st.firstPoll = now
	}
	type answer struct {
		name   string
		report instanceReport
		err    error
	}
	answers := make(chan answer, len(pods))
	asked := 0
	for _, pod := range pods {
		if pod.Status.PodIP == "" || pod.Metadata.DeletionTimestamp != "" && pod.Status.Phase != "Running" {
			continue
		}
		asked++
		go func() {
			address := fmt.Sprintf("%s:%d", pod.Status.PodIP, c.statusPort)
			report, err := readInstanceReport(ctx, c.poller, address)
			answers <- answer{pod.Metadata.Name, report, err}
		}()
	}
	for range asked {
		a := <-answers
		if a.err != nil {
			continue
		}
		st.lastSeen[a.name] = now
		st.reports[a.name] = a.report
	}
}

// silentFor is how long the manager of name has not answered.
func (st *redisState) silentFor(name string, now time.Time) time.Duration {
	seen, ok := st.lastSeen[name]
	if !ok {
		seen = st.firstPoll
	}
	return now.Sub(seen)
}

// fresh returns the report of name if its manager answered the latest
// poll.
func (st *redisState) fresh(name string, now time.Time, within time.Duration) (instanceReport, bool) {
	if st.silentFor(name, now) > within {
		return instanceReport{}, false
	}
	report, ok := st.reports[name]
	return report, ok
}
