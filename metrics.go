package main

// The operator's metrics are on a private registry, so a scrape reads
// only what this file registers. Every series is set when the fact it
// reports happens, and a scrape reads memory only.

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsPrefix = "redis_operator_"

// The reasons a failover or switchover starts, by the label that
// counts them.
const (
	reasonPrimaryUnreachable = "PrimaryUnreachable"
	reasonNodeCordoned       = "NodeCordoned"
	reasonPodDeleted         = "PodDeleted"
	reasonRollout            = "Rollout"
	reasonScaleDown          = "ScaleDown"
)

// The resources the operator watches, by the name that labels their
// restarts.
var watchedResources = []string{"redis", "pods", "leases", "nodes", "statefulsets", "backups", "scheduledbackups"}

type metrics struct {
	registry *prometheus.Registry

	reconcileErrors prometheus.Counter
	watchRestarts   *prometheus.CounterVec
	failovers       prometheus.Counter
	switchovers     *prometheus.CounterVec
	leader          prometheus.Gauge
	replicaLag      *prometheus.GaugeVec
	polls           *prometheus.CounterVec
	pollDuration    prometheus.Histogram
	passes          *prometheus.CounterVec
	passDuration    prometheus.Histogram
	statusWrites    *prometheus.CounterVec
	apiRequests     *prometheus.CounterVec
}

func newMetrics(version string) *metrics {
	m := &metrics{
		registry: prometheus.NewRegistry(),
		reconcileErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: metricsPrefix + "reconcile_errors_total",
			Help: "Reconciles of a Redis that failed.",
		}),
		watchRestarts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricsPrefix + "watch_restarts_total",
			Help: "Watches that ended and opened again, by resource.",
		}, []string{"resource"}),
		failovers: prometheus.NewCounter(prometheus.CounterOpts{
			Name: metricsPrefix + "failovers_total",
			Help: "Failovers started, counted once the start is written to the Redis's status.",
		}),
		switchovers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricsPrefix + "switchovers_total",
			Help: "Planned switchovers started, by reason, counted once the start is written to the Redis's status.",
		}, []string{"reason"}),
		replicaLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricsPrefix + "replica_lag_bytes",
			Help: "How many bytes the holder of a Redis in replica mode is behind its source, as the operator last measured it.",
		}, []string{"namespace", "redis"}),
		polls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricsPrefix + "polls_total",
			Help: "Polls of a Redis's instance managers, by outcome: informative when a manager answered, unanswered when none did, canceled when the operator stopped first.",
		}, []string{"outcome"}),
		pollDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    metricsPrefix + "poll_duration_seconds",
			Help:    "How long a poll of a Redis's instance managers took, from the first request to the last answer.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2},
		}),
		passes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricsPrefix + "passes_total",
			Help: "Reconcile passes, by what woke them: start, event, poll, or backstop.",
		}, []string{"cause"}),
		passDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    metricsPrefix + "reconcile_duration_seconds",
			Help:    "How long a reconcile pass over every Redis and backup took.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),
		statusWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricsPrefix + "status_writes_total",
			Help: "Status writes that landed, by resource.",
		}, []string{"resource"}),
		apiRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricsPrefix + "api_requests_total",
			Help: "Requests to the Kubernetes API server, by verb, resource, and the code it answered, or error for no answer.",
		}, []string{"verb", "resource", "code"}),
		leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: metricsPrefix + "leader",
			Help: "1 while this replica holds the Lease and does the work, and 0 while it waits as a standby.",
		}),
	}

	// build_info always holds 1, and its label is the release. This is
	// the Prometheus convention for a string value.
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: metricsPrefix + "build_info",
		Help: "The release this operator runs, as a label on a gauge that is always 1.",
	}, []string{"version"})
	buildInfo.WithLabelValues(version).Set(1)

	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		buildInfo,
		m.reconcileErrors, m.watchRestarts, m.failovers, m.switchovers, m.leader, m.replicaLag, m.polls, m.pollDuration, m.passes, m.passDuration, m.statusWrites, m.apiRequests,
	)

	// Every series starts at zero on the first scrape. Prometheus
	// computes a rate from two readings, and a counter that first
	// appears at 1 has no earlier reading.
	for _, resource := range watchedResources {
		m.watchRestarts.WithLabelValues(resource)
	}
	for _, cause := range []string{passStart, passEvent, passPoll, passBackstop} {
		m.passes.WithLabelValues(cause)
	}
	for _, resource := range []string{redisResource.Resource, backupsResource.Resource, scheduledBackupsResource.Resource} {
		m.statusWrites.WithLabelValues(resource)
	}
	for _, outcome := range []string{pollInformative, pollUnanswered, pollCanceled} {
		m.polls.WithLabelValues(outcome)
	}
	for _, reason := range []string{reasonNodeCordoned, reasonPodDeleted, reasonRollout, reasonScaleDown} {
		m.switchovers.WithLabelValues(reason)
	}
	return m
}

func (m *metrics) watchRestarted(resource string) func() {
	return m.watchRestarts.WithLabelValues(resource).Inc
}

// The causes of a pass, by the label that counts them.
const (
	passStart    = "start"
	passEvent    = "event"
	passPoll     = "poll"
	passBackstop = "backstop"
)

func (m *metrics) observeRequest(verb, resource, code string) {
	m.apiRequests.WithLabelValues(verb, resource, code).Inc()
}

func (m *metrics) wroteStatus(resource kubeResource) {
	m.statusWrites.WithLabelValues(resource.Resource).Inc()
}

func (m *metrics) observePoll(outcome string, took time.Duration) {
	m.polls.WithLabelValues(outcome).Inc()
	m.pollDuration.Observe(took.Seconds())
}

func (m *metrics) setLeader(leading bool) {
	if leading {
		m.leader.Set(1)
	} else {
		m.leader.Set(0)
	}
}

func (m *metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A collector that fails breaks the scrape, so Prometheus does
		// not store a document with a gap in it.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

// serve listens on address and serves /metrics until ctx ends. It
// listens before it returns, so a port that is in use is an error for
// the caller, and it returns the address it listens on.
func (m *metrics) serve(ctx context.Context, address string) (net.Addr, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.handler())
	// A client that connects and sends no headers holds a goroutine
	// until this timeout ends it.
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	context.AfterFunc(ctx, func() { _ = server.Close() })
	go func() { _ = server.Serve(listener) }()
	return listener.Addr(), nil
}
