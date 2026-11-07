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
		m.reconcileErrors, m.watchRestarts, m.failovers, m.switchovers, m.leader,
	)

	// Every series starts at zero on the first scrape. Prometheus
	// computes a rate from two readings, and a counter that first
	// appears at 1 has no earlier reading.
	for _, resource := range watchedResources {
		m.watchRestarts.WithLabelValues(resource)
	}
	for _, reason := range []string{reasonNodeCordoned, reasonPodDeleted, reasonRollout, reasonScaleDown} {
		m.switchovers.WithLabelValues(reason)
	}
	return m
}

func (m *metrics) watchRestarted(resource string) func() {
	return m.watchRestarts.WithLabelValues(resource).Inc
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
