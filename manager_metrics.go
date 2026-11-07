package main

// The instance manager's metrics, on its status port at /metrics: its
// own process's CPU and memory, apart from the server's in the same
// container, and its requests to the API server by verb, resource, and
// code. They are on a private registry, so a scrape reads only these.

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type managerMetrics struct {
	registry    *prometheus.Registry
	apiRequests *prometheus.CounterVec
}

func newManagerMetrics() *managerMetrics {
	m := &managerMetrics{
		registry: prometheus.NewRegistry(),
		apiRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricsPrefix + "manager_api_requests_total",
			Help: "The instance manager's requests to the Kubernetes API server, by verb, resource, and the code it answered, or error for no answer.",
		}, []string{"verb", "resource", "code"}),
	}
	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.apiRequests,
	)
	return m
}

func (m *managerMetrics) observeRequest(verb, resource, code string) {
	m.apiRequests.WithLabelValues(verb, resource, code).Inc()
}

func (m *managerMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError})
}
