package main

import (
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// scrape reads the metrics the way Prometheus does, over HTTP, and
// returns the sample lines without the comments.
func scrape(t *testing.T, m *metrics) []string {
	t.Helper()
	address, err := m.serve(t.Context(), "127.0.0.1:0")
	mustSucceed(t, err)
	resp, err := http.Get("http://" + address.String() + "/metrics")
	mustSucceed(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	mustSucceed(t, err)
	var samples []string
	for line := range strings.Lines(string(body)) {
		if !strings.HasPrefix(line, "#") {
			samples = append(samples, strings.TrimSpace(line))
		}
	}
	return samples
}

func TestMetricsStartEverySeriesAtZero(t *testing.T) {
	t.Parallel()
	samples := scrape(t, newMetrics("dev"))

	for _, want := range []string{
		`redis_operator_build_info{version="dev"} 1`,
		"redis_operator_failovers_total 0",
		`redis_operator_switchovers_total{reason="NodeCordoned"} 0`,
		`redis_operator_watch_restarts_total{resource="redis"} 0`,
		"redis_operator_leader 0",
		`redis_operator_polls_total{outcome="unanswered"} 0`,
		`redis_operator_passes_total{cause="backstop"} 0`,
		`redis_operator_status_writes_total{resource="redis"} 0`,
	} {
		mustMatch(t, slices.Contains(samples, want), true)
	}
}
