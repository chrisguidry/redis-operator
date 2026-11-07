package main

import (
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// The status port serves the manager's own process metrics and its
// count of API requests.
func TestTheStatusPortServesTheManagersMetrics(t *testing.T) {
	t.Parallel()
	h := startManagerHarness(t, "redis", 1)
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })
	h.instances["cache-0"].manager.readings.observeRequest("get", "redis", "200")

	resp, err := http.Get("http://" + net.JoinHostPort(h.ip("cache-0"), strconv.Itoa(h.statusAt)) + "/metrics")
	mustSucceed(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	mustSucceed(t, err)

	mustMatch(t, strings.Contains(string(body), "process_resident_memory_bytes"), true)
	mustMatch(t, strings.Contains(string(body), `redis_operator_manager_api_requests_total{code="200",resource="redis",verb="get"} 1`), true)
}
