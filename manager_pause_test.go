package main

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// statusCode is the status that the manager's endpoint at path answers.
func statusCode(t *testing.T, m *manager, path string) int {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	address, err := m.serveStatus(ctx)
	mustSucceed(t, err)
	resp, err := http.Get(fmt.Sprintf("http://%s%s", address, path))
	mustSucceed(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// A manager that SIGSTOP froze answers its probes as soon as SIGCONT
// resumes it, before its next step, so its report from before the pause
// would say it is ready while its Lease and its server's role may have
// moved on. Each case beats the manager's watch at offsets before now
// and takes a ready report at stepAt before now.
func TestAPausedManagerReportsNothingUntilItStepsAgain(t *testing.T) {
	cases := []struct {
		name   string
		beats  []time.Duration
		stepAt time.Duration
		want   int
	}{
		{"running", []time.Duration{200 * time.Millisecond, 0}, 200 * time.Millisecond, http.StatusOK},
		{"resumed, before its next step", []time.Duration{30 * time.Second, 0}, 30 * time.Second, http.StatusServiceUnavailable},
		{"resumed, after its next step", []time.Duration{30 * time.Second, 10 * time.Millisecond}, 0, http.StatusOK},
		{"resumed, before its watch ran", []time.Duration{30 * time.Second}, 30 * time.Second, http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		for _, path := range []string{"/ready", "/status"} {
			t.Run(c.name+" "+path, func(t *testing.T) {
				m := newManager(managerConfig{
					podName: "cache-0", podIP: "127.0.0.2", statusPort: freePort(t), pauseGap: time.Second,
				}, nil, &syncBuffer{})
				now := time.Now()
				for _, ago := range c.beats {
					m.beat(now.Add(-ago))
				}
				m.setReport(instanceReport{Name: "cache-0", Role: reportPrimary, HoldsLease: true, Ready: true}, now.Add(-c.stepAt))

				mustMatch(t, statusCode(t, m, path), c.want)
			})
		}
	}
}
