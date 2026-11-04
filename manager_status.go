package main

// The instance manager's report. Each step writes what the manager
// says about its instance, and the status endpoints serve it: /status
// to the operator and to the peers, and /ready and /healthz to the
// kubelet's probes.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

// instanceReport is what an instance manager says about itself at
// /status, for the operator and for its peers.
type instanceReport struct {
	Name       string `json:"name"`
	Role       string `json:"role"`
	HoldsLease bool   `json:"holdsLease"`
	Target     string `json:"target"`
	Offset     int64  `json:"offset"`
	Replid     string `json:"replid"`
	LinkUp     bool   `json:"linkUp"`
	Following  string `json:"following,omitempty"`
	HasData    bool   `json:"hasData"`
	Ready      bool   `json:"ready"`
	Stopping   bool   `json:"stopping,omitempty"`
	// SyncedReplicas is how many replicas a primary streams to.
	SyncedReplicas int `json:"syncedReplicas,omitempty"`
	// MinReplicasToWrite is the min-replicas-to-write a primary's server
	// runs with. A replica's report leaves it out.
	MinReplicasToWrite *int   `json:"minReplicasToWrite,omitempty"`
	Error              string `json:"error,omitempty"`
}

// The roles a report names.
const (
	reportPrimary  = "primary"
	reportReplica  = "replica"
	reportDetached = "detached"
	reportStarting = "starting"
)

func (m *manager) reportFrom(info replicationInfo, err error) instanceReport {
	report := instanceReport{
		Name:       m.config.podName,
		HoldsLease: m.lease.held(),
		Target:     m.target,
		Offset:     info.offset,
		Replid:     info.replid,
		HasData:    m.hasData,
		Stopping:   m.stopping,
	}
	switch {
	case info.role == "master":
		report.Role = reportPrimary
		report.Ready = report.HoldsLease
		report.SyncedReplicas = info.onlineReplicas
	case info.masterHost == detachedHost:
		report.Role = reportDetached
	default:
		report.Role = reportReplica
		report.LinkUp = info.linkUp
		report.Following = info.masterHost
		report.Ready = info.linkUp
	}
	if err != nil {
		report.Error = errorText(err)
	}
	return report
}

// setReport keeps the report of the step that started at takenAt.
func (m *manager) setReport(report instanceReport, takenAt time.Time) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.report, m.reportTakenAt = report, takenAt
}

func (m *manager) currentReport() instanceReport {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.report
}

// serveStatus serves /status, /ready, and /healthz until ctx ends.
func (m *manager) serveStatus(ctx context.Context) (net.Addr, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(m.config.podIP, strconv.Itoa(m.config.statusPort)))
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	// The operator and the peers take a refused /status as a manager
	// that did not answer, which is what a paused one is until it steps.
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		report, live := m.liveReport(time.Now())
		if !live {
			http.Error(w, "paused since the last step", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(report)
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if report, live := m.liveReport(time.Now()); !live || !report.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if m.currentReport().Role == reportStarting && m.currentReport().Error != "" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	context.AfterFunc(ctx, func() { _ = server.Close() })
	go func() { _ = server.Serve(listener) }()
	return listener.Addr(), nil
}

// readInstanceReport reads one instance manager's /status.
func readInstanceReport(ctx context.Context, client *http.Client, address string) (instanceReport, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/status", nil)
	if err != nil {
		return instanceReport{}, err
	}
	resp, err := client.Do(request)
	if err != nil {
		return instanceReport{}, err
	}
	defer drain(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return instanceReport{}, fmt.Errorf("GET %s/status: %s", address, resp.Status)
	}
	var report instanceReport
	return report, json.NewDecoder(resp.Body).Decode(&report)
}
