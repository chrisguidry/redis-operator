package main

// A manager whose process stops, as SIGSTOP or a frozen cgroup stops
// it, answers HTTP again the moment it resumes, before its step loop
// has read anything. Its last report then describes the world from
// before the pause: it may say the instance holds the Lease and is a
// ready primary long after the Lease expired and another instance took
// it. The status endpoints serve no report until a step that started
// after the pause has replaced it.
//
// The age of the report alone cannot tell a pause from a slow step. A
// step waits on the API server, on its peers, and on a FAILOVER, and a
// primary keeps serving while the API server is down, so a slow step
// must not make it unready. The watch below runs beside the step loop
// and beats several times per pauseGap. A process that ran sees its
// beats close together whatever its steps do, and a process that
// stopped finds a gap when it resumes.

import (
	"context"
	"time"
)

// pauseGap is the longest the manager's watch may go without a beat
// before the manager counts it as paused.
const pauseGap = 2 * time.Second

// watchForPauses beats until ctx ends.
func (m *manager) watchForPauses(ctx context.Context) {
	m.beat(time.Now())
	ticker := time.NewTicker(m.config.pauseGap / 4)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				m.beat(now)
			}
		}
	}()
}

// beat records that the manager's process ran at now, and a pause when
// the last beat is more than pauseGap before it.
func (m *manager) beat(now time.Time) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if !m.lastBeat.IsZero() && now.Sub(m.lastBeat) > m.config.pauseGap {
		m.resumedAt = now
	}
	m.lastBeat = now
}

// liveReport returns the report and whether it describes the server
// now: no pause began after its step started. A request that arrives
// after a pause and before the watch's next beat finds the last beat
// too old.
func (m *manager) liveReport(now time.Time) (instanceReport, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	live := now.Sub(m.lastBeat) <= m.config.pauseGap && !m.reportTakenAt.Before(m.resumedAt)
	return m.report, live
}
