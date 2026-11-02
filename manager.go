package main

// The instance manager runs in every pod, in front of the server. It
// reads the operator's decision, status.targetPrimary on the Redis, and
// the primary Lease, and makes its server's role agree with them once a
// step:
//
//   - The holder of the Lease renews it and runs its server as primary
//     while it is the target. When the target moves to another instance
//     it hands the role over with FAILOVER and releases the Lease. When
//     the target is pending, the primary failed in the operator's eyes,
//     and it steps down.
//   - The target takes the Lease when it may (manager_lease.go), and
//     only if its server holds data, or no primary ever ran, so an
//     instance that restarted empty never serves an empty dataset as
//     primary.
//   - Every other instance follows the Lease holder, or detaches when
//     there is none or a failover is pending.
//
// A holder that cannot renew the Lease for the renew deadline asks its
// peers. If one cannot be reached or names another target, the holder
// detaches its server and closes its clients. If every peer agrees with
// it, the API server alone is down, and it keeps serving.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// The instance manager's timings. They are fields of managerConfig so a
// test can shorten them.
const (
	primaryLeaseDuration = 15 * time.Second
	primaryRenewDeadline = 10 * time.Second
	primaryRetryPeriod   = 2 * time.Second
	managerStepPeriod    = time.Second
	switchoverTimeout    = 10 * time.Second
	peerTimeout          = time.Second
	// handoverWait is how long a primary that is being deleted waits
	// for the operator to name the next primary before it stops anyway.
	handoverWait = 20 * time.Second
	// demotionHold is the longest a demoted primary holds its clients'
	// writes waiting for its pod to lose the primary label, and
	// routeSettle how long kube-proxy may take to stop routing to it
	// after that (manager_demote.go).
	demotionHold = 30 * time.Second
	routeSettle  = 5 * time.Second
	statusPort   = 9123
	serverPort   = 6379
)

var podsResource = kubeResource{"", "v1", "pods"}

type managerConfig struct {
	namespace  string
	redisName  string
	podName    string
	podIP      string
	port       int
	statusPort int
	// loopback is whether the server also listens on 127.0.0.1. Tests
	// leave it off, because their instances share one port on one host.
	loopback   bool
	dataDir    string
	configPath string
	password   string
	// backupDir and restoreDir are where the backup volume and the
	// volume a new Redis restores from are mounted.
	backupDir  string
	restoreDir string
	// tlsSecret holds the server's certificate, or is empty without TLS.
	// The manager writes its files to tlsDir and serves TLS on tlsPort.
	tlsSecret string
	tlsDir    string
	tlsPort   int

	leaseDuration   time.Duration
	renewDeadline   time.Duration
	retryPeriod     time.Duration
	stepPeriod      time.Duration
	failoverTimeout time.Duration
	handoverWait    time.Duration
	demotionHold    time.Duration
	routeSettle     time.Duration
}

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
	SyncedReplicas int    `json:"syncedReplicas,omitempty"`
	Error          string `json:"error,omitempty"`
}

// The roles a report names.
const (
	reportPrimary  = "primary"
	reportReplica  = "replica"
	reportDetached = "detached"
	reportStarting = "starting"
)

type manager struct {
	config  managerConfig
	client  *kubeClient
	server  *server
	lease   *primaryLease
	log     io.Writer
	peers   *http.Client
	persist bool

	// What the last step read from the API server. switchingOver is
	// whether the operator moved the target with a switchover, and not
	// a failover.
	target         string
	switchingOver  bool
	everHadPrimary bool
	minReplicas    int
	instances      int
	podIPs         map[string]string

	hasData       bool
	lastRenew     time.Time
	stopping      bool
	stopStarted   time.Time
	askedHandover bool
	lastLine      string
	// labeledPrimary is whether the last read of this pod found the
	// primary role label, and is true when the read failed. leaving is a
	// step-down that waits for routing to move (manager_demote.go).
	labeledPrimary bool
	leaving        *leaving

	// backupBusy is true while a snapshot runs in the background.
	backupBusy atomic.Bool
	// replicaMode is the decision that the holder follows replicaSource,
	// and masterauth the password the server replicates with now.
	replicaMode   bool
	replicaSource ReplicaSource
	masterauth    string

	// enableAOF is true when the server started from a restored snapshot
	// with its append-only file off, and must turn it on once it runs.
	enableAOF bool

	// The users and the TLS of the server (manager_users.go,
	// manager_tls.go), and the faults note last wrote, by topic.
	userState
	tlsState
	notes map[string]string

	mutex  sync.Mutex
	report instanceReport
}

func newManager(config managerConfig, client *kubeClient, log io.Writer) *manager {
	return &manager{
		config: config,
		client: client,
		server: &server{addr: net.JoinHostPort(config.podIP, strconv.Itoa(config.port)), password: config.password},
		lease: &primaryLease{
			client: client, namespace: config.namespace, name: primaryLeaseName(config.redisName),
			identity: config.podName, duration: config.leaseDuration,
		},
		log:    log,
		peers:  &http.Client{Timeout: peerTimeout},
		podIPs: map[string]string{},
		report: instanceReport{Name: config.podName, Role: reportStarting},
	}
}

// run starts the server and keeps its role in step until stop ends and
// the server has stopped. A primary that stop ends first hands its role
// over, so the server stops as a replica.
func (m *manager) run(stop context.Context) error {
	work, cancel := context.WithCancel(context.Background())
	defer cancel()

	redis, err := m.readRedisUntilAnswered(stop)
	if err != nil {
		return err
	}
	settings, err := m.settings(redis)
	if err != nil {
		return err
	}
	if settings.tls, err = m.startTLS(stop, redis); err != nil {
		return err
	}
	m.persist = settings.persistent
	m.masterauth = m.config.password
	m.lease.owner = OwnerReference{APIVersion: apiVersion, Kind: redisKind, Name: redis.Metadata.Name, UID: redis.Metadata.UID}
	m.hasData = m.persist && dataOnDisk(m.config.dataDir)
	if err := m.releaseLeaseIfEmpty(work); err != nil {
		return err
	}
	restored, err := m.restoreIfNeeded(work, redis)
	if err != nil {
		return err
	}
	if restored {
		m.hasData = true
		// A server with its append-only file on and no such file starts
		// empty and ignores the snapshot, so it starts with the file off
		// and turns it on once the snapshot is loaded.
		settings.restoredSnapshot = true
		m.enableAOF = settings.persistent
	}

	cmd, err := startServer(settings)
	if err != nil {
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	defer m.server.close()

	address, err := m.serveStatus(work)
	if err != nil {
		_ = cmd.Process.Kill()
		return err
	}
	m.say(fmt.Sprintf("%s: started %s, status on %s", m.config.podName, serverBinary(redis.Spec.engine()), address))

	ticker := time.NewTicker(m.config.stepPeriod)
	defer ticker.Stop()
	for {
		select {
		case err := <-exited:
			if m.stopping {
				return nil
			}
			return fmt.Errorf("%s exited: %v", serverBinary(redis.Spec.engine()), err)
		case <-stop.Done():
			if !m.stopping {
				m.stopping, m.stopStarted = true, time.Now()
				m.say(m.config.podName + ": stopping")
			}
		case <-ticker.C:
		}
		m.step(work)
		if m.stopping && m.readyToStop() {
			m.server.shutdown(work, m.persist)
			select {
			case <-exited:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-exited
			}
			return nil
		}
	}
}

// readyToStop says whether a stopping manager may stop its server: it
// holds no Lease, so it is no primary, it is the only instance, so no
// other can take the role, or it waited as long as it may for the
// operator to name the next one.
func (m *manager) readyToStop() bool {
	return !m.lease.held() || m.instances == 1 || time.Since(m.stopStarted) > m.config.handoverWait
}

func (m *manager) readRedisUntilAnswered(ctx context.Context) (Redis, error) {
	for {
		var redis Redis
		err := m.client.get(ctx, redisResource, m.config.namespace, m.config.redisName, &redis)
		if err == nil {
			return redis, nil
		}
		m.say(fmt.Sprintf("%s: cannot read redis %s/%s: %s", m.config.podName, m.config.namespace, m.config.redisName, errorText(err)))
		select {
		case <-ctx.Done():
			return Redis{}, ctx.Err()
		case <-time.After(m.config.stepPeriod):
		}
	}
}

func (m *manager) settings(redis Redis) (serverSettings, error) {
	maxmemory, err := redis.Spec.maxmemoryBytes()
	if err != nil {
		return serverSettings{}, err
	}
	return serverSettings{
		engine:             redis.Spec.engine(),
		bindIP:             m.config.podIP,
		loopback:           m.config.loopback,
		port:               m.config.port,
		dataDir:            m.config.dataDir,
		configPath:         m.config.configPath,
		password:           m.config.password,
		maxmemory:          maxmemory,
		maxmemoryPolicy:    redis.Spec.maxmemoryPolicy(),
		persistent:         redis.Spec.persistent(),
		minReplicasToWrite: redis.Spec.minReplicasToWrite(),
	}, nil
}

// dataOnDisk says whether a persistent server will load data when it
// starts: an append-only directory or a snapshot is in its data
// directory.
func dataOnDisk(dir string) bool {
	for _, name := range []string{"appendonlydir", "dump.rdb"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return true
		}
	}
	return false
}

// releaseLeaseIfEmpty releases a Lease that this pod still holds from
// before its container restarted, when the server will start with no
// data. Serving as primary then would hand every replica an empty
// dataset, so the operator fails over to a replica instead.
func (m *manager) releaseLeaseIfEmpty(ctx context.Context) error {
	if m.hasData {
		return nil
	}
	now := time.Now()
	if err := m.lease.read(ctx, now); err != nil {
		return fmt.Errorf("reading the primary lease: %w", err)
	}
	if !m.lease.held() {
		return nil
	}
	m.say(m.config.podName + ": restarted with no data while holding the primary lease; releasing it")
	return m.lease.release(ctx, now)
}

// step reads the decision and the Lease, and makes the server's role
// agree with them.
func (m *manager) step(ctx context.Context) {
	now := time.Now()
	apiErr := m.readDecision(ctx, now)
	info, err := m.server.replication(ctx)
	if err != nil {
		m.setReport(instanceReport{Name: m.config.podName, Role: reportStarting, Error: errorText(err)})
		return
	}
	if info.role == "slave" && info.linkUp && info.masterHost != detachedHost {
		m.hasData = true
	}
	if m.enableAOF {
		if _, err := m.server.do(ctx, "CONFIG", "SET", "appendonly", "yes"); err == nil {
			m.enableAOF = false
		}
	}
	if apiErr == nil {
		m.keepAccess(ctx)
	}
	// A server that has no data yet, such as a replica before its first
	// sync, would upload an empty snapshot as a completed backup.
	if apiErr == nil && m.hasData {
		m.startBackups(ctx)
	}

	var actErr error
	if m.lease.held() {
		actErr = m.stepAsHolder(ctx, now, apiErr, info)
	} else if apiErr == nil {
		actErr = m.stepAsOther(ctx, now, info)
	}
	if actErr != nil {
		m.say(fmt.Sprintf("%s: %s", m.config.podName, errorText(actErr)))
	}

	// The report reads the server again, so it states the role the step
	// left behind.
	if after, err := m.server.replication(ctx); err == nil {
		info = after
	}
	m.setReport(m.reportFrom(info, actErr))
}

func (m *manager) readDecision(ctx context.Context, now time.Time) error {
	// Until a read of this pod says otherwise, it may still carry the
	// primary label.
	m.labeledPrimary = true
	var redis Redis
	if err := m.client.get(ctx, redisResource, m.config.namespace, m.config.redisName, &redis); err != nil {
		return err
	}
	m.target = redis.Status.TargetPrimary
	m.switchingOver = redis.Status.Switchover != nil
	m.everHadPrimary = redis.Status.CurrentPrimary != ""
	m.minReplicas = redis.minReplicasToWrite()
	m.instances = redis.Spec.instances()
	m.replicaMode = redis.Status.replicating() && redis.Spec.Replica != nil
	if redis.Spec.Replica != nil {
		m.replicaSource = redis.Spec.Replica.Source
	}
	if err := m.lease.read(ctx, now); err != nil {
		return err
	}
	var pods struct {
		Items []Pod `json:"items"`
	}
	if err := m.client.list(ctx, podsResource, m.config.namespace, clusterLabel+"="+m.config.redisName, &pods); err != nil {
		return err
	}
	ips := map[string]string{}
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" {
			ips[pod.Metadata.Name] = pod.Status.PodIP
		}
		if pod.Metadata.Name == m.config.podName {
			m.labeledPrimary = pod.Metadata.Labels[roleLabel] == rolePrimary
		}
	}
	m.podIPs = ips
	m.readAccessDecision(redis, pods.Items)
	return nil
}

// stepAsHolder runs the primary, or hands the role over.
func (m *manager) stepAsHolder(ctx context.Context, now time.Time, apiErr error, info replicationInfo) error {
	if apiErr == nil && now.Sub(m.lastRenew) >= m.config.retryPeriod {
		if err := m.lease.renew(ctx, now); err == nil {
			m.lastRenew = now
		} else {
			apiErr = err
		}
	}
	if m.lastRenew.IsZero() {
		m.lastRenew = now
	}
	if now.Sub(m.lastRenew) > m.config.renewDeadline {
		return m.isolationCheck(ctx, info)
	}
	if apiErr != nil {
		// The decision cannot be read, and the Lease is recent, so the
		// primary keeps serving as it is.
		return nil
	}
	if m.stopping && m.target == m.config.podName && !m.askedHandover {
		m.askedHandover = true
		if err := m.askForHandover(ctx); err != nil {
			return err
		}
	}
	switch m.target {
	case m.config.podName:
		if m.replicaMode {
			return m.followSource(ctx, info)
		}
		if err := m.resumeWrites(ctx); err != nil {
			return err
		}
		if err := m.setMasterauth(ctx, m.config.password); err != nil {
			return err
		}
		if err := m.server.promote(ctx, info); err != nil {
			return err
		}
		m.line(m.config.podName + ": primary")
		return m.server.setMinReplicasToWrite(ctx, m.minReplicas)
	case targetPending, "":
		m.line(m.config.podName + ": stepping down, a failover is pending")
		return m.stepDownForFailover(ctx, now, info)
	default:
		if !m.switchingOver && !m.replicaMode {
			// A failover named the next primary while this instance, which
			// the API server still reaches, held the Lease. Its replicas
			// detached for the failover, and may not reach it at all, so a
			// FAILOVER to the next primary could never finish.
			m.line(fmt.Sprintf("%s: stepping down, a failover named %s", m.config.podName, m.target))
			return m.stepDownForFailover(ctx, now, info)
		}
		return m.handOver(ctx, now, info)
	}
}

// stepDownForFailover detaches the server once routing has moved off
// this pod, then releases the Lease.
func (m *manager) stepDownForFailover(ctx context.Context, now time.Time, info replicationInfo) error {
	detached, err := m.stepDown(ctx, now, info, func() error { return m.server.detach(ctx, info, m.config.port) })
	if !detached {
		return err
	}
	return m.lease.release(ctx, now)
}

// handOver moves the primary role to the target with FAILOVER, once
// routing has moved off this pod, then releases the Lease. An aborted
// FAILOVER leaves this instance primary, and the operator gives the
// switchover up.
func (m *manager) handOver(ctx context.Context, now time.Time, info replicationInfo) error {
	if m.replicaMode {
		// The holder is a replica of the source, so there is no primary
		// role to hand over: the next holder follows the source itself.
		m.say(fmt.Sprintf("%s: giving the lease to %s, which follows the source in its place", m.config.podName, m.target))
		return m.lease.release(ctx, now)
	}
	ip := m.podIPs[m.target]
	if ip == "" {
		return fmt.Errorf("cannot hand the primary role to %s: its pod has no IP", m.target)
	}
	handed, err := m.stepDown(ctx, now, info, func() error {
		port, err := m.peerPort(ctx, m.target)
		if err != nil {
			return err
		}
		m.say(fmt.Sprintf("%s: handing the primary role to %s", m.config.podName, m.target))
		return m.server.failoverTo(ctx, ip, port, m.config.failoverTimeout)
	})
	if err != nil {
		return fmt.Errorf("handing the primary role to %s: %w", m.target, err)
	}
	if !handed {
		return nil
	}
	return m.lease.release(ctx, time.Now())
}

// askForHandover marks this pod, so the operator switches over before
// the pod stops.
func (m *manager) askForHandover(ctx context.Context) error {
	m.say(m.config.podName + ": asking the operator to move the primary role before this pod stops")
	return m.client.mergePatch(ctx, podsResource, m.config.namespace, m.config.podName, map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{switchoverAnnotation: time.Now().UTC().Format(time.RFC3339)}},
	}, nil)
}

// isolationCheck runs when the holder has not renewed its Lease for the
// renew deadline.
func (m *manager) isolationCheck(ctx context.Context, info replicationInfo) error {
	if m.peersAgree(ctx) {
		m.line(m.config.podName + ": cannot renew the primary lease, but every peer agrees this instance is primary; serving")
		return m.resumeWrites(ctx)
	}
	m.line(m.config.podName + ": cannot renew the primary lease and a peer disagrees or is unreachable; fencing")
	return m.demote(ctx, info, func() error { return m.server.detach(ctx, info, m.config.port) })
}

// peersAgree asks every other instance's manager which instance it
// takes for the target.
func (m *manager) peersAgree(ctx context.Context) bool {
	for name, ip := range m.podIPs {
		if name == m.config.podName {
			continue
		}
		report, err := m.readPeer(ctx, ip)
		if err != nil || report.Target != m.config.podName {
			return false
		}
	}
	return true
}

func (m *manager) readPeer(ctx context.Context, ip string) (instanceReport, error) {
	return readInstanceReport(ctx, m.peers, net.JoinHostPort(ip, strconv.Itoa(m.config.statusPort)))
}

// stepAsOther takes the Lease when this instance is the target and may,
// and follows the holder otherwise.
func (m *manager) stepAsOther(ctx context.Context, now time.Time, info replicationInfo) error {
	holder := m.lease.holder()
	fit := m.hasData || !m.everHadPrimary
	switch {
	case m.target == m.config.podName && fit && !m.stopping && m.lease.takeable(now):
		if err := m.lease.take(ctx, now); err != nil {
			if errors.Is(err, errConflict) {
				return nil
			}
			return err
		}
		m.lastRenew = now
		m.hasData = true
		m.say(m.config.podName + ": took the primary lease")
		if err := m.resumeWrites(ctx); err != nil {
			return err
		}
		if m.replicaMode {
			return m.followSource(ctx, info)
		}
		if err := m.server.promote(ctx, info); err != nil {
			return err
		}
		return m.server.setMinReplicasToWrite(ctx, m.minReplicas)
	case m.target == m.config.podName && info.role == "master":
		// FAILOVER made this server the primary, and the old holder has
		// not released the Lease yet.
		return nil
	case m.target == targetPending:
		m.line(m.config.podName + ": detached, a failover is pending")
		return m.demote(ctx, info, func() error { return m.server.detach(ctx, info, m.config.port) })
	case holder != "" && holder != m.config.podName && m.podIPs[holder] != "":
		if err := m.setMasterauth(ctx, m.config.password); err != nil {
			return err
		}
		m.line(m.config.podName + ": following " + holder)
		return m.demote(ctx, info, func() error {
			port, err := m.peerPort(ctx, holder)
			if err != nil {
				return err
			}
			return m.server.follow(ctx, info, m.podIPs[holder], port)
		})
	case holder == "" && m.switchingOver && info.role == "slave" && m.podIPs[m.target] != "" && info.masterHost == m.podIPs[m.target]:
		// FAILOVER made this server a replica of the target, which takes
		// the empty Lease on its next step. The target counts this link
		// toward min-replicas-to-write, so detaching would make the new
		// primary refuse writes with NOREPLICAS until this instance
		// followed it again. Keeping the link loses nothing a detach
		// would protect: what the target takes streams to this instance,
		// which is the one the operator names again if the switchover
		// fails.
		m.line(m.config.podName + ": following " + m.target + ", which the switchover made primary")
		return nil
	default:
		m.line(m.config.podName + ": detached, no primary holds the lease")
		return m.demote(ctx, info, func() error { return m.server.detach(ctx, info, m.config.port) })
	}
}

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

func (m *manager) setReport(report instanceReport) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.report = report
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
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.currentReport())
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !m.currentReport().Ready {
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

// say writes one line to the log.
func (m *manager) say(line string) {
	_, _ = fmt.Fprintln(m.log, line)
}

// line writes a line when it differs from the last one line wrote, so
// a steady state logs once and not on every step.
func (m *manager) line(text string) {
	if text != m.lastLine {
		m.lastLine = text
		m.say(text)
	}
}

// note writes text for topic when it differs from what note last wrote
// for it, so a fault that repeats on every step logs once. An empty
// text clears the topic.
func (m *manager) note(topic, text string) {
	if m.notes == nil {
		m.notes = map[string]string{}
	}
	if m.notes[topic] == text {
		return
	}
	m.notes[topic] = text
	if text != "" {
		m.say(text)
	}
}

// installManager copies this binary to dir, from the init container
// into the volume the server's container runs it from.
func installManager(dir string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	data, err := os.ReadFile(self)
	if err != nil {
		return err
	}
	target := filepath.Join(dir, "redis-operator")
	if err := os.WriteFile(target+".tmp", data, 0o755); err != nil {
		return err
	}
	return os.Rename(target+".tmp", target)
}
