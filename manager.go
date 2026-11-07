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
	primaryRetryPeriod   = 3 * time.Second
	managerStepPeriod    = time.Second
	// managerLeaseReadPeriod is how often an instance that neither holds
	// nor is the target reads the primary Lease, and managerSlowReadPeriod
	// how often any instance lists its pods and Backups and reads its
	// Secrets (manager_reads.go). minReplicasRecheck is how often the
	// primary reads min-replicas-to-write back from its server, to catch
	// a change made by hand.
	managerLeaseReadPeriod = 2 * time.Second
	managerSlowReadPeriod  = 10 * time.Second
	minReplicasRecheck     = 30 * time.Second
	switchoverTimeout      = 10 * time.Second
	peerTimeout            = time.Second
	// handoverWait is how long a primary that is being deleted waits
	// for the operator to name the next primary before it stops anyway.
	handoverWait = 20 * time.Second
	// demotionHold is the longest a demoted primary holds its clients'
	// writes waiting for its pod to lose the primary label, and
	// routeSettle how long kube-proxy may take to stop routing to it
	// after that (manager_demote.go).
	demotionHold = 30 * time.Second
	routeSettle  = 5 * time.Second
	// fullSyncDelay is the servers' own default.
	fullSyncDelay = 5 * time.Second
	statusPort    = 9123
	serverPort    = 6379
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

	leaseDuration      time.Duration
	renewDeadline      time.Duration
	retryPeriod        time.Duration
	stepPeriod         time.Duration
	leaseReadPeriod    time.Duration
	slowReadPeriod     time.Duration
	minReplicasRecheck time.Duration
	failoverTimeout    time.Duration
	handoverWait       time.Duration
	demotionHold       time.Duration
	routeSettle        time.Duration
	pauseGap           time.Duration
	fullSyncDelay      time.Duration

	// clock is the time the manager's deadlines are measured on: the
	// Lease's expiry, the handover wait, and the demotion hold. Nil is
	// the wall clock. The pause watch always measures the wall clock,
	// since a pause is a gap in real time.
	clock func() time.Time
}

// now reads the clock the manager's deadlines are measured on.
func (m *manager) now() time.Time {
	if m.config.clock == nil {
		return time.Now()
	}
	return m.config.clock()
}

type manager struct {
	config managerConfig
	client *kubeClient
	server *server
	lease  *primaryLease
	log    io.Writer
	// readings is served on the status port.
	readings *managerMetrics
	peers    *http.Client
	persist  bool

	// What the last step read from the API server. switchingOver is
	// whether the operator moved the target with a switchover, and not
	// a failover.
	target         string
	switchingOver  bool
	everHadPrimary bool
	minReplicas    int
	// primaryMinReplicas is the min-replicas-to-write the primary's
	// server runs, as status.instances last recorded it.
	primaryMinReplicas *int
	instances          int
	specInstances      int
	pods               []Pod
	podIPs             map[string]string
	// When the Lease, the pods, the Backups, and the Secrets of the users
	// and of TLS were last read (manager_reads.go).
	leaseReadAt   time.Time
	podsReadAt    time.Time
	backupsReadAt time.Time
	accessReadAt  time.Time
	// minApplied is the min-replicas-to-write this server runs, as the
	// manager last read or set it, and minCheckedAt when it last read it
	// back.
	minApplied   *int
	minCheckedAt time.Time
	// completedBackups holds the UID of each Backup this instance
	// uploaded, so a Backup whose Completed patch did not land, or that
	// a list read before the patch, is not taken again. The snapshot's
	// goroutine writes it.
	completedBackups sync.Map

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

	// The report the status endpoints serve, when the step that wrote
	// it started, and what the pause watch saw (manager_pause.go).
	mutex         sync.Mutex
	report        instanceReport
	reportTakenAt time.Time
	lastBeat      time.Time
	resumedAt     time.Time
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
		log:      log,
		readings: newManagerMetrics(),
		peers:    &http.Client{Timeout: peerTimeout},
		podIPs:   map[string]string{},
		report:   instanceReport{Name: config.podName, Role: reportStarting},
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
	if !m.persist {
		if err := clearData(m.config.dataDir); err != nil {
			return err
		}
	}
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

	m.watchForPauses(work)
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
				m.stopping, m.stopStarted = true, m.now()
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
	return !m.lease.held() || m.instances == 1 || m.now().Sub(m.stopStarted) > m.config.handoverWait
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
		fullSyncDelay:      m.config.fullSyncDelay,
	}, nil
}

// step reads the decision and the Lease, and makes the server's role
// agree with them.
func (m *manager) step(ctx context.Context) {
	// The report's time is on the wall clock, the clock the pause
	// watch's beats are on.
	started := time.Now()
	api, cancel := m.apiContext(ctx)
	defer cancel()
	apiErr := m.readDecision(api, m.now())
	// The clock is read again after the read, which can take as long as
	// its deadline, so a slow read never makes the Lease look younger
	// than it is.
	now := m.now()
	info, err := m.server.replication(ctx)
	if err != nil {
		m.setReport(instanceReport{Name: m.config.podName, Role: reportStarting, Error: errorText(err)}, started)
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
	if apiErr == nil && due(now, m.accessReadAt, m.config.slowReadPeriod) {
		m.accessReadAt = now
		m.keepAccess(api)
	}
	// A server that has no data yet, such as a replica before its first
	// sync, would upload an empty snapshot as a completed backup.
	if apiErr == nil && m.hasData && due(now, m.backupsReadAt, m.config.slowReadPeriod) {
		m.backupsReadAt = now
		m.startBackups(api)
	}

	var actErr error
	if m.lease.held() {
		actErr = m.stepAsHolder(ctx, api, apiErr, info)
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
	report := m.reportFrom(info, actErr)
	if info.role == "master" {
		if applied, err := m.appliedMinReplicas(ctx); err == nil {
			report.MinReplicasToWrite = &applied
		}
	}
	m.setReport(report, started)
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
