package main

// The Redis resource: one primary and zero or more replicas.
// deploy/redis-crd.yaml is its schema, written by hand, and
// api_redis_crd_test.go checks the two agree.

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	apiGroup   = "redis.guid.foo"
	apiVersion = apiGroup + "/v1alpha1"
	redisKind  = "Redis"
)

var redisResource = kubeResource{apiGroup, "v1alpha1", "redis"}

// The engines, and the image each runs by default. The default moves
// with the operator's releases.
const (
	engineRedis  = "redis"
	engineValkey = "valkey"
)

var defaultImages = map[string]string{
	engineRedis:  "redis:8.10.3",
	engineValkey: "valkey/valkey:9.0.7",
}

// targetPending is the targetPrimary that says the primary failed and
// the operator has not named the next one yet. Every instance that sees
// it stops following the old primary.
const targetPending = "pending"

type Redis struct {
	APIVersion string      `json:"apiVersion,omitempty"`
	Kind       string      `json:"kind,omitempty"`
	Metadata   ObjectMeta  `json:"metadata"`
	Spec       RedisSpec   `json:"spec"`
	Status     RedisStatus `json:"status,omitempty"`
}

type RedisSpec struct {
	Engine    string `json:"engine,omitempty"`
	Image     string `json:"image,omitempty"`
	Instances int    `json:"instances,omitempty"`
	// Memory is the container's memory limit, as a Kubernetes quantity.
	Memory             string            `json:"memory,omitempty"`
	MaxmemoryPolicy    string            `json:"maxmemoryPolicy,omitempty"`
	Storage            *StorageSpec      `json:"storage,omitempty"`
	MinReplicasToWrite *int              `json:"minReplicasToWrite,omitempty"`
	CPU                string            `json:"cpu,omitempty"`
	NodeSelector       map[string]string `json:"nodeSelector,omitempty"`
	Tolerations        []map[string]any  `json:"tolerations,omitempty"`
	PriorityClassName  string            `json:"priorityClassName,omitempty"`
	Backup             *BackupStore      `json:"backup,omitempty"`
	Bootstrap          *Bootstrap        `json:"bootstrap,omitempty"`
	Replica            *ReplicaSpec      `json:"replica,omitempty"`
}

type StorageSpec struct {
	Size             string `json:"size"`
	StorageClassName string `json:"storageClassName,omitempty"`
}

type RedisStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// TargetPrimary is the operator's decision: the instance that should
	// be primary, or "pending" while a failover chooses one.
	TargetPrimary string `json:"targetPrimary,omitempty"`
	// CurrentPrimary is the instance that holds the primary Lease. It is
	// empty until the first primary takes the Lease, which tells an
	// instance with no data that it may start the dataset.
	CurrentPrimary string            `json:"currentPrimary,omitempty"`
	ReadyInstances int               `json:"readyInstances,omitempty"`
	Instances      []InstanceStatus  `json:"instances,omitempty"`
	Switchover     *SwitchoverStatus `json:"switchover,omitempty"`
	Replica        *ReplicaStatus    `json:"replica,omitempty"`
	// Image is the image every instance runs, and Rollout a change to
	// another that is under way or failed.
	Image      string         `json:"image,omitempty"`
	Rollout    *RolloutStatus `json:"rollout,omitempty"`
	Conditions []Condition    `json:"conditions,omitempty"`
}

type InstanceStatus struct {
	Name   string `json:"name"`
	Role   string `json:"role"`
	Ready  bool   `json:"ready"`
	Offset int64  `json:"offset"`
}

// SwitchoverStatus records a planned switchover while it runs.
type SwitchoverStatus struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Reason    string `json:"reason"`
	StartedAt string `json:"startedAt"`
}

// The defaults, applied wherever the spec is read, so a spec that
// leaves a field out means the same thing to the operator and to the
// instance managers.

func (s RedisSpec) engine() string {
	if s.Engine == "" {
		return engineRedis
	}
	return s.Engine
}

func (s RedisSpec) image() string {
	if s.Image != "" {
		return s.Image
	}
	return defaultImages[s.engine()]
}

func (s RedisSpec) instances() int {
	if s.Instances < 1 {
		return 1
	}
	return s.Instances
}

func (s RedisSpec) memory() string {
	if s.Memory == "" {
		return "256Mi"
	}
	return s.Memory
}

func (s RedisSpec) maxmemoryPolicy() string {
	if s.MaxmemoryPolicy == "" {
		return "noeviction"
	}
	return s.MaxmemoryPolicy
}

func (s RedisSpec) minReplicasToWrite() int {
	if s.MinReplicasToWrite != nil {
		return *s.MinReplicasToWrite
	}
	if s.instances() > 1 {
		return 1
	}
	return 0
}

func (s RedisSpec) persistent() bool {
	return s.Storage != nil && s.Storage.Size != ""
}

// maxmemoryShare is the part of the container's memory that the server
// may fill with data. A snapshot or a full resync forks the server, and
// every page that changes during the fork is copied, so the rest is
// headroom that keeps the kernel from killing the container.
const maxmemoryShare = 0.7

func (s RedisSpec) maxmemoryBytes() (int64, error) {
	limit, err := parseQuantity(s.memory())
	if err != nil {
		return 0, fmt.Errorf("spec.memory: %w", err)
	}
	return int64(float64(limit) * maxmemoryShare), nil
}

// parseQuantity reads a Kubernetes memory quantity, such as 512Mi or
// 1G, as bytes. It takes the binary and decimal suffixes, and no
// fractions, which the CRD's pattern also refuses.
func parseQuantity(quantity string) (int64, error) {
	suffixes := []struct {
		suffix string
		scale  int64
	}{
		{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40},
		{"k", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12},
	}
	for _, s := range suffixes {
		if number, found := strings.CutSuffix(quantity, s.suffix); found {
			value, err := strconv.ParseInt(number, 10, 64)
			if err != nil || value <= 0 {
				return 0, fmt.Errorf("%q is not a whole number of %s", quantity, s.suffix)
			}
			return value * s.scale, nil
		}
	}
	value, err := strconv.ParseInt(quantity, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%q is not a memory quantity such as 512Mi", quantity)
	}
	return value, nil
}

// instanceName is the pod name of the instance with ordinal i, which
// the StatefulSet gives it.
func instanceName(redis string, i int) string { return fmt.Sprintf("%s-%d", redis, i) }

// The labels on every pod. clusterLabel selects a Redis's pods, and
// roleLabel, which the operator writes from the Lease, selects the
// primary or the replicas for the Services and the budgets.
const (
	clusterLabel = apiGroup + "/cluster"
	roleLabel    = apiGroup + "/role"
	rolePrimary  = "primary"
	roleReplica  = "replica"
)

// switchoverAnnotation is how an instance manager whose pod is being
// deleted asks the operator for a switchover. Its value is the time
// of the request.
const switchoverAnnotation = apiGroup + "/switchover-requested"

// primaryLeaseName is the Lease the primary's instance manager holds.
func primaryLeaseName(redis string) string { return redis + "-primary" }
