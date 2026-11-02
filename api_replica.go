package main

// Replica mode: a Redis that replicates from a source outside the
// operator until it is promoted. plans/completed/03-adoption.md describes it.

// ReplicaSpec names the source a Redis replicates from while Enabled.
type ReplicaSpec struct {
	Enabled bool          `json:"enabled"`
	Source  ReplicaSource `json:"source"`
}

type ReplicaSource struct {
	Host     string          `json:"host,omitempty"`
	Port     int             `json:"port,omitempty"`
	Password *SecretKeyRef   `json:"password,omitempty"`
	Sentinel *SentinelSource `json:"sentinel,omitempty"`
}

// SentinelSource follows whichever primary these sentinels name.
// Password is the sentinels' own requirepass, which is separate from
// the source's.
type SentinelSource struct {
	MasterName string        `json:"masterName"`
	Addresses  []string      `json:"addresses"`
	Password   *SecretKeyRef `json:"password,omitempty"`
}

type SecretKeyRef struct {
	Secret string `json:"secret"`
	Key    string `json:"key"`
}

// sentinelPassword is the Secret key of the sentinels' password, or nil.
func (s ReplicaSource) sentinelPassword() *SecretKeyRef {
	if s.Sentinel == nil {
		return nil
	}
	return s.Sentinel.Password
}

func (s ReplicaSource) port() int {
	if s.Port == 0 {
		return serverPort
	}
	return s.Port
}

// The phases of replica mode.
const (
	replicaReplicating = "Replicating"
	replicaPromoting   = "Promoting"
	replicaPromoted    = "Promoted"
)

type ReplicaStatus struct {
	Phase        string `json:"phase,omitempty"`
	Source       string `json:"source,omitempty"`
	SourceOffset int64  `json:"sourceOffset,omitempty"`
	Lag          int64  `json:"lag,omitempty"`
}

// replicating reports whether the decision is that the holder follows
// the source.
func (s RedisStatus) replicating() bool {
	return s.Replica != nil && (s.Replica.Phase == replicaReplicating || s.Replica.Phase == replicaPromoting)
}
