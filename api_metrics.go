package main

// The metrics exporter that runs beside each instance.
// plans/completed/01-high-availability.md describes it.

type MetricsSpec struct {
	// Enabled is true when left out, so every Redis has metrics unless
	// its spec turns them off.
	Enabled *bool  `json:"enabled,omitempty"`
	Image   string `json:"image,omitempty"`
	// CheckSingleKeys are the keys whose value or length the exporter
	// reports, in its check-single-keys form: a key in database 0, or
	// db<n>=<key> in another.
	CheckSingleKeys []string `json:"checkSingleKeys,omitempty"`
}

func (s RedisSpec) metricsEnabled() bool {
	return s.Metrics == nil || s.Metrics.Enabled == nil || *s.Metrics.Enabled
}

// exporterImage is the redis_exporter image every instance runs beside
// its server. redis_exporter reads Valkey as well as Redis, so both
// engines run the same image.
func (s RedisSpec) exporterImage(defaults images) string {
	if s.Metrics != nil && s.Metrics.Image != "" {
		return s.Metrics.Image
	}
	return defaults.exporter
}
