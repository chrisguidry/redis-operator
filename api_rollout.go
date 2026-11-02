package main

// A staged change of spec.image: a canary replica proves the new image
// before the rest roll, and a failed canary is rolled back.
// plans/completed/04-upgrades.md describes it.

// The phases of a rollout.
const (
	rolloutCanary  = "Canary"
	rolloutRolling = "Rolling"
	rolloutFailed  = "Failed"
)

type RolloutStatus struct {
	// Image is the image the rollout moves to.
	Image     string `json:"image"`
	Phase     string `json:"phase"`
	Canary    string `json:"canary,omitempty"`
	StartedAt string `json:"startedAt"`
	Reason    string `json:"reason,omitempty"`
}

// servingImage is the image the StatefulSet's template runs: the
// rollout's while it rolls, and otherwise the one every instance runs.
func servingImage(r *Redis) string {
	if ro := r.Status.Rollout; ro != nil && (ro.Phase == rolloutCanary || ro.Phase == rolloutRolling) {
		return ro.Image
	}
	if r.Status.Image != "" {
		return r.Status.Image
	}
	return r.Spec.image()
}
