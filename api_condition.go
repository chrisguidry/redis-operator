package main

// Condition mirrors metav1.Condition, the shape Kubernetes uses
// everywhere. Anyone who reads kubectl describe output on a Pod
// already knows how to read one of these.
//
// ObservedGeneration records which metadata.generation the condition
// judged. Generation counts spec edits, so a reader can tell "Ready,
// for the spec as it stands" from "Ready, but for a spec two edits
// ago".
type Condition struct {
	Type               string          `json:"type"`
	Status             ConditionStatus `json:"status"`
	ObservedGeneration int64           `json:"observedGeneration,omitempty"`
	Reason             string          `json:"reason,omitempty"`
	Message            string          `json:"message,omitempty"`
	LastTransitionTime string          `json:"lastTransitionTime"`
}

// ConditionStatus is the three-valued verdict a condition carries.
type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// withCondition returns a copy of conditions with next in place of the
// condition of the same type, or added at the end. next keeps the
// earlier lastTransitionTime when its status did not change, because
// the time says when the verdict changed, not when it was last judged.
func withCondition(conditions []Condition, next Condition) []Condition {
	updated := make([]Condition, 0, len(conditions)+1)
	placed := false
	for _, held := range conditions {
		if held.Type != next.Type {
			updated = append(updated, held)
			continue
		}
		if held.Status == next.Status && held.LastTransitionTime != "" {
			next.LastTransitionTime = held.LastTransitionTime
		}
		updated = append(updated, next)
		placed = true
	}
	if !placed {
		updated = append(updated, next)
	}
	return updated
}

// withoutCondition returns a copy of conditions with no condition of
// the type named.
func withoutCondition(conditions []Condition, kind string) []Condition {
	var kept []Condition
	for _, held := range conditions {
		if held.Type != kind {
			kept = append(kept, held)
		}
	}
	return kept
}

// findCondition returns the condition of the type named, and false when
// there is none.
func findCondition(conditions []Condition, kind string) (Condition, bool) {
	for _, held := range conditions {
		if held.Type == kind {
			return held, true
		}
	}
	return Condition{}, false
}
