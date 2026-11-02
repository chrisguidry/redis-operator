package main

// The Kubernetes objects the operator and the instance managers read,
// with only the fields they use.

var (
	nodesResource        = kubeResource{"", "v1", "nodes"}
	statefulSetsResource = kubeResource{"apps", "v1", "statefulsets"}
)

type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     PodSpec    `json:"spec"`
	Status   PodStatus  `json:"status"`
}

type PodSpec struct {
	NodeName string `json:"nodeName,omitempty"`
}

type PodStatus struct {
	Phase             string            `json:"phase,omitempty"`
	PodIP             string            `json:"podIP,omitempty"`
	Conditions        []PodCondition    `json:"conditions,omitempty"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty"`
}

// ContainerStatus says why a container is not running, such as
// ImagePullBackOff or CrashLoopBackOff.
type ContainerStatus struct {
	Name  string `json:"name"`
	State struct {
		Waiting *struct {
			Reason  string `json:"reason,omitempty"`
			Message string `json:"message,omitempty"`
		} `json:"waiting,omitempty"`
	} `json:"state"`
}

// waitingReason is why the pod's server container waits, or "".
func (p Pod) waitingReason() string {
	for _, status := range p.Status.ContainerStatuses {
		if w := status.State.Waiting; w != nil && w.Reason != "" {
			if w.Message != "" {
				return w.Reason + ": " + w.Message
			}
			return w.Reason
		}
	}
	return ""
}

type PodCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

// ready reports the pod's Ready condition, which its readiness probe
// sets.
func (p Pod) ready() bool {
	for _, condition := range p.Status.Conditions {
		if condition.Type == "Ready" {
			return condition.Status == "True"
		}
	}
	return false
}

type Node struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     NodeSpec   `json:"spec"`
}

type NodeSpec struct {
	Unschedulable bool `json:"unschedulable,omitempty"`
}

// StatefulSet carries the revision fields the operator rolls pods by.
type StatefulSet struct {
	Metadata ObjectMeta        `json:"metadata"`
	Status   StatefulSetStatus `json:"status"`
}

type StatefulSetStatus struct {
	UpdateRevision string `json:"updateRevision,omitempty"`
}
