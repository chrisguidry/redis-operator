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
	NodeName   string         `json:"nodeName,omitempty"`
	Containers []PodContainer `json:"containers,omitempty"`
	Volumes    []PodVolume    `json:"volumes,omitempty"`
}

// PodContainer and PodVolume carry the parts of a pod's spec that tell
// which template it was made from.
type PodContainer struct {
	Name         string        `json:"name"`
	VolumeMounts []VolumeMount `json:"volumeMounts,omitempty"`
}

type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
}

type PodVolume struct {
	Name                  string `json:"name"`
	PersistentVolumeClaim *struct {
		ClaimName string `json:"claimName"`
	} `json:"persistentVolumeClaim,omitempty"`
	NFS *NFSVolume `json:"nfs,omitempty"`
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

// waitingReason is why one of the pod's containers waits, or "". A
// container other than the server's is named, so a reason that comes
// from the exporter's image does not read as the server's.
func (p Pod) waitingReason() string {
	for _, status := range p.Status.ContainerStatuses {
		if w := status.State.Waiting; w != nil && w.Reason != "" {
			reason := w.Reason
			if w.Message != "" {
				reason += ": " + w.Message
			}
			if status.Name != "redis" {
				reason = "the " + status.Name + " container: " + reason
			}
			return reason
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

// StatefulSet carries the scale the operator holds during a scale-down
// and the revision fields it rolls pods by.
type StatefulSet struct {
	Metadata ObjectMeta        `json:"metadata"`
	Spec     StatefulSetSpec   `json:"spec"`
	Status   StatefulSetStatus `json:"status"`
}

type StatefulSetSpec struct {
	Replicas int `json:"replicas,omitempty"`
}

type StatefulSetStatus struct {
	UpdateRevision string `json:"updateRevision,omitempty"`
}
