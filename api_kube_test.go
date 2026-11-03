package main

import "testing"

// waitingPod is a pod whose containers wait for the reasons given, by
// container name.
func waitingPod(reasons map[string]string) Pod {
	var pod Pod
	for name, reason := range reasons {
		var status ContainerStatus
		status.Name = name
		status.State.Waiting = &struct {
			Reason  string `json:"reason,omitempty"`
			Message string `json:"message,omitempty"`
		}{Reason: reason}
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, status)
	}
	return pod
}

func TestAWaitingReasonNamesAContainerOtherThanTheServer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		reasons map[string]string
		want    string
	}{
		{"nothing waits", nil, ""},
		{"the server", map[string]string{"redis": "ImagePullBackOff"}, "ImagePullBackOff"},
		{"the exporter", map[string]string{exporterContainer: "ImagePullBackOff"}, "the metrics container: ImagePullBackOff"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mustMatch(t, waitingPod(c.reasons).waitingReason(), c.want)
		})
	}
}
