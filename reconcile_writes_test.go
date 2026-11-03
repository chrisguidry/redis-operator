package main

import (
	"fmt"
	"testing"
)

// minReplicas is the min-replicas-to-write the primary runs with.
func (h *harness) minReplicas() int {
	r := h.redis()
	return r.minReplicasToWrite()
}

// addInstance makes the pod of ordinal i, as the StatefulSet controller
// does when the Redis scales up.
func (h *harness) addInstance(i int) {
	node := fmt.Sprintf("node-%d", i)
	h.api.create(nodesResource, Node{Metadata: ObjectMeta{Name: node}})
	name := instanceName(h.name, i)
	h.api.create(podsResource, Pod{
		Metadata: ObjectMeta{Name: name, Namespace: testNamespace, Labels: map[string]string{clusterLabel: h.name, revisionLabel: "rev-1"}},
		Spec:     PodSpec{NodeName: node},
		Status:   PodStatus{Phase: "Running", PodIP: h.ip(name)},
	})
	h.serveReport(name)
}

func TestAScaleUpRaisesMinReplicasToWriteOnlyOnceAReplicaHasSynced(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.steady("cache-0", 1)
	h.pass()
	mustMatch(t, h.minReplicas(), 0)

	h.setInstances(3)
	h.addInstance(1)
	h.addInstance(2)
	h.setReport(instanceReport{Name: "cache-1", Role: reportReplica, Target: "cache-0", HasData: false})
	h.pass()
	mustMatch(t, h.minReplicas(), 0)

	// A replica's own report can outlive it: the name of an instance a
	// scale-down removed comes back with the scale-up. Only the
	// primary's count of the replicas it streams to raises the value.
	h.setReport(instanceReport{Name: "cache-1", Role: reportReplica, Target: "cache-0", LinkUp: true, Following: h.ip("cache-0"), HasData: true, Ready: true})
	h.pass()
	mustMatch(t, h.minReplicas(), 0)

	h.steady("cache-0", 2)
	h.pass()
	mustMatch(t, h.minReplicas(), 1)
}

func TestMinReplicasToWriteStaysUpWhenTheReplicasGoAway(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()
	mustMatch(t, h.minReplicas(), 1)

	h.silence("cache-1")
	h.silence("cache-2")
	h.pass()

	mustMatch(t, h.minReplicas(), 1)
}

func TestMinReplicasToWriteFollowsTheSpecDown(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)
	h.steady("cache-0", 3)
	h.pass()

	h.setInstances(1)
	h.pass()

	mustMatch(t, h.minReplicas(), 0)
}

func TestANewRedisStartsWithTheMinReplicasToWriteOfItsSpec(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 3)

	h.pass()

	mustMatch(t, h.minReplicas(), 1)
}
