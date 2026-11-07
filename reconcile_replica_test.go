package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// replicaOf declares replica mode on the harness's Redis for a source at
// host and port, with its password Secret.
func (h *harness) replicaOf(host string, port int, enabled bool) {
	h.t.Helper()
	var held map[string]any
	if !h.api.read(secretsResource, testNamespace, "source", &held) {
		h.api.create(secretsResource, map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": "source", "namespace": testNamespace},
			"data":     map[string]any{"password": []byte(sourcePassword)},
		})
	}
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["replica"] = toObject(h.t, ReplicaSpec{Enabled: enabled, Source: ReplicaSource{
			Host: host, Port: port, Password: &SecretKeyRef{Secret: "source", Key: "password"},
		}})
	})
}

// adoptingHolder sets the reports of a Redis whose holder follows a
// source at offset.
func (h *harness) adoptingHolder(sourceHost string, offset int64) {
	h.holdLease("cache-0")
	h.setReport(instanceReport{Name: "cache-0", Role: reportReplica, HoldsLease: true, Target: "cache-0", Offset: offset, LinkUp: true, Following: sourceHost, HasData: true, Ready: true})
	h.setReport(instanceReport{Name: "cache-1", Role: reportReplica, Target: "cache-0", Offset: offset, LinkUp: true, Following: h.ip("cache-0"), HasData: true, Ready: true})
}

func sourceOffsetOf(t *testing.T, source *testServer) int64 {
	t.Helper()
	info, err := readReplication(context.Background(), sourceClient(t, source))
	mustSucceed(t, err)
	return info.offset
}

func TestANewRedisNamingASourceReplicatesAndReportsItsLag(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.replicaOf(host, port, true)

	h.pass()
	mustMatch(t, h.redis().Status.Replica.Phase, replicaReplicating)
	h.adoptingHolder(host, 0)
	h.pass()

	replica := h.redis().Status.Replica
	mustMatch(t, replica.Source, fmt.Sprintf("%s:%d", host, port))
	mustMatch(t, replica.Lag, replica.SourceOffset)
	mustMatch(t, slices.Contains(scrape(t, h.c.readings), fmt.Sprintf(`redis_operator_replica_lag_bytes{namespace=%q,redis="cache"} %d`, testNamespace, replica.Lag)), true)
}

// lagSeries lists the replica lag series the operator exports.
func lagSeries(t *testing.T, m *metrics) []string {
	t.Helper()
	var series []string
	for _, sample := range scrape(t, m) {
		if strings.HasPrefix(sample, "redis_operator_replica_lag_bytes{") {
			series = append(series, sample)
		}
	}
	return series
}

// A Redis that is deleted while it replicates exports no lag after it.
func TestADeletedRedisExportsNoLag(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.replicaOf(host, port, true)
	h.pass()
	h.adoptingHolder(host, 0)
	h.pass()

	h.api.delete(redisResource, testNamespace, h.name)
	h.pass()

	mustMatch(t, len(lagSeries(t, h.c.readings)), 0)
}

func TestPromotionPausesTheSourceAndWaitsForTheHolder(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	_, err := sourceClient(t, source).do(context.Background(), "SET", "before", "cutover")
	mustSucceed(t, err)
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.replicaOf(host, port, true)
	h.pass()
	h.adoptingHolder(host, 0)
	h.pass()

	h.replicaOf(host, port, false)
	h.pass()

	mustMatch(t, h.redis().Status.Replica.Phase, replicaPromoting)
	paused, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err = sourceClient(t, source).do(paused, "SET", "during", "pause")
	mustMatch(t, err != nil, true)

	h.adoptingHolder(host, sourceOffsetOf(t, source))
	h.pass()

	mustMatch(t, h.redis().Status.Replica.Phase, replicaPromoted)
}

// A client that moves to the Redis once Adopted is True must find the
// holder taking writes, and the holder's manager stops following the
// source only on its next step after the phase changes.
func TestAdoptedTurnsTrueOnlyOnceTheHolderTakesWrites(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 2)
	port := freePort(t)
	h.replicaOf("127.0.0.1", port, true)
	h.pass()
	h.adoptingHolderUnlinked("127.0.0.1")
	h.pass()

	h.replicaOf("127.0.0.1", port, false)
	h.pass()
	adopted, _ := findCondition(h.redis().Status.Conditions, "Adopted")
	mustMatch(t, adopted.Status, ConditionFalse)
	mustMatch(t, adopted.Reason, "TakingWrites")

	h.steady("cache-0", 2)
	h.pass()
	adopted, _ = findCondition(h.redis().Status.Conditions, "Adopted")
	mustMatch(t, adopted.Status, ConditionTrue)
	mustMatch(t, adopted.Reason, "Promoted")
}

func TestPromotionWithASourceNeitherTheOperatorNorTheHolderReachesPromotesAtOnce(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 2)
	port := freePort(t)
	h.replicaOf("127.0.0.1", port, true)
	h.pass()
	h.adoptingHolderUnlinked("127.0.0.1")
	h.pass()

	h.replicaOf("127.0.0.1", port, false)
	h.pass()

	mustMatch(t, h.redis().Status.Replica.Phase, replicaPromoted)
}

// The operator's own connection can fail where the holder's does not,
// as when a NetworkPolicy admits only the Redis pods, so a holder with
// its link up says the source is alive and may still take writes.
func TestPromotionWithASourceOnlyTheHolderReachesWaits(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 2)
	port := freePort(t)
	h.replicaOf("127.0.0.1", port, true)
	h.pass()
	h.adoptingHolder("127.0.0.1", 0)
	h.pass()

	h.replicaOf("127.0.0.1", port, false)
	h.pass()

	mustMatch(t, h.redis().Status.Replica.Phase, replicaReplicating)
	adopted, _ := findCondition(h.redis().Status.Conditions, "Adopted")
	mustMatch(t, adopted.Status, ConditionFalse)
	mustMatch(t, adopted.Reason, "SourceUnreachableFromOperator")
	mustMatch(t, strings.HasSuffix(adopted.Message, "To promote without pausing the source, remove spec.replica"), true)
}

func TestReplicaModeDoesNotStartOnARedisThatServed(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 2)
	h.steady("cache-0", 2)
	h.pass()

	h.replicaOf("127.0.0.1", 6379, true)
	h.pass()

	mustMatch(t, h.redis().Status.Replica == nil, true)
	adopted, _ := findCondition(h.redis().Status.Conditions, "Adopted")
	mustMatch(t, adopted.Reason, "AlreadyPrimary")
}

// adoptingHolderUnlinked sets the reports of a Redis whose holder lost
// its link to the source, so it cannot catch up.
func (h *harness) adoptingHolderUnlinked(sourceHost string) {
	h.holdLease("cache-0")
	h.setReport(instanceReport{Name: "cache-0", Role: reportReplica, HoldsLease: true, Target: "cache-0", Following: sourceHost, HasData: true, Ready: true})
	h.setReport(instanceReport{Name: "cache-1", Role: reportReplica, Target: "cache-0", LinkUp: true, Following: h.ip("cache-0"), HasData: true, Ready: true})
}

func TestAPromotionThatDoesNotCatchUpWithinThePauseDoesNotPromote(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.replicaOf(host, port, true)
	h.pass()
	h.adoptingHolderUnlinked(host)
	h.pass()
	h.replicaOf(host, port, false)
	h.pass()
	mustMatch(t, h.redis().Status.Replica.Phase, replicaPromoting)

	h.passFor(sourcePause, time.Second)

	mustMatch(t, h.redis().Status.Replica.Phase, replicaReplicating)
	adopted, _ := findCondition(h.redis().Status.Conditions, "Adopted")
	mustMatch(t, adopted.Status, ConditionFalse)
	mustMatch(t, adopted.Reason, "CatchUpTimedOut")
}

func TestAPromotionThatTimedOutPausesTheSourceAgainAfterTheBackoff(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.replicaOf(host, port, true)
	h.pass()
	h.adoptingHolderUnlinked(host)
	h.pass()
	h.replicaOf(host, port, false)
	h.pass()
	h.passFor(sourcePause, time.Second)

	h.passFor(defaultTimings.switchoverBackoff, time.Second)
	mustMatch(t, h.redis().Status.Replica.Phase, replicaPromoting)
	h.adoptingHolder(host, sourceOffsetOf(t, source))
	h.pass()

	mustMatch(t, h.redis().Status.Replica.Phase, replicaPromoted)
}

// refuseClientCommands takes the CLIENT command from the source's
// default user, as an ACL that forbids it does, or gives it back.
func refuseClientCommands(t *testing.T, source *testServer, refuse bool) {
	t.Helper()
	rule := "+client"
	if refuse {
		rule = "-client"
	}
	_, err := sourceClient(t, source).do(context.Background(), "ACL", "SETUSER", "default", rule)
	mustSucceed(t, err)
}

func TestASourceThatRefusesThePauseIsNotPromotedFrom(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	refuseClientCommands(t, source, true)
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.replicaOf(host, port, true)
	h.pass()
	h.adoptingHolder(host, 0)
	h.pass()

	h.replicaOf(host, port, false)
	h.pass()

	mustMatch(t, h.redis().Status.Replica.Phase, replicaReplicating)
	adopted, _ := findCondition(h.redis().Status.Conditions, "Adopted")
	mustMatch(t, adopted.Status, ConditionFalse)
	mustMatch(t, adopted.Reason, "PauseRefused")
	mustMatch(t, strings.Contains(adopted.Message, "NOPERM"), true)
	mustMatch(t, strings.Contains(adopted.Message, "remove spec.replica"), true)
}

func TestASourceThatRefusedThePauseIsPausedAgainAfterTheBackoff(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	refuseClientCommands(t, source, true)
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.replicaOf(host, port, true)
	h.pass()
	h.adoptingHolder(host, 0)
	h.pass()
	h.replicaOf(host, port, false)
	h.pass()
	refuseClientCommands(t, source, false)

	h.passFor(defaultTimings.switchoverBackoff, time.Second)

	mustMatch(t, h.redis().Status.Replica.Phase, replicaPromoting)
}

// promoteFrom declares replica mode for a source at host and port whose
// password is in the Secret secret, then turns it off, as a person
// promoting the Redis does.
func (h *harness) promoteFrom(host string, port int, secret string) {
	h.t.Helper()
	for _, enabled := range []bool{true, false} {
		h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
			object["spec"].(map[string]any)["replica"] = toObject(h.t, ReplicaSpec{Enabled: enabled, Source: ReplicaSource{
				Host: host, Port: port, Password: &SecretKeyRef{Secret: secret, Key: "password"},
			}})
		})
		h.pass()
		h.adoptingHolder(host, 0)
		h.pass()
	}
}

func TestASourceThatRefusesThePasswordIsNotPromotedFrom(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.api.create(secretsResource, map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "wrong", "namespace": testNamespace},
		"data":     map[string]any{"password": []byte("not-" + sourcePassword)},
	})

	h.promoteFrom(host, port, "wrong")

	mustMatch(t, h.redis().Status.Replica.Phase, replicaReplicating)
	adopted, _ := findCondition(h.redis().Status.Conditions, "Adopted")
	mustMatch(t, adopted.Status, ConditionFalse)
	mustMatch(t, adopted.Reason, "SourceRefused")
	mustMatch(t, strings.Contains(adopted.Message, "remove spec.replica"), true)
}

func TestAPromotionWhosePasswordSecretIsMissingWaits(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)

	h.promoteFrom(host, port, "missing")

	mustMatch(t, h.redis().Status.Replica.Phase, replicaReplicating)
	adopted, _ := findCondition(h.redis().Status.Conditions, "Adopted")
	mustMatch(t, adopted.Status, ConditionFalse)
	mustMatch(t, adopted.Reason, "PasswordUnreadable")
	mustMatch(t, strings.Contains(adopted.Message, "missing"), true)
}

func TestTheOperatorFindsTheSourceThroughASentinelWithAPassword(t *testing.T) {
	t.Parallel()
	source := startSource(t, "redis")
	sentinel := startSentinel(t, source, sentinelPassword)
	host, port := sourceHostPort(source)
	h := startHarness(t, 2)
	h.replicaOf(host, port, true)
	h.api.create(secretsResource, map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "sentinel", "namespace": testNamespace},
		"data":     map[string]any{"password": []byte(sentinelPassword)},
	})
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		object["spec"].(map[string]any)["replica"] = toObject(h.t, ReplicaSpec{Enabled: true, Source: ReplicaSource{
			Password: &SecretKeyRef{Secret: "source", Key: "password"},
			Sentinel: &SentinelSource{MasterName: "mymaster", Addresses: []string{sentinel}, Password: &SecretKeyRef{Secret: "sentinel", Key: "password"}},
		}})
	})
	h.pass()
	h.adoptingHolder(host, 0)

	h.pass()

	reachable, _ := findCondition(h.redis().Status.Conditions, "SourceReachable")
	mustMatch(t, reachable.Status, ConditionTrue)
	mustMatch(t, h.redis().Status.Replica.Source, fmt.Sprintf("%s:%d", host, port))
}

// While a Redis replicates, its source holds the dataset, so instances
// that all restart empty copy it again and lose nothing.
func TestAReplicatingRedisWhoseInstancesRestartEmptyCopiesTheSourceAgain(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.replicaOf("127.0.0.1", freePort(t), true)
	h.pass()
	h.adoptingHolder("127.0.0.1", 100)
	h.pass()
	mustMatch(t, h.redis().Status.CurrentPrimary, "cache-0")

	h.holdLease("")
	h.setReport(instanceReport{Name: "cache-0", Role: reportDetached, Target: "cache-0"})
	h.passFor(10*time.Second, 2*time.Second)

	status := h.redis().Status
	mustMatch(t, status.TargetPrimary, "cache-0")
	mustMatch(t, status.CurrentPrimary, "")
	mustMatch(t, status.Replica.Phase, replicaReplicating)
	_, lost := findCondition(status.Conditions, "DataLost")
	mustMatch(t, lost, false)
}

// The lag moves with every write to the source, so the status records
// it only when it crosses a bucket, or when it holds none yet.
func TestTheLagIsRecordedOnlyWhenItCrossesABucket(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		held     ReplicaStatus
		offset   int64
		lag      int64
		recorded ReplicaStatus
	}{
		{"the first measure", ReplicaStatus{}, 100, 0, ReplicaStatus{SourceOffset: 100}},
		{"a lag that stays caught up", ReplicaStatus{SourceOffset: 100}, 900, 0, ReplicaStatus{SourceOffset: 100}},
		{"a lag that stays near", ReplicaStatus{SourceOffset: 100, Lag: 10}, 900, 800, ReplicaStatus{SourceOffset: 100, Lag: 10}},
		{"a lag that falls behind", ReplicaStatus{SourceOffset: 100, Lag: 10}, 9 << 20, 8 << 20, ReplicaStatus{SourceOffset: 9 << 20, Lag: 8 << 20}},
		{"a lag that catches up", ReplicaStatus{SourceOffset: 100, Lag: 10}, 900, 0, ReplicaStatus{SourceOffset: 900}},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			replica := one.held

			recordLag(&replica, one.offset, one.lag)

			mustMatch(t, replica, one.recorded)
		})
	}
}
