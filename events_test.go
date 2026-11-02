package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// eventsAbout returns the Events the fake holds about the object of
// one kind and name, in the order they were created.
func (f *fakeKube) eventsAbout(kind, name string) []kubeEvent {
	f.t.Helper()
	f.mutex.Lock()
	var held []map[string]any
	for key, object := range f.objects {
		if key.resource == eventsResource {
			held = append(held, object)
		}
	}
	f.mutex.Unlock()
	created := func(object map[string]any) int {
		n, _ := strconv.Atoi(strings.TrimPrefix(stringAt(metadataOf(object), "uid"), "uid-"))
		return n
	}
	slices.SortFunc(held, func(a, b map[string]any) int { return created(a) - created(b) })
	var out []kubeEvent
	for _, object := range held {
		var event kubeEvent
		fromObject(f.t, object, &event)
		if event.InvolvedObject.Kind == kind && event.InvolvedObject.Name == name {
			out = append(out, event)
		}
	}
	return out
}

// eventLines returns "Type Reason: message" for each Event about the
// object, the way `kubectl describe` lists them.
func (f *fakeKube) eventLines(kind, name string) []string {
	var lines []string
	for _, event := range f.eventsAbout(kind, name) {
		lines = append(lines, event.Type+" "+event.Reason+": "+event.Message)
	}
	return lines
}

// waitForEvent waits until an Event about the object has the reason,
// and returns it.
func (f *fakeKube) waitForEvent(kind, name, reason string) kubeEvent {
	f.t.Helper()
	var found kubeEvent
	eventually(f.t, fmt.Sprintf("an Event %s on %s %s", reason, kind, name), func() bool {
		for _, event := range f.eventsAbout(kind, name) {
			if event.Reason == reason {
				found = event
				return true
			}
		}
		return false
	})
	return found
}

var testEventObject = objectReference{APIVersion: apiVersion, Kind: redisKind, Namespace: testNamespace, Name: "cache", UID: "uid-cache"}

type recorderFixture struct {
	api   *fakeKube
	clock *testClock
	log   *syncBuffer
	r     *eventRecorder
}

// startRecorder runs a recorder against a fake API server, with a
// retry wait short enough that a test sees every attempt.
func startRecorder(t *testing.T) *recorderFixture {
	t.Helper()
	f := &recorderFixture{
		api:   startFakeKube(t),
		clock: &testClock{at: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)},
		log:   &syncBuffer{},
	}
	f.r = newEventRecorder(f.api.client, f.clock.now, newLineLog(f.log))
	go f.r.run(t.Context(), time.Millisecond, time.Second)
	return f
}

func TestRecorderWritesACoreEventAboutTheObject(t *testing.T) {
	f := startRecorder(t)

	f.r.warning(testEventObject, "FailoverStarted", "failing over from cache-0")

	event := f.api.waitForEvent(redisKind, "cache", "FailoverStarted")
	mustMatch(t, event.Type, eventWarning)
	mustMatch(t, event.Message, "failing over from cache-0")
	mustMatch(t, event.InvolvedObject, testEventObject)
	mustMatch(t, event.Metadata.Namespace, testNamespace)
	mustMatch(t, event.Count, int32(1))
	mustMatch(t, event.ReportingComponent, eventComponent)
	mustMatch(t, event.Source.Component, eventComponent)
	mustMatch(t, event.FirstTimestamp.Equal(f.clock.now()), true)
}

func TestRecorderCountsARepeatOnTheEventItWrote(t *testing.T) {
	f := startRecorder(t)
	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")
	f.api.waitForEvent(redisKind, "cache", "SwitchoverStarted")
	f.clock.advance(time.Minute)

	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")

	eventually(t, "the Event counts two", func() bool {
		events := f.api.eventsAbout(redisKind, "cache")
		return len(events) == 1 && events[0].Count == 2
	})
	event := f.api.eventsAbout(redisKind, "cache")[0]
	mustMatch(t, event.LastTimestamp.Equal(f.clock.now()), true)
	mustMatch(t, event.FirstTimestamp.Equal(f.clock.now().Add(-time.Minute)), true)
}

func TestRecorderWritesADifferentMessageAsANewEvent(t *testing.T) {
	f := startRecorder(t)
	f.r.normal(testEventObject, "SwitchoverStarted", "from cache-0 to cache-1")

	f.r.normal(testEventObject, "SwitchoverStarted", "from cache-1 to cache-2")

	eventually(t, "two Events", func() bool { return len(f.api.eventsAbout(redisKind, "cache")) == 2 })
}

func TestRecorderStartsANewEventAfterTheSeriesWindow(t *testing.T) {
	f := startRecorder(t)
	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")
	f.api.waitForEvent(redisKind, "cache", "SwitchoverStarted")
	f.clock.advance(eventSeriesWindow)

	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")

	eventually(t, "two Events", func() bool { return len(f.api.eventsAbout(redisKind, "cache")) == 2 })
}

func TestRecorderStartsANewEventWhenTheTTLDeletedTheLastOne(t *testing.T) {
	f := startRecorder(t)
	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")
	first := f.api.waitForEvent(redisKind, "cache", "SwitchoverStarted")
	f.api.delete(eventsResource, testNamespace, first.Metadata.Name)

	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")

	eventually(t, "a new Event", func() bool { return len(f.api.eventsAbout(redisKind, "cache")) == 1 })
	mustMatch(t, f.api.eventsAbout(redisKind, "cache")[0].Count, int32(1))
}

func TestRecorderSendsAFailedWriteAgain(t *testing.T) {
	f := startRecorder(t)
	path := eventsResource.path(testNamespace, "")
	f.api.refuseNext("POST", path, 503, eventAttempts-1)

	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")

	f.api.waitForEvent(redisKind, "cache", "SwitchoverStarted")
	mustMatch(t, f.api.requestCount("POST", path, false), eventAttempts)
	mustMatch(t, f.log.lines()[0], "")
}

func TestRecorderGivesUpAfterItsAttemptsWithOneLogLine(t *testing.T) {
	f := startRecorder(t)
	path := eventsResource.path(testNamespace, "")
	f.api.refuse("POST", path, 503)

	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")
	f.r.normal(testEventObject, "SwitchoverDone", "the primary moved")

	eventually(t, "every attempt", func() bool { return f.api.requestCount("POST", path, false) == 2*eventAttempts })
	eventually(t, "the log line", func() bool { return strings.Contains(strings.Join(f.log.lines(), "\n"), "writing the Normal Event") })
	mustMatch(t, len(f.log.lines()), 1)
}

func TestRecorderDropsAnEventInANamespaceThatIsGone(t *testing.T) {
	f := startRecorder(t)
	path := eventsResource.path(testNamespace, "")
	f.api.refuse("POST", path, 404)

	f.r.normal(testEventObject, "SwitchoverStarted", "moving the primary")

	eventually(t, "one attempt", func() bool { return f.api.requestCount("POST", path, false) == 1 })
	f.r.normal(testEventObject, "SwitchoverDone", "the primary moved")
	eventually(t, "the next Event's attempt", func() bool { return f.api.requestCount("POST", path, false) == 2 })
	mustMatch(t, f.log.lines()[0], "")
}

func TestLeavingNamespaceTellsANamespaceDeleteFromOtherRefusals(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"gone", errNotFound, true},
		{"terminating", &statusError{code: 403, text: `{"details":{"causes":[{"reason":"NamespaceTerminating"}]}}`}, true},
		{"forbidden by RBAC", &statusError{code: 403, text: "events is forbidden"}, false},
		{"unavailable", &statusError{code: 503, text: "the API server is restarting"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mustMatch(t, leavingNamespace(tc.err), tc.want)
		})
	}
}

func TestRecorderDropsAnEventWhenItsQueueIsFull(t *testing.T) {
	log := &syncBuffer{}
	clock := &testClock{at: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	r := newEventRecorder(startFakeKube(t).client, clock.now, newLineLog(log))

	for range eventQueueLength + 2 {
		r.normal(testEventObject, "SwitchoverStarted", "moving the primary")
	}

	mustMatch(t, r.dropped.Load(), uint64(2))
	mustMatch(t, len(log.lines()), 1)
}

func TestCutTextShortensAtACharacterBoundary(t *testing.T) {
	cases := []struct {
		text  string
		limit int
		want  string
	}{
		{"short", 10, "short"},
		{"abcdefghij", 6, "abc…"},
		{"ééééé", 6, "é…"},
	}
	for _, tc := range cases {
		mustMatch(t, cutText(tc.text, tc.limit), tc.want)
	}
}
