package main

// The operator posts a Kubernetes Event for each decision a person
// wants to read in `kubectl describe redis` or `kubectl get events`: a
// condition that changed, a failover, a switchover, a step of a
// rollout, a promotion, and a backup that started, completed, or
// failed. A condition answers what is true now, and an Event what just
// happened. The log line of each decision stays, because a log holds
// every attempt and detail, and the API server deletes an Event an
// hour after its last write.
//
// The Events are core/v1, as liken's kubernetes/events package writes
// them, which `kubectl describe` and `kubectl get events` read. They
// state no eventTime, because the API server applies the stricter
// checks of events.k8s.io/v1 only to an Event that states one.
//
// A post never slows or fails a pass. It puts the Event on a bounded
// queue and returns, and one goroutine writes the queue, each request
// under a timeout. A full queue drops the Event. A write that fails is
// sent again a few times, and then given up with one log line. A
// repeat of the same Event on the same object within the series window
// patches count and lastTimestamp on the Event already written, so a
// condition that flaps prints one line, such as "(x12 over 8m)", and
// writes no new object to etcd.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

var eventsResource = kubeResource{"", "v1", "events"}

// eventComponent is the reportingComponent of every Event, which
// `kubectl describe` prints in its From column.
const eventComponent = "redis-operator"

const (
	eventNormal  = "Normal"
	eventWarning = "Warning"
)

// The limits the API server's validation states for an Event's reason
// and message. A longer one is cut to fit, so no Event is refused for
// its length.
const (
	maxEventReason  = 128
	maxEventMessage = 1024
)

// eventQueueLength bounds the Events that wait to be written. The
// operator posts a few Events for each object in an hour, so a full
// queue means the API server stopped answering.
const eventQueueLength = 256

// eventAttempts is how many times the recorder sends one write.
const eventAttempts = 3

type objectReference struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name"`
	UID        string `json:"uid,omitempty"`
}

func referenceTo(kind string, meta ObjectMeta) objectReference {
	return objectReference{APIVersion: apiVersion, Kind: kind, Namespace: meta.Namespace, Name: meta.Name, UID: meta.UID}
}

type eventSource struct {
	Component string `json:"component,omitempty"`
	Host      string `json:"host,omitempty"`
}

// eventMeta is the part of an Event's metadata the recorder writes and
// reads back. A create sends generateName, and the API server answers
// the name it chose, which a repeat patches.
type eventMeta struct {
	Name         string `json:"name,omitempty"`
	GenerateName string `json:"generateName,omitempty"`
	Namespace    string `json:"namespace,omitempty"`
}

// kubeEvent is a core/v1 Event as the recorder writes it.
type kubeEvent struct {
	APIVersion         string          `json:"apiVersion"`
	Kind               string          `json:"kind"`
	Metadata           eventMeta       `json:"metadata"`
	InvolvedObject     objectReference `json:"involvedObject"`
	Reason             string          `json:"reason"`
	Message            string          `json:"message"`
	Type               string          `json:"type"`
	Source             eventSource     `json:"source"`
	FirstTimestamp     time.Time       `json:"firstTimestamp"`
	LastTimestamp      time.Time       `json:"lastTimestamp"`
	Count              int32           `json:"count"`
	ReportingComponent string          `json:"reportingComponent"`
	ReportingInstance  string          `json:"reportingInstance"`
}

type eventRecorder struct {
	client   *kubeClient
	now      func() time.Time
	log      *lineLog
	instance string
	queue    chan kubeEvent
	dropped  atomic.Uint64
	// series is read and written only by the goroutine that writes the
	// queue.
	series *eventSeries
}

func newEventRecorder(client *kubeClient, now func() time.Time, log *lineLog) *eventRecorder {
	// The host name of a pod is the pod's name, which tells the
	// operator's replicas apart.
	instance, _ := os.Hostname()
	return &eventRecorder{
		client: client, now: now, log: log, instance: instance,
		queue:  make(chan kubeEvent, eventQueueLength),
		series: newEventSeries(),
	}
}

func (r *eventRecorder) normal(object objectReference, reason, message string) {
	r.post(object, eventNormal, reason, message)
}

func (r *eventRecorder) warning(object objectReference, reason, message string) {
	r.post(object, eventWarning, reason, message)
}

// post queues an Event and returns at once. A full queue drops it.
func (r *eventRecorder) post(object objectReference, kind, reason, message string) {
	now := r.now().UTC().Truncate(time.Second)
	event := kubeEvent{
		APIVersion:         "v1",
		Kind:               "Event",
		Metadata:           eventMeta{GenerateName: object.Name + ".", Namespace: object.Namespace},
		InvolvedObject:     object,
		Reason:             cutText(reason, maxEventReason),
		Message:            cutText(message, maxEventMessage),
		Type:               kind,
		Source:             eventSource{Component: eventComponent, Host: r.instance},
		FirstTimestamp:     now,
		LastTimestamp:      now,
		Count:              1,
		ReportingComponent: eventComponent,
		ReportingInstance:  r.instance,
	}
	select {
	case r.queue <- event:
	default:
		if r.dropped.Add(1) == 1 {
			r.log.printf("events: dropped the %s Event %s on %s %s/%s: %d Events wait to be written", kind, reason, object.Kind, object.Namespace, object.Name, eventQueueLength)
		}
	}
}

// run writes the queue until ctx ends. retryWait is the time between
// two sends of one write, and timeout bounds each request.
func (r *eventRecorder) run(ctx context.Context, retryWait, timeout time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-r.queue:
			r.write(ctx, event, retryWait, timeout)
		}
	}
}

// write sends one Event, and sends it again after a failure, up to
// eventAttempts times. An API server that restarts refuses
// connections for some seconds, which the retries cover.
func (r *eventRecorder) write(ctx context.Context, event kubeEvent, retryWait, timeout time.Duration) {
	for attempt := 1; ; attempt++ {
		err := r.writeOnce(ctx, event, timeout)
		if err == nil || leavingNamespace(err) {
			r.log.clear("events", topicReconcile)
			return
		}
		if attempt == eventAttempts {
			r.log.fault("events", topicReconcile, fmt.Sprintf("writing the %s Event %s on %s %s/%s: %s",
				event.Type, event.Reason, event.InvolvedObject.Kind, event.InvolvedObject.Namespace, event.InvolvedObject.Name, errorText(err)))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryWait):
		}
	}
}

// writeOnce patches the Event of a series that is still open, or
// creates a new Event.
func (r *eventRecorder) writeOnce(ctx context.Context, event kubeEvent, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	key := eventKey(event)
	namespace := event.Metadata.Namespace
	if held, ok := r.series.get(key); ok && event.LastTimestamp.Sub(held.last) < eventSeriesWindow {
		count := held.count + 1
		patch := map[string]any{"count": count, "lastTimestamp": event.LastTimestamp}
		err := r.client.mergePatch(ctx, eventsResource, namespace, held.name, patch, nil)
		if err == nil {
			r.series.put(key, seriesEntry{name: held.name, count: count, last: event.LastTimestamp})
			return nil
		}
		if !errors.Is(err, errNotFound) {
			return err
		}
		// The API server's TTL deleted the Event, so the series starts
		// again.
		r.series.remove(key)
	}
	var created kubeEvent
	if err := r.client.create(ctx, eventsResource, namespace, event, &created); err != nil {
		return err
	}
	r.series.put(key, seriesEntry{name: created.Metadata.Name, count: 1, last: event.LastTimestamp})
	return nil
}

// leavingNamespace reports whether a write failed because the Event's
// namespace is gone or being deleted. The namespace's delete would
// remove the Event with it, so no one would read it, and the recorder
// drops it with no retry and no log line. A create answers 404 only
// when the namespace is gone, because writeOnce handles the 404 of a
// patch.
func leavingNamespace(err error) bool {
	if errors.Is(err, errNotFound) {
		return true
	}
	status, ok := errors.AsType[*statusError](err)
	return ok && status.code == http.StatusForbidden && strings.Contains(status.text, "NamespaceTerminating")
}

// cutText shortens text to at most limit bytes, at a character
// boundary, and marks the cut with an ellipsis.
func cutText(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	const ellipsis = "…"
	end := limit - len(ellipsis)
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + ellipsis
}
