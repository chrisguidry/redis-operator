package main

// A watch is a GET whose response does not end. The API server holds
// the connection open and writes one JSON event for each change. Each
// event changes the watch's store, and an event that changes what the
// operator reads wakes the reconcile loop (kube_store.go). The pass
// reads the store.
//
// The loop keeps the three rules of a watch that liken's operator guide
// sets out:
//
//   - A stream that ends, cleanly or with a dropped connection, resumes
//     from the last resourceVersion it delivered, with no list. A watch
//     from no version replays every object, so it costs a list.
//   - Only a 410 Gone lists again at once, because the API server no
//     longer holds the version. A 410 from a watch that a list after a
//     410 opened, and that ended within minLife, waits out the backoff
//     first, so an API server that answers 410 to every version is
//     listed at the backoff's pace. An ERROR
//     event of another code, or an event that does not decode, waits
//     out the backoff and then lists, because the store may have missed
//     what the rest of the stream held.
//   - A watch's life decides the wait. One that ran for minLife from its
//     200 worked, whatever ended it, and resets the backoff. One that
//     ended sooner failed and grows it, so a server that accepts a watch
//     and closes it at once costs a request per backoff, not a loop.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// collectionWatch keeps one collection in its store, and wakes the
// reconcile loop when it changes.
type collectionWatch struct {
	client    *kubeClient
	resource  kubeResource
	namespace string
	// selector limits the watch to the objects whose labels match it,
	// and fields to those whose fields match it, such as
	// "metadata.name=gear". Empty watches every object in the
	// collection.
	selector string
	fields   string
	store    *objectStore
	wake     chan<- struct{}
	// restarted runs each time the watch opens a stream after the first,
	// so a metric counts the restarts.
	restarted func()

	// pause is the first wait after a failure, and backoff the longest:
	// failures in a row double the wait up to it. minLife is how long a
	// watch must run after its 200 to count as one that worked. They are
	// fields so a test runs in milliseconds.
	pause   time.Duration
	backoff time.Duration
	minLife time.Duration
}

// trim decodes an object of the collection into the operator's type
// for it, and encodes that again. trimTo gives one for each type.
func newCollectionWatch(client *kubeClient, resource kubeResource, namespace, selector string, trim func(json.RawMessage) (json.RawMessage, error), wake chan<- struct{}, restarted func()) *collectionWatch {
	return &collectionWatch{
		client:    client,
		resource:  resource,
		namespace: namespace,
		selector:  selector,
		store: newObjectStore(trim, func(key, reason string) {
			fmt.Fprintf(os.Stderr, "watching %s: skipping %s: %s\n", resource, key, reason)
		}),
		wake:      wake,
		restarted: restarted,
		pause:     2 * time.Second,
		backoff:   time.Minute,
		minLife:   time.Second,
	}
}

// streamEnd is why a watch stream ended.
type streamEnd int

const (
	// streamClosed is a stream that the server closed or the connection
	// dropped. The next watch resumes from its last version.
	streamClosed streamEnd = iota
	// streamGone is a 410 Gone, as the response or as an ERROR event.
	streamGone
	// streamBroken is an ERROR event of another code, or an event that
	// does not decode.
	streamBroken
	// streamRefused is a watch that the API server did not accept, or
	// could not be reached for.
	streamRefused
)

// streamResult is what one watch stream did: why it ended, the last
// version it delivered, and how long it ran from its 200. A stream
// that got no 200 ran for no time.
type streamResult struct {
	end     streamEnd
	version string
	life    time.Duration
	reason  string
}

// run watches from resourceVersion until ctx ends. The caller lists
// first, so the store is full before the watch starts. A list after a
// 410 or a broken stream replaces the store and wakes the loop, so the
// store holds every change that the gap held, and an object deleted in
// the gap is gone from it.
//
// A fault writes one line when its reason changes, and one line when
// the API server accepts a watch again.
func (w *collectionWatch) run(ctx context.Context, resourceVersion string) {
	wait := w.pause
	fault := ""
	failed := func(reason string) {
		if reason != fault {
			fmt.Fprintf(os.Stderr, "watching %s: %s\n", w.resource, reason)
		}
		fault = reason
	}
	backOff := func() bool {
		waited := sleep(ctx, wait)
		wait = min(wait*2, w.backoff)
		return waited
	}
	accepted := func() {
		if fault != "" {
			fmt.Fprintf(os.Stderr, "watching %s again\n", w.resource)
		}
		fault = ""
	}
	relist, fromGoneList := false, false
	for opened := 0; ctx.Err() == nil; {
		if relist {
			version, err := w.list(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				failed("listing to resume: " + err.Error())
				if !backOff() {
					return
				}
				continue
			}
			resourceVersion, relist = version, false
			poke(w.wake)
		}
		if opened > 0 {
			w.restarted()
		}
		opened++
		result := w.stream(ctx, resourceVersion, accepted)
		if ctx.Err() != nil {
			return
		}
		resourceVersion = result.version
		ran := result.life >= w.minLife
		if ran {
			wait = w.pause
		}
		afterGoneList := fromGoneList
		fromGoneList = false
		switch {
		case result.end == streamGone:
			relist, fromGoneList = true, true
			if afterGoneList && !ran && !backOff() {
				return
			}
		case result.end == streamBroken:
			failed(result.reason)
			relist = true
			if !backOff() {
				return
			}
		case result.end == streamRefused:
			failed(result.reason)
			if !backOff() {
				return
			}
		case !ran:
			if !backOff() {
				return
			}
		}
	}
}

// stream opens one watch from resourceVersion and reads it to its end.
// accepted runs when the API server answers 200.
func (w *collectionWatch) stream(ctx context.Context, resourceVersion string, accepted func()) streamResult {
	resp, err := w.client.watchSelected(ctx, w.resource, w.namespace, w.selector, w.fields, resourceVersion)
	if err != nil {
		return streamResult{end: streamRefused, version: resourceVersion, reason: err.Error()}
	}
	// The body is closed, not drained, because a drain would wait on a
	// stream that the server keeps open.
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		opened := time.Now()
		accepted()
		version, end, reason := readWatchEvents(bufio.NewReader(resp.Body), w.store, resourceVersion, w.wake)
		return streamResult{end: end, version: version, life: time.Since(opened), reason: reason}
	case http.StatusGone:
		return streamResult{end: streamGone, version: resourceVersion}
	}
	return streamResult{end: streamRefused, version: resourceVersion, reason: resp.Status + ": " + responseText(resp.Body)}
}

// list fills the store from a list of the collection, and returns the
// list's resourceVersion, where the next watch starts.
func (w *collectionWatch) list(ctx context.Context) (string, error) {
	var list struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Items []json.RawMessage `json:"items"`
	}
	if err := w.client.listSelected(ctx, w.resource, w.namespace, w.selector, w.fields, &list); err != nil {
		return "", err
	}
	w.store.replace(list.Items)
	return list.Metadata.ResourceVersion, nil
}

// readWatchEvents applies events to the store until the stream ends,
// and returns the resourceVersion of the last one and why the stream
// ended. The events arrive in the order of the changes, so each one
// replaces what the store holds. An event wakes the loop only when the
// store says the change matters to a pass. A BOOKMARK moves the
// version and wakes nothing.
//
// The API server writes one event on each line, so a line that does
// not decode is found as soon as it ends, while the server holds the
// stream open. A line that the connection cut before its newline is a
// dropped stream, not a broken one.
//
// An ERROR event ends the stream: a 410 Gone says the API server no
// longer has the version, and any other code says the stream is in a
// state the loop cannot read on from. An event that does not decode,
// or whose metadata does not, has no key to apply, so it ends the
// stream too, and the list that follows puts the store right. An
// object that decodes as an event but not into the operator's type
// leaves the store, and the stream goes on (kube_store.go).
func readWatchEvents(reader *bufio.Reader, store *objectStore, resourceVersion string, wake chan<- struct{}) (string, streamEnd, string) {
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return resourceVersion, streamClosed, ""
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event struct {
			Type   string          `json:"type"`
			Object json.RawMessage `json:"object"`
		}
		if err := json.Unmarshal(line, &event); err != nil {
			return resourceVersion, streamBroken, "an event that does not decode: " + err.Error()
		}
		if event.Type == "ERROR" {
			var status struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			}
			_ = json.Unmarshal(event.Object, &status)
			if status.Code == http.StatusGone {
				return resourceVersion, streamGone, ""
			}
			return resourceVersion, streamBroken, fmt.Sprintf("an ERROR event: %d %s", status.Code, status.Message)
		}
		meta, err := readStoredMeta(event.Object)
		if err != nil {
			return resourceVersion, streamBroken, "an event whose metadata does not decode: " + err.Error()
		}
		if version := meta.Metadata.ResourceVersion; version != "" {
			resourceVersion = version
		}
		changed := false
		switch event.Type {
		case "ADDED", "MODIFIED":
			changed = store.put(meta.key(), event.Object)
		case "DELETED":
			changed = store.remove(meta.key())
		}
		if changed {
			poke(wake)
		}
	}
}

// poke wakes the loop without blocking. The channel has a buffer of
// one, so many changes before the next pass wake it once.
func poke(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

// sleep waits and returns false if ctx ended first.
func sleep(ctx context.Context, wait time.Duration) bool {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// firstList lists until a list works, with the same backoff as a
// broken watch, and logs each new reason once. A refused first list is
// often a moment of version skew: a new operator reaches a collection
// before the RBAC rule that allows it, because GitOps applied the new
// image first. Ending the operator for it would leave the pod in a
// crash loop whose delay outlasts the skew. An error that cannot clear,
// such as a 404 for a resource the API server does not serve, returns
// at once, so it ends the operator and shows in the pod's restarts.
func (w *collectionWatch) firstList(ctx context.Context) (string, error) {
	wait := w.pause
	fault := ""
	for {
		version, err := w.list(ctx)
		if err == nil {
			if fault != "" {
				fmt.Fprintf(os.Stderr, "listing %s: recovered\n", w.resource)
			}
			return version, nil
		}
		if !transient(err) {
			return "", err
		}
		if reason := errorText(err); reason != fault {
			fmt.Fprintf(os.Stderr, "listing %s: %s\n", w.resource, reason)
			fault = reason
		}
		if !sleep(ctx, wait) {
			return "", ctx.Err()
		}
		wait = min(wait*2, w.backoff)
	}
}
