package main

// A watch is a GET whose response does not end. The API server holds
// the connection open and writes one JSON event for each change. Each
// event changes the watch's store and wakes the reconcile loop, and the
// pass reads the store. Only a new watch lists the collection.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// collectionWatch keeps one collection in its store, and wakes the
// reconcile loop on every change to it.
type collectionWatch struct {
	client    *kubeClient
	resource  kubeResource
	namespace string
	// selector limits the watch to the objects whose labels match it.
	// Empty watches every object in the collection.
	selector string
	store    *objectStore
	wake     chan<- struct{}
	// restarted runs each time the watch opens a stream after the first,
	// so a metric counts the restarts.
	restarted func()

	// pause is the wait after a stream ends, before the list. backoff is
	// the longest wait after failures in a row, which double the pause
	// each time. They are fields so a test runs in milliseconds.
	pause   time.Duration
	backoff time.Duration
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
	}
}

// run watches from resourceVersion until ctx ends. The caller lists
// first, so the store is full before the watch starts. When a stream
// ends, for a dropped connection or a 410 Gone, the watch lists the
// collection again, which replaces the store, wakes the loop, and
// watches again from the list's resourceVersion. The store then holds
// every change that the gap held, and an object deleted in the gap is
// gone from it.
//
// A refused watch or a failed list writes one line when the reason
// changes, and one line when the watch works again. The wait doubles
// with each failure in a row, so a fault that lasts costs one request
// a minute.
func (w *collectionWatch) run(ctx context.Context, resourceVersion string) {
	wait := w.pause
	fault := ""
	failed := func(reason string) {
		if reason != fault {
			fmt.Fprintf(os.Stderr, "watching %s: %s\n", w.resource, reason)
		}
		fault = reason
		wait = min(wait*2, w.backoff)
	}
	for first := true; ctx.Err() == nil; first = false {
		if !first {
			w.restarted()
		}
		refused := w.stream(ctx, &resourceVersion)
		if !sleep(ctx, wait) {
			return
		}
		if refused != "" {
			failed(refused)
			continue
		}
		version, err := w.list(ctx)
		if err != nil {
			failed("listing to resume: " + err.Error())
			continue
		}
		if fault != "" {
			fmt.Fprintf(os.Stderr, "watching %s again\n", w.resource)
		}
		fault, wait = "", w.pause
		resourceVersion = version
		poke(w.wake)
	}
}

// stream opens one watch and reads it to its end, and moves
// resourceVersion to the last event. It returns the reason the API
// server refused the watch, or an empty string. A 410 Gone is not a
// refusal: the list that follows gives a current resourceVersion.
func (w *collectionWatch) stream(ctx context.Context, resourceVersion *string) string {
	resp, err := w.client.watch(ctx, w.resource, w.namespace, w.selector, *resourceVersion)
	if err != nil {
		return err.Error()
	}
	// The body is closed, not drained, because a drain would wait on a
	// stream that the server keeps open.
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		*resourceVersion = readWatchEvents(json.NewDecoder(resp.Body), w.store, *resourceVersion, w.wake)
		return ""
	case http.StatusGone:
		return ""
	}
	return resp.Status + ": " + responseText(resp.Body)
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
	if err := w.client.list(ctx, w.resource, w.namespace, w.selector, &list); err != nil {
		return "", err
	}
	w.store.replace(list.Items)
	return list.Metadata.ResourceVersion, nil
}

// readWatchEvents applies events to the store until the stream ends,
// and returns the resourceVersion of the last one. The events arrive
// in the order of the changes, so each one replaces what the store
// holds. An ERROR event ends the stream: the API server sends a 410
// Gone this way when it no longer has the version the watch started
// from. An object that does not decode leaves the store, and the
// stream goes on. An event with no readable metadata has no key to
// remove, so it ends the stream, and the list that follows puts the
// store right. A BOOKMARK moves the version and wakes nothing.
func readWatchEvents(decoder *json.Decoder, store *objectStore, resourceVersion string, wake chan<- struct{}) string {
	for {
		var event struct {
			Type   string          `json:"type"`
			Object json.RawMessage `json:"object"`
		}
		if err := decoder.Decode(&event); err != nil || event.Type == "ERROR" {
			return resourceVersion
		}
		meta, err := readStoredMeta(event.Object)
		if err != nil {
			return resourceVersion
		}
		if version := meta.Metadata.ResourceVersion; version != "" {
			resourceVersion = version
		}
		switch event.Type {
		case "ADDED", "MODIFIED":
			store.put(meta.key(), event.Object)
		case "DELETED":
			store.remove(meta.key())
		default:
			continue
		}
		poke(wake)
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
