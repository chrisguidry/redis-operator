package main

import (
	"context"
	"net/http"
	"sync"
	"testing"
)

// Each request is named by its Kubernetes verb, its resource, and the
// code the API server answered, the way the API server's own metrics
// name it.
func TestEachRequestIsNamedByVerbResourceAndCode(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	var mutex sync.Mutex
	var seen []string
	api.client.observe = func(verb, resource, code string) {
		mutex.Lock()
		defer mutex.Unlock()
		seen = append(seen, verb+" "+resource+" "+code)
	}
	ctx := context.Background()

	var held widget
	_ = api.client.get(ctx, widgets, "shop", "gear", &held)
	_ = api.client.get(ctx, widgets, "shop", "missing", &held)
	_ = api.client.list(ctx, widgets, "", "", &widgetList{})
	_ = api.client.mergePatch(ctx, widgets, "shop", "gear", map[string]any{"spec": map[string]any{"size": "large"}}, nil)
	_ = api.client.mergePatchStatus(ctx, widgets, "shop", "gear", map[string]any{"status": map[string]any{"phase": "Ready"}}, nil)
	_ = api.client.create(ctx, widgets, "shop", newWidget("shop", "bolt"), nil)
	resp, err := api.client.watch(ctx, widgets, "", "", "0")
	mustSucceed(t, err)
	_ = resp.Body.Close()
	_ = api.client.delete(ctx, widgets, "shop", "bolt", "", "")

	mutex.Lock()
	defer mutex.Unlock()
	mustMatch(t, len(seen), 8)
	for index, want := range []string{
		"get widgets 200", "get widgets 404", "list widgets 200", "patch widgets 200",
		"patch widgets/status 200", "create widgets 200", "watch widgets 200", "delete widgets 200",
	} {
		mustMatch(t, seen[index], want)
	}
}

// A request that got no answer counts under the code "error".
func TestARequestWithNoAnswerCountsAsAnError(t *testing.T) {
	t.Parallel()
	client := testKubeClient(t, func(w http.ResponseWriter, _ *http.Request) {
		panic(http.ErrAbortHandler)
	})
	var seen string
	client.observe = func(verb, resource, code string) { seen = verb + " " + resource + " " + code }

	_ = client.get(context.Background(), podsResource, "shop", "gear", &Pod{})

	mustMatch(t, seen, "get pods error")
}
