package main

// These tests hold fakeKube to the rules of the API server that the
// reconciler depends on. Each one goes through kubeClient, so they
// also prove the client's paths, methods, and bodies.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
)

func TestFakeKubeGetsAnObjectThatATestCreated(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	var got widget
	mustSucceed(t, api.client.get(t.Context(), widgets, "shop", "gear", &got))

	mustMatch(t, got.Metadata.Name, "gear")
	mustMatch(t, got.Metadata.ResourceVersion, "1")
	mustMatch(t, got.Metadata.Generation, 1)
	mustMatch(t, got.Metadata.UID, "uid-1")
	mustMatch(t, got.Spec["size"], any("small"))
}

func TestFakeKubeAnswersNotFoundForAMissingObject(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)

	err := api.client.get(t.Context(), widgets, "shop", "gear", &widget{})

	mustMatch(t, err, errNotFound)
}

func TestFakeKubeListsOneNamespaceOrAll(t *testing.T) {
	t.Parallel()
	cases := []struct {
		namespace string
		names     string
	}{
		{"", "nut,bolt,gear"},
		{"shop", "bolt,gear"},
		{"garage", "nut"},
		{"empty", ""},
	}
	for _, one := range cases {
		t.Run(one.namespace, func(t *testing.T) {
			api := startFakeKube(t)
			api.create(widgets, newWidget("garage", "nut"))
			api.create(widgets, newWidget("shop", "gear"))
			api.create(widgets, newWidget("shop", "bolt"))

			var list widgetList
			mustSucceed(t, api.client.list(t.Context(), widgets, one.namespace, "", &list))

			mustMatch(t, list.Metadata.ResourceVersion, "3")
			mustMatch(t, widgetNames(list.Items), one.names)
		})
	}
}

// widgetNames joins the names in the order of the list, which is by
// namespace, then by name.
func widgetNames(items []widget) string {
	names := []string{}
	for _, item := range items {
		names = append(names, item.Metadata.Name)
	}
	return strings.Join(names, ",")
}

func TestFakeKubeServesCoreResources(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	configMaps := kubeResource{Version: "v1", Resource: "configmaps"}
	api.create(configMaps, map[string]any{"metadata": map[string]any{"namespace": "shop", "name": "settings"}, "data": map[string]any{"color": "blue"}})

	var got struct {
		Data map[string]string `json:"data"`
	}
	mustSucceed(t, api.client.get(t.Context(), configMaps, "shop", "settings", &got))

	mustMatch(t, got.Data["color"], "blue")
	mustMatch(t, api.requests()[0].Path, "/api/v1/namespaces/shop/configmaps/settings")
}

func TestFakeKubeIncrementsGenerationOnlyForASpecChange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		change     func(object map[string]any)
		generation int
		version    string
	}{
		{"spec", func(object map[string]any) { object["spec"] = map[string]any{"size": "large"} }, 2, "2"},
		{"labels", func(object map[string]any) { metadataOf(object)["labels"] = map[string]any{"team": "a"} }, 1, "2"},
		{"nothing", func(object map[string]any) {}, 1, "1"},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			api := startFakeKube(t)
			api.create(widgets, newWidget("shop", "gear"))

			api.update(widgets, "shop", "gear", one.change)

			var got widget
			api.read(widgets, "shop", "gear", &got)
			mustMatch(t, got.Metadata.Generation, one.generation)
			mustMatch(t, got.Metadata.ResourceVersion, one.version)
		})
	}
}

// openWidgetWatch opens a watch on one namespace's widgets, and
// returns a function that reads the next event.
func openWidgetWatch(t *testing.T, api *fakeKube, namespace, resourceVersion string) func() (string, widget) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	t.Cleanup(cancel)
	resp, err := api.client.watch(ctx, widgets, namespace, "", resourceVersion)
	mustSucceed(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	mustMatch(t, resp.StatusCode, http.StatusOK)
	decoder := json.NewDecoder(resp.Body)
	return func() (string, widget) {
		t.Helper()
		var event struct {
			Type   string `json:"type"`
			Object widget `json:"object"`
		}
		mustSucceed(t, decoder.Decode(&event))
		return event.Type, event.Object
	}
}

func TestFakeKubeWatchStreamsChangesAfterAVersion(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	api.create(widgets, newWidget("garage", "nut"))
	next := openWidgetWatch(t, api, "shop", "2")

	api.create(widgets, newWidget("shop", "bolt"))
	api.update(widgets, "shop", "gear", func(object map[string]any) { object["spec"] = map[string]any{"size": "large"} })
	api.update(widgets, "garage", "nut", func(object map[string]any) { object["spec"] = map[string]any{"size": "large"} })
	api.delete(widgets, "shop", "bolt")

	kind, object := next()
	mustMatch(t, kind+" "+object.Metadata.Name+" "+object.Metadata.ResourceVersion, "ADDED bolt 3")
	kind, object = next()
	mustMatch(t, kind+" "+object.Metadata.Name+" "+object.Metadata.ResourceVersion, "MODIFIED gear 4")
	kind, object = next()
	mustMatch(t, kind+" "+object.Metadata.Name+" "+object.Metadata.ResourceVersion, "DELETED bolt 6")
}

func TestFakeKubeWatchFromZeroStartsWithEveryObject(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	next := openWidgetWatch(t, api, "", "0")

	api.create(widgets, newWidget("shop", "bolt"))

	kind, object := next()
	mustMatch(t, kind+" "+object.Metadata.Name, "ADDED gear")
	kind, object = next()
	mustMatch(t, kind+" "+object.Metadata.Name, "ADDED bolt")
}

func TestFakeKubeWatchFromACompactedVersionIsGone(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	api.create(widgets, newWidget("shop", "bolt"))
	api.compact()
	resp, err := api.client.watch(t.Context(), widgets, "", "", "1")
	mustSucceed(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	var event struct {
		Type   string `json:"type"`
		Object struct {
			Code int `json:"code"`
		} `json:"object"`
	}
	mustSucceed(t, json.NewDecoder(resp.Body).Decode(&event))

	mustMatch(t, event.Type, "ERROR")
	mustMatch(t, event.Object.Code, http.StatusGone)
}

func TestFakeKubeSetsFinalizersAtTheVersionTheClientRead(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	var got widget
	err := api.client.setFinalizers(t.Context(), widgets, "shop", "gear", "1", []string{"example.com/cleanup"}, &got)

	mustSucceed(t, err)
	mustMatch(t, slices.Equal(got.Metadata.Finalizers, []string{"example.com/cleanup"}), true)
	mustMatch(t, got.Metadata.ResourceVersion, "2")
	mustMatch(t, got.Metadata.Generation, 1)
	mustMatch(t, api.requests()[0].ContentType, "application/merge-patch+json")
}

func TestFakeKubeRefusesFinalizersAtAnOldVersion(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	api.update(widgets, "shop", "gear", func(object map[string]any) { object["spec"] = map[string]any{"size": "large"} })

	err := api.client.setFinalizers(t.Context(), widgets, "shop", "gear", "1", []string{"example.com/cleanup"}, nil)

	mustMatch(t, err, errConflict)
	var got widget
	api.read(widgets, "shop", "gear", &got)
	mustMatch(t, len(got.Metadata.Finalizers), 0)
}

func TestFakeKubeDeletesAnObjectWithNoFinalizers(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	api.delete(widgets, "shop", "gear")

	mustMatch(t, api.read(widgets, "shop", "gear", &widget{}), false)
}

func TestFakeKubeKeepsADeletedObjectUntilTheLastFinalizerIsRemoved(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	gear := newWidget("shop", "gear")
	gear.Metadata.Finalizers = []string{"example.com/one", "example.com/two"}
	api.create(widgets, gear)

	api.delete(widgets, "shop", "gear")
	var deleting widget
	mustMatch(t, api.read(widgets, "shop", "gear", &deleting), true)
	mustMatch(t, deleting.Metadata.DeletionTimestamp != "", true)

	mustSucceed(t, api.client.setFinalizers(t.Context(), widgets, "shop", "gear", deleting.Metadata.ResourceVersion, []string{"example.com/two"}, nil))
	mustMatch(t, api.read(widgets, "shop", "gear", &widget{}), true)

	mustSucceed(t, api.client.setFinalizers(t.Context(), widgets, "shop", "gear", "3", nil, nil))
	mustMatch(t, api.read(widgets, "shop", "gear", &widget{}), false)
}

func TestFakeKubeRefusesANewFinalizerOnADeletedObject(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	gear := newWidget("shop", "gear")
	gear.Metadata.Finalizers = []string{"example.com/one"}
	api.create(widgets, gear)
	api.delete(widgets, "shop", "gear")

	err := api.client.setFinalizers(t.Context(), widgets, "shop", "gear", "2", []string{"example.com/one", "example.com/two"}, nil)

	mustMatch(t, err != nil && !errors.Is(err, errConflict), true)
}

func TestFakeKubeAppliesStatusAndKeepsTheSpec(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	var got widget
	err := api.client.applyStatus(t.Context(), widgets, "Widget", "shop", "gear", map[string]any{"color": "blue"}, &got)

	mustSucceed(t, err)
	mustMatch(t, got.Status["color"], any("blue"))
	mustMatch(t, got.Spec["size"], any("small"))
	mustMatch(t, got.Metadata.Generation, 1)
	sent := api.requests()[0]
	mustMatch(t, sent.Path, "/apis/example.com/v1/namespaces/shop/widgets/gear/status")
	mustMatch(t, sent.ContentType, "application/apply-patch+yaml")
	mustMatch(t, sent.Query.Get("fieldManager"), "redis-operator")
	mustMatch(t, sent.Query.Get("force"), "true")
}

// A field that another writer set stays. A field that this manager
// applied before and leaves out now is removed, the way server-side
// apply removes it.
func TestFakeKubeApplyRemovesOnlyTheFieldsTheManagerDropped(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	gear := newWidget("shop", "gear")
	gear.Status = map[string]any{"owner": "someone else"}
	api.create(widgets, gear)
	first := map[string]any{"color": "blue", "probe": map[string]any{"result": "fail", "reason": "timeout"}}
	mustSucceed(t, api.client.applyStatus(t.Context(), widgets, "Widget", "shop", "gear", first, nil))

	second := map[string]any{"probe": map[string]any{"result": "pass"}}
	mustSucceed(t, api.client.applyStatus(t.Context(), widgets, "Widget", "shop", "gear", second, nil))

	var got widget
	api.read(widgets, "shop", "gear", &got)
	encoded, _ := json.Marshal(got.Status)
	mustMatch(t, string(encoded), `{"owner":"someone else","probe":{"result":"pass"}}`)
}

func TestFakeKubeApplyOfTheSameStatusChangesNothing(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	status := map[string]any{"color": "blue"}
	mustSucceed(t, api.client.applyStatus(t.Context(), widgets, "Widget", "shop", "gear", status, nil))

	var got widget
	mustSucceed(t, api.client.applyStatus(t.Context(), widgets, "Widget", "shop", "gear", status, &got))

	mustMatch(t, got.Metadata.ResourceVersion, "2")
}

// This is the shape of a reconciler test: seed an object, run the
// writes one pass makes, and read back the finalizers and the status.
func TestFakeKubeHoldsTheResultOfAPass(t *testing.T) {
	t.Parallel()
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	var read widget
	mustSucceed(t, api.client.get(t.Context(), widgets, "shop", "gear", &read))
	mustSucceed(t, api.client.setFinalizers(t.Context(), widgets, "shop", "gear", read.Metadata.ResourceVersion, []string{"example.com/cleanup"}, nil))
	mustSucceed(t, api.client.applyStatus(t.Context(), widgets, "Widget", "shop", "gear", map[string]any{"observedGeneration": read.Metadata.Generation}, nil))

	var got widget
	api.read(widgets, "shop", "gear", &got)
	mustMatch(t, slices.Equal(got.Metadata.Finalizers, []string{"example.com/cleanup"}), true)
	mustMatch(t, got.Status["observedGeneration"], any(float64(1)))
}
