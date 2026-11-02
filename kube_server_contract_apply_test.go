package main

// These tests hold fakeKube to the API server's rules for a label
// selector, an apply of a whole object, and a delete. Each one goes
// through kubeClient, so they also prove the client's requests.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// labeledWidget is a widget with the labels given.
func labeledWidget(t *testing.T, namespace, name string, labels map[string]string) map[string]any {
	t.Helper()
	object := toObject(t, newWidget(namespace, name))
	metadataOf(object)["labels"] = labels
	return object
}

func TestFakeKubeListsOnlyTheSelectedObjects(t *testing.T) {
	api := startFakeKube(t)
	api.create(widgets, labeledWidget(t, "shop", "gear", map[string]string{"team": "a"}))
	api.create(widgets, labeledWidget(t, "shop", "bolt", map[string]string{"team": "b"}))
	api.create(widgets, newWidget("shop", "nut"))

	var list widgetList
	mustSucceed(t, api.client.list(t.Context(), widgets, "", "team=a", &list))

	mustMatch(t, widgetNames(list.Items), "gear")
	mustMatch(t, api.requests()[0].Query.Get("labelSelector"), "team=a")
}

func TestFakeKubeWatchStreamsOnlyTheSelectedObjects(t *testing.T) {
	api := startFakeKube(t)
	resp, err := api.client.watch(t.Context(), widgets, "", "team=a", "0")
	mustSucceed(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	api.create(widgets, newWidget("shop", "nut"))
	api.create(widgets, labeledWidget(t, "shop", "gear", map[string]string{"team": "a"}))

	var event struct {
		Type   string `json:"type"`
		Object widget `json:"object"`
	}
	mustSucceed(t, json.NewDecoder(resp.Body).Decode(&event))
	mustMatch(t, event.Type+" "+event.Object.Metadata.Name, "ADDED gear")
	mustMatch(t, api.requests()[0].Query.Get("labelSelector"), "team=a")
}

func TestFakeKubeApplyCreatesAnObject(t *testing.T) {
	api := startFakeKube(t)

	var got widget
	err := api.client.apply(t.Context(), widgets, "shop", "gear", newWidget("shop", "gear"), &got)

	mustSucceed(t, err)
	mustMatch(t, got.Metadata.UID, "uid-1")
	mustMatch(t, got.Spec["size"], any("small"))
	sent := api.requests()[0]
	mustMatch(t, sent.Path, "/apis/example.com/v1/namespaces/shop/widgets/gear")
	mustMatch(t, sent.ContentType, "application/apply-patch+yaml")
	mustMatch(t, sent.Query.Get("fieldManager"), "redis-operator")
	mustMatch(t, sent.Query.Get("force"), "true")
}

// An apply takes the fields it states from any other writer, keeps the
// fields only another writer set, and removes a field it stated before
// and does not state now.
func TestFakeKubeApplyTakesOverAnObject(t *testing.T) {
	api := startFakeKube(t)
	gear := newWidget("shop", "gear")
	gear.Spec = map[string]any{"size": "large", "color": "red"}
	api.create(widgets, gear)
	first := newWidget("shop", "gear")
	first.Spec = map[string]any{"size": "small", "shape": "round"}
	mustSucceed(t, api.client.apply(t.Context(), widgets, "shop", "gear", first, nil))

	mustSucceed(t, api.client.apply(t.Context(), widgets, "shop", "gear", newWidget("shop", "gear"), nil))

	var got widget
	api.read(widgets, "shop", "gear", &got)
	mustMatchMap(t, got.Spec, map[string]any{"size": "small", "color": "red"})
}

func TestFakeKubeApplyOfTheSameObjectChangesNothing(t *testing.T) {
	api := startFakeKube(t)
	mustSucceed(t, api.client.apply(t.Context(), widgets, "shop", "gear", newWidget("shop", "gear"), nil))

	var got widget
	mustSucceed(t, api.client.apply(t.Context(), widgets, "shop", "gear", newWidget("shop", "gear"), &got))

	mustMatch(t, got.Metadata.ResourceVersion, "1")
}

func TestFakeKubeDeletesAnObjectThroughTheClient(t *testing.T) {
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	mustSucceed(t, api.client.delete(t.Context(), widgets, "shop", "gear", "uid-1", ""))

	mustMatch(t, api.read(widgets, "shop", "gear", &widget{}), false)
	mustMatch(t, api.requests()[0].Method, "DELETE")
}

func TestFakeKubeAnswersNotFoundToADeleteOfAMissingObject(t *testing.T) {
	api := startFakeKube(t)

	err := api.client.delete(t.Context(), widgets, "shop", "gear", "uid-1", "")

	mustMatch(t, err, errNotFound)
}

// A watch with a selector sends DELETED for an object that stops
// matching it, as the API server's watch cache does, so the client's
// store drops an object it no longer selects.
func TestFakeKubeWatchDeletesAnObjectThatLosesItsLabel(t *testing.T) {
	api := startFakeKube(t)
	api.create(widgets, labeledWidget(t, "shop", "gear", map[string]string{"team": "a"}))
	resp, err := api.client.watch(t.Context(), widgets, "", "team=a", "1")
	mustSucceed(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	api.update(widgets, "shop", "gear", func(object map[string]any) {
		delete(metadataOf(object), "labels")
	})

	var event struct {
		Type   string `json:"type"`
		Object widget `json:"object"`
	}
	mustSucceed(t, json.NewDecoder(resp.Body).Decode(&event))
	mustMatch(t, event.Type+" "+event.Object.Metadata.Name, "DELETED gear")
}

// A delete states the uid it read. An object made again under the same
// name has another uid, and the API server refuses to delete it.
func TestFakeKubeRefusesADeleteOfAnotherUID(t *testing.T) {
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	err := api.client.delete(t.Context(), widgets, "shop", "gear", "uid-0", "")

	mustMatch(t, err, errConflict)
	mustMatch(t, api.read(widgets, "shop", "gear", &widget{}), true)
}

func TestFakeKubeAnswersARefusedRequest(t *testing.T) {
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	api.refuse("DELETE", "/apis/example.com/v1/namespaces/shop/widgets/gear", http.StatusForbidden)

	err := api.client.delete(t.Context(), widgets, "shop", "gear", "uid-1", "")

	mustMatch(t, strings.Contains(err.Error(), "403 Forbidden"), true)
	mustMatch(t, api.read(widgets, "shop", "gear", &widget{}), true)
}
