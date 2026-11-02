package main

// These tests hold fakeKube to the API server's rules for a create and
// a merge patch through kubeClient, which the Lease relies on.

import "testing"

func TestFakeKubeCreatesAnObjectThroughTheClient(t *testing.T) {
	api := startFakeKube(t)

	var got widget
	err := api.client.create(t.Context(), widgets, "shop", newWidget("shop", "gear"), &got)

	mustSucceed(t, err)
	mustMatch(t, got.Metadata.ResourceVersion, "1")
	mustMatch(t, api.read(widgets, "shop", "gear", &widget{}), true)
	mustMatch(t, api.requests()[0].Method, "POST")
}

func TestFakeKubeRefusesACreateOfANameThatExists(t *testing.T) {
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	err := api.client.create(t.Context(), widgets, "shop", newWidget("shop", "gear"), nil)

	mustMatch(t, err, errConflict)
}

func TestFakeKubeMergesAPatchAtTheVersionTheClientRead(t *testing.T) {
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))

	var got map[string]any
	err := api.client.mergePatch(t.Context(), widgets, "shop", "gear", map[string]any{
		"metadata": map[string]any{"resourceVersion": "1"},
		"spec":     map[string]any{"size": "large"},
	}, &got)

	mustSucceed(t, err)
	mustMatch(t, got["spec"].(map[string]any)["size"], any("large"))
}

func TestFakeKubeRefusesAMergePatchAtAnOldVersion(t *testing.T) {
	api := startFakeKube(t)
	api.create(widgets, newWidget("shop", "gear"))
	api.update(widgets, "shop", "gear", func(object map[string]any) {
		object["spec"] = map[string]any{"size": "medium"}
	})

	err := api.client.mergePatch(t.Context(), widgets, "shop", "gear", map[string]any{
		"metadata": map[string]any{"resourceVersion": "1"},
		"spec":     map[string]any{"size": "large"},
	}, nil)

	mustMatch(t, err, errConflict)
}
