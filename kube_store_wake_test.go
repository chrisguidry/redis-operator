package main

// The wake rule: a change wakes a pass only when the trimmed object
// differs from the stored copy, ignoring its resourceVersion and the
// status the operator last wrote. Every change still reaches the store.

import (
	"encoding/json"
	"testing"
)

// gadget is a trimmed type with a spec and a status, as the operator's
// own resources are.
type gadget struct {
	Metadata struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Spec struct {
		Size string `json:"size,omitempty"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase,omitempty"`
	} `json:"status"`
}

// gadgetObject is a gadget as the API server sends it, with a field the
// trimmed type leaves out.
func gadgetObject(version, size, phase string) json.RawMessage {
	return json.RawMessage(`{"metadata":{"name":"gear","namespace":"shop","resourceVersion":"` + version + `","managedFields":[{"time":"` + version + `"}]},` +
		`"spec":{"size":"` + size + `"},"status":{"phase":"` + phase + `"}}`)
}

func TestAChangeWakesOnlyWhenWhatThePassReadsChanged(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		wrote string
		next  json.RawMessage
		wakes bool
	}{
		{"a new resourceVersion and a field the trimmed type leaves out", "", gadgetObject("2", "small", "Ready"), false},
		{"a spec change", "", gadgetObject("2", "large", "Ready"), true},
		{"another writer's status", "", gadgetObject("2", "small", "Failed"), true},
		{"the status the operator wrote", `{"phase":"Failed"}`, gadgetObject("2", "small", "Failed"), false},
		{"another status than the operator wrote", `{"phase":"Failed"}`, gadgetObject("2", "small", "Pending"), true},
		{"the operator's status and a spec change", `{"phase":"Failed"}`, gadgetObject("2", "large", "Failed"), true},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			t.Parallel()
			store, _ := newTestStore(trimTo[gadget])
			store.put("shop/gear", gadgetObject("1", "small", "Ready"))
			if one.wrote != "" {
				store.wroteStatus("shop/gear", json.RawMessage(one.wrote))
			}

			mustMatch(t, store.put("shop/gear", one.next), one.wakes)
			mustMatchMap(t, storedVersions(t, store), map[string]string{"shop/gear": "2"})
		})
	}
}

func TestAddsAndDeletesWakeWhenTheStoreHeldOrGainsAnObject(t *testing.T) {
	t.Parallel()
	store, _ := newTestStore(trimTo[gadget])

	mustMatch(t, store.put("shop/gear", gadgetObject("1", "small", "Ready")), true)
	mustMatch(t, store.remove("shop/gear"), true)
	mustMatch(t, store.remove("shop/gear"), false)
}

// An object that no longer decodes leaves the store, which the pass
// must learn of.
func TestAnObjectThatStopsDecodingWakes(t *testing.T) {
	t.Parallel()
	store, _ := newTestStore(trimTo[gadget])
	store.put("shop/gear", gadgetObject("1", "small", "Ready"))

	mustMatch(t, store.put("shop/gear", json.RawMessage(`{"metadata":{"name":"gear","namespace":"shop"},"spec":{"size":7}}`)), true)
	mustMatchMap(t, storedVersions(t, store), map[string]string{})
}

// A store that holds more than the passes act on can limit which keys
// wake a pass. Every change still reaches the store.
func TestAStoreCanLimitWhichKeysWake(t *testing.T) {
	t.Parallel()
	store, _ := newTestStore(trimTo[gadget])
	store.wakeOnly(func(key string) bool { return key == "shop/gear" })

	mustMatch(t, store.put("shop/gear", gadgetObject("1", "small", "Ready")), true)
	mustMatch(t, store.put("shop/bolt", gadgetObject("2", "small", "Ready")), false)
	mustMatch(t, store.remove("shop/bolt"), false)
	mustMatchMap(t, storedVersions(t, store), map[string]string{"shop/gear": "1"})
}

// A relist that drops an object forgets the status the operator wrote
// to it, so the status of an object made again under that key wakes a
// pass like any other writer's.
func TestARelistForgetsTheStatusWrittenToAnObjectItDropped(t *testing.T) {
	t.Parallel()
	store, _ := newTestStore(trimTo[gadget])
	store.put("shop/gear", gadgetObject("1", "small", "Ready"))
	store.wroteStatus("shop/gear", json.RawMessage(`{"phase":"Failed"}`))
	store.replace(nil)
	store.put("shop/gear", gadgetObject("2", "small", "Ready"))

	mustMatch(t, store.put("shop/gear", gadgetObject("3", "small", "Failed")), true)
}

// get answers the trimmed copy the store holds, which a write compares
// its status with.
func TestGetAnswersTheTrimmedCopy(t *testing.T) {
	t.Parallel()
	store, _ := newTestStore(trimTo[gadget])
	store.put("shop/gear", gadgetObject("1", "small", "Ready"))

	held, ok := store.get("shop/gear")
	_, missing := store.get("shop/bolt")

	mustMatch(t, string(held), `{"metadata":{"name":"gear","namespace":"shop","resourceVersion":"1"},"spec":{"size":"small"},"status":{"phase":"Ready"}}`)
	mustMatch(t, ok, true)
	mustMatch(t, missing, false)
}
