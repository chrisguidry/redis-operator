package main

// widgets is a namespaced custom resource that the tests of the client
// and of fakeKube use. It is in no API group this operator serves, so
// a test of the client reads apart from a test of the reconciler.
var widgets = kubeResource{Group: "example.com", Version: "v1", Resource: "widgets"}

type widget struct {
	APIVersion string         `json:"apiVersion,omitempty"`
	Kind       string         `json:"kind,omitempty"`
	Metadata   widgetMeta     `json:"metadata"`
	Spec       map[string]any `json:"spec,omitempty"`
	Status     map[string]any `json:"status,omitempty"`
}

type widgetMeta struct {
	Name              string   `json:"name"`
	Namespace         string   `json:"namespace,omitempty"`
	UID               string   `json:"uid,omitempty"`
	ResourceVersion   string   `json:"resourceVersion,omitempty"`
	Generation        int      `json:"generation,omitempty"`
	Finalizers        []string `json:"finalizers,omitempty"`
	DeletionTimestamp string   `json:"deletionTimestamp,omitempty"`
}

type widgetList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []widget `json:"items"`
}

func newWidget(namespace, name string) widget {
	return widget{
		APIVersion: "example.com/v1",
		Kind:       "Widget",
		Metadata:   widgetMeta{Namespace: namespace, Name: name},
		Spec:       map[string]any{"size": "small"},
	}
}
