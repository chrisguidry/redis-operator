package main

// The wire types are hand-written, the way healthchecks-operator and the
// liken-sh operators write theirs. The Kubernetes API is HTTPS that
// serves JSON, and client-go would bring informers, work queues, and
// generated types for a dozen structs. Each type carries only the fields
// this operator reads or writes; the API server fills in the rest.

// ObjectMeta carries what this operator reads or writes.
type ObjectMeta struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	// UID tells an object from an earlier one of the same name, which a
	// delete and a create leave behind.
	UID               string            `json:"uid,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	OwnerReferences   []OwnerReference  `json:"ownerReferences,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
}

// OwnerReference makes the garbage collector delete an object when its
// owner is deleted.
type OwnerReference struct {
	APIVersion         string `json:"apiVersion"`
	Kind               string `json:"kind"`
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Controller         bool   `json:"controller,omitempty"`
	BlockOwnerDeletion bool   `json:"blockOwnerDeletion,omitempty"`
}
