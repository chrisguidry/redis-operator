package main

// The children of each Redis, as a pass applies them: the objects
// children.go makes, applied only when their bodies change, and the
// connection Secret, whose password the operator makes once.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// ensureChildren applies each child whose body changed since the
// operator last applied it, and keeps the Secret. replicas is the
// StatefulSet's scale (reconcile_scale.go).
func (c *controller) ensureChildren(ctx context.Context, r *Redis, st *redisState, w *world, replicas int, primaryLabeled bool) error {
	if err := c.ensureSecret(ctx, r, st); err != nil {
		return err
	}
	for _, each := range children(r, c.operatorImage, managerSecrets(r, w), restoreVolume(r, w), replicas, primaryLabeled) {
		err := c.ensureChild(ctx, r, st, each)
		if !each.optional {
			if err != nil {
				return err
			}
			continue
		}
		subject := "redis " + objectKey(r.Metadata)
		if err != nil {
			c.log.fault(subject, topicOptional, fmt.Sprintf("%s; is cert-manager installed?", errorText(err)))
			continue
		}
		c.log.clear(subject, topicOptional)
	}
	return nil
}

func (c *controller) ensureChild(ctx context.Context, r *Redis, st *redisState, each child) error {
	key := each.resource.String() + "/" + each.name
	if each.remove {
		if st.applied[key] == removedChild {
			return nil
		}
		err := c.client.delete(ctx, each.resource, r.Metadata.Namespace, each.name, "", "")
		if err != nil && !errors.Is(err, errNotFound) {
			return fmt.Errorf("deleting %s %s: %w", each.resource, each.name, err)
		}
		st.applied[key] = removedChild
		return nil
	}
	body, err := json.Marshal(each.body)
	if err != nil {
		return err
	}
	if st.applied[key] == string(body) {
		return nil
	}
	if err := c.client.apply(ctx, each.resource, r.Metadata.Namespace, each.name, each.body, nil); err != nil {
		return fmt.Errorf("applying %s %s: %w", each.resource, each.name, err)
	}
	st.applied[key] = string(body)
	return nil
}

// ensureSecret creates the connection Secret once, with a new password,
// and rewrites its other keys when a change to the spec changes them,
// such as turning TLS on. It reads the Secret only when what those keys
// should say differs from what it last made them say.
func (c *controller) ensureSecret(ctx context.Context, r *Redis, st *redisState) error {
	name := secretName(r.Metadata.Name)
	shape, err := json.Marshal(secretData(r, ""))
	if err != nil {
		return err
	}
	if st.secretData == string(shape) {
		return nil
	}
	var existing struct {
		Data map[string][]byte `json:"data"`
	}
	err = c.client.get(ctx, secretsResource, r.Metadata.Namespace, name, &existing)
	switch {
	case errors.Is(err, errNotFound):
		if err := c.client.create(ctx, secretsResource, r.Metadata.Namespace, secret(r, newPassword()), nil); err != nil && !errors.Is(err, errConflict) {
			return fmt.Errorf("creating secret %s: %w", name, err)
		}
		c.log.printf("redis %s: created secret %s", objectKey(r.Metadata), name)
	case err != nil:
		return fmt.Errorf("reading secret %s: %w", name, err)
	default:
		password := string(existing.Data["password"])
		if password == "" {
			return fmt.Errorf("secret %s has no password; delete it, and the operator makes a new one", name)
		}
		want := secretData(r, password)
		changes := map[string]any{}
		for _, key := range secretKeys {
			value, wanted := want[key]
			current, present := existing.Data[key]
			switch {
			case wanted && string(current) != value:
				changes[key] = []byte(value)
			case !wanted && present:
				changes[key] = nil
			}
		}
		if len(changes) > 0 {
			if err := c.client.mergePatch(ctx, secretsResource, r.Metadata.Namespace, name, map[string]any{"data": changes}, nil); err != nil {
				return fmt.Errorf("updating secret %s: %w", name, err)
			}
			c.log.printf("redis %s: updated the addresses in secret %s", objectKey(r.Metadata), name)
		}
	}
	st.secretData = string(shape)
	return nil
}

// removedChild marks in redisState.applied a child the operator
// deleted, so it deletes it once and not on every pass.
const removedChild = "removed"

func newPassword() string {
	bytes := make([]byte, 24)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// managerSecrets names the Secrets r's instance managers may read: the
// users' passwords, the servers' certificate, and the credentials of
// r's object stores, its own and the store of the Backup it restores
// from.
func managerSecrets(r *Redis, w *world) []string {
	var names []string
	for _, name := range r.Spec.credentialSecrets(r.Metadata.Name) {
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	if r.Spec.Backup != nil && r.Spec.Backup.Volume == nil && r.Spec.Backup.Credentials.Secret != "" && !slices.Contains(names, r.Spec.Backup.Credentials.Secret) {
		names = append(names, r.Spec.Backup.Credentials.Secret)
	}
	if replica := r.Spec.Replica; replica != nil {
		for _, ref := range []*SecretKeyRef{replica.Source.Password, replica.Source.sentinelPassword()} {
			if ref != nil && !slices.Contains(names, ref.Secret) {
				names = append(names, ref.Secret)
			}
		}
	}
	if b := r.Spec.Bootstrap; b != nil && b.Recovery != nil && b.Recovery.Backup != "" {
		for _, backup := range w.backups {
			if backup.Metadata.Namespace == r.Metadata.Namespace && backup.Metadata.Name == b.Recovery.Backup &&
				backup.Status.Store != nil && !slices.Contains(names, backup.Status.Store.Credentials.Secret) {
				names = append(names, backup.Status.Store.Credentials.Secret)
			}
		}
	}
	return names
}

// restoreVolume is the volume a new Redis restores from, when its
// recovery names a Backup on a volume. It stays mounted, read-only,
// while the spec names the recovery, because removing it would change
// the pod template and roll every pod after the restore.
func restoreVolume(r *Redis, w *world) *BackupVolume {
	b := r.Spec.Bootstrap
	if b == nil || b.Recovery == nil {
		return nil
	}
	if b.Recovery.Backup == "" {
		if r.Spec.Backup != nil && r.Spec.Backup.Volume != nil {
			return r.Spec.Backup.Volume
		}
		return nil
	}
	for _, backup := range w.backups {
		if backup.Metadata.Namespace == r.Metadata.Namespace && backup.Metadata.Name == b.Recovery.Backup &&
			backup.Status.Store != nil && backup.Status.Store.Volume != nil {
			return backup.Status.Store.Volume
		}
	}
	return nil
}
