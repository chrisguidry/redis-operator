package main

// The instance manager's part of ACL users. Servers do not replicate
// their users, so every instance manager sets the spec's users on its
// own server, replicas included, and a replica that a failover promotes
// already has them.

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
)

// userState is what the manager keeps about its server's users.
type userState struct {
	users []RedisUser
	// usersSet holds a digest of the command that set each user, by
	// name. It lives as long as the server, because a server that
	// restarts starts with a new manager.
	usersSet map[string]string
}

// applyUsers sets each user whose password or rules changed since it
// last set them, and deletes each user it set that the spec no longer
// names. A user a person made by hand is left alone.
func (m *manager) applyUsers(ctx context.Context) {
	if m.usersSet == nil {
		m.usersSet = map[string]string{}
	}
	secrets := map[string]map[string][]byte{}
	named := map[string]bool{}
	for _, user := range m.users {
		named[user.Name] = true
		topic := "user " + user.Name
		password, err := secretValue(ctx, m.client, m.config.namespace, user.Password, secrets)
		if err == nil && password == "" {
			err = errors.New("the password is empty")
		}
		if err != nil {
			m.note(topic, fmt.Sprintf("%s: cannot set user %s: %s", m.config.podName, user.Name, errorText(err)))
			continue
		}
		command := user.setUser(password)
		sum := sha256.Sum256([]byte(strings.Join(command, "\x00")))
		digest := string(sum[:])
		if m.usersSet[user.Name] == digest {
			m.note(topic, "")
			continue
		}
		if _, err := m.server.do(ctx, command...); err != nil {
			m.note(topic, fmt.Sprintf("%s: cannot set user %s: %s", m.config.podName, user.Name, errorText(err)))
			continue
		}
		m.usersSet[user.Name] = digest
		m.note(topic, "")
		m.say(fmt.Sprintf("%s: set user %s", m.config.podName, user.Name))
	}
	for name := range m.usersSet {
		if named[name] {
			continue
		}
		if _, err := m.server.do(ctx, "ACL", "DELUSER", name); err != nil {
			m.note("user "+name, fmt.Sprintf("%s: cannot delete user %s: %s", m.config.podName, name, errorText(err)))
			continue
		}
		delete(m.usersSet, name)
		m.note("user "+name, "")
		m.say(fmt.Sprintf("%s: deleted user %s", m.config.podName, name))
	}
}

// secretValue reads one key of a Secret, through cache, so users that
// share a Secret cost one read a step.
func secretValue(ctx context.Context, client *kubeClient, namespace string, ref SecretKeyRef, cache map[string]map[string][]byte) (string, error) {
	data, ok := cache[ref.Secret]
	if !ok {
		var secret struct {
			Data map[string][]byte `json:"data"`
		}
		if err := client.get(ctx, secretsResource, namespace, ref.Secret, &secret); err != nil {
			return "", fmt.Errorf("reading secret %s: %w", ref.Secret, err)
		}
		data = secret.Data
		cache[ref.Secret] = data
	}
	value, ok := data[ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %s has no key %s", ref.Secret, ref.Key)
	}
	return string(value), nil
}
