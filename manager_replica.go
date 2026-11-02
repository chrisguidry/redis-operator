package main

// The instance manager's part of replica mode: the Lease holder follows
// the source outside the operator instead of serving as primary, and
// authenticates to it with the source's password. Every other instance
// follows the holder, as it always does.

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"
)

// sourceAddress resolves where the source's primary is: the host the
// spec names, or the primary its sentinels name, asked with their
// password.
func sourceAddress(ctx context.Context, source ReplicaSource, namespace, sentinelPassword string) (string, int, error) {
	if source.Sentinel == nil {
		return qualifyHost(source.Host, namespace), source.port(), nil
	}
	for _, address := range source.Sentinel.Addresses {
		address = qualifyHost(address, namespace)
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := dialRedis(attempt, address, sentinelPassword)
		if err != nil {
			cancel()
			continue
		}
		reply, err := conn.do(attempt, "SENTINEL", "GET-MASTER-ADDR-BY-NAME", source.Sentinel.MasterName)
		_ = conn.Close()
		cancel()
		items, ok := reply.([]any)
		if err != nil || !ok || len(items) != 2 {
			continue
		}
		host, _ := items[0].(string)
		port, convErr := strconv.Atoi(fmt.Sprint(items[1]))
		if host != "" && convErr == nil {
			return host, port, nil
		}
	}
	return "", 0, fmt.Errorf("no sentinel of %v names a primary for %s", source.Sentinel.Addresses, source.Sentinel.MasterName)
}

// readSecretKey reads one key of a Secret.
func readSecretKey(ctx context.Context, client *kubeClient, namespace string, ref SecretKeyRef) (string, error) {
	var secret struct {
		Data map[string][]byte `json:"data"`
	}
	if err := client.get(ctx, secretsResource, namespace, ref.Secret, &secret); err != nil {
		return "", fmt.Errorf("reading secret %s: %w", ref.Secret, err)
	}
	value, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %s has no key %s", ref.Secret, ref.Key)
	}
	return string(value), nil
}

// readSentinelPassword reads the password of the source's sentinels,
// or none when the spec names none.
func readSentinelPassword(ctx context.Context, client *kubeClient, namespace string, source ReplicaSource) (string, error) {
	ref := source.sentinelPassword()
	if ref == nil {
		return "", nil
	}
	return readSecretKey(ctx, client, namespace, *ref)
}

// followSource makes the holder's server a replica of the source.
func (m *manager) followSource(ctx context.Context, info replicationInfo) error {
	password := ""
	if ref := m.replicaSource.Password; ref != nil {
		value, err := readSecretKey(ctx, m.client, m.config.namespace, *ref)
		if err != nil {
			return err
		}
		password = value
	}
	if err := m.setMasterauth(ctx, password); err != nil {
		return err
	}
	// The source is outside the operator, and is followed over
	// plaintext.
	if err := m.setTLSReplication(ctx, false); err != nil {
		return err
	}
	sentinelPassword, err := readSentinelPassword(ctx, m.client, m.config.namespace, m.replicaSource)
	if err != nil {
		return err
	}
	host, port, err := sourceAddress(ctx, m.replicaSource, m.config.namespace, sentinelPassword)
	if err != nil {
		return err
	}
	// A source named by a hostname is followed by its address, so
	// the server's INFO and the report name what it connected to.
	if ips, err := net.DefaultResolver.LookupHost(ctx, host); err == nil && len(ips) > 0 {
		host = ips[0]
	}
	m.line(fmt.Sprintf("%s: following the source %s", m.config.podName, net.JoinHostPort(host, strconv.Itoa(port))))
	return m.server.follow(ctx, info, host, port)
}

// setMasterauth sets the password the server authenticates with when
// it replicates. In replica mode the holder uses the source's, and
// every other time it uses this Redis's own.
func (m *manager) setMasterauth(ctx context.Context, password string) error {
	if m.masterauth == password {
		return nil
	}
	if _, err := m.server.do(ctx, "CONFIG", "SET", "masterauth", password); err != nil {
		return err
	}
	m.masterauth = password
	return nil
}
