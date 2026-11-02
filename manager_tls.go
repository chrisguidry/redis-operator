package main

// The instance manager's part of TLS. It reads the certificate, key,
// and CA from their Secret through the API server, writes them where
// the server reads them, and loads them again into the running server
// when the Secret changes, so a rotated certificate needs no restart.
//
// It also decides, on each step, whether the server keeps its
// plaintext port, and whether each replication link uses TLS. Both
// depend on which pods serve TLS, so a roll that turns TLS on or off
// keeps every instance able to reach every other.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

// The files the server reads, named as the keys of the Secret, which
// are the keys cert-manager writes.
const (
	tlsCertFile = "tls.crt"
	tlsKeyFile  = "tls.key"
	tlsCAFile   = "ca.crt"
)

// tlsState is what the manager keeps about its server's TLS.
type tlsState struct {
	// ca is the CA the manager verifies its own server against.
	ca trustedCA
	// loaded is the digest of the files the server last loaded.
	loaded string
	// tlsOnly is the spec's wish to close the plaintext port, and
	// podTLS which pods of the Redis serve TLS, by name.
	tlsOnly bool
	podTLS  map[string]bool
	// plaintextOpen and tlsReplication are what the server runs with now.
	plaintextOpen  bool
	tlsReplication bool
}

// trustedCA verifies a server's chain against the CA the manager read
// last. The pool is replaced when the CA rotates, while the tls.Config
// that reads it is made once, so a connection opened beside the step
// loop, such as a backup's, never races with a rotation.
type trustedCA struct {
	pool atomic.Pointer[x509.CertPool]
}

func (c *trustedCA) set(pemBytes []byte) error {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return errors.New("ca.crt holds no PEM certificate")
	}
	c.pool.Store(pool)
	return nil
}

// clientConfig checks the server's chain and not its name. The manager
// dials its own pod's IP, which no certificate from an issuer names,
// and the servers check their peers the same way when they replicate.
func (c *trustedCA) clientConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("the server sent no certificate")
			}
			options := x509.VerifyOptions{Roots: c.pool.Load(), Intermediates: x509.NewCertPool()}
			for _, cert := range state.PeerCertificates[1:] {
				options.Intermediates.AddCert(cert)
			}
			_, err := state.PeerCertificates[0].Verify(options)
			return err
		},
	}
}

// tlsMaterial is the contents of each file, by its name.
type tlsMaterial map[string][]byte

func readTLSMaterial(ctx context.Context, client *kubeClient, namespace, secret string) (tlsMaterial, error) {
	var object struct {
		Data map[string][]byte `json:"data"`
	}
	if err := client.get(ctx, secretsResource, namespace, secret, &object); err != nil {
		return nil, fmt.Errorf("reading secret %s: %w", secret, err)
	}
	material := tlsMaterial{}
	for _, name := range []string{tlsCertFile, tlsKeyFile, tlsCAFile} {
		if len(object.Data[name]) == 0 {
			return nil, fmt.Errorf("secret %s has no %s", secret, name)
		}
		material[name] = object.Data[name]
	}
	if _, err := tls.X509KeyPair(material[tlsCertFile], material[tlsKeyFile]); err != nil {
		return nil, fmt.Errorf("secret %s: %w", secret, err)
	}
	return material, nil
}

func (t tlsMaterial) digest() string {
	sum := sha256.Sum256(bytes.Join([][]byte{t[tlsCertFile], t[tlsKeyFile], t[tlsCAFile]}, []byte{0}))
	return hex.EncodeToString(sum[:])
}

// write puts each file in dir by a rename, so the server never reads
// half of one.
func (t tlsMaterial) write(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for name, contents := range t {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path+".tmp", contents, 0o600); err != nil {
			return err
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) servesTLS() bool { return m.config.tlsSecret != "" }

// startTLS waits for the certificate's Secret, writes its files, and
// points the manager's own connection at the TLS port. It returns what
// the server's configuration says about TLS, or nil without it.
func (m *manager) startTLS(ctx context.Context, redis Redis) (*serverTLS, error) {
	if !m.servesTLS() {
		return nil, nil
	}
	for {
		material, err := readTLSMaterial(ctx, m.client, m.config.namespace, m.config.tlsSecret)
		if err == nil {
			err = m.load(material)
		}
		if err == nil {
			break
		}
		m.note("tls", fmt.Sprintf("%s: waiting for the TLS certificate: %s", m.config.podName, errorText(err)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.config.stepPeriod):
		}
	}
	m.note("tls", "")
	m.server.tls = m.ca.clientConfig()
	m.server.addr = net.JoinHostPort(m.config.podIP, strconv.Itoa(m.config.tlsPort))
	// A server that closes its plaintext port starts without it, and
	// opens it on the first step that finds a pod without TLS.
	m.tlsOnly = redis.Spec.tlsOnly()
	m.plaintextOpen = !m.tlsOnly
	return &serverTLS{port: m.config.tlsPort, dir: m.config.tlsDir, plaintextClosed: m.tlsOnly}, nil
}

// load writes the files and trusts their CA.
func (m *manager) load(material tlsMaterial) error {
	if err := material.write(m.config.tlsDir); err != nil {
		return fmt.Errorf("writing the TLS files: %w", err)
	}
	if err := m.ca.set(material[tlsCAFile]); err != nil {
		return err
	}
	m.loaded = material.digest()
	return nil
}

// readAccessDecision takes what the spec and the pods say about TLS
// and the users, from a step's read of them.
func (m *manager) readAccessDecision(redis Redis, pods []Pod) {
	m.users = redis.Spec.Users
	m.tlsOnly = redis.Spec.tlsOnly()
	m.podTLS = map[string]bool{}
	for _, pod := range pods {
		m.podTLS[pod.Metadata.Name] = pod.Metadata.Annotations[tlsAnnotation] == "true"
	}
}

// keepAccess makes the server's users, certificate, and plaintext port
// agree with the step's read of the spec.
func (m *manager) keepAccess(ctx context.Context) {
	m.applyUsers(ctx)
	if !m.servesTLS() {
		return
	}
	m.reloadTLS(ctx)
	m.applyPlaintext(ctx)
}

// reloadTLS loads a changed certificate into the running server. Both
// engines load the files again on CONFIG SET of their paths, even when
// the paths are the same.
func (m *manager) reloadTLS(ctx context.Context) {
	material, err := readTLSMaterial(ctx, m.client, m.config.namespace, m.config.tlsSecret)
	if err != nil {
		m.note("tls", fmt.Sprintf("%s: keeping the loaded TLS certificate: %s", m.config.podName, errorText(err)))
		return
	}
	if material.digest() == m.loaded {
		m.note("tls", "")
		return
	}
	if err := m.load(material); err != nil {
		m.note("tls", fmt.Sprintf("%s: keeping the loaded TLS certificate: %s", m.config.podName, errorText(err)))
		return
	}
	_, err = m.server.do(ctx, "CONFIG", "SET",
		"tls-cert-file", filepath.Join(m.config.tlsDir, tlsCertFile),
		"tls-key-file", filepath.Join(m.config.tlsDir, tlsKeyFile),
		"tls-ca-cert-file", filepath.Join(m.config.tlsDir, tlsCAFile))
	if err != nil {
		// The next step finds the files differ from what loaded and tries
		// again.
		m.loaded = ""
		m.note("tls", fmt.Sprintf("%s: loading the new TLS certificate: %s", m.config.podName, errorText(err)))
		return
	}
	m.note("tls", "")
	m.say(m.config.podName + ": loaded a new TLS certificate")
}

// applyPlaintext closes the plaintext port when the spec asks for TLS
// alone and every pod serves TLS, and opens it while any pod does not,
// since that pod's server has no certificate to reach this one with.
func (m *manager) applyPlaintext(ctx context.Context) {
	open := !m.tlsOnly
	for _, serves := range m.podTLS {
		if !serves {
			open = true
		}
	}
	if open == m.plaintextOpen {
		return
	}
	port := 0
	if open {
		port = m.config.port
	}
	if _, err := m.server.do(ctx, "CONFIG", "SET", "port", strconv.Itoa(port)); err != nil {
		m.note("plaintext", fmt.Sprintf("%s: setting the plaintext port to %d: %s", m.config.podName, port, errorText(err)))
		return
	}
	m.note("plaintext", "")
	m.plaintextOpen = open
	if open {
		m.say(fmt.Sprintf("%s: serving plaintext on %d", m.config.podName, port))
	} else {
		m.say(m.config.podName + ": closed the plaintext port; every instance serves TLS")
	}
}

// peerPort is the port to replicate from the instance named peer on,
// after it sets tls-replication to match: TLS when this pod and the
// peer's both serve it, and plaintext otherwise.
func (m *manager) peerPort(ctx context.Context, peer string) (int, error) {
	useTLS := m.servesTLS() && m.podTLS[peer]
	if err := m.setTLSReplication(ctx, useTLS); err != nil {
		return 0, err
	}
	if useTLS {
		return m.config.tlsPort, nil
	}
	return m.config.port, nil
}

func (m *manager) setTLSReplication(ctx context.Context, on bool) error {
	if on == m.tlsReplication {
		return nil
	}
	value := "no"
	if on {
		value = "yes"
	}
	if _, err := m.server.do(ctx, "CONFIG", "SET", "tls-replication", value); err != nil {
		return err
	}
	m.tlsReplication = on
	return nil
}
