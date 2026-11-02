package main

// Each test here has its own servers, ports, and fake API server, so
// they run in parallel, which keeps the suite within go test's timeout.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// testCA signs certificates the way an issuer does, for servers that
// tests start with TLS.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	mustSucceed(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	mustSucceed(t, err)
	cert, err := x509.ParseCertificate(der)
	mustSucceed(t, err)
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue signs a server certificate named name, which names no host:
// the instances check the chain and not the name.
func (ca *testCA) issue(t *testing.T, name string) tlsMaterial {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	mustSucceed(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	mustSucceed(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	mustSucceed(t, err)
	return tlsMaterial{
		tlsCertFile: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		tlsKeyFile:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		tlsCAFile:   ca.pem,
	}
}

type testSecret struct {
	Metadata ObjectMeta        `json:"metadata"`
	Data     map[string][]byte `json:"data"`
}

// putSecret creates the Secret name with data, or replaces its data.
func putSecret(api *fakeKube, name string, data map[string][]byte) {
	var existing map[string]any
	if !api.read(secretsResource, testNamespace, name, &existing) {
		api.create(secretsResource, testSecret{Metadata: ObjectMeta{Name: name, Namespace: testNamespace}, Data: data})
		return
	}
	encoded := toObject(api.t, testSecret{Data: data})["data"]
	api.update(secretsResource, testNamespace, name, func(object map[string]any) { object["data"] = encoded })
}

// annotateTLS sets on each pod whether its template serves TLS, as the
// StatefulSet's template does.
func (h *managerHarness) annotateTLS(serves bool, names ...string) {
	for _, name := range names {
		h.api.update(podsResource, testNamespace, name, func(object map[string]any) {
			meta := object["metadata"].(map[string]any)
			if serves {
				meta["annotations"] = map[string]any{tlsAnnotation: "true"}
			} else {
				delete(meta, "annotations")
			}
		})
	}
}

// tlsHarness declares a Redis with TLS from the Secret cache-tls,
// signed by ca, whose pods all serve TLS.
func tlsHarness(t *testing.T, engine string, instances int, ca *testCA, change func(*TLSSpec)) *managerHarness {
	t.Helper()
	h := startManagerHarnessOn(t, startFakeKube(t), "cache", engine, instances, func(spec *RedisSpec) {
		spec.TLS = &TLSSpec{Secret: "cache-tls"}
		if change != nil {
			change(spec.TLS)
		}
	})
	putSecret(h.api, "cache-tls", ca.issue(t, "first"))
	var names []string
	for i := range instances {
		names = append(names, instanceName("cache", i))
	}
	h.annotateTLS(true, names...)
	h.tlsSecret = "cache-tls"
	return h
}

// tlsClient reaches the server of name on its TLS port, trusting ca.
func (h *managerHarness) tlsClient(name string, ca *testCA) *server {
	trust := &trustedCA{}
	mustSucceed(h.t, trust.set(ca.pem))
	s := &server{addr: net.JoinHostPort(h.ip(name), strconv.Itoa(h.tlsPort)), password: testPassword, tls: trust.clientConfig()}
	h.t.Cleanup(s.close)
	return s
}

// servedName is the common name of the certificate the server of name
// serves now.
func (h *managerHarness) servedName(name string) string {
	conn, err := tls.Dial("tcp", net.JoinHostPort(h.ip(name), strconv.Itoa(h.tlsPort)), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return ""
	}
	defer conn.Close()
	return conn.ConnectionState().PeerCertificates[0].Subject.CommonName
}

// plaintextRefused says whether the server of name refuses connections
// on its plaintext port.
func (h *managerHarness) plaintextRefused(name string) bool {
	conn, err := net.Dial("tcp", net.JoinHostPort(h.ip(name), strconv.Itoa(h.port)))
	if err == nil {
		_ = conn.Close()
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

func TestInstancesThatServeTLSReplicateOverTLSAndCloseThePlaintextPort(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			ca := newTestCA(t, "ca")
			h := tlsHarness(t, engine, 2, ca, nil)
			h.setTarget("cache-0")

			h.start("cache-0", "")
			h.start("cache-1", "")

			h.waitFor("cache-1 replicates cache-0", func() bool {
				report := h.report("cache-1")
				return report.LinkUp && report.Following == h.ip("cache-0") && h.report("cache-0").SyncedReplicas == 1
			})
			info, err := h.tlsClient("cache-1", ca).replication(context.Background())
			mustSucceed(t, err)
			mustMatch(t, info.masterPort, h.tlsPort)
			h.waitFor("both plaintext ports close", func() bool {
				return h.plaintextRefused("cache-0") && h.plaintextRefused("cache-1")
			})
			_, err = h.tlsClient("cache-0", ca).do(context.Background(), "SET", "key", "value")
			mustSucceed(t, err)
		})
	}
}

// A roll that turns TLS on leaves a pod without TLS beside pods with
// it. The pod without it must still follow the primary, and take the
// primary role from it.
func TestAnInstanceWithoutTLSReplicatesOverPlaintextAndTakesASwitchover(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			ca := newTestCA(t, "ca")
			h := tlsHarness(t, engine, 2, ca, nil)
			h.annotateTLS(false, "cache-1")
			h.setTarget("cache-0")
			h.start("cache-0", "")
			h.tlsSecret = ""
			h.start("cache-1", "")
			h.waitFor("cache-1 replicates cache-0", func() bool {
				return h.report("cache-1").LinkUp && h.report("cache-0").SyncedReplicas == 1
			})
			mustMatch(t, replication(t, h.client("cache-1")).masterPort, h.port)
			mustMatch(t, h.plaintextRefused("cache-0"), false)
			h.setCurrentPrimary("cache-0")

			h.switchTo("cache-1")

			h.waitFor("cache-1 holds the lease and cache-0 follows it", func() bool {
				return h.holder() == "cache-1" && h.report("cache-1").Ready && h.report("cache-0").LinkUp &&
					h.report("cache-0").Following == h.ip("cache-1")
			})
		})
	}
}

func TestThePlaintextPortFollowsTheSpecAndThePods(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t, "ca")
	h := tlsHarness(t, "redis", 2, ca, nil)
	h.annotateTLS(false, "cache-1")
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.waitFor("cache-0 is primary", func() bool { return h.report("cache-0").Role == reportPrimary })
	mustMatch(t, h.plaintextRefused("cache-0"), false)

	h.annotateTLS(true, "cache-1")
	h.waitFor("the plaintext port closes once every pod serves TLS", func() bool { return h.plaintextRefused("cache-0") })

	h.api.update(redisResource, testNamespace, "cache", func(object map[string]any) {
		object["spec"].(map[string]any)["tls"] = map[string]any{"secret": "cache-tls", "plaintext": true}
	})
	h.waitFor("the plaintext port opens when the spec asks for it", func() bool { return !h.plaintextRefused("cache-0") })
}

func TestARotatedCertificateIsServedWithoutARestart(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			ca := newTestCA(t, "ca")
			h := tlsHarness(t, engine, 1, ca, nil)
			h.setTarget("cache-0")
			h.start("cache-0", "")
			h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })
			mustMatch(t, h.servedName("cache-0"), "first")
			before := serverRunID(t, h.tlsClient("cache-0", ca))

			next := newTestCA(t, "next ca")
			putSecret(h.api, "cache-tls", next.issue(t, "second"))

			h.waitFor("cache-0 serves the new certificate", func() bool { return h.servedName("cache-0") == "second" })
			mustMatch(t, serverRunID(t, h.tlsClient("cache-0", next)), before)
			// The manager trusts the new CA on its next connection.
			fresh := h.instances["cache-0"].manager.server.another()
			t.Cleanup(fresh.close)
			_, err := fresh.do(context.Background(), "PING")
			mustSucceed(t, err)
		})
	}
}

func serverRunID(t *testing.T, s *server) string {
	t.Helper()
	reply, err := s.do(context.Background(), "INFO", "server")
	mustSucceed(t, err)
	for line := range strings.SplitSeq(reply.(string), "\n") {
		if id, found := strings.CutPrefix(strings.TrimSpace(line), "run_id:"); found {
			return id
		}
	}
	t.Fatal("INFO server has no run_id")
	return ""
}

func TestAManagerWaitsForItsCertificateBeforeItStartsTheServer(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t, "ca")
	h := startManagerHarnessOn(t, startFakeKube(t), "cache", "redis", 1, func(spec *RedisSpec) {
		spec.TLS = &TLSSpec{Secret: "cache-tls"}
	})
	h.annotateTLS(true, "cache-0")
	h.tlsSecret = "cache-tls"
	h.setTarget("cache-0")
	h.start("cache-0", "")
	time.Sleep(300 * time.Millisecond)
	mustMatch(t, h.servedName("cache-0"), "")

	putSecret(h.api, "cache-tls", ca.issue(t, "first"))

	h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })
	mustMatch(t, h.servedName("cache-0"), "first")
}

func TestASecretWithoutTheFilesIsRefused(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t, "ca")
	full := ca.issue(t, "first")
	other := ca.issue(t, "other")
	cases := []struct {
		name string
		data map[string][]byte
		want string
	}{
		{"no ca.crt", map[string][]byte{tlsCertFile: full[tlsCertFile], tlsKeyFile: full[tlsKeyFile]}, "has no ca.crt"},
		{"a key of another certificate", map[string][]byte{tlsCertFile: full[tlsCertFile], tlsKeyFile: other[tlsKeyFile], tlsCAFile: ca.pem}, "private key does not match"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			api := startFakeKube(t)
			putSecret(api, "cache-tls", c.data)

			_, err := readTLSMaterial(context.Background(), api.client, testNamespace, "cache-tls")

			mustMatch(t, err != nil && strings.Contains(err.Error(), c.want), true)
		})
	}
}
