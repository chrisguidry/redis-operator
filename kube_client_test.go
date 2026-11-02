package main

import (
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKubeResourcePaths(t *testing.T) {
	jobs := kubeResource{Group: "batch", Version: "v1", Resource: "jobs"}
	secrets := kubeResource{Version: "v1", Resource: "secrets"}
	projects := kubeResource{Group: "redis.guid.foo", Version: "v1alpha1", Resource: "redis"}
	cases := []struct {
		resource  kubeResource
		namespace string
		name      string
		want      string
	}{
		{jobs, "", "", "/apis/batch/v1/jobs"},
		{jobs, "backups", "", "/apis/batch/v1/namespaces/backups/jobs"},
		{secrets, "backups", "api-key", "/api/v1/namespaces/backups/secrets/api-key"},
		{projects, "", "internal", "/apis/redis.guid.foo/v1alpha1/redis/internal"},
	}
	for _, one := range cases {
		t.Run(one.want, func(t *testing.T) {
			mustMatch(t, one.resource.path(one.namespace, one.name), one.want)
		})
	}
}

// serviceAccountFor writes the files the kubelet mounts in a pod, for
// an API server at server, and points the client's environment at it.
func serviceAccountFor(t *testing.T, server *httptest.Server) {
	t.Helper()
	dir := t.TempDir()
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	mustSucceed(t, os.WriteFile(filepath.Join(dir, "ca.crt"), certificate, 0o600))
	mustSucceed(t, os.WriteFile(filepath.Join(dir, "token"), []byte("secret-token\n"), 0o600))
	was := serviceAccountDir
	serviceAccountDir = dir
	t.Cleanup(func() { serviceAccountDir = was })

	address, err := url.Parse(server.URL)
	mustSucceed(t, err)
	host, port, err := net.SplitHostPort(address.Host)
	mustSucceed(t, err)
	t.Setenv("KUBERNETES_SERVICE_HOST", host)
	t.Setenv("KUBERNETES_SERVICE_PORT", port)
}

// The client trusts the mounted CA, and sends the mounted token.
func TestInClusterClientReadsTheServiceAccount(t *testing.T) {
	var authorization string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(server.Close)
	serviceAccountFor(t, server)

	client, err := inClusterKubeClient()
	mustSucceed(t, err)
	mustSucceed(t, client.get(t.Context(), widgets, "shop", "gear", &widget{}))

	mustMatch(t, authorization, "Bearer secret-token")
}

func TestInClusterClientNeedsTheServiceEnvironment(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	_, err := inClusterKubeClient()

	mustMatch(t, err != nil, true)
}

// testKubeClient talks to handler and waits milliseconds where the
// API server asks for seconds.
func testKubeClient(t *testing.T, handler http.HandlerFunc) *kubeClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := newKubeClient(server.URL, server.Client(), "")
	client.retryUnit = time.Millisecond
	return client
}

// The API server answers 429 while it starts the storage for a new
// CRD. The client waits and asks again.
func TestAThrottledRequestIsAskedAgain(t *testing.T) {
	answers := 0
	client := testKubeClient(t, func(w http.ResponseWriter, r *http.Request) {
		answers++
		if answers < 3 {
			w.Header().Set("Retry-After", "1")
			writeKubeStatus(w, http.StatusTooManyRequests, "storage is (re)initializing")
			return
		}
		_, _ = w.Write([]byte(`{"metadata":{"name":"gear"}}`))
	})

	var got widget
	mustSucceed(t, client.get(t.Context(), widgets, "shop", "gear", &got))

	mustMatch(t, got.Metadata.Name, "gear")
	mustMatch(t, answers, 3)
}

func TestRetryAfterReadsSeconds(t *testing.T) {
	cases := []struct {
		header string
		want   time.Duration
	}{
		{"3", 3 * time.Second},
		{"", time.Second},
		{"soon", time.Second},
		{"0", time.Second},
	}
	for _, one := range cases {
		t.Run(one.header, func(t *testing.T) {
			mustMatch(t, retryAfter(one.header, time.Second), one.want)
		})
	}
}

// An error includes the API server's own message, so a log line or a
// status condition says what went wrong.
func TestAnErrorIncludesTheServersMessage(t *testing.T) {
	client := testKubeClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeKubeStatus(w, http.StatusForbidden, `secrets "api-key" is forbidden`)
	})

	err := client.get(t.Context(), widgets, "shop", "gear", &widget{})

	mustMatch(t, strings.Contains(err.Error(), `403 Forbidden: {"apiVersion":"v1","code":403,"kind":"Status","message":"secrets \"api-key\" is forbidden"`), true)
	mustMatch(t, errors.Is(err, errNotFound), false)
}
