package main

// This is a Kubernetes client written against the HTTP API. The API is
// HTTPS that serves JSON, and client-go would bring informers, work
// queues, and generated types that this operator does not use.
//
// Every pod has what it needs to reach the API server. Kubernetes sets
// two environment variables with the server's in-cluster address, and
// the kubelet mounts a CA certificate and a ServiceAccount token at a
// known path.

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// serviceAccountDir is a variable so a test can point it at a directory
// it controls.
var serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// These two answers are normal states, not faults. The caller answers
// an absent object by skipping it, and a conflict by reading again.
// statusError is an answer outside 2xx that is not a 404 or a 409,
// with the code kept so a caller can tell a refusal that may clear
// from one that cannot.
type statusError struct {
	code int
	text string
}

func (e *statusError) Error() string { return e.text }

// transient reports whether err may clear if the request is tried
// again: a refusal by RBAC, which a GitOps apply can fix at any moment,
// an overloaded or failing server, or a network error.
func transient(err error) bool {
	if status, ok := errors.AsType[*statusError](err); ok {
		return status.code == http.StatusForbidden || status.code == http.StatusTooManyRequests || status.code >= 500
	}
	var network net.Error
	return errors.As(err, &network)
}

var (
	errNotFound = errors.New("not found")
	errConflict = errors.New("conflict: the object changed since it was read")
)

// fieldManager names this operator's server-side apply writes, so the
// API server gives it the fields it applies and leaves other writers'
// fields alone.
const fieldManager = "redis-operator"

const (
	jsonContentType       = "application/json"
	mergePatchContentType = "application/merge-patch+json"
	// The apply media type is named for YAML and accepts JSON, because
	// YAML is a superset of JSON.
	applyContentType = "application/apply-patch+yaml"
)

// kubeResource names one kind of object in the API, such as
// {"", "v1", "secrets"} or {"batch", "v1", "jobs"}.
type kubeResource struct {
	Group    string
	Version  string
	Resource string
}

// path is the URL path of one namespace's collection, or of one object
// when name is set. An empty namespace gives the collection across all
// namespaces, which is also the path of a cluster-scoped resource.
func (r kubeResource) path(namespace, name string) string {
	path := "/apis/" + r.Group + "/" + r.Version
	if r.Group == "" {
		path = "/api/" + r.Version
	}
	if namespace != "" {
		path += "/namespaces/" + url.PathEscape(namespace)
	}
	path += "/" + r.Resource
	if name != "" {
		path += "/" + url.PathEscape(name)
	}
	return path
}

func (r kubeResource) String() string {
	if r.Group == "" {
		return r.Resource
	}
	return r.Resource + "." + r.Group
}

type kubeClient struct {
	base        string
	http        *http.Client
	credentials string
	// retryUnit is one second of a 429's Retry-After. It is a field so a
	// test waits milliseconds.
	retryUnit time.Duration
	// observe, when set, counts each request by its verb, its resource,
	// and its answer's code (kube_requests.go).
	observe func(verb, resource, code string)
}

// newKubeClient builds a client from its parts. inClusterKubeClient
// reads them from the pod. A test passes an httptest server's URL and
// no credentials, so the client reads no token.
func newKubeClient(base string, httpClient *http.Client, credentials string) *kubeClient {
	return &kubeClient{base: base, http: httpClient, credentials: credentials, retryUnit: time.Second}
}

func inClusterKubeClient() (*kubeClient, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in a cluster: KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT is unset")
	}
	// The client trusts the cluster's CA and not the system store, so it
	// accepts this API server and no other server on the address.
	caPEM, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("reading the service account CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("the service account CA has no certificates")
	}
	return newKubeClient("https://"+net.JoinHostPort(host, port), &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: roots},
			// Each timeout bounds a server that stops answering. There is
			// no timeout for the whole request, because a watch response
			// never ends, and a deadline would cut the stream.
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 10 * time.Second}).DialContext,
			ResponseHeaderTimeout: 10 * time.Second,
			IdleConnTimeout:       30 * time.Second,
		},
	}, serviceAccountDir), nil
}

// send makes one request and returns the open response. The context
// governs the whole exchange, the body included, so cancelling it ends
// the read of a watch stream.
func (c *kubeClient) send(ctx context.Context, method, path, contentType string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return nil, err
	}
	// The kubelet replaces the mounted token before it expires, so the
	// client reads the file on every request. A token held in memory
	// would start to get 401 answers.
	if c.credentials != "" {
		token, err := os.ReadFile(c.credentials + "/token")
		if err != nil {
			return nil, fmt.Errorf("reading the service account token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	req.Header.Set("Accept", jsonContentType)
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if c.observe != nil {
		resource, _ := requestResource(path)
		c.observe(requestVerb(method, path, req.URL.Query().Get("watch") == "true"), resource, requestCode(resp, err))
	}
	return resp, err
}

// request makes one request and decodes the answer into out. A 429
// waits as long as the API server asks, then asks again, until ctx
// ends. Any other status outside 2xx is an error with the server's own
// message.
func (c *kubeClient) request(ctx context.Context, method, path, contentType string, body []byte, out any) error {
	for {
		resp, err := c.send(ctx, method, path, contentType, body)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			defer drain(resp.Body)
			return decodeAnswer(resp, method, path, out)
		}
		wait := retryAfter(resp.Header.Get("Retry-After"), c.retryUnit)
		message := responseText(resp.Body)
		drain(resp.Body)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, message)
		case <-time.After(wait):
		}
	}
}

func decodeAnswer(resp *http.Response, method, path string, out any) error {
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return errNotFound
	case resp.StatusCode == http.StatusConflict:
		return errConflict
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		return &statusError{code: resp.StatusCode, text: fmt.Sprintf("%s %s: %s: %s", method, path, resp.Status, responseText(resp.Body))}
	case out == nil:
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// retryAfter reads how long a 429 asks the client to wait. The API
// server sends 429 for a second or two while the storage for a new CRD
// starts. Without a usable header, the client waits one unit.
func retryAfter(header string, unit time.Duration) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds > 0 {
		return time.Duration(seconds) * unit
	}
	return unit
}

// responseText is the start of an answer's body, for an error that
// includes the server's own text. The API server ends its Status body
// with a newline, which the text leaves out, so a log line stays on
// one line.
func responseText(body io.Reader) string {
	message, _ := io.ReadAll(io.LimitReader(body, 2048))
	return strings.TrimRightFunc(string(message), unicode.IsSpace)
}

// drain reads what the caller left in the body, then closes it. Go
// returns a connection to its pool only when the body reaches EOF, so
// an early close costs a new connection and TLS handshake.
func drain(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 4<<20))
	_ = body.Close()
}

// get reads one object into out. An empty namespace reads a
// cluster-scoped object.
func (c *kubeClient) get(ctx context.Context, resource kubeResource, namespace, name string, out any) error {
	return c.request(ctx, http.MethodGet, resource.path(namespace, name), "", nil, out)
}

// list reads a collection into out, which is a list type with
// metadata.resourceVersion and items. An empty namespace lists every
// namespace. A selector, such as "team=a", lists only the objects whose
// labels match it, and an empty one lists them all. The list's
// resourceVersion is where a watch resumes.
func (c *kubeClient) list(ctx context.Context, resource kubeResource, namespace, selector string, out any) error {
	return c.listSelected(ctx, resource, namespace, selector, "", out)
}

// listSelected is list with a field selector too, such as
// "metadata.name=gear", which RBAC matches against a rule's
// resourceNames.
func (c *kubeClient) listSelected(ctx context.Context, resource kubeResource, namespace, selector, fields string, out any) error {
	path := resource.path(namespace, "")
	query := url.Values{}
	if selector != "" {
		query.Set("labelSelector", selector)
	}
	if fields != "" {
		query.Set("fieldSelector", fields)
	}
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	return c.request(ctx, http.MethodGet, path, "", nil, out)
}

// watch opens a stream of changes to a collection, starting after
// resourceVersion, for the objects that selector matches. The caller
// owns the response body. Bookmarks keep the resume point current
// while nothing changes.
func (c *kubeClient) watch(ctx context.Context, resource kubeResource, namespace, selector, resourceVersion string) (*http.Response, error) {
	return c.watchSelected(ctx, resource, namespace, selector, "", resourceVersion)
}

// watchSelected is watch with a field selector too.
func (c *kubeClient) watchSelected(ctx context.Context, resource kubeResource, namespace, selector, fields, resourceVersion string) (*http.Response, error) {
	query := url.Values{
		"watch":               {"true"},
		"allowWatchBookmarks": {"true"},
		"resourceVersion":     {resourceVersion},
	}
	if selector != "" {
		query.Set("labelSelector", selector)
	}
	if fields != "" {
		query.Set("fieldSelector", fields)
	}
	return c.send(ctx, http.MethodGet, resource.path(namespace, "")+"?"+query.Encode(), "", nil)
}

// create creates an object. An object of the same name that already
// exists is errConflict. out receives the created object, and may be
// nil.
func (c *kubeClient) create(ctx context.Context, resource kubeResource, namespace string, object, out any) error {
	body, err := json.Marshal(object)
	if err != nil {
		return err
	}
	return c.request(ctx, http.MethodPost, resource.path(namespace, ""), jsonContentType, body, out)
}

// mergePatch changes an object with a JSON merge patch. A patch that
// states metadata.resourceVersion is refused with errConflict if the
// object changed since the caller read that version. out receives the
// patched object, and may be nil.
func (c *kubeClient) mergePatch(ctx context.Context, resource kubeResource, namespace, name string, patch, out any) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return c.request(ctx, http.MethodPatch, resource.path(namespace, name), mergePatchContentType, body, out)
}

// mergePatchStatus changes an object's status subresource with a JSON
// merge patch. A patch that states metadata.resourceVersion is refused
// with errConflict if the object changed since the caller read it, so
// a decision written from a stale read never lands.
func (c *kubeClient) mergePatchStatus(ctx context.Context, resource kubeResource, namespace, name string, patch, out any) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return c.request(ctx, http.MethodPatch, resource.path(namespace, name)+"/status", mergePatchContentType, body, out)
}

// setFinalizers replaces an object's finalizers with a merge patch that
// states the resourceVersion the caller read, so a finalizer never
// comes off an object whose newer version the caller has not seen. out
// receives the patched object, and may be nil.
func (c *kubeClient) setFinalizers(ctx context.Context, resource kubeResource, namespace, name, resourceVersion string, finalizers []string, out any) error {
	if finalizers == nil {
		finalizers = []string{}
	}
	type meta struct {
		ResourceVersion string   `json:"resourceVersion"`
		Finalizers      []string `json:"finalizers"`
	}
	return c.mergePatch(ctx, resource, namespace, name, struct {
		Metadata meta `json:"metadata"`
	}{meta{resourceVersion, finalizers}}, out)
}

// apply writes an object with server-side apply, and creates it when
// it does not exist. object states every field this operator owns,
// and its apiVersion, kind, name, and namespace. The API server
// removes a field this manager applied before and no longer states.
// force takes a field from another manager, so an object that another
// tool created becomes this operator's. out receives the written
// object, and may be nil.
func (c *kubeClient) apply(ctx context.Context, resource kubeResource, namespace, name string, object, out any) error {
	body, err := json.Marshal(object)
	if err != nil {
		return err
	}
	query := url.Values{"fieldManager": {fieldManager}, "force": {"true"}}
	path := resource.path(namespace, name) + "?" + query.Encode()
	return c.request(ctx, http.MethodPatch, path, applyContentType, body, out)
}

// delete deletes one object, on the condition that its uid is uid. An
// object made again under the same name has another uid, and the API
// server refuses the delete with errConflict, so the caller deletes
// only the object it read. An empty uid deletes the object whatever its
// uid. An object that is already gone is errNotFound, which the caller
// can take as done.
func (c *kubeClient) delete(ctx context.Context, resource kubeResource, namespace, name, uid, resourceVersion string) error {
	type preconditions struct {
		// The API server reads a uid that is present and empty as a
		// condition no object meets, so an empty one is left out.
		UID             *string `json:"uid,omitempty"`
		ResourceVersion string  `json:"resourceVersion,omitempty"`
	}
	var condition preconditions
	condition.ResourceVersion = resourceVersion
	if uid != "" {
		condition.UID = &uid
	}
	body, err := json.Marshal(struct {
		APIVersion    string        `json:"apiVersion"`
		Kind          string        `json:"kind"`
		Preconditions preconditions `json:"preconditions"`
	}{"v1", "DeleteOptions", condition})
	if err != nil {
		return err
	}
	return c.request(ctx, http.MethodDelete, resource.path(namespace, name), jsonContentType, body, nil)
}

// statusApply is the partial object an apply sends: the identity the
// API server matches, and the status. It has no spec, so an apply
// never states a field that a person declared.
type statusApply struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace,omitempty"`
	} `json:"metadata"`
	Status any `json:"status"`
}

// applyStatus writes an object's status subresource with server-side
// apply. The API server removes a field this manager applied before
// and no longer states, so a value that stops being true does not stay
// behind. force settles a conflict for this manager, because no other
// writer owns these status fields. out receives the written object,
// and may be nil.
func (c *kubeClient) applyStatus(ctx context.Context, resource kubeResource, kind, namespace, name string, status, out any) error {
	apply := statusApply{APIVersion: resource.Group + "/" + resource.Version, Kind: kind, Status: status}
	if resource.Group == "" {
		apply.APIVersion = resource.Version
	}
	apply.Metadata.Name = name
	apply.Metadata.Namespace = namespace
	body, err := json.Marshal(apply)
	if err != nil {
		return err
	}
	query := url.Values{"fieldManager": {fieldManager}, "force": {"true"}}
	path := resource.path(namespace, name) + "/status?" + query.Encode()
	return c.request(ctx, http.MethodPatch, path, applyContentType, body, out)
}
