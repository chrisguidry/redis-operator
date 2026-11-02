package main

// A small client for S3-compatible object stores: PUT an object of
// known size, GET one, and DELETE one, signed with AWS Signature
// Version 4. Backups need nothing more, and an SDK would bring far more
// than the instance manager uses.
//
// Uploads sign UNSIGNED-PAYLOAD, so a snapshot streams from disk
// without being read twice to hash it. URLs are path-style
// (https://endpoint/bucket/key), which every S3-compatible store
// accepts.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	unsignedPayload = "UNSIGNED-PAYLOAD"
	emptyPayload    = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	amzDateFormat   = "20060102T150405Z"
)

// s3Store is one bucket and key prefix in an S3-compatible store.
type s3Store struct {
	endpoint  *url.URL
	region    string
	bucket    string
	prefix    string
	accessKey string
	secretKey string
	http      *http.Client
	now       func() time.Time
}

// parseDestination reads s3://bucket/prefix into its bucket and its
// prefix, which has no leading or trailing slash.
func parseDestination(destination string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(destination, "s3://")
	if !ok {
		return "", "", fmt.Errorf("destination %q does not start with s3://", destination)
	}
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("destination %q names no bucket", destination)
	}
	return bucket, strings.Trim(prefix, "/"), nil
}

// newS3Store builds a client for destination. A single-label endpoint
// host resolves in namespace (hosts.go).
func newS3Store(endpoint, region, destination, accessKey, secretKey, namespace string) (*s3Store, error) {
	bucket, prefix, err := parseDestination(destination)
	if err != nil {
		return nil, err
	}
	if region == "" {
		region = "us-east-1"
	}
	if endpoint == "" {
		endpoint = "https://s3." + region + ".amazonaws.com"
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("endpoint %q is not a URL such as https://s3.example.com", endpoint)
	}
	parsed.Host = qualifyHost(parsed.Host, namespace)
	return &s3Store{
		endpoint: parsed, region: region, bucket: bucket, prefix: prefix,
		accessKey: accessKey, secretKey: secretKey,
		http: &http.Client{Timeout: 30 * time.Minute}, now: time.Now,
	}, nil
}

// objectURL is the path-style URL of key in the bucket.
func (s *s3Store) objectURL(key string) string {
	u := *s.endpoint
	segments := append([]string{s.bucket}, strings.Split(key, "/")...)
	for i, segment := range segments {
		segments[i] = uriEncode(segment)
	}
	u.RawPath = strings.TrimSuffix(u.Path, "/") + "/" + strings.Join(segments, "/")
	u.Path, _ = url.PathUnescape(u.RawPath)
	return u.String()
}

// put uploads size bytes from body as key.
func (s *s3Store) put(ctx context.Context, key string, body io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.objectURL(key), body)
	if err != nil {
		return err
	}
	req.ContentLength = size
	return s.send(req, unsignedPayload, nil)
}

// get downloads key. The caller closes the body.
func (s *s3Store) get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.objectURL(key), nil)
	if err != nil {
		return nil, 0, err
	}
	var resp *http.Response
	if err := s.send(req, emptyPayload, &resp); err != nil {
		return nil, 0, err
	}
	return resp.Body, resp.ContentLength, nil
}

// delete removes key. A key that is already gone is no error, as S3
// itself answers.
func (s *s3Store) delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.objectURL(key), nil)
	if err != nil {
		return err
	}
	return s.send(req, emptyPayload, nil)
}

// errNoSuchKey is a GET of a key the store does not hold.
var errNoSuchKey = errors.New("the object store holds no such key")

// send signs and sends req. When keep is not nil, a successful
// response is handed to the caller open; otherwise its body is read
// and closed.
func (s *s3Store) send(req *http.Request, payloadHash string, keep **http.Response) error {
	signRequest(req, payloadHash, s.accessKey, s.secretKey, s.region, "s3", s.now())
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if keep != nil {
			*keep = resp
			return nil
		}
		drain(resp.Body)
		return nil
	}
	message := responseText(resp.Body)
	drain(resp.Body)
	if resp.StatusCode == http.StatusNotFound && req.Method == http.MethodGet {
		return fmt.Errorf("%w: %s", errNoSuchKey, req.URL.Path)
	}
	return fmt.Errorf("%s %s: %s: %s", req.Method, req.URL.Path, resp.Status, message)
}

// signRequest adds AWS Signature Version 4 to req. It signs the Host
// header and every header the request carries, and sets X-Amz-Date and
// X-Amz-Content-Sha256 first. The steps and their names follow AWS's
// documentation of the signing process.
func signRequest(req *http.Request, payloadHash, accessKey, secretKey, region, service string, now time.Time) {
	stamp := now.UTC().Format(amzDateFormat)
	req.Header.Set("X-Amz-Date", stamp)
	if service == "s3" {
		req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	}
	signature, signedHeaders := signature(req, payloadHash, secretKey, region, service, stamp)
	scope := strings.Join([]string{stamp[:8], region, service, "aws4_request"}, "/")
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, signedHeaders, signature))
}

func signature(req *http.Request, payloadHash, secretKey, region, service, stamp string) (string, string) {
	headers := map[string]string{"host": req.URL.Host}
	for name, values := range req.Header {
		headers[strings.ToLower(name)] = strings.Join(values, ",")
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for name := range headers {
			if !yield(name) {
				return
			}
		}
	})
	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name + ":" + strings.TrimSpace(headers[name]) + "\n")
	}
	signedHeaders := strings.Join(names, ";")
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonical := strings.Join([]string{
		req.Method,
		path,
		canonicalQuery(req.URL.Query()),
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := strings.Join([]string{stamp[:8], region, service, "aws4_request"}, "/")
	toSign := strings.Join([]string{"AWS4-HMAC-SHA256", stamp, scope, hexSHA256(canonical)}, "\n")
	key := hmacSHA256([]byte("AWS4"+secretKey), stamp[:8])
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, service)
	key = hmacSHA256(key, "aws4_request")
	return hex.EncodeToString(hmacSHA256(key, toSign)), signedHeaders
}

func canonicalQuery(query url.Values) string {
	var pairs []string
	for name, values := range query {
		for _, value := range values {
			pairs = append(pairs, uriEncode(name)+"="+uriEncode(value))
		}
	}
	slices.Sort(pairs)
	return strings.Join(pairs, "&")
}

// uriEncode escapes every byte except the unreserved characters of RFC
// 3986, as SigV4 asks.
func uriEncode(value string) string {
	var out strings.Builder
	for _, b := range []byte(value) {
		switch {
		case b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z', b >= '0' && b <= '9', b == '-', b == '_', b == '.', b == '~':
			out.WriteByte(b)
		default:
			fmt.Fprintf(&out, "%%%02X", b)
		}
	}
	return out.String()
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func hexSHA256(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}
