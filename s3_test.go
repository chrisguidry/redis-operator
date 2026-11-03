package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The example in AWS's documentation of Signature Version 4: an IAM
// ListUsers request, signed at a known time with the documented example
// keys.
func TestTheSignerMatchesAWSsExample(t *testing.T) {
	t.Parallel()
	req, err := http.NewRequest(http.MethodGet, "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08", nil)
	mustSucceed(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	at := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)

	signRequest(req, emptyPayload, "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "us-east-1", "iam", at)

	mustMatch(t, req.Header.Get("Authorization"),
		"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, "+
			"SignedHeaders=content-type;host;x-amz-date, "+
			"Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7")
}

func TestParseDestination(t *testing.T) {
	t.Parallel()
	cases := []struct {
		destination, bucket, prefix, err string
	}{
		{"s3://backups/redis/house", "backups", "redis/house", ""},
		{"s3://backups/", "backups", "", ""},
		{"s3://backups", "backups", "", ""},
		{"https://backups", "", "", `destination "https://backups" does not start with s3://`},
		{"s3:///prefix", "", "", `destination "s3:///prefix" names no bucket`},
	}
	for _, c := range cases {
		t.Run(c.destination, func(t *testing.T) {
			bucket, prefix, err := parseDestination(c.destination)
			if c.err != "" {
				mustMatch(t, err.Error(), c.err)
				return
			}
			mustSucceed(t, err)
			mustMatch(t, bucket, c.bucket)
			mustMatch(t, prefix, c.prefix)
		})
	}
}

const (
	testAccessKey = "test-access-key"
	testSecretKey = "test-secret-key"
	testBucket    = "backups"
)

// testS3 is one bucket of the package's SeaweedFS server.
type testS3 struct {
	endpoint string
	bucket   string
}

// The package's SeaweedFS server starts on the first test that needs a
// store, since it takes seconds to start, and stops when the tests
// end. Each test gets a bucket of its own, so tests that list or delete
// objects see only their own.
var sharedStore struct {
	once     sync.Once
	endpoint string
	err      error
	stop     func()
	buckets  atomic.Int64
}

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedStore.stop != nil {
		sharedStore.stop()
	}
	os.Exit(code)
}

// startTestStore creates a new bucket on the package's SeaweedFS S3
// server, which has one identity, and starts the server first if no
// test has.
func startTestStore(t *testing.T) testS3 {
	t.Helper()
	sharedStore.once.Do(func() { sharedStore.endpoint, sharedStore.stop, sharedStore.err = startSeaweedFS() })
	if sharedStore.err != nil {
		t.Fatal(sharedStore.err)
	}
	bucket := fmt.Sprintf("test-%d", sharedStore.buckets.Add(1))
	store := testStore(t, sharedStore.endpoint, "s3://"+bucket)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, sharedStore.endpoint+"/"+bucket, nil)
	mustSucceed(t, store.send(req, emptyPayload, nil))
	return testS3{endpoint: sharedStore.endpoint, bucket: bucket}
}

// startSeaweedFS runs a real SeaweedFS S3 server with one identity, and
// returns its endpoint and how to stop it.
func startSeaweedFS() (string, func(), error) {
	binary, err := exec.LookPath("weed")
	if err != nil {
		return "", nil, fmt.Errorf("weed is not on PATH; AGENTS.md says how to install it: %v", err)
	}
	dir, err := os.MkdirTemp("", "redis-operator-seaweedfs-")
	if err != nil {
		return "", nil, err
	}
	config, err := json.Marshal(map[string]any{"identities": []map[string]any{{
		"name":        "test",
		"credentials": []map[string]string{{"accessKey": testAccessKey, "secretKey": testSecretKey}},
		"actions":     []string{"Admin", "Read", "Write", "List", "Tagging"},
	}}})
	if err != nil {
		return "", nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "s3.json"), config, 0o600); err != nil {
		return "", nil, err
	}
	port := func() int {
		port, portErr := takePort()
		if portErr != nil {
			err = portErr
		}
		return port
	}
	ports := map[string]int{"master": port(), "volume": port(), "filer": port(), "s3": port()}
	args := []string{"server", "-dir=" + dir, "-ip=127.0.0.1", "-ip.bind=127.0.0.1", "-s3", "-filer",
		fmt.Sprintf("-master.port=%d", ports["master"]), fmt.Sprintf("-master.port.grpc=%d", port()),
		fmt.Sprintf("-volume.port=%d", ports["volume"]), fmt.Sprintf("-volume.port.grpc=%d", port()),
		fmt.Sprintf("-filer.port=%d", ports["filer"]), fmt.Sprintf("-filer.port.grpc=%d", port()),
		fmt.Sprintf("-s3.port=%d", ports["s3"]), fmt.Sprintf("-s3.port.grpc=%d", port()),
		"-s3.config=" + filepath.Join(dir, "s3.json"), "-volume.max=50"}
	if err != nil {
		return "", nil, err
	}
	cmd := exec.Command(binary, args...)
	cmd.Dir = dir
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}
	stop := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = os.RemoveAll(dir)
	}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", ports["s3"])
	store, err := newS3Store(endpoint, "us-east-1", "s3://ready", testAccessKey, testSecretKey, testNamespace)
	if err != nil {
		stop()
		return "", nil, err
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodPut, endpoint+"/ready", nil)
		if err := store.send(req, emptyPayload, nil); err == nil || strings.Contains(err.Error(), "BucketAlreadyExists") {
			break
		} else if time.Now().After(deadline) {
			stop()
			return "", nil, fmt.Errorf("seaweedfs did not create a bucket within 60 seconds: %v\nseaweedfs said:\n%s", err, output.String())
		}
		time.Sleep(250 * time.Millisecond)
	}
	// A new SeaweedFS server answers its first write slowly, in about as
	// long as a test gives an upload, and later writes at once. One write
	// here takes that wait inside the startup deadline, before a test
	// starts timing anything.
	writable, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if err := store.put(writable, "ready", strings.NewReader("ready"), int64(len("ready"))); err != nil {
		stop()
		return "", nil, err
	}
	return endpoint, stop, nil
}

func testStore(t *testing.T, endpoint, destination string) *s3Store {
	t.Helper()
	store, err := newS3Store(endpoint, "us-east-1", destination, testAccessKey, testSecretKey, testNamespace)
	mustSucceed(t, err)
	return store
}

func TestTheStoreKeepsWhatItIsGiven(t *testing.T) {
	t.Parallel()
	s3 := startTestStore(t)
	store := testStore(t, s3.endpoint, "s3://"+s3.bucket+"/some prefix")
	ctx := context.Background()
	body := bytes.Repeat([]byte("redis"), 100_000)

	mustSucceed(t, store.put(ctx, "example/cache/snapshot one.rdb", bytes.NewReader(body), int64(len(body))))

	reader, size, err := store.get(ctx, "example/cache/snapshot one.rdb")
	mustSucceed(t, err)
	defer reader.Close()
	got, err := io.ReadAll(reader)
	mustSucceed(t, err)
	mustMatch(t, size, int64(len(body)))
	mustMatch(t, bytes.Equal(got, body), true)
}

func TestADeletedObjectIsGone(t *testing.T) {
	t.Parallel()
	s3 := startTestStore(t)
	store := testStore(t, s3.endpoint, "s3://"+s3.bucket)
	ctx := context.Background()
	mustSucceed(t, store.put(ctx, "key", strings.NewReader("value"), 5))

	mustSucceed(t, store.delete(ctx, "key"))

	_, _, err := store.get(ctx, "key")
	mustMatch(t, errors.Is(err, errNoSuchKey), true)
}

func TestAWrongSecretIsRefused(t *testing.T) {
	t.Parallel()
	s3 := startTestStore(t)
	store, err := newS3Store(s3.endpoint, "", "s3://"+s3.bucket, testAccessKey, "wrong", testNamespace)
	mustSucceed(t, err)

	err = store.put(context.Background(), "key", strings.NewReader("value"), 5)

	mustMatch(t, err != nil && strings.Contains(err.Error(), "403"), true)
}
