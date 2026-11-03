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

// startTestStore runs a real SeaweedFS S3 server with one identity and
// one bucket, and returns its endpoint.
func startTestStore(t *testing.T) string {
	t.Helper()
	binary, err := exec.LookPath("weed")
	if err != nil {
		t.Fatalf("weed is not on PATH; AGENTS.md says how to install it: %v", err)
	}
	dir := t.TempDir()
	config, err := json.Marshal(map[string]any{"identities": []map[string]any{{
		"name":        "test",
		"credentials": []map[string]string{{"accessKey": testAccessKey, "secretKey": testSecretKey}},
		"actions":     []string{"Admin", "Read", "Write", "List", "Tagging"},
	}}})
	mustSucceed(t, err)
	mustSucceed(t, os.WriteFile(filepath.Join(dir, "s3.json"), config, 0o600))
	ports := map[string]int{"master": freePort(t), "volume": freePort(t), "filer": freePort(t), "s3": freePort(t)}
	args := []string{"server", "-dir=" + dir, "-ip=127.0.0.1", "-ip.bind=127.0.0.1", "-s3", "-filer",
		fmt.Sprintf("-master.port=%d", ports["master"]), fmt.Sprintf("-master.port.grpc=%d", freePort(t)),
		fmt.Sprintf("-volume.port=%d", ports["volume"]), fmt.Sprintf("-volume.port.grpc=%d", freePort(t)),
		fmt.Sprintf("-filer.port=%d", ports["filer"]), fmt.Sprintf("-filer.port.grpc=%d", freePort(t)),
		fmt.Sprintf("-s3.port=%d", ports["s3"]), fmt.Sprintf("-s3.port.grpc=%d", freePort(t)),
		"-s3.config=" + filepath.Join(dir, "s3.json"), "-volume.max=5"}
	cmd := exec.Command(binary, args...)
	cmd.Dir = dir
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	mustSucceed(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("seaweedfs said:\n%s", output.String())
		}
	})
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", ports["s3"])
	store := testStore(t, endpoint, "s3://"+testBucket)
	deadline := time.Now().Add(60 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodPut, endpoint+"/"+testBucket, nil)
		if err := store.send(req, emptyPayload, nil); err == nil || strings.Contains(err.Error(), "BucketAlreadyExists") {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("seaweedfs did not create the bucket within 60 seconds: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	// A new SeaweedFS server answers its first write slowly, in about as
	// long as a test gives an upload, and later writes at once. One write
	// here takes that wait inside the startup deadline, before a test
	// starts timing anything.
	writable, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	mustSucceed(t, store.put(writable, "ready", strings.NewReader("ready"), int64(len("ready"))))
	mustSucceed(t, store.delete(writable, "ready"))
	return endpoint
}

func testStore(t *testing.T, endpoint, destination string) *s3Store {
	t.Helper()
	store, err := newS3Store(endpoint, "us-east-1", destination, testAccessKey, testSecretKey, testNamespace)
	mustSucceed(t, err)
	return store
}

func TestTheStoreKeepsWhatItIsGiven(t *testing.T) {
	t.Parallel()
	store := testStore(t, startTestStore(t), "s3://"+testBucket+"/some prefix")
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
	store := testStore(t, startTestStore(t), "s3://"+testBucket)
	ctx := context.Background()
	mustSucceed(t, store.put(ctx, "key", strings.NewReader("value"), 5))

	mustSucceed(t, store.delete(ctx, "key"))

	_, _, err := store.get(ctx, "key")
	mustMatch(t, errors.Is(err, errNoSuchKey), true)
}

func TestAWrongSecretIsRefused(t *testing.T) {
	t.Parallel()
	endpoint := startTestStore(t)
	store, err := newS3Store(endpoint, "", "s3://"+testBucket, testAccessKey, "wrong", testNamespace)
	mustSucceed(t, err)

	err = store.put(context.Background(), "key", strings.NewReader("value"), 5)

	mustMatch(t, err != nil && strings.Contains(err.Error(), "403"), true)
}
