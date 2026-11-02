package main

import "testing"

func TestAShortHostResolvesInTheRedisNamespace(t *testing.T) {
	cases := []struct{ host, want string }{
		{"store", "store.example.svc"},
		{"store:8333", "store.example.svc:8333"},
		{"store.other", "store.other"},
		{"s3.example.com", "s3.example.com"},
		{"10.0.0.5", "10.0.0.5"},
		{"10.0.0.5:6379", "10.0.0.5:6379"},
		{"localhost", "localhost"},
	}
	for _, c := range cases {
		t.Run(c.host, func(t *testing.T) {
			mustMatch(t, qualifyHost(c.host, "example"), c.want)
		})
	}
}

func TestAShortEndpointResolvesInTheRedisNamespace(t *testing.T) {
	store, err := newS3Store("http://store:8333", "", "s3://backups", "a", "b", "example")
	mustSucceed(t, err)

	mustMatch(t, store.endpoint.Host, "store.example.svc:8333")
}
