package main

import (
	"encoding/json"
	"slices"
	"testing"
)

// templateOf builds the StatefulSet of a Redis with spec, and reads its
// pod template through JSON, as the API server receives it.
func templateOf(t *testing.T, spec RedisSpec) (map[string]string, map[string]templateContainer) {
	t.Helper()
	r := &Redis{Metadata: ObjectMeta{Name: "cache", Namespace: testNamespace}, Spec: spec}
	raw, err := json.Marshal(statefulSet(r, "operator-image", nil, 1).body)
	mustSucceed(t, err)
	var set struct {
		Spec struct {
			Template struct {
				Metadata struct {
					Annotations map[string]string `json:"annotations"`
				} `json:"metadata"`
			} `json:"template"`
		} `json:"spec"`
	}
	mustSucceed(t, json.Unmarshal(raw, &set))
	return set.Spec.Template.Metadata.Annotations, templateContainers(t, spec)
}

func envValue(c templateContainer, name string) string {
	for _, env := range c.Env {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

func portNamed(c templateContainer, name string) int {
	for _, port := range c.Ports {
		if port.Name == name {
			return port.ContainerPort
		}
	}
	return 0
}

func TestAPodWithTLSServesItAndSaysSo(t *testing.T) {
	cases := []struct {
		name       string
		tls        *TLSSpec
		annotation string
		secret     string
		port       int
		exporter   []string
	}{
		{"without tls", nil, "", "", 0, []string{"-redis.addr=redis://127.0.0.1:6379"}},
		{"with an issuer", &TLSSpec{IssuerRef: &IssuerRef{Name: "ca"}}, "true", "cache-tls", tlsPort,
			[]string{"-redis.addr=rediss://127.0.0.1:6380", "-skip-tls-verification"}},
		{"with a secret", &TLSSpec{Secret: "own-tls", Plaintext: true}, "true", "own-tls", tlsPort,
			[]string{"-redis.addr=rediss://127.0.0.1:6380", "-skip-tls-verification"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			annotations, containers := templateOf(t, RedisSpec{TLS: c.tls})

			mustMatch(t, annotations[tlsAnnotation], c.annotation)
			mustMatch(t, envValue(containers["redis"], "TLS_SECRET"), c.secret)
			mustMatch(t, portNamed(containers["redis"], "redis-tls"), c.port)
			for _, arg := range c.exporter {
				mustMatch(t, slices.Contains(containers[exporterContainer].Args, arg), true)
			}
		})
	}
}

// A change to plaintext is read at run time, so it does not roll the
// pods.
func TestPlaintextLeavesThePodTemplateAlone(t *testing.T) {
	r := func(plaintext bool) *Redis {
		return &Redis{Metadata: ObjectMeta{Name: "cache", Namespace: testNamespace}, Spec: RedisSpec{TLS: &TLSSpec{Secret: "own-tls", Plaintext: plaintext}}}
	}
	open, err := json.Marshal(statefulSet(r(true), "operator-image", nil, 1).body)
	mustSucceed(t, err)
	closed, err := json.Marshal(statefulSet(r(false), "operator-image", nil, 1).body)
	mustSucceed(t, err)

	mustMatch(t, string(open), string(closed))
}

func TestEveryServiceServesTLSBesidePlaintext(t *testing.T) {
	r := &Redis{Metadata: ObjectMeta{Name: "cache", Namespace: testNamespace}, Spec: RedisSpec{TLS: &TLSSpec{Secret: "own-tls"}}}
	for _, each := range children(r, "operator-image", nil, nil, 1) {
		if each.resource != servicesResource {
			continue
		}
		t.Run(each.name, func(t *testing.T) {
			ports := each.body["spec"].(map[string]any)["ports"].([]map[string]any)

			mustMatch(t, len(ports), 2)
			mustMatch(t, ports[1]["name"], any("redis-tls"))
			mustMatch(t, ports[1]["port"], any(tlsPort))
		})
	}
}

func TestTheCertificateCoversTheServicesAndEveryPod(t *testing.T) {
	r := &Redis{Metadata: ObjectMeta{Name: "cache", Namespace: "apps"}, Spec: RedisSpec{TLS: &TLSSpec{IssuerRef: &IssuerRef{Name: "ca", Kind: "ClusterIssuer"}}}}

	c := certificate(r)

	spec := c.body["spec"].(map[string]any)
	mustMatch(t, c.name, "cache-tls")
	mustMatch(t, c.remove, false)
	mustMatch(t, spec["secretName"], any("cache-tls"))
	mustMatchMap(t, spec["issuerRef"].(map[string]string), map[string]string{"name": "ca", "kind": "ClusterIssuer", "group": "cert-manager.io"})
	names := spec["dnsNames"].([]string)
	for _, name := range []string{
		"cache-rw", "cache-rw.apps", "cache-rw.apps.svc", "cache-rw.apps.svc.cluster.local",
		"cache-ro.apps.svc", "*.cache-nodes.apps.svc", "*.cache-nodes.apps.svc.cluster.local",
	} {
		mustMatch(t, slices.Contains(names, name), true)
	}
	mustMatch(t, slices.Equal(spec["ipAddresses"].([]string), []string{"127.0.0.1"}), true)
}

func TestARedisWithoutAnIssuerHasNoCertificate(t *testing.T) {
	cases := []struct {
		name string
		tls  *TLSSpec
	}{
		{"without tls", nil},
		{"with its own secret", &TLSSpec{Secret: "own-tls"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &Redis{Metadata: ObjectMeta{Name: "cache", Namespace: testNamespace}, Spec: RedisSpec{TLS: c.tls}}

			mustMatch(t, certificate(r).remove, true)
		})
	}
}

func TestTheConnectionSecretNamesTheWayIn(t *testing.T) {
	cases := []struct {
		name string
		tls  *TLSSpec
		want map[string]string
	}{
		{"without tls", nil, map[string]string{
			"password": "pw", "host": "cache-rw.apps.svc", "port": "6379",
			"uri": "redis://:pw@cache-rw.apps.svc:6379", "ro-uri": "redis://:pw@cache-ro.apps.svc:6379",
		}},
		{"with tls and plaintext", &TLSSpec{Secret: "own-tls", Plaintext: true}, map[string]string{
			"password": "pw", "host": "cache-rw.apps.svc", "port": "6379",
			"uri": "redis://:pw@cache-rw.apps.svc:6379", "ro-uri": "redis://:pw@cache-ro.apps.svc:6379",
			"tls-port": "6380", "tls-uri": "rediss://:pw@cache-rw.apps.svc:6380", "tls-ro-uri": "rediss://:pw@cache-ro.apps.svc:6380",
		}},
		{"with tls alone", &TLSSpec{Secret: "own-tls"}, map[string]string{
			"password": "pw", "host": "cache-rw.apps.svc", "port": "6380",
			"uri": "rediss://:pw@cache-rw.apps.svc:6380", "ro-uri": "rediss://:pw@cache-ro.apps.svc:6380",
			"tls-port": "6380", "tls-uri": "rediss://:pw@cache-rw.apps.svc:6380", "tls-ro-uri": "rediss://:pw@cache-ro.apps.svc:6380",
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &Redis{Metadata: ObjectMeta{Name: "cache", Namespace: "apps"}, Spec: RedisSpec{TLS: c.tls}}

			mustMatchMap(t, secretData(r, "pw"), c.want)
		})
	}
}
