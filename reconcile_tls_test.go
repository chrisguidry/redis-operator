package main

import (
	"slices"
	"strings"
	"testing"
)

// editSpec changes the harness's Redis's spec, as a person's edit does.
func (h *harness) editSpec(change func(spec map[string]any)) {
	h.api.update(redisResource, testNamespace, h.name, func(object map[string]any) {
		change(object["spec"].(map[string]any))
	})
}

// secretData reads the connection Secret's data as text.
func (h *harness) secretData() map[string]string {
	var held testSecret
	if !h.api.read(secretsResource, testNamespace, secretName(h.name), &held) {
		h.t.Fatal("the connection secret is gone")
	}
	data := map[string]string{}
	for key, value := range held.Data {
		data[key] = string(value)
	}
	return data
}

func TestARedisWithAnIssuerGetsACertificate(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.editSpec(func(spec map[string]any) {
		spec["tls"] = map[string]any{"issuerRef": map[string]any{"name": "ca", "kind": "ClusterIssuer"}}
	})

	h.pass()

	var certificate struct {
		Spec struct {
			SecretName string            `json:"secretName"`
			IssuerRef  map[string]string `json:"issuerRef"`
		} `json:"spec"`
	}
	mustMatch(t, h.api.read(certificatesResource, testNamespace, "cache-tls", &certificate), true)
	mustMatch(t, certificate.Spec.SecretName, "cache-tls")
	mustMatch(t, certificate.Spec.IssuerRef["kind"], "ClusterIssuer")
}

func TestMovingToAUsersOwnSecretDeletesTheCertificate(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.editSpec(func(spec map[string]any) {
		spec["tls"] = map[string]any{"issuerRef": map[string]any{"name": "ca"}}
	})
	h.pass()

	h.editSpec(func(spec map[string]any) { spec["tls"] = map[string]any{"secret": "own-tls"} })
	h.pass()

	var gone map[string]any
	mustMatch(t, h.api.read(certificatesResource, testNamespace, "cache-tls", &gone), false)
}

// Where cert-manager is missing, the API server has no Certificates,
// and the rest of the pass still runs, so a running Redis keeps its
// failover.
func TestAMissingCertManagerFailsOnlyTheCertificate(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.api.refuse("PATCH", certificatesResource.path(testNamespace, "cache-tls"), 404)
	h.editSpec(func(spec map[string]any) {
		spec["tls"] = map[string]any{"issuerRef": map[string]any{"name": "ca"}}
	})

	h.pass()

	mustMatch(t, h.redis().Status.TargetPrimary, "cache-0")
	lines := h.log.lines()
	mustMatch(t, slices.ContainsFunc(lines, func(line string) bool { return strings.Contains(line, "is cert-manager installed?") }), true)
}

func TestTheManagersMayReadTheUsersAndTheCertificatesSecrets(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.editSpec(func(spec map[string]any) {
		spec["users"] = []any{
			map[string]any{"name": "app", "password": map[string]any{"secret": "app-user", "key": "password"}, "rules": "+@all"},
			map[string]any{"name": "reader", "password": map[string]any{"secret": "app-user", "key": "reader"}, "rules": "+@read"},
		}
		spec["tls"] = map[string]any{"secret": "own-tls"}
	})

	h.pass()

	var role struct {
		Rules []struct {
			Resources     []string `json:"resources"`
			ResourceNames []string `json:"resourceNames"`
		} `json:"rules"`
	}
	h.api.read(rolesResource, testNamespace, "cache", &role)
	index := slices.IndexFunc(role.Rules, func(rule struct {
		Resources     []string `json:"resources"`
		ResourceNames []string `json:"resourceNames"`
	}) bool {
		return slices.Contains(rule.Resources, "secrets")
	})
	mustMatch(t, slices.Equal(role.Rules[index].ResourceNames, []string{"app-user", "own-tls"}), true)
}

func TestTurningTLSOnAndOffRewritesTheConnectionSecretAndKeepsItsPassword(t *testing.T) {
	t.Parallel()
	h := startHarness(t, 1)
	h.pass()
	password := h.secretData()["password"]

	h.editSpec(func(spec map[string]any) { spec["tls"] = map[string]any{"secret": "own-tls"} })
	h.pass()
	on := h.secretData()

	h.editSpec(func(spec map[string]any) { delete(spec, "tls") })
	h.pass()
	off := h.secretData()

	mustMatch(t, on["password"], password)
	mustMatch(t, on["uri"], "rediss://:"+password+"@cache-rw.example.svc:6380")
	mustMatch(t, on["tls-ro-uri"], "rediss://:"+password+"@cache-ro.example.svc:6380")
	mustMatch(t, off["password"], password)
	mustMatch(t, off["uri"], "redis://:"+password+"@cache-rw.example.svc:6379")
	_, kept := off["tls-uri"]
	mustMatch(t, kept, false)
}
