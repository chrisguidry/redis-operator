package main

// TLS for a Redis's servers, with a certificate that cert-manager issues
// or that the user keeps in a Secret. plans/completed/01-high-availability.md
// describes it.

const (
	// tlsPort serves TLS beside the plaintext port, so a roll that turns
	// TLS on or off keeps a port that every instance can reach.
	tlsPort = 6380
	// tlsAnnotation on a pod says its server serves TLS. The pod
	// template sets it, so it always matches how the pod's manager
	// configured its server.
	tlsAnnotation = apiGroup + "/tls"
)

type TLSSpec struct {
	IssuerRef *IssuerRef `json:"issuerRef,omitempty"`
	// Secret holds tls.crt, tls.key, and ca.crt.
	Secret string `json:"secret,omitempty"`
	// Plaintext keeps the plaintext port open beside TLS.
	Plaintext bool `json:"plaintext,omitempty"`
}

// IssuerRef names the cert-manager issuer of the Redis's Certificate.
type IssuerRef struct {
	Name  string `json:"name"`
	Kind  string `json:"kind,omitempty"`
	Group string `json:"group,omitempty"`
}

func (s RedisSpec) tlsEnabled() bool { return s.TLS != nil }

// tlsOnly is whether the spec asks for the plaintext port to close.
func (s RedisSpec) tlsOnly() bool { return s.TLS != nil && !s.TLS.Plaintext }

// tlsSecret is the Secret that holds the servers' certificate: the
// user's own, or the one cert-manager writes for the Certificate the
// operator makes, or "" without TLS.
func (s RedisSpec) tlsSecret(redis string) string {
	switch {
	case s.TLS == nil:
		return ""
	case s.TLS.Secret != "":
		return s.TLS.Secret
	}
	return certificateName(redis)
}

func certificateName(redis string) string { return redis + "-tls" }
