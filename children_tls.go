package main

// The cert-manager Certificate a Redis with an issuer gets. It names
// every address a client or a peer dials, so one certificate serves
// every instance.

import "fmt"

var certificatesResource = kubeResource{"cert-manager.io", "v1", "certificates"}

func certificate(r *Redis) child {
	name := certificateName(r.Metadata.Name)
	c := child{resource: certificatesResource, name: name, optional: true}
	if r.Spec.TLS == nil || r.Spec.TLS.IssuerRef == nil {
		c.remove = true
		return c
	}
	issuer := *r.Spec.TLS.IssuerRef
	if issuer.Kind == "" {
		issuer.Kind = "Issuer"
	}
	if issuer.Group == "" {
		issuer.Group = "cert-manager.io"
	}
	c.body = map[string]any{
		"apiVersion": "cert-manager.io/v1", "kind": "Certificate", "metadata": childMeta(r, name, nil),
		"spec": map[string]any{
			"secretName": name,
			"issuerRef":  map[string]string{"name": issuer.Name, "kind": issuer.Kind, "group": issuer.Group},
			"dnsNames":   certificateNames(r),
			// redis-cli in the server's container reaches it on loopback.
			"ipAddresses": []string{"127.0.0.1"},
		},
	}
	return c
}

// certificateNames are the Services' names in each form a client in the
// cluster may write, and every pod's name under the headless Service.
func certificateNames(r *Redis) []string {
	name, namespace := r.Metadata.Name, r.Metadata.Namespace
	var names []string
	for _, service := range []string{name + "-rw", name + "-ro"} {
		names = append(names, service, service+"."+namespace,
			fmt.Sprintf("%s.%s.svc", service, namespace), fmt.Sprintf("%s.%s.svc.cluster.local", service, namespace))
	}
	return append(names,
		fmt.Sprintf("*.%s-nodes.%s.svc", name, namespace),
		fmt.Sprintf("*.%s-nodes.%s.svc.cluster.local", name, namespace))
}
