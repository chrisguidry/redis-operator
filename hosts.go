package main

// A host a Redis names, such as an object store's endpoint or a source
// to adopt, is resolved the way a pod in the Redis's namespace resolves
// it. A pod's resolver searches its own namespace first, so a name of
// one label, such as "store", means a Service there. The operator runs
// in a namespace of its own, so it qualifies such a name before it
// connects: "store" in the namespace example is store.example.svc.

import (
	"net"
	"strings"
)

func qualifyHost(host, namespace string) string {
	name, port, err := net.SplitHostPort(host)
	if err != nil {
		name, port = host, ""
	}
	if name != "localhost" && net.ParseIP(name) == nil && !strings.Contains(name, ".") {
		name += "." + namespace + ".svc"
	}
	if port == "" {
		return name
	}
	return net.JoinHostPort(name, port)
}
