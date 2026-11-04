package main

// The exporter container in each instance's pod. It reads its own
// server over 127.0.0.1, so every instance reports itself, replicas
// included, under redis_exporter's series names, which existing
// dashboards and alert rules already query.

import (
	"fmt"
	"strings"
)

const (
	exporterContainer = "metrics"
	exporterPort      = 9121
)

func exporter(r *Redis, defaults images) map[string]any {
	args := []string{
		fmt.Sprintf("-redis.addr=redis://127.0.0.1:%d", serverPort),
		fmt.Sprintf("-web.listen-address=:%d", exporterPort),
	}
	if r.Spec.tlsEnabled() {
		// The connection never leaves the pod, so checking the server's
		// certificate would add only a way for the exporter to fail.
		args[0] = fmt.Sprintf("-redis.addr=rediss://127.0.0.1:%d", tlsPort)
		args = append(args, "-skip-tls-verification")
	}
	if r.Spec.Metrics != nil && len(r.Spec.Metrics.CheckSingleKeys) > 0 {
		args = append(args, "-check-single-keys="+strings.Join(r.Spec.Metrics.CheckSingleKeys, ","))
	}
	return map[string]any{
		"name":  exporterContainer,
		"image": r.Spec.exporterImage(defaults),
		"args":  args,
		// The password stays out of the arguments, which anyone who can
		// read the pod sees.
		"env": []map[string]any{
			{"name": "REDIS_PASSWORD", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": secretName(r.Metadata.Name), "key": "password"}}},
		},
		"ports": []map[string]any{{"name": "metrics", "containerPort": exporterPort}},
		// The limit bounds what an exporter that leaks can take from the
		// node the server shares with it.
		"resources": map[string]any{
			"requests": map[string]any{"cpu": "10m", "memory": "16Mi"},
			"limits":   map[string]any{"memory": "128Mi"},
		},
		"securityContext": map[string]any{
			"allowPrivilegeEscalation": false,
			"readOnlyRootFilesystem":   true,
			"capabilities":             map[string]any{"drop": []string{"ALL"}},
		},
	}
}
