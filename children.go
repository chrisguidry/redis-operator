package main

// The objects the operator makes for each Redis, as the server-side
// apply bodies it sends. Every object is owned by the Redis, so the
// garbage collector deletes them with it, and every name starts with
// the Redis's name.

import (
	"fmt"
	"strconv"
)

var (
	serviceAccountsResource = kubeResource{"", "v1", "serviceaccounts"}
	rolesResource           = kubeResource{"rbac.authorization.k8s.io", "v1", "roles"}
	roleBindingsResource    = kubeResource{"rbac.authorization.k8s.io", "v1", "rolebindings"}
	servicesResource        = kubeResource{"", "v1", "services"}
	secretsResource         = kubeResource{"", "v1", "secrets"}
	budgetsResource         = kubeResource{"policy", "v1", "poddisruptionbudgets"}
)

// child is one object to apply, or to delete when remove is true: an
// object the Redis needs only for some specs, such as the budget on the
// primary, which a single instance must not have.
type child struct {
	resource kubeResource
	name     string
	body     map[string]any
	remove   bool
	// optional is a child whose failed write the operator logs and
	// passes over, so the rest of the pass still runs: a Certificate,
	// whose API exists only where cert-manager is installed.
	optional bool
}

func ownerOf(r *Redis) OwnerReference {
	return OwnerReference{
		APIVersion: apiVersion, Kind: redisKind, Name: r.Metadata.Name, UID: r.Metadata.UID,
		Controller: true, BlockOwnerDeletion: true,
	}
}

func childMeta(r *Redis, name string, labels map[string]string) map[string]any {
	all := map[string]string{clusterLabel: r.Metadata.Name}
	for key, value := range labels {
		all[key] = value
	}
	return map[string]any{
		"name":            name,
		"namespace":       r.Metadata.Namespace,
		"labels":          all,
		"ownerReferences": []OwnerReference{ownerOf(r)},
	}
}

func secretName(redis string) string { return redis + "-app" }

// children lists every object for r except its Secret, which the
// operator creates once and never rewrites, because it holds the
// password. replicas is the StatefulSet's scale (reconcile_scale.go).
func children(r *Redis, operatorImage string, secrets []string, restore *BackupVolume, replicas int) []child {
	name := r.Metadata.Name
	rules := []map[string]any{
		{"apiGroups": []string{apiGroup}, "resources": []string{"redis"}, "resourceNames": []string{name}, "verbs": []string{"get"}},
		{"apiGroups": []string{"coordination.k8s.io"}, "resources": []string{"leases"}, "verbs": []string{"create"}},
		{"apiGroups": []string{"coordination.k8s.io"}, "resources": []string{"leases"}, "resourceNames": []string{primaryLeaseName(name)}, "verbs": []string{"get", "patch", "update"}},
		{"apiGroups": []string{""}, "resources": []string{"pods"}, "verbs": []string{"get", "list", "patch"}},
		// A manager takes the snapshot a running Backup names it for,
		// and records how it went.
		{"apiGroups": []string{apiGroup}, "resources": []string{"backups"}, "verbs": []string{"get", "list", "patch"}},
		{"apiGroups": []string{apiGroup}, "resources": []string{"backups/status"}, "verbs": []string{"patch"}},
	}
	if len(secrets) > 0 {
		// The Secrets the managers read, and no other: the users'
		// passwords, the certificate, and the object store's credentials.
		rules = append(rules, map[string]any{"apiGroups": []string{""}, "resources": []string{"secrets"}, "resourceNames": secrets, "verbs": []string{"get"}})
	}
	list := []child{
		{resource: serviceAccountsResource, name: name, body: map[string]any{
			"apiVersion": "v1", "kind": "ServiceAccount", "metadata": childMeta(r, name, nil),
		}},
		{resource: rolesResource, name: name, body: map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": childMeta(r, name, nil),
			// What an instance manager reads and writes: its Redis, the
			// primary Lease, the pods of its Redis, whose IPs it follows
			// and whose annotation asks for a switchover, and its backups.
			"rules": rules,
		}},
		{resource: roleBindingsResource, name: name, body: map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": childMeta(r, name, nil),
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": name},
			"subjects": []map[string]any{{"kind": "ServiceAccount", "name": name, "namespace": r.Metadata.Namespace}},
		}},
		service(r, name+"-rw", map[string]string{clusterLabel: name, roleLabel: rolePrimary}, false),
		service(r, name+"-ro", map[string]string{clusterLabel: name, roleLabel: roleReplica}, false),
		service(r, name+"-nodes", map[string]string{clusterLabel: name}, true),
		{resource: budgetsResource, name: name + "-replicas", body: map[string]any{
			"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": childMeta(r, name+"-replicas", nil),
			// One replica may go at a time. The budget states how many
			// must stay, because the disruption controller counts a
			// maxUnavailable budget against the StatefulSet's whole
			// scale, primary included, and would allow no eviction. It
			// counts the StatefulSet's scale and not spec.instances, so a
			// scale-down that waits keeps the pods it waits for.
			"spec": map[string]any{
				"minAvailable": max(replicas-2, 0),
				"selector":     map[string]any{"matchLabels": map[string]string{clusterLabel: name, roleLabel: roleReplica}},
			},
		}},
		statefulSet(r, operatorImage, restore, replicas),
		// An eviction cannot take the primary. The operator switches over
		// when the primary's node is cordoned, and the old primary is
		// then a replica that this budget does not cover. A single
		// pod has nothing to switch to, and the budget would block
		// every drain of its node, so it has none.
		{resource: budgetsResource, name: name + "-primary", body: map[string]any{
			"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": childMeta(r, name+"-primary", nil),
			"spec": map[string]any{
				"minAvailable": 1,
				"selector":     map[string]any{"matchLabels": map[string]string{clusterLabel: name, roleLabel: rolePrimary}},
			},
		}, remove: replicas <= 1},
		// Last, so a failed apply where cert-manager is missing leaves
		// every other child applied.
		certificate(r),
	}
	return list
}

func service(r *Redis, name string, selector map[string]string, headless bool) child {
	ports := []map[string]any{{"name": "redis", "port": serverPort, "targetPort": "redis"}}
	if r.Spec.tlsEnabled() {
		ports = append(ports, map[string]any{"name": "redis-tls", "port": tlsPort, "targetPort": "redis-tls"})
	}
	spec := map[string]any{
		"selector": selector,
		"ports":    ports,
	}
	if headless {
		spec["clusterIP"] = "None"
		spec["publishNotReadyAddresses"] = true
	}
	return child{resource: servicesResource, name: name, body: map[string]any{
		"apiVersion": "v1", "kind": "Service", "metadata": childMeta(r, name, nil), "spec": spec,
	}}
}

func statefulSet(r *Redis, operatorImage string, restore *BackupVolume, replicas int) child {
	name := r.Metadata.Name
	podLabels := map[string]string{clusterLabel: name}
	env := []map[string]any{
		{"name": "POD_NAME", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.name"}}},
		{"name": "POD_NAMESPACE", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.namespace"}}},
		{"name": "POD_IP", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "status.podIP"}}},
		{"name": "REDIS_NAME", "value": name},
		{"name": "REDIS_PASSWORD", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": secretName(name), "key": "password"}}},
	}
	ports := []map[string]any{{"name": "redis", "containerPort": serverPort}, {"name": "status", "containerPort": statusPort}}
	templateMeta := map[string]any{"labels": podLabels}
	if secret := r.Spec.tlsSecret(name); secret != "" {
		env = append(env, map[string]any{"name": "TLS_SECRET", "value": secret})
		ports = append(ports, map[string]any{"name": "redis-tls", "containerPort": tlsPort})
		// Each manager reads which of its peers serve TLS from this, to
		// choose how to replicate from them (manager_tls.go).
		templateMeta["annotations"] = map[string]string{tlsAnnotation: "true"}
	}
	resources := map[string]any{
		"requests": map[string]any{"memory": r.Spec.memory()},
		"limits":   map[string]any{"memory": r.Spec.memory()},
	}
	if r.Spec.CPU != "" {
		resources["requests"].(map[string]any)["cpu"] = r.Spec.CPU
	}
	probe := func(path string, period, failures int) map[string]any {
		return map[string]any{
			"httpGet":          map[string]any{"path": path, "port": "status"},
			"periodSeconds":    period,
			"failureThreshold": failures,
		}
	}
	containers := []map[string]any{{
		"name":           "redis",
		"image":          servingImage(r),
		"command":        []string{"/manager/redis-operator", "manager"},
		"env":            env,
		"ports":          ports,
		"resources":      resources,
		"readinessProbe": probe("/ready", 2, 1),
		"livenessProbe":  probe("/healthz", 10, 3),
		"volumeMounts":   mounts(r, restore),
	}}
	if r.Spec.metricsEnabled() {
		containers = append(containers, exporter(r))
	}
	podSpec := map[string]any{
		"serviceAccountName": name,
		// A primary that is deleted waits for its switchover inside this
		// grace period, and then stops its server.
		"terminationGracePeriodSeconds": 30,
		// Both engines' images run their server as uid 999, and a
		// volume's files must belong to it.
		"securityContext": map[string]any{"runAsNonRoot": true, "runAsUser": 999, "runAsGroup": 999, "fsGroup": 999},
		"affinity": map[string]any{"podAntiAffinity": map[string]any{
			"requiredDuringSchedulingIgnoredDuringExecution": []map[string]any{{
				"labelSelector": map[string]any{"matchLabels": podLabels},
				"topologyKey":   "kubernetes.io/hostname",
			}},
		}},
		"initContainers": []map[string]any{{
			"name":         "install-manager",
			"image":        operatorImage,
			"args":         []string{"install", "/manager"},
			"volumeMounts": []map[string]any{{"name": "manager", "mountPath": "/manager"}},
		}},
		"containers": containers,
		"volumes":    volumes(r, restore),
	}
	if len(r.Spec.NodeSelector) > 0 {
		podSpec["nodeSelector"] = r.Spec.NodeSelector
	}
	if len(r.Spec.Tolerations) > 0 {
		podSpec["tolerations"] = r.Spec.Tolerations
	}
	if r.Spec.PriorityClassName != "" {
		podSpec["priorityClassName"] = r.Spec.PriorityClassName
	}
	spec := map[string]any{
		"replicas":    replicas,
		"serviceName": name + "-nodes",
		// The operator decides which pod restarts and when, so a change
		// rolls the replicas, switches over, then rolls the old primary.
		"updateStrategy":      map[string]any{"type": "OnDelete"},
		"podManagementPolicy": "Parallel",
		"selector":            map[string]any{"matchLabels": podLabels},
		"template": map[string]any{
			"metadata": templateMeta,
			"spec":     podSpec,
		},
	}
	if r.Spec.persistent() {
		claim := map[string]any{
			"accessModes": []string{"ReadWriteOnce"},
			"resources":   map[string]any{"requests": map[string]any{"storage": r.Spec.Storage.Size}},
		}
		if r.Spec.Storage.StorageClassName != "" {
			claim["storageClassName"] = r.Spec.Storage.StorageClassName
		}
		spec["volumeClaimTemplates"] = []map[string]any{{"metadata": map[string]any{"name": "data"}, "spec": claim}}
		// An instance that a scale-down removes takes its volume with it.
		// A later scale-up starts that instance empty, and it copies the
		// primary's data in a full sync, as it would from stale data.
		// Deleting the Redis leaves every volume.
		spec["persistentVolumeClaimRetentionPolicy"] = map[string]any{"whenScaled": "Delete", "whenDeleted": "Retain"}
	}
	return child{resource: statefulSetsResource, name: name, body: map[string]any{
		"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": childMeta(r, name, nil), "spec": spec,
	}}
}

func volumes(r *Redis, restore *BackupVolume) []map[string]any {
	list := []map[string]any{{"name": "manager", "emptyDir": map[string]any{}}}
	if !r.Spec.persistent() {
		list = append(list, map[string]any{"name": "data", "emptyDir": map[string]any{}})
	}
	if b := r.Spec.Backup; b != nil && b.Volume != nil {
		list = append(list, b.Volume.podVolume("backups"))
	}
	if restore != nil {
		list = append(list, restore.podVolume("restore"))
	}
	return list
}

func mounts(r *Redis, restore *BackupVolume) []map[string]any {
	list := []map[string]any{
		{"name": "manager", "mountPath": "/manager"},
		{"name": "data", "mountPath": "/data"},
	}
	if b := r.Spec.Backup; b != nil && b.Volume != nil {
		list = append(list, map[string]any{"name": "backups", "mountPath": backupMount})
	}
	if restore != nil {
		list = append(list, map[string]any{"name": "restore", "mountPath": restoreMount, "readOnly": true})
	}
	return list
}

// secret is the connection Secret: the password, which the operator
// makes once, and the addresses a client needs.
func secret(r *Redis, password string) map[string]any {
	data := map[string][]byte{}
	for key, value := range secretData(r, password) {
		data[key] = []byte(value)
	}
	return map[string]any{
		"apiVersion": "v1", "kind": "Secret", "metadata": childMeta(r, secretName(r.Metadata.Name), nil),
		"type": "Opaque",
		"data": data,
	}
}

// secretKeys lists every key secretData can write, so a key that a
// change to the spec drops can be removed.
var secretKeys = []string{"password", "host", "port", "uri", "ro-uri", "tls-port", "tls-uri", "tls-ro-uri"}

// secretData is what the connection Secret holds. With TLS it also
// names the TLS port, and once the plaintext port closes, port, uri,
// and ro-uri name TLS too, since nothing answers plaintext.
func secretData(r *Redis, password string) map[string]string {
	name := r.Metadata.Name
	host := fmt.Sprintf("%s-rw.%s.svc", name, r.Metadata.Namespace)
	readHost := fmt.Sprintf("%s-ro.%s.svc", name, r.Metadata.Namespace)
	uri := func(scheme, host string, port int) string {
		return fmt.Sprintf("%s://:%s@%s:%d", scheme, password, host, port)
	}
	data := map[string]string{
		"password": password,
		"host":     host,
		"port":     strconv.Itoa(serverPort),
		"uri":      uri("redis", host, serverPort),
		"ro-uri":   uri("redis", readHost, serverPort),
	}
	if !r.Spec.tlsEnabled() {
		return data
	}
	data["tls-port"] = strconv.Itoa(tlsPort)
	data["tls-uri"] = uri("rediss", host, tlsPort)
	data["tls-ro-uri"] = uri("rediss", readHost, tlsPort)
	if r.Spec.tlsOnly() {
		data["port"], data["uri"], data["ro-uri"] = data["tls-port"], data["tls-uri"], data["tls-ro-uri"]
	}
	return data
}
