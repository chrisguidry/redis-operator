package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// templateContainer is the part of a pod template's container that
// these tests read.
type templateContainer struct {
	Name  string   `json:"name"`
	Image string   `json:"image"`
	Args  []string `json:"args"`
	Env   []struct {
		Name      string `json:"name"`
		ValueFrom struct {
			SecretKeyRef struct {
				Name string `json:"name"`
				Key  string `json:"key"`
			} `json:"secretKeyRef"`
		} `json:"valueFrom"`
	} `json:"env"`
	Ports []struct {
		Name          string `json:"name"`
		ContainerPort int    `json:"containerPort"`
	} `json:"ports"`
}

// templateContainers builds the StatefulSet of a Redis with spec, and
// reads its pod template's containers by name, through JSON, as the API
// server receives them.
func templateContainers(t *testing.T, spec RedisSpec) map[string]templateContainer {
	t.Helper()
	r := &Redis{Metadata: ObjectMeta{Name: "cache", Namespace: testNamespace}, Spec: spec}
	raw, err := json.Marshal(statefulSet(r, "operator-image", nil, 1).body)
	mustSucceed(t, err)
	var set struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []templateContainer `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	mustSucceed(t, json.Unmarshal(raw, &set))
	byName := map[string]templateContainer{}
	for _, c := range set.Spec.Template.Spec.Containers {
		byName[c.Name] = c
	}
	return byName
}

func enabled(on bool) *bool { return &on }

func TestTheExporterRunsBesideEveryInstanceUnlessDisabled(t *testing.T) {
	cases := []struct {
		name    string
		metrics *MetricsSpec
		want    bool
	}{
		{"no metrics field", nil, true},
		{"metrics without enabled", &MetricsSpec{}, true},
		{"enabled", &MetricsSpec{Enabled: enabled(true)}, true},
		{"disabled", &MetricsSpec{Enabled: enabled(false)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			containers := templateContainers(t, RedisSpec{Metrics: c.metrics})

			_, found := containers[exporterContainer]
			mustMatch(t, found, c.want)
			mustMatch(t, containers["redis"].Name, "redis")
		})
	}
}

func TestTheExporterServesOnThePortNamedMetrics(t *testing.T) {
	exporter := templateContainers(t, RedisSpec{})[exporterContainer]

	mustMatch(t, len(exporter.Ports), 1)
	mustMatch(t, exporter.Ports[0].Name, "metrics")
	mustMatch(t, exporter.Ports[0].ContainerPort, exporterPort)
}

func TestTheExporterScrapesItsOwnInstanceWithThePassword(t *testing.T) {
	exporter := templateContainers(t, RedisSpec{})[exporterContainer]

	mustMatch(t, flagValue(exporter.Args, "-redis.addr"), "redis://127.0.0.1:6379")
	mustMatch(t, len(exporter.Env), 1)
	mustMatch(t, exporter.Env[0].Name, "REDIS_PASSWORD")
	mustMatch(t, exporter.Env[0].ValueFrom.SecretKeyRef.Name, "cache-app")
	mustMatch(t, exporter.Env[0].ValueFrom.SecretKeyRef.Key, "password")
}

func TestTheExporterImageIsPinnedUnlessTheSpecNamesOne(t *testing.T) {
	cases := []struct {
		name    string
		metrics *MetricsSpec
		want    string
	}{
		{"pinned", nil, defaultExporterImage},
		{"named", &MetricsSpec{Image: "registry.example.com/redis_exporter:v1"}, "registry.example.com/redis_exporter:v1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exporter := templateContainers(t, RedisSpec{Metrics: c.metrics})[exporterContainer]

			mustMatch(t, exporter.Image, c.want)
		})
	}
}

// flagValue is the value of the flag name in args, or "" when args do
// not set it.
func flagValue(args []string, name string) string {
	for _, arg := range args {
		if value, found := strings.CutPrefix(arg, name+"="); found {
			return value
		}
	}
	return ""
}

func TestTheExporterChecksTheSingleKeysTheSpecNames(t *testing.T) {
	cases := []struct {
		name string
		keys []string
		want string
	}{
		{"none", nil, ""},
		{"one", []string{"celery"}, "celery"},
		{"several, one in another database", []string{"celery", "db1=jobs"}, "celery,db1=jobs"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exporter := templateContainers(t, RedisSpec{Metrics: &MetricsSpec{CheckSingleKeys: c.keys}})[exporterContainer]

			mustMatch(t, flagValue(exporter.Args, "-check-single-keys"), c.want)
		})
	}
}
