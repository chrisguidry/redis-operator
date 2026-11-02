package main

// These tests run the CRD through the same code the API server runs:
// the structural schema validator for the shape, and the CEL validator
// for the rules the schema states, so an object the cluster would refuse
// fails here instead.

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	celschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

const redisCRD = "deploy/redis-crd.yaml"

// crds lists every definition this repository ships.
var crds = []string{redisCRD, "deploy/backups-crd.yaml", "deploy/scheduledbackups-crd.yaml"}

func loadCRD(t *testing.T) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	return loadCRDFrom(t, redisCRD)
}

func loadCRDFrom(t *testing.T, path string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	raw, err := os.ReadFile(path)
	mustSucceed(t, err)
	crd := &apiextensionsv1.CustomResourceDefinition{}
	mustSucceed(t, yaml.UnmarshalStrict(raw, crd))
	return crd
}

func TestTheCRDNamesTheResourceTheOperatorWatches(t *testing.T) {
	crd := loadCRD(t)

	mustMatch(t, crd.Spec.Group, redisResource.Group)
	mustMatch(t, crd.Spec.Names.Plural, redisResource.Resource)
	mustMatch(t, crd.Spec.Names.Kind, redisKind)
	mustMatch(t, crd.Spec.Versions[0].Name, redisResource.Version)
	mustMatch(t, crd.Spec.Versions[0].Subresources.Status != nil, true)
}

// The API server runs the whole definition through this before it
// serves the resource, and that pass compiles each CEL rule and
// estimates its cost.
func TestTheAPIServerWouldAcceptEveryCRD(t *testing.T) {
	for _, path := range crds {
		t.Run(path, func(t *testing.T) {
			scheme := runtime.NewScheme()
			install.Install(scheme)
			internal := &apiextensions.CustomResourceDefinition{}
			mustSucceed(t, scheme.Convert(loadCRDFrom(t, path), internal, nil))
			internal.Status.StoredVersions = []string{internal.Spec.Versions[0].Name}

			errs := crdvalidation.ValidateCustomResourceDefinition(t.Context(), internal)

			if len(errs) > 0 {
				t.Fatalf("the API server would refuse the definition: %v", errs)
			}
		})
	}
}

// Every spec field the Go types read is in its schema, so no field the
// operator acts on is one the API server drops.
func TestEverySchemaHoldsItsSpecFields(t *testing.T) {
	cases := []struct {
		path string
		spec reflect.Type
	}{
		{redisCRD, reflect.TypeFor[RedisSpec]()},
		{"deploy/backups-crd.yaml", reflect.TypeFor[BackupSpec]()},
		{"deploy/scheduledbackups-crd.yaml", reflect.TypeFor[ScheduledBackupSpec]()},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			properties := loadCRDFrom(t, c.path).Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties
			var missing []string
			for i := range c.spec.NumField() {
				name, _, _ := strings.Cut(c.spec.Field(i).Tag.Get("json"), ",")
				if _, ok := properties[name]; !ok {
					missing = append(missing, name)
				}
			}
			mustMatch(t, strings.Join(missing, ","), "")
		})
	}
}

func internalSchema(t *testing.T) *apiextensions.JSONSchemaProps {
	t.Helper()
	scheme := runtime.NewScheme()
	install.Install(scheme)
	internal := &apiextensions.JSONSchemaProps{}
	mustSucceed(t, scheme.Convert(loadCRD(t).Spec.Versions[0].Schema.OpenAPIV3Schema, internal, nil))
	return internal
}

// validate runs both passes over object, the check that a list keyed
// by a field holds each key once, and the CEL transition rules against
// old when it is not nil, the way the API server answers an update.
func validate(t *testing.T, object, old map[string]any) field.ErrorList {
	t.Helper()
	schema := internalSchema(t)
	validator, _, err := validation.NewSchemaValidator(schema)
	mustSucceed(t, err)
	errs := validation.ValidateCustomResource(field.NewPath(""), object, validator)
	structural, err := structuralschema.NewStructural(schema)
	mustSucceed(t, err)
	var oldObject any
	if old != nil {
		oldObject = old
	}
	celErrs, _ := celschema.NewValidator(structural, true, celconfig.PerCallLimit).Validate(
		t.Context(), field.NewPath(""), structural, object, oldObject, celconfig.RuntimeCELCostBudget)
	errs = append(errs, listtype.ValidateListSetsAndMaps(field.NewPath(""), structural, object)...)
	return append(errs, celErrs...)
}

func redisObject(spec map[string]any) map[string]any {
	return map[string]any{
		"apiVersion": apiVersion, "kind": redisKind,
		"metadata": map[string]any{"name": "cache", "namespace": "example"},
		"spec":     spec,
	}
}

func TestTheExampleIsValid(t *testing.T) {
	raw, err := os.ReadFile("deploy/examples/redis.yaml")
	mustSucceed(t, err)
	var object map[string]any
	mustSucceed(t, yaml.Unmarshal(raw, &object))

	errs := validate(t, object, nil)

	mustMatch(t, len(errs), 0)
}

func TestTheCRDRefuses(t *testing.T) {
	cases := []struct {
		name string
		spec map[string]any
		want string
	}{
		{"an unknown engine", map[string]any{"engine": "memcached"}, "spec.engine"},
		{"no instances", map[string]any{"instances": 0}, "spec.instances"},
		{"a fractional memory", map[string]any{"memory": "1.5Gi"}, "spec.memory"},
		{"as many required replicas as instances", map[string]any{"instances": 2, "minReplicasToWrite": 2}, "minReplicasToWrite must be less than instances"},
		{"a single key with a comma", map[string]any{"metrics": map[string]any{"checkSingleKeys": []any{"a,b"}}}, "spec.metrics.checkSingleKeys"},
		{"an empty single key", map[string]any{"metrics": map[string]any{"checkSingleKeys": []any{""}}}, "spec.metrics.checkSingleKeys"},
		{"a user named default", map[string]any{"users": []any{user("default", "+@all")}}, "the default user is the one in <name>-app"},
		{"a user name with a space", map[string]any{"users": []any{user("my app", "+@all")}}, "spec.users[0].name"},
		{"two users of one name", map[string]any{"users": []any{user("app", "+get"), user("app", "+set")}}, "Duplicate value"},
		{"a user with no rules", map[string]any{"users": []any{user("app", "")}}, "spec.users[0].rules"},
		{"a rule that sets a password", map[string]any{"users": []any{user("app", "+@all >hunter2")}}, "rules cannot set a password"},
		{"a rule that sets a hashed password", map[string]any{"users": []any{user("app", "#abc +@all")}}, "rules cannot set a password"},
		{"a rule that removes the password", map[string]any{"users": []any{user("app", "+@all nopass")}}, "rules cannot set a password"},
		{"a rule that turns the user off", map[string]any{"users": []any{user("app", "OFF +@all")}}, "rules cannot set a password"},
		{"a rule that resets the user", map[string]any{"users": []any{user("app", "+@all reset")}}, "rules cannot set a password"},
		{"tls with an issuer and a secret", map[string]any{"tls": map[string]any{"issuerRef": map[string]any{"name": "ca"}, "secret": "cache-tls"}}, "either a cert-manager issuerRef or a secret"},
		{"tls with neither", map[string]any{"tls": map[string]any{"plaintext": true}}, "either a cert-manager issuerRef or a secret"},
		{"an issuer of an unknown kind", map[string]any{"tls": map[string]any{"issuerRef": map[string]any{"name": "ca", "kind": "Vault"}}}, "spec.tls.issuerRef.kind"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			errs := validate(t, redisObject(c.spec), nil)

			mustMatch(t, strings.Contains(errs.ToAggregate().Error(), c.want), true)
		})
	}
}

// user is one entry of spec.users, with its password in the Secret
// app-user.
func user(name, rules string) map[string]any {
	return map[string]any{"name": name, "password": map[string]any{"secret": "app-user", "key": "password"}, "rules": rules}
}

func TestTheCRDAccepts(t *testing.T) {
	cases := []struct {
		name string
		spec map[string]any
	}{
		{"users with key patterns, channels, and a selector", map[string]any{"users": []any{
			user("app", "~app:* &app:* +@all -@dangerous"),
			user("reader.v2", "+@read ~* (~audit:* +xadd) %R~ro:*"),
			user("ON-call", "+info"),
		}}},
		{"tls from an issuer", map[string]any{"tls": map[string]any{"issuerRef": map[string]any{"name": "ca", "kind": "ClusterIssuer"}}}},
		{"tls from a secret, with plaintext", map[string]any{"tls": map[string]any{"secret": "cache-tls", "plaintext": true}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			errs := validate(t, redisObject(c.spec), nil)

			mustMatch(t, errs.ToAggregate(), nil)
		})
	}
}

func TestTheEngineAndStorageCannotChange(t *testing.T) {
	cases := []struct {
		name     string
		old, new map[string]any
		want     string
	}{
		{"engine", map[string]any{"engine": "redis"}, map[string]any{"engine": "valkey"}, "engine cannot change"},
		{"storage", map[string]any{"storage": map[string]any{"size": "1Gi"}}, map[string]any{"storage": map[string]any{"size": "2Gi"}}, "storage cannot change"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			errs := validate(t, redisObject(c.new), redisObject(c.old))

			mustMatch(t, strings.Contains(errs.ToAggregate().Error(), c.want), true)
		})
	}
}
