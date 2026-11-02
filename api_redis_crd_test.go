package main

// These tests run the CRD through the same code the API server runs:
// the structural schema validator for the shape, and the CEL validator
// for the rules the schema states, so an object the cluster would refuse
// fails here instead.

import (
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	celschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

const redisCRD = "deploy/redis-crd.yaml"

func loadCRD(t *testing.T) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	raw, err := os.ReadFile(redisCRD)
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
func TestTheAPIServerWouldAcceptTheCRD(t *testing.T) {
	scheme := runtime.NewScheme()
	install.Install(scheme)
	internal := &apiextensions.CustomResourceDefinition{}
	mustSucceed(t, scheme.Convert(loadCRD(t), internal, nil))
	internal.Status.StoredVersions = []string{internal.Spec.Versions[0].Name}

	errs := crdvalidation.ValidateCustomResourceDefinition(t.Context(), internal)

	if len(errs) > 0 {
		t.Fatalf("the API server would refuse the definition: %v", errs)
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

// validate runs both passes over object, and the CEL transition rules
// against old when it is not nil, the way the API server answers an
// update.
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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			errs := validate(t, redisObject(c.spec), nil)

			mustMatch(t, strings.Contains(errs.ToAggregate().Error(), c.want), true)
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

// Every spec field the Go type reads is in the schema, so no field the
// operator acts on is one the API server drops.
func TestTheSchemaHoldsEverySpecField(t *testing.T) {
	properties := loadCRD(t).Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties
	fields := reflect.TypeFor[RedisSpec]()

	var missing []string
	for i := range fields.NumField() {
		name, _, _ := strings.Cut(fields.Field(i).Tag.Get("json"), ",")
		if _, ok := properties[name]; !ok {
			missing = append(missing, name)
		}
	}

	mustMatch(t, strings.Join(missing, ","), "")
	mustMatch(t, slices.Contains(slices.Collect(func(yield func(string) bool) {
		for name := range properties {
			if !yield(name) {
				return
			}
		}
	}), "engine"), true)
}
