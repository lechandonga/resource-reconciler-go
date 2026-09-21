package validation

import (
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/defaults"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

func validSpec() v1.Spec {
	return v1.Spec{Replicas: 1, Image: "nginx:1", DependsOn: []string{}, Config: map[string]string{}}
}

func TestValidSpecPasses(t *testing.T) {
	if f := ValidateSpec(validSpec()); f != nil {
		t.Fatalf("valid spec rejected: %v", f)
	}
}

func TestRejectReasonsAreDistinguishable(t *testing.T) {
	cases := []struct {
		mutate func(*v1.Spec)
		reason string
	}{
		{func(s *v1.Spec) { s.Replicas = 0 }, v1.ReasonReplicasOutOfRange},
		{func(s *v1.Spec) { s.Replicas = defaults.MaxReplicas + 1 }, v1.ReasonReplicasOutOfRange},
		{func(s *v1.Spec) { s.Image = "" }, v1.ReasonInvalidImage},
		{func(s *v1.Spec) { s.Image = "ng inx:1" }, v1.ReasonInvalidImage},
		{func(s *v1.Spec) { s.DependsOn = []string{"a", "a"} }, v1.ReasonDuplicateDependency},
		{func(s *v1.Spec) { s.DependsOn = []string{""} }, v1.ReasonEmptyDependency},
		{func(s *v1.Spec) { s.Config = map[string]string{"bad key": "v"} }, v1.ReasonInvalidConfigKey},
	}
	for i, tc := range cases {
		s := validSpec()
		tc.mutate(&s)
		f := ValidateSpec(s)
		if f == nil {
			t.Fatalf("case %d: expected rejection %s", i, tc.reason)
		}
		if f.Reason != tc.reason {
			t.Fatalf("case %d: reason=%s want=%s", i, f.Reason, tc.reason)
		}
	}
}

func TestNameValidation(t *testing.T) {
	if f := ValidateName("valid-1"); f != nil {
		t.Fatal(f)
	}
	if f := ValidateName(""); f == nil || f.Reason != v1.ReasonMissingName {
		t.Fatalf("empty name: %+v", f)
	}
	if f := ValidateName("UPPER"); f == nil || f.Reason != v1.ReasonInvalidName {
		t.Fatalf("bad name: %+v", f)
	}
}
