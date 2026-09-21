package defaults

import (
	"reflect"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

func TestApplyIsDeterministicAndIdempotent(t *testing.T) {
	w := &v1.Workload{Spec: v1.Spec{}}
	Apply(w)
	first := *w
	Apply(w)
	second := *w
	if !reflect.DeepEqual(first.Spec, second.Spec) {
		t.Fatalf("defaults not idempotent: %+v vs %+v", first.Spec, second.Spec)
	}
	if w.Spec.Replicas != DefaultReplicas || w.Spec.Image != DefaultImage {
		t.Fatalf("defaults not applied: %+v", w.Spec)
	}
	if w.ObjectMeta.Annotations[v1.AnnDefaultsApplied] != v1.DefaultsSchemaVersion {
		t.Fatal("defaults schema annotation missing")
	}
	// Explicit values are preserved.
	w2 := &v1.Workload{Spec: v1.Spec{Replicas: 5, Image: "x:1"}}
	Apply(w2)
	if w2.Spec.Replicas != 5 || w2.Spec.Image != "x:1" {
		t.Fatal("explicit values overwritten by defaults")
	}
	Apply(nil) // must not panic
}
