// Package defaults fills omitted values with deterministic defaults.
package defaults

import "github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"

// Default values. Exposed as constants so tests can assert exact semantics.
const (
	DefaultReplicas = 1
	DefaultImage    = "busybox:latest"
	MaxReplicas     = 10
)

// Apply mutates w in place, filling defaults and stamping the defaults
// schema version annotation.
//
// Determinism guarantees:
//   - The same input always produces the same output.
//   - Apply is idempotent: applying it twice yields an object equal to
//     applying it once.
//   - nil and empty collections are normalized to allocated empty collections
//     in memory, so downstream comparisons never distinguish "unset" from
//     "set to empty".
func Apply(w *v1.Workload) {
	if w == nil {
		return
	}
	w.Spec = SpecDefaults(w.Spec)
	if w.ObjectMeta.Annotations == nil {
		w.ObjectMeta.Annotations = map[string]string{}
	}
	if w.ObjectMeta.Annotations[v1.AnnDefaultsApplied] == "" {
		w.ObjectMeta.Annotations[v1.AnnDefaultsApplied] = v1.DefaultsSchemaVersion
	}
}

// SpecDefaults returns the defaulted spec without mutating the input.
func SpecDefaults(s v1.Spec) v1.Spec {
	if s.Replicas == 0 {
		s.Replicas = DefaultReplicas
	}
	if s.Image == "" {
		s.Image = DefaultImage
	}
	if s.DependsOn == nil {
		s.DependsOn = []string{}
	}
	if s.Config == nil {
		s.Config = map[string]string{}
	}
	return s
}
