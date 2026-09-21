package integration

import (
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

// TestIllegalSpecsAreRejected drives the admission chain with a matrix of
// illegal input and asserts a specific, distinguishable reason code for each.
func TestIllegalSpecsAreRejected(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	cases := []struct {
		name   string
		raw    string
		reason string
	}{
		{
			name:   "malformed json",
			raw:    `{not json`,
			reason: v1.ReasonInvalidSpec,
		},
		{
			name: "unsupported version",
			raw: `{
			  "apiVersion": "core.example.com/v9",
			  "kind": "Workload",
			  "metadata": {"name": "x"},
			  "spec": {"replicas": 1}
			}`,
			reason: v1.ReasonUnsupportedVersion,
		},
		{
			name: "wrong kind",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Service",
			  "metadata": {"name": "x"}
			}`,
			reason: v1.ReasonInvalidKind,
		},
		{
			name: "missing name",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {},
			  "spec": {}
			}`,
			reason: v1.ReasonMissingName,
		},
		{
			name: "bad name",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "UPPER_CASE"},
			  "spec": {}
			}`,
			reason: v1.ReasonInvalidName,
		},
		{
			name: "replicas too high",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "spec": {"replicas": 11}
			}`,
			reason: v1.ReasonReplicasOutOfRange,
		},
		{
			name: "replicas negative",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "spec": {"replicas": -3}
			}`,
			reason: v1.ReasonReplicasOutOfRange,
		},
		{
			name: "invalid image tag",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "spec": {"image": "nginx:bad tag!"}
			}`,
			reason: v1.ReasonInvalidImage,
		},
		{
			name: "duplicate dependency",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "spec": {"dependsOn": ["a", "a"]}
			}`,
			reason: v1.ReasonDuplicateDependency,
		},
		{
			name: "empty dependency entry",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "spec": {"dependsOn": [""]}
			}`,
			reason: v1.ReasonEmptyDependency,
		},
		{
			name: "unknown top level field",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "mystery": 42
			}`,
			reason: v1.ReasonUnknownField,
		},
		{
			name: "unknown spec field",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "spec": {"priority": "high"}
			}`,
			reason: v1.ReasonUnknownField,
		},
		{
			name: "wrong field type",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "spec": {"replicas": "three"}
			}`,
			reason: v1.ReasonInvalidSpec,
		},
		{
			name: "invalid config key",
			raw: `{
			  "apiVersion": "core.example.com/v1",
			  "kind": "Workload",
			  "metadata": {"name": "w1"},
			  "spec": {"config": {"bad key!": "v"}}
			}`,
			reason: v1.ReasonInvalidConfigKey,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dec := env.submit(tc.raw)
			if dec.Allowed {
				t.Fatalf("expected rejection, got allowed object: %+v", dec.Workload)
			}
			if dec.Reason != tc.reason {
				t.Fatalf("reason = %q, want %q (message: %s)", dec.Reason, tc.reason, dec.Message)
			}
		})
	}
	env.dumpLogs()
}

// TestDefaultsAreDeterministicAndStable verifies that the same input gets the
// same defaults every time, both at admission and on repeated reads.
func TestDefaultsAreDeterministicAndStable(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	raw := `{
	  "apiVersion": "core.example.com/v1",
	  "kind": "Workload",
	  "metadata": {"name": "d1"},
	  "spec": {}
	}`
	dec := env.submit(raw)
	if !dec.Allowed {
		t.Fatalf("admission: %s: %s", dec.Reason, dec.Message)
	}
	if got := dec.Workload.Spec.Replicas; got != 1 {
		t.Fatalf("default replicas = %d, want 1", got)
	}
	if got := dec.Workload.Spec.Image; got != "busybox:latest" {
		t.Fatalf("default image = %q, want busybox:latest", got)
	}
	if dec.Workload.ObjectMeta.Annotations[v1.AnnDefaultsApplied] != v1.DefaultsSchemaVersion {
		t.Fatalf("defaults annotation missing: %+v", dec.Workload.ObjectMeta.Annotations)
	}

	w := env.waitPhase("d1", v1.PhaseAvailable)

	// Repeated reads produce identical default semantics.
	for i := 0; i < 5; i++ {
		r := env.get("d1")
		if r.Spec.Replicas != w.Spec.Replicas || r.Spec.Image != w.Spec.Image {
			t.Fatalf("read %d defaults drifted: %+v vs %+v", i, r.Spec, w.Spec)
		}
	}

	// Submitting the exact same manifest again must not change generation or
	// resource version (no no-op rewrite, no duplicate side effects).
	rvBefore := w.ObjectMeta.ResourceVersion
	genBefore := w.ObjectMeta.Generation
	dec2 := env.submit(raw)
	if !dec2.Allowed {
		t.Fatalf("second submit rejected: %s", dec2.Reason)
	}
	if dec2.Workload.ObjectMeta.Generation != genBefore {
		t.Fatalf("generation changed on no-op resubmit: %d -> %d", genBefore, dec2.Workload.ObjectMeta.Generation)
	}
	if dec2.Workload.ObjectMeta.ResourceVersion != rvBefore {
		t.Fatalf("resource version changed on no-op resubmit: %d -> %d", rvBefore, dec2.Workload.ObjectMeta.ResourceVersion)
	}
}
