package integration

import (
	"encoding/json"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/convert"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1beta1"
)

// TestV1Beta1IsUpgraded verifies historical manifests are accepted, upgraded
// to v1 semantics and converge.
func TestV1Beta1IsUpgraded(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	raw := `{
	  "apiVersion": "core.example.com/v1beta1",
	  "kind": "Workload",
	  "metadata": {"name": "legacy"},
	  "spec": {
	    "replicaCount": 2,
	    "containerImage": "alpine:3.19",
	    "depends": [],
	    "config": {"mode": "fast"}
	  }
	}`
	dec := env.submit(raw)
	if !dec.Allowed {
		t.Fatalf("v1beta1 manifest rejected: %s: %s", dec.Reason, dec.Message)
	}
	w := dec.Workload
	if w.Spec.Replicas != 2 {
		t.Fatalf("upgraded replicas = %d, want 2", w.Spec.Replicas)
	}
	if w.Spec.Image != "alpine:3.19" {
		t.Fatalf("upgraded image = %q", w.Spec.Image)
	}
	if w.Spec.Config["mode"] != "fast" {
		t.Fatalf("config lost during upgrade: %+v", w.Spec.Config)
	}
	if got := w.ObjectMeta.Annotations[v1.AnnStoredAPIVersion]; got != v1beta1.Version {
		t.Fatalf("stored-api-version = %q, want %q", got, v1beta1.Version)
	}

	avail := env.waitPhase("legacy", v1.PhaseAvailable)
	if len(avail.Status.Instances) != 2 {
		t.Fatalf("instances = %d, want 2", len(avail.Status.Instances))
	}
}

// TestV1Beta1DefaultsBackfilled verifies an object admitted through the
// historical schema with omitted fields gets deterministic defaults.
func TestV1Beta1DefaultsBackfilled(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	raw := `{
	  "apiVersion": "core.example.com/v1beta1",
	  "kind": "Workload",
	  "metadata": {"name": "legacy-defaults"},
	  "spec": {}
	}`
	dec := env.submit(raw)
	if !dec.Allowed {
		t.Fatalf("rejected: %s: %s", dec.Reason, dec.Message)
	}
	if dec.Workload.Spec.Replicas != 1 || dec.Workload.Spec.Image != "busybox:latest" {
		t.Fatalf("v1beta1 defaults wrong: %+v", dec.Workload.Spec)
	}
}

// TestUnknownV1Beta1FieldRejected verifies an old version does not silently
// accept a v1-only field.
func TestUnknownV1Beta1FieldRejected(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	raw := `{
	  "apiVersion": "core.example.com/v1beta1",
	  "kind": "Workload",
	  "metadata": {"name": "legacy-bad"},
	  "spec": {"replicas": 3}
	}`
	dec := env.submit(raw)
	if dec.Allowed {
		t.Fatal("v1 field on v1beta1 manifest must be rejected, not guessed")
	}
	if dec.Reason != v1.ReasonUnknownField {
		t.Fatalf("reason = %q, want %q", dec.Reason, v1.ReasonUnknownField)
	}
}

// TestUnsupportedVersionRejected covers forward-compatibility: future and
// foreign versions are refused explicitly.
func TestUnsupportedVersionRejected(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	for _, av := range []string{
		"core.example.com/v2",
		"core.example.com/v0",
		"other.example.com/v1",
		"",
	} {
		raw := `{"apiVersion":"` + av + `","kind":"Workload","metadata":{"name":"x"},"spec":{}}`
		dec := env.submit(raw)
		if dec.Allowed || dec.Reason != v1.ReasonUnsupportedVersion {
			t.Fatalf("apiVersion=%q: got allowed=%v reason=%q, want rejection %q",
				av, dec.Allowed, dec.Reason, v1.ReasonUnsupportedVersion)
		}
	}
}

func storedEnvelope(apiVersion, spec string) json.RawMessage {
	return json.RawMessage(`{
	  "apiVersion": "` + apiVersion + `",
	  "kind": "Workload",
	  "metadata": {"name": "ghost", "resourceVersion": 1, "generation": 1},
	  "spec": ` + spec + `,
	  "status": {"phase": "Pending", "observedGeneration": 0}
	}`)
}

// TestPersistedObjectRejectsUnknownVersion checks the load boundary: raw
// on-disk JSON with an unsupported version fails loudly; v1beta1-shaped
// historical JSON is upgraded and defaulted.
func TestPersistedObjectRejectsUnknownVersion(t *testing.T) {
	if _, err := convert.DecodeStored("core.example.com/v2",
		storedEnvelope("core.example.com/v2", `{"replicas":1,"image":"busybox:latest"}`)); err == nil {
		t.Fatal("expected unsupported stored version to be rejected")
	}

	w2, err := convert.DecodeStored("core.example.com/v1",
		storedEnvelope("core.example.com/v1", `{"replicas":0,"image":"","dependsOn":null,"config":null}`))
	if err != nil {
		t.Fatalf("canonical v1 stored object should decode: %v", err)
	}
	if w2.Spec.Replicas != 1 || w2.Spec.Image != "busybox:latest" {
		t.Fatalf("stored defaults not backfilled: %+v", w2.Spec)
	}

	if _, err := convert.DecodeStored("core.example.com/v1",
		storedEnvelope("core.example.com/v1", `{"replicas":1,"futureField":true}`)); err == nil {
		t.Fatal("expected unknown stored spec field to be rejected")
	}

	w3, err := convert.DecodeStored("core.example.com/v1beta1",
		storedEnvelope("core.example.com/v1beta1",
			`{"replicaCount":3,"containerImage":"alpine:3","depends":[],"config":{}}`))
	if err != nil {
		t.Fatalf("historical v1beta1 stored object should decode: %v", err)
	}
	if w3.Spec.Replicas != 3 || w3.Spec.Image != "alpine:3" {
		t.Fatalf("v1beta1 stored upgrade wrong: %+v", w3.Spec)
	}
}
