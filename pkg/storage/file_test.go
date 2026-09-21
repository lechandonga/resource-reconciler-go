package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

func newWorkload(name string) *v1.Workload {
	return &v1.Workload{
		APIVersion: v1.TypeMetaAPIVersion,
		Kind:       v1.Kind,
		ObjectMeta: v1.ObjectMeta{Name: name},
		Spec: v1.Spec{
			Replicas:  1,
			Image:     "busybox:latest",
			DependsOn: []string{},
			Config:    map[string]string{},
		},
	}
}

func TestCreateUpdateConflictAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	ctx := context.Background()

	st, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := st.Create(ctx, newWorkload("a"))
	if err != nil {
		t.Fatal(err)
	}
	if w.ObjectMeta.ResourceVersion == 0 {
		t.Fatal("create must assign a resource version")
	}
	rv := w.ObjectMeta.ResourceVersion

	// Stale write rejected.
	w.Spec.Replicas = 3
	if _, err := st.Update(ctx, w, rv-1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update: %v", err)
	}
	// Fresh write accepted and bumps RV.
	updated, err := st.Update(ctx, w, rv)
	if err != nil {
		t.Fatalf("fresh update: %v", err)
	}
	if updated.ObjectMeta.ResourceVersion == rv {
		t.Fatal("update must bump resource version")
	}
	if updated.Spec.Replicas != 3 {
		t.Fatal("update did not persist spec")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Reload: exact state survives a restart.
	st2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st2.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas != 3 || got.ObjectMeta.ResourceVersion != updated.ObjectMeta.ResourceVersion {
		t.Fatalf("reloaded state wrong: %+v rv=%d", got.Spec, got.ObjectMeta.ResourceVersion)
	}

	// Next RV must continue past all assigned versions (no reuse after
	// restart that could silently accept a stale write).
	w2 := newWorkload("b")
	created, err := st2.Create(ctx, w2)
	if err != nil {
		t.Fatal(err)
	}
	if created.ObjectMeta.ResourceVersion <= updated.ObjectMeta.ResourceVersion {
		t.Fatalf("rv reused after restart: %d <= %d",
			created.ObjectMeta.ResourceVersion, updated.ObjectMeta.ResourceVersion)
	}
}

func TestDeleteIsIdempotentAndOCC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	ctx := context.Background()
	st, _ := NewFileStore(path)
	w, _ := st.Create(ctx, newWorkload("a"))

	if err := st.Delete(ctx, "a", w.ObjectMeta.ResourceVersion-1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete: %v", err)
	}
	if err := st.Delete(ctx, "a", w.ObjectMeta.ResourceVersion); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.Delete(ctx, "a", w.ObjectMeta.ResourceVersion); err != nil {
		t.Fatalf("second delete should be a no-op: %v", err)
	}
	if _, err := st.Get(ctx, "a"); !errors.As(err, new(*NotFoundError)) {
		t.Fatalf("expected NotFoundError, got %v", err)
	}
}

func TestGetReturnsDeepCopy(t *testing.T) {
	st, _ := NewFileStore("")
	ctx := context.Background()
	w, _ := st.Create(ctx, newWorkload("a"))
	w.Spec.Image = "mutated:9"
	again, _ := st.Get(ctx, "a")
	if again.Spec.Image == "mutated:9" {
		t.Fatal("Get returned a mutable reference into persisted state")
	}
}
