package actual

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
)

func state(id string, rep int) *apis.ActualState {
	return &apis.ActualState{ResourceID: id, Replicas: rep, Mode: apis.ModeOn, GraceSeconds: 30, Ready: true}
}

func TestDirStoreRoundTripAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s1, err := NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Save(ctx, "alpha", state("res-alpha", 2)); err != nil {
		t.Fatal(err)
	}

	// A brand-new instance over the same directory must observe exactly
	// the durable state, with identity intact.
	s2, err := NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s2.Load(ctx, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if got.ResourceID != "res-alpha" || got.Replicas != 2 {
		t.Fatalf("reloaded state = %+v", got)
	}
}

func TestDirStoreRemoveIdempotent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "a", state("r", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, "a"); err != nil {
		t.Fatalf("second remove must be a no-op: %v", err)
	}
	if _, err := s.Load(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load after remove err=%v, want ErrNotFound", err)
	}
}

func TestDirStorePagedListingBoundedAndOrdered(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a0", "a1", "a2", "a3", "a4"}
	for _, k := range want {
		if err := s.Save(ctx, k, state("r-"+k, 1)); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	cursor := ""
	for {
		keys, next, err := s.ListPage(ctx, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, keys...)
		if next == "" {
			break
		}
		cursor = next
	}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listed %v, want %v", got, want)
		}
	}
}

func TestDirStoreIgnoresForeignFiles(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, "w1", state("r", 1)); err != nil {
		t.Fatal(err)
	}
	// A foreign snapshot and junk must not appear as actual resources.
	if err := os.WriteFile(filepath.Join(dir, "snap.json"), []byte(`{"formatVersion":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stray.json"), []byte(`not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "UPPER.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	keys, _, err := s.ListPage(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "w1" {
		t.Fatalf("foreign files leaked into actual listing: %v", keys)
	}
}

func TestDirStoreRejectsUnsafeKeys(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "..", "../escape", "a/b", "a b"} {
		if err := s.Save(ctx, bad, state("r", 1)); err == nil {
			t.Fatalf("Save(%q) succeeded, want rejection", bad)
		}
	}
}

func TestDirStoreContextCancellation(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDirStore(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Save(ctx, "a", state("r", 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save err=%v, want context.Canceled", err)
	}
	if _, _, err := s.ListPage(ctx, "", 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListPage err=%v, want context.Canceled", err)
	}
}

func TestMemStorePaging(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	for _, k := range []string{"c", "a", "b"} {
		if err := s.Save(ctx, k, state("r", 1)); err != nil {
			t.Fatal(err)
		}
	}
	keys, next, err := s.ListPage(ctx, "", 2)
	if err != nil || len(keys) != 2 || keys[0] != "a" || keys[1] != "b" || next != "b" {
		t.Fatalf("page1=%v next=%q err=%v", keys, next, err)
	}
	keys2, next2, err := s.ListPage(ctx, next, 2)
	if err != nil || len(keys2) != 1 || keys2[0] != "c" || next2 != "" {
		t.Fatalf("page2=%v next=%q err=%v", keys2, next2, err)
	}
}
