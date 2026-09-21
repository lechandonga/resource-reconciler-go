package storage

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
)

func mkObj(name string) *apis.RawObject {
	return &apis.RawObject{APIVersion: apis.APIVersionV2, Kind: apis.KindWidget, Metadata: apis.ObjectMeta{Name: name}}
}

func TestCreateGetUpdateConflict(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	if _, _, err := s.Create(ctx, mkObj("a")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(ctx, mkObj("a")); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second create: %v", err)
	}
	obj, rv, err := s.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	obj.Metadata.Labels = map[string]string{"v": "1"}

	// stale rv must be rejected
	stale := rv + 1000
	if _, _, err := s.Update(ctx, obj, stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update expected conflict, got %v", err)
	}
	updated, newRV, err := s.Update(ctx, obj, rv)
	if err != nil {
		t.Fatalf("valid update: %v", err)
	}
	if newRV != rv+1 {
		t.Fatalf("rv = %d, want %d", newRV, rv+1)
	}
	// old rv is now stale
	if _, _, err := s.Update(ctx, updated, rv); !errors.Is(err, ErrConflict) {
		t.Fatalf("reuse of old rv expected conflict, got %v", err)
	}
	// returned objects are detached
	updated.Metadata.Labels["v"] = "mutated"
	again, _, _ := s.Get(ctx, "a")
	if again.Metadata.Labels["v"] != "1" {
		t.Fatal("store leaked a mutable reference")
	}
	if _, _, err := s.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestConcurrentCASExactlyOneWinnerPerVersion(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	if _, _, err := s.Create(ctx, mkObj("a")); err != nil {
		t.Fatal(err)
	}
	const n = 32
	var wg sync.WaitGroup
	var wins, conflicts int64
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for {
				obj, rv, err := s.Get(ctx, "a")
				if err != nil {
					t.Error(err)
					return
				}
				obj.Metadata.Labels = map[string]string{"v": "x"}
				if _, _, err := s.Update(ctx, obj, rv); err != nil {
					if errors.Is(err, ErrConflict) {
						mu.Lock()
						conflicts++
						mu.Unlock()
						continue
					}
					t.Error(err)
					return
				}
				mu.Lock()
				wins++
				mu.Unlock()
				return
			}
		}()
	}
	wg.Wait()
	if wins != n {
		t.Fatalf("wins = %d, want %d (each goroutine must eventually win once)", wins, n)
	}
	if conflicts == 0 {
		t.Fatal("expected at least one conflict under contention")
	}
	_, rv, _ := s.Get(ctx, "a")
	if rv != 1+int64(n) {
		t.Fatalf("final rv = %d, want %d", rv, 1+n)
	}
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.json")
	s := NewMemStore()
	ctx := context.Background()
	obj, rv, _ := s.Create(ctx, mkObj("a"))
	if rv != 1 {
		t.Fatalf("initial rv = %d", rv)
	}
	obj.Status = &apis.Status{ObservedGeneration: 7, SpecHash: "h",
		Conditions: []apis.Condition{{Type: apis.CondReady, Status: "True", Reason: apis.ReasonAccepted}}}
	var err error
	if _, rv, err = s.Update(ctx, obj, rv); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveToFile(path); err != nil {
		t.Fatal(err)
	}

	s2 := NewMemStore()
	if err := s2.LoadFile(path); err != nil {
		t.Fatal(err)
	}
	got, gotRV, err := s2.Get(ctx, "a")
	if err != nil || gotRV != rv {
		t.Fatalf("reload: rv=%d err=%v", gotRV, err)
	}
	if got.Status.ObservedGeneration != 7 || got.Status.SpecHash != "h" {
		t.Fatalf("status lost across restart: %+v", got.Status)
	}
	// rv semantics preserved after restart
	got.Status.SpecHash = "h2"
	if _, _, err := s2.Update(ctx, got, gotRV-1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update after restart: %v", err)
	}
}

func TestSnapshotRejectsUnknownFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.json")
	s := NewMemStore()
	if _, _, err := s.Create(context.Background(), mkObj("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveToFile(path); err != nil {
		t.Fatal(err)
	}

	// 1) unknown format version is rejected, not guessed at
	data, err := osReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(stringsReplaceAll(string(data), `"formatVersion": 1`, `"formatVersion": 99`))
	if err := writeFile(path, data); err != nil {
		t.Fatal(err)
	}
	if err := NewMemStore().LoadFile(path); err == nil {
		t.Fatal("unknown snapshot format version was silently accepted")
	}

	// 2) torn/corrupt file is rejected
	if err := writeFile(path, []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	if err := NewMemStore().LoadFile(path); err == nil {
		t.Fatal("corrupt snapshot was silently accepted")
	}
}
