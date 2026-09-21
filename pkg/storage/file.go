package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

// FileStore is a file-backed Store: the whole database is serialized to a
// single JSON file via an atomic rename, so a process restart sees exactly
// the last acknowledged state. Every write carries the monotonically
// increasing nextRV counter, which is itself persisted.
type FileStore struct {
	mu      sync.Mutex
	path    string
	nextRV  int64
	objects map[string]*v1.Workload
	closed  bool
}

// snapshot is the on-disk envelope.
type snapshot struct {
	NextRV  int64                   `json:"nextRV"`
	Objects map[string]*v1.Workload `json:"objects"`
}

// NewFileStore opens (or creates) the database at path and loads any state
// written by a previous process.
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, nextRV: 1, objects: map[string]*v1.Workload{}}
	if path == "" {
		return s, nil
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		var snap snapshot
		if err := json.Unmarshal(raw, &snap); err != nil {
			return nil, fmt.Errorf("storage: corrupt database %s: %w", path, err)
		}
		if snap.Objects != nil {
			s.objects = snap.Objects
		}
		if snap.NextRV > s.nextRV {
			s.nextRV = snap.NextRV
		}
	case errors.Is(err, os.ErrNotExist):
		// fresh database
	default:
		return nil, err
	}
	return s, nil
}

func (s *FileStore) Create(ctx context.Context, w *v1.Workload) (*v1.Workload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("storage: store closed")
	}
	if _, exists := s.objects[w.ObjectMeta.Name]; exists {
		return nil, fmt.Errorf("storage: workload already exists: %s", w.ObjectMeta.Name)
	}
	stored := deepCopy(w)
	now := time.Now().UTC()
	stored.ObjectMeta.CreationTimestamp = now
	stored.ObjectMeta.ResourceVersion = s.nextRV
	s.nextRV++
	if stored.ObjectMeta.Generation == 0 {
		stored.ObjectMeta.Generation = 1
	}
	if stored.Status.Phase == "" {
		stored.Status.Phase = v1.PhasePending
	}
	s.objects[stored.ObjectMeta.Name] = stored
	if err := s.persistLocked(); err != nil {
		delete(s.objects, stored.ObjectMeta.Name)
		s.nextRV--
		return nil, err
	}
	return deepCopy(stored), nil
}

func (s *FileStore) Get(ctx context.Context, name string) (*v1.Workload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.objects[name]
	if !ok {
		return nil, &NotFoundError{Name: name}
	}
	return deepCopy(w), nil
}

func (s *FileStore) List(ctx context.Context) ([]*v1.Workload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*v1.Workload, 0, len(s.objects))
	for _, w := range s.objects {
		out = append(out, deepCopy(w))
	}
	// Deterministic order.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].ObjectMeta.Name > out[j].ObjectMeta.Name; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out, nil
}

func (s *FileStore) Update(ctx context.Context, w *v1.Workload, expectedRV int64) (*v1.Workload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("storage: store closed")
	}
	current, ok := s.objects[w.ObjectMeta.Name]
	if !ok {
		return nil, &NotFoundError{Name: w.ObjectMeta.Name}
	}
	if current.ObjectMeta.ResourceVersion != expectedRV {
		return nil, fmt.Errorf("%w: expected %d, current %d",
			ErrConflict, expectedRV, current.ObjectMeta.ResourceVersion)
	}
	stored := deepCopy(w)
	stored.ObjectMeta.ResourceVersion = s.nextRV
	s.nextRV++
	s.objects[stored.ObjectMeta.Name] = stored
	if err := s.persistLocked(); err != nil {
		// Roll back so the acknowledged state stays consistent.
		s.objects[current.ObjectMeta.Name] = current
		s.nextRV--
		return nil, err
	}
	return deepCopy(stored), nil
}

func (s *FileStore) Delete(ctx context.Context, name string, expectedRV int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("storage: store closed")
	}
	current, ok := s.objects[name]
	if !ok {
		return nil // idempotent
	}
	if current.ObjectMeta.ResourceVersion != expectedRV {
		return fmt.Errorf("%w: expected %d, current %d",
			ErrConflict, expectedRV, current.ObjectMeta.ResourceVersion)
	}
	delete(s.objects, name)
	if err := s.persistLocked(); err != nil {
		s.objects[name] = current // rollback
		return err
	}
	return nil
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.persistLocked()
}

// persistLocked writes the snapshot atomically: write temp file, fsync,
// rename over the target. Caller must hold s.mu.
func (s *FileStore) persistLocked() error {
	if s.path == "" {
		return nil
	}
	snap := snapshot{NextRV: s.nextRV, Objects: s.objects}
	raw, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return fmt.Errorf("storage: marshal: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".db-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return err
	}
	return nil
}

// deepCopy returns a value-independent copy so callers can never mutate
// persisted state through an earlier returned pointer.
func deepCopy(w *v1.Workload) *v1.Workload {
	raw, err := json.Marshal(w)
	if err != nil {
		panic("storage: internal marshal failure: " + err.Error())
	}
	var out v1.Workload
	if err := json.Unmarshal(raw, &out); err != nil {
		panic("storage: internal unmarshal failure: " + err.Error())
	}
	return &out
}
