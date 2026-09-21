// Package storage is the in-process simulation of the control-plane
// persistence layer. Every write is guarded by a monotonic resource
// version implementing optimistic concurrency control.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
)

// Sentinel errors.
var (
	ErrNotFound      = errors.New("object not found")
	ErrConflict      = errors.New("object was modified concurrently")
	ErrAlreadyExists = errors.New("object already exists")
)

type entry struct {
	obj *apis.RawObject
	rv  int64
}

// MemStore is an in-memory implementation. SaveToFile/LoadFile turn it
// into a restart-surviving store for the demo and restart tests.
type MemStore struct {
	mu   sync.RWMutex
	data map[string]*entry
}

// NewMemStore creates an empty store.
func NewMemStore() *MemStore {
	return &MemStore{data: map[string]*entry{}}
}

// Get returns a detached copy of the object and its current resource
// version. The copy is detached: mutating it never changes the store.
func (s *MemStore) Get(_ context.Context, name string) (*apis.RawObject, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[name]
	if !ok {
		return nil, 0, ErrNotFound
	}
	return clone(e.obj), e.rv, nil
}

// List returns detached copies of all objects, ordered by name for
// deterministic iteration.
func (s *MemStore) List(_ context.Context) ([]*apis.RawObject, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.data))
	for n := range s.data {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]*apis.RawObject, 0, len(names))
	for _, n := range names {
		out = append(out, clone(s.data[n].obj))
	}
	return out, nil
}

// Create inserts a new object. It is stored at resource version 1.
func (s *MemStore) Create(_ context.Context, obj *apis.RawObject) (*apis.RawObject, int64, error) {
	if obj == nil || obj.Metadata.Name == "" {
		return nil, 0, fmt.Errorf("cannot store object without metadata.name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[obj.Metadata.Name]; ok {
		return nil, 0, ErrAlreadyExists
	}
	stored := clone(obj)
	e := &entry{obj: stored, rv: 1}
	s.data[stored.Metadata.Name] = e
	return clone(stored), 1, nil
}

// Update replaces an object only if expectedRV matches the stored
// resource version. A mismatch returns ErrConflict so callers retry from
// fresh state instead of overwriting a newer write. Each successful
// update bumps the resource version by one.
func (s *MemStore) Update(_ context.Context, obj *apis.RawObject, expectedRV int64) (*apis.RawObject, int64, error) {
	if obj == nil {
		return nil, 0, fmt.Errorf("cannot update nil object")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[obj.Metadata.Name]
	if !ok {
		return nil, 0, ErrNotFound
	}
	if e.rv != expectedRV {
		return nil, e.rv, ErrConflict
	}
	stored := clone(obj)
	e.obj = stored
	e.rv++
	return clone(stored), e.rv, nil
}

// Delete removes an object only if expectedRV matches. A mismatch
// returns ErrConflict so callers never delete a newer revision.
func (s *MemStore) Delete(_ context.Context, name string, expectedRV int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[name]
	if !ok {
		return ErrNotFound
	}
	if e.rv != expectedRV {
		return ErrConflict
	}
	delete(s.data, name)
	return nil
}

// snapshot is the on-disk wire format. Versioning the snapshot keeps
// restart compatibility explicit.
type snapshot struct {
	FormatVersion int              `json:"formatVersion"`
	Objects       []snapshotObject `json:"objects"`
}

type snapshotObject struct {
	ResourceVersion int64           `json:"resourceVersion"`
	Object          *apis.RawObject `json:"object"`
}

// SaveToFile persists the whole store atomically (temp file in the same
// directory + rename), so a crash mid-write leaves either the old or
// the new snapshot, never a torn file.
func (s *MemStore) SaveToFile(path string) error {
	s.mu.RLock()
	names := make([]string, 0, len(s.data))
	for n := range s.data {
		names = append(names, n)
	}
	sort.Strings(names)
	snap := snapshot{FormatVersion: 1}
	for _, n := range names {
		snap.Objects = append(snap.Objects, snapshotObject{
			ResourceVersion: s.data[n].rv,
			Object:          clone(s.data[n].obj),
		})
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// LoadFile replaces the store contents from a snapshot file.
func (s *MemStore) LoadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("corrupt snapshot %s: %w", path, err)
	}
	if snap.FormatVersion != 1 {
		return fmt.Errorf("unsupported snapshot format version %d", snap.FormatVersion)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = map[string]*entry{}
	for _, so := range snap.Objects {
		if so.Object == nil || so.Object.Metadata.Name == "" {
			return errors.New("snapshot contains object without metadata.name")
		}
		rv := so.ResourceVersion
		if rv < 1 {
			rv = 1
		}
		s.data[so.Object.Metadata.Name] = &entry{obj: clone(so.Object), rv: rv}
	}
	return nil
}

func clone(o *apis.RawObject) *apis.RawObject {
	if o == nil {
		return nil
	}
	data, err := json.Marshal(o)
	if err != nil {
		panic(fmt.Sprintf("internal: cannot marshal stored object: %v", err))
	}
	var out apis.RawObject
	if err := json.Unmarshal(data, &out); err != nil {
		panic(fmt.Sprintf("internal: cannot unmarshal stored object: %v", err))
	}
	return &out
}
