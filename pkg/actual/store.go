// Package actual persists the "real world" side of the controller: the
// simulated resources owned by the actuator. Unlike the control-plane
// store, the contents here represent work that already took effect, so
// they must survive process crashes and restarts without losing resource
// identity.
//
// Two backends are provided:
//
//   - MemStore: process-local, used by unit tests that do not restart.
//   - DirStore: one JSON file per resource written atomically
//     (temp file + fsync + rename) in a directory. It survives hard
//     process kills and is what makes restart reconciliation possible.
//
// Listing is cursor based: callers scan pages instead of loading every
// resource into memory at once, and every operation honours context
// cancellation.
package actual

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

// ErrNotFound means no durable actual resource exists for the key.
var ErrNotFound = errors.New("actual resource not found")

// Store is the persistence contract for actual resources.
type Store interface {
	// Load returns a detached copy of the resource owned by key.
	Load(ctx context.Context, key string) (*apis.ActualState, error)
	// Save writes the resource durably (atomic on the directory backend).
	Save(ctx context.Context, key string, state *apis.ActualState) error
	// Remove deletes the durable record. Removing a missing record is a
	// no-op so retried deletions stay idempotent across crashes.
	Remove(ctx context.Context, key string) error
	// ListPage returns up to limit distinct keys strictly after cursor,
	// in lexicographic order, plus the cursor to continue with ("" when
	// the scan is finished). A non-positive limit is rejected.
	ListPage(ctx context.Context, cursor string, limit int) (keys []string, next string, err error)
}

// MemStore keeps actual resources in process memory.
type MemStore struct {
	mu    sync.RWMutex
	state map[string]*apis.ActualState
}

// NewMemStore creates an empty in-memory actual store.
func NewMemStore() *MemStore {
	return &MemStore{state: map[string]*apis.ActualState{}}
}

func (s *MemStore) Load(_ context.Context, key string) (*apis.ActualState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x, ok := s.state[key]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *x
	return &cp, nil
}

func (s *MemStore) Save(_ context.Context, key string, state *apis.ActualState) error {
	if state == nil {
		return fmt.Errorf("actual: cannot save nil state for %q", key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *state
	s.state[key] = &cp
	return nil
}

func (s *MemStore) Remove(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state, key)
	return nil
}

func (s *MemStore) ListPage(_ context.Context, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		return nil, "", fmt.Errorf("actual: page limit must be positive, got %d", limit)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.state))
	for n := range s.state {
		if n > cursor {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) <= limit {
		return names, "", nil
	}
	return names[:limit], names[limit-1], nil
}

// fileRecord is the on-disk wire format for one actual resource.
type fileRecord struct {
	FormatVersion int               `json:"formatVersion"`
	Key           string            `json:"key"`
	Actual        *apis.ActualState `json:"actual"`
}

const (
	fileFormatVersion = 1
	fileSuffix        = ".json"
)

// DirStore persists one resource per file inside a directory. Every
// save is a temp-file + fsync + rename, so a crash at any point leaves
// either the previous file or the new one, never a torn record.
type DirStore struct {
	dir string
}

// NewDirStore opens (creating if needed) the persistence directory.
func NewDirStore(ctx context.Context, dir string) (*DirStore, error) {
	if dir == "" {
		return nil, errors.New("actual: empty directory")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("actual: create dir %q: %w", dir, err)
	}
	return &DirStore{dir: dir}, nil
}

// Dir reports the backing directory (used by the demo/tests).
func (s *DirStore) Dir() string { return s.dir }

func (s *DirStore) path(key string) (string, error) {
	if _, err := s.recordKey(nil, key); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, key+fileSuffix), nil
}

// recordKey validates key without touching disk (ctx unused but kept on
// path for symmetry).
func (s *DirStore) recordKey(_ context.Context, key string) (string, error) {
	// Object names are admission-validated ([a-z0-9-]{1,63}); reject
	// anything that could escape the directory as defence in depth.
	if key == "" || key == "." || key == ".." {
		return "", fmt.Errorf("actual: invalid resource key %q", key)
	}
	for _, r := range key {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			return "", fmt.Errorf("actual: invalid resource key %q", key)
		}
	}
	return key, nil
}

func (s *DirStore) loadRecord(key string) (*fileRecord, error) {
	if _, err := s.recordKey(nil, key); err != nil {
		return nil, err
	}
	p := filepath.Join(s.dir, key+fileSuffix)
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var rec fileRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *DirStore) Load(ctx context.Context, key string) (*apis.ActualState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rec, err := s.loadRecord(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("actual: corrupt record %q: %w", key, err)
	}
	if rec.FormatVersion != fileFormatVersion {
		return nil, fmt.Errorf("actual: unsupported record version %d for %q", rec.FormatVersion, key)
	}
	if rec.Key != key || rec.Actual == nil {
		return nil, fmt.Errorf("actual: record %q is malformed", key)
	}
	cp := *rec.Actual
	return &cp, nil
}

func (s *DirStore) Save(ctx context.Context, key string, state *apis.ActualState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("actual: cannot save nil state for %q", key)
	}
	p, err := s.path(key)
	if err != nil {
		return err
	}
	rec := fileRecord{FormatVersion: fileFormatVersion, Key: key, Actual: state}
	data, err := json.MarshalIndent(&rec, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".actual-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, p); err != nil {
		return err
	}
	committed = true
	// Best-effort directory fsync so the rename itself is durable. A
	// failure here does not poison the in-memory view: the file exists.
	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (s *DirStore) Remove(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ListPage scans record filenames, so a page costs O(limit) memory
// regardless of how many resources exist on disk.
func (s *DirStore) ListPage(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		return nil, "", fmt.Errorf("actual: page limit must be positive, got %d", limit)
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, "", err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if len(n) <= len(fileSuffix) || n[len(n)-len(fileSuffix):] != fileSuffix {
			continue
		}
		key := n[:len(n)-len(fileSuffix)]
		// Only files that are both valid keys and valid records count as
		// actual resources: a control-plane snapshot or stray file sharing
		// the directory must never be audited as an orphan resource.
		if _, err := s.recordKey(nil, key); err != nil {
			continue
		}
		if rec, err := s.loadRecord(key); err != nil || rec.Key != key {
			continue
		}
		if key > cursor {
			names = append(names, key)
		}
	}
	sort.Strings(names)
	if len(names) <= limit {
		return names, "", nil
	}
	return names[:limit], names[limit-1], nil
}
