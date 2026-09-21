package dataplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// MemClient is the file-backed, failure-injectable implementation of Client.
type MemClient struct {
	mu        sync.Mutex
	statePath string
	instances map[string]Instance // key = workload "/" index
	hook      Hook
	closed    bool
}

type memSnapshot struct {
	Instances []Instance `json:"instances"`
}

// Hook allows tests to inject classified failures. Returning nil means "let
// the operation proceed". Hooks must be safe for concurrent use.
type Hook func(op string, id InstanceID, generation int64) error

// NewMemClient loads data-plane state from statePath (empty means in-memory
// only). The hook, when non-nil, can inject failures per operation.
func NewMemClient(statePath string, hook Hook) (*MemClient, error) {
	c := &MemClient{
		statePath: statePath,
		instances: map[string]Instance{},
		hook:      hook,
	}
	if statePath == "" {
		return c, nil
	}
	raw, err := os.ReadFile(statePath)
	switch {
	case err == nil:
		var snap memSnapshot
		if err := json.Unmarshal(raw, &snap); err != nil {
			return nil, fmt.Errorf("dataplane: corrupt state %s: %w", statePath, err)
		}
		for _, in := range snap.Instances {
			c.instances[key(in.ID)] = in
		}
	case errors.Is(err, os.ErrNotExist):
		// fresh
	default:
		return nil, err
	}
	return c, nil
}

func key(id InstanceID) string { return id.Workload + "/" + fmt.Sprintf("%06d", id.Index) }

// SetHook replaces (or clears) the failure-injection hook. Safe for
// concurrent use with in-flight operations.
func (c *MemClient) SetHook(h Hook) {
	c.mu.Lock()
	c.hook = h
	c.mu.Unlock()
}

// EnsureInstance creates or updates the instance. Repeated calls with the
// same (id, image, generation) produce no repeated side effect.
func (c *MemClient) EnsureInstance(ctx context.Context, id InstanceID, image string, generation int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("dataplane: client closed")
	}
	// Inject failure before any state change, so a failed call leaves no
	// partial effect behind.
	if c.hook != nil {
		if err := c.hook("ensure", id, generation); err != nil {
			return err
		}
	}
	if err := classifyImage(image); err != nil {
		return err
	}
	existing, present := c.instances[key(id)]
	if present && existing.Image == image && existing.Generation == generation {
		return nil // idempotent no-op: nothing to apply
	}
	c.instances[key(id)] = Instance{ID: id, Image: image, Generation: generation}
	return c.persistLocked()
}

// RemoveInstance deletes the instance. Removing an absent instance is a
// successful no-op (idempotent deletion).
func (c *MemClient) RemoveInstance(ctx context.Context, id InstanceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("dataplane: client closed")
	}
	if c.hook != nil {
		if err := c.hook("remove", id, 0); err != nil {
			return err
		}
	}
	if _, present := c.instances[key(id)]; !present {
		return nil
	}
	delete(c.instances, key(id))
	return c.persistLocked()
}

// ListInstances returns all instances of one workload ordered by index.
func (c *MemClient) ListInstances(ctx context.Context, workload string) ([]Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := workload + "/"
	out := []Instance{}
	for _, in := range c.instances {
		if strings.HasPrefix(key(in.ID), prefix) {
			out = append(out, in)
		}
	}
	// Insertion sort by index — small slices, deterministic order.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].ID.Index > out[j].ID.Index; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out, nil
}

func (c *MemClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.persistLocked()
}

func (c *MemClient) persistLocked() error {
	if c.statePath == "" {
		return nil
	}
	snap := memSnapshot{Instances: make([]Instance, 0, len(c.instances))}
	for _, in := range c.instances {
		snap.Instances = append(snap.Instances, in)
	}
	raw, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return fmt.Errorf("dataplane: marshal: %w", err)
	}
	dir := filepath.Dir(c.statePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".dp-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, c.statePath); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// classifyImage deterministically rejects images that can never start.
// Convention used by the simulator:
//   - image name ending in "/bad" or tag ":broken" -> deterministic failure
//   - image containing "flake" -> transient failure (handled by the hook in
//     tests instead; here it is still accepted by the data plane)
func classifyImage(image string) error {
	if strings.HasSuffix(image, "/bad") {
		return &Failure{
			Class:   ClassDeterministic,
			Code:    "ImageImmutableError",
			Message: "image pulls deterministically fail: " + image,
		}
	}
	if strings.HasSuffix(image, ":broken") {
		return &Failure{
			Class:   ClassDeterministic,
			Code:    "ImageImmutableError",
			Message: "image manifest is broken: " + image,
		}
	}
	return nil
}
