// Package integration contains the controller's end-to-end tests. The focus is
// on abnormal paths: illegal specs, version compatibility, concurrent
// updates, failure classification and restart recovery. All tests must pass
// under -race.
package integration

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/admission"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/controller"
	"github.com/lechandonga/resource-reconciler-go/pkg/dataplane"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

// testEnv is one isolated controller runtime backed by temp files.
type testEnv struct {
	t      *testing.T
	dir    string
	dbPath string
	dpPath string
	store  *storage.FileStore
	dp     *dataplane.MemClient
	ctrl   *controller.Controller
	logMu  sync.Mutex
	logs   []string
	hook   dataplane.Hook
	hookMu sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()
	env := &testEnv{
		t:      t,
		dir:    dir,
		dbPath: filepath.Join(dir, "control-plane.json"),
		dpPath: filepath.Join(dir, "data-plane.json"),
	}
	env.start()
	return env
}

// start (re)builds store/dp/controller from persisted files. Used both for
// the initial start and for simulated process restarts.
func (e *testEnv) start() {
	st, err := storage.NewFileStore(e.dbPath)
	if err != nil {
		e.t.Fatalf("open store: %v", err)
	}
	e.hookMu.RLock()
	hook := e.hook
	e.hookMu.RUnlock()
	dp, err := dataplane.NewMemClient(e.dpPath, hook)
	if err != nil {
		e.t.Fatalf("open dp: %v", err)
	}
	e.store, e.dp = st, dp

	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.ctrl = controller.New(controller.Config{
		Store:    st,
		DP:       dp,
		Reviewer: admission.NewChain(),
		Workers:  4,
		QueueCap: 8,
		Now:      func() time.Time { return time.Unix(1700000000, 0).UTC() },
		Backoff:  func(int) time.Duration { return 2 * time.Millisecond },
		Log:      e.recordLog,
	})
	if err := e.ctrl.Start(e.ctx); err != nil {
		e.t.Fatalf("start: %v", err)
	}
}

func (e *testEnv) stop() {
	if e.cancel != nil {
		e.cancel()
	}
	if e.ctrl != nil {
		e.ctrl.Stop()
	}
}

// restart simulates a process crash/restart: everything is rebuilt from the
// persisted files. A new hook can be installed before the restart.
func (e *testEnv) restart(hook dataplane.Hook) {
	e.stop()
	e.hookMu.Lock()
	e.hook = hook
	e.hookMu.Unlock()
	e.start()
}

func (e *testEnv) setHook(h dataplane.Hook) {
	e.hookMu.Lock()
	e.hook = h
	e.hookMu.Unlock()
	if e.dp != nil {
		e.dp.SetHook(h)
	}
}

func (e *testEnv) recordLog(format string, args ...any) {
	line := sprintf(format, args...)
	e.logMu.Lock()
	e.logs = append(e.logs, line)
	e.logMu.Unlock()
}

// dumpLogs prints every recorded line: tests show inputs, decisions and
// final states, satisfying traceability requirements.
func (e *testEnv) dumpLogs() {
	e.logMu.Lock()
	defer e.logMu.Unlock()
	e.t.Logf("--- controller log (%d lines) ---", len(e.logs))
	for _, l := range e.logs {
		e.t.Log(l)
	}
	e.t.Log("--- end log ---")
}

func (e *testEnv) submit(raw string) admission.Decision {
	return e.ctrl.Submit(e.ctx, []byte(raw))
}

func (e *testEnv) get(name string) *v1.Workload {
	w, err := e.ctrl.Get(e.ctx, name)
	if err != nil {
		e.t.Fatalf("get %s: %v", name, err)
	}
	return w
}

func (e *testEnv) waitPhase(name string, phases ...v1.Phase) *v1.Workload {
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	w, err := e.ctrl.WaitForPhase(ctx, name, phases...)
	if err != nil {
		e.dumpLogs()
		e.t.Fatalf("waitPhase %s: %v", name, err)
	}
	return w
}

func (e *testEnv) eventually(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// waitConverged waits until name is Available AND has observed its latest
// generation with the expected replica count — i.e. the newest desired state
// has actually been reached, not a stale Available from before the update.
func (e *testEnv) waitConverged(name string, replicas int) *v1.Workload {
	ok := e.eventually(func() bool {
		w, err := e.ctrl.Get(e.ctx, name)
		if err != nil {
			return false
		}
		return w.Status.Phase == v1.PhaseAvailable &&
			w.Status.ObservedGeneration == w.ObjectMeta.Generation &&
			len(w.Status.Instances) == replicas
	})
	if !ok {
		e.dumpLogs()
		w, _ := e.ctrl.Get(e.ctx, name)
		e.t.Fatalf("waitConverged %s replicas=%d: last state=%s", name, replicas, summarizeForTest(w))
	}
	return e.get(name)
}

func summarizeForTest(w *v1.Workload) string {
	if w == nil {
		return "<gone>"
	}
	return sprintf("phase=%s gen=%d observed=%d instances=%d attempts=%d",
		w.Status.Phase, w.ObjectMeta.Generation, w.Status.ObservedGeneration,
		len(w.Status.Instances), w.Status.Attempts)
}
