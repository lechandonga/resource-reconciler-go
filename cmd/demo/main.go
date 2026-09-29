// Command demo runs a local scenario against the in-process declarative
// controller and prints, for every step, the input, the decision (with
// reason code) and the resulting state.
//
// Run with:
//
//	go run ./cmd/demo
//
// Everything is in-process: the "control plane" is an in-memory store
// with atomic snapshot files, the "cluster" is a fake actuator.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/controller"
	"github.com/lechandonga/resource-reconciler-go/pkg/engine"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

func main() {
	snapPath := flag.String("snapshot", filepath.Join(os.TempDir(), "reconciler-demo-snapshot.json"), "snapshot file used for the restart step")
	actualPath := flag.String("actual", filepath.Join(os.TempDir(), "reconciler-demo-actual.json"), "durable actual backend file")
	crash := flag.String("crash", "", "inject a crash at a barrier: AfterEnsure|AfterRemove (key set by -crash-key)")
	crashKey := flag.String("crash-key", "a", "object key the crash hook applies to")
	recoverOnly := flag.Bool("recover-only", false, "boot, run startup reconciliation, settle and exit (recovery verification)")
	flag.Parse()

	if *crash != "" && *crash != string(engine.CrashAfterEnsure) && *crash != string(engine.CrashAfterRemove) {
		fmt.Fprintf(os.Stderr, "unknown crash point %q\n", *crash)
		os.Exit(2)
	}

	// A crash run must leave the durable files on disk so the plain
	// follow-up run demonstrates recovery; skip the in-process scenario.
	if *crash != "" {
		runCrashDemo(*snapPath, *actualPath, *crash, *crashKey)
		return
	}
	if *recoverOnly {
		runRecoverOnly(*snapPath, *actualPath)
		return
	}
	runMainDemo(*snapPath, *actualPath)
}

func runRecoverOnly(snapPath, actualPath string) {
	store := storage.NewMemStore()
	must(store.LoadFile(snapPath))
	act, err := engine.NewDurableFakeActuator(actualPath)
	must(err)
	sink := &engine.WriterSink{W: os.Stdout}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := controller.New(controller.Config{
		Workers: 4, QueueCapacity: 64, Store: store, Actuator: act,
		Clock: engine.RealClock{}, Backoff: engine.DefaultBackoff(), Sink: sink,
		Jitter: deterministicHalfJitter, Recovery: controller.DefaultRecoveryConfig(),
	})
	must(err)
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	// Persist the control plane continuously, as the real persistence
	// layer would, so the adopted record survives this process exit and a
	// subsequent run shows "Unchanged" rather than re-adopting.
	stopSnap := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopSnap:
				_ = store.SaveToFile(snapPath)
				return
			case <-t.C:
				_ = store.SaveToFile(snapPath)
			}
		}
	}()
	waitIdle(c)

	all, _ := store.List(ctx)
	fmt.Printf("\n== recovery result: objects=%d actual-resources=%d ==\n", len(all), act.CountActual())
	for _, o := range all {
		id := "<none>"
		if a := act.SnapshotActual(o.Metadata.Name); a != nil {
			id = a.ResourceID
		}
		rec := "<none>"
		if o.Status != nil && o.Status.Actual != nil {
			rec = o.Status.Actual.ResourceID
		}
		fmt.Printf("  %s: actualID=%s recordedID=%s deleting=%v sideEffects(lifetime)=%d\n",
			o.Metadata.Name, id, rec, o.Metadata.DeletionTimestamp != nil,
			act.SideEffectCount(o.Metadata.Name))
	}
	close(stopSnap)
	cancel()
	<-done
}

func runMainDemo(snapPath, actualPath string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = os.Remove(snapPath)
	_ = os.Remove(actualPath)

	store := storage.NewMemStore()
	act, err := engine.NewDurableFakeActuator(actualPath)
	must(err)
	clk := engine.RealClock{}
	sink := &engine.WriterSink{W: os.Stdout}

	c, err := controller.New(controller.Config{
		Workers: 2, QueueCapacity: 32, Store: store, Actuator: act, Clock: clk,
		Backoff: engine.DefaultBackoff(), Sink: sink, Jitter: deterministicHalfJitter,
		Recovery: controller.DefaultRecoveryConfig(),
	})
	must(err)

	runDone := make(chan struct{})
	go func() { _ = c.Run(ctx); close(runDone) }()

	section("1. declare desired state (a and b, b depends on a)")
	must(c.Apply(ctx, widget(apis.APIVersionV1, "legacy", `{"replicas":2,"mode":"Off"}`)))
	must(c.Apply(ctx, widget(apis.APIVersionV2, "a", `{"replicas":3,"mode":"On"}`)))
	must(c.Apply(ctx, widget(apis.APIVersionV2, "b", `{"replicas":1,"mode":"Auto","graceSeconds":45,"dependsOn":["a"]}`)))
	waitIdle(c)
	dump(ctx, store, "legacy", "a", "b")

	section("2. repeat identical desired state (must be a no-op)")
	before := rv(ctx, store, "a")
	must(c.Apply(ctx, widget(apis.APIVersionV2, "a", `{"replicas":3,"mode":"On"}`)))
	waitIdle(c)
	after := rv(ctx, store, "a")
	fmt.Printf("a resourceVersion before=%d after=%d (equal => no unnecessary rewrite)\n", before, after)

	section("3. drift: change desired replicas, expect a single update")
	must(c.Apply(ctx, widget(apis.APIVersionV2, "a", `{"replicas":5,"mode":"On"}`)))
	waitIdle(c)
	fmt.Printf("a side effects so far: %d\n", act.SideEffectCount("a"))

	section("4. illegal specs are rejected with distinct reasons")
	reject(ctx, c, widget(apis.APIVersionV2, "bad-replicas", `{"replicas":99}`))
	reject(ctx, c, widget("reconciler.example.com/v9", "bad-version", `{}`))
	reject(ctx, c, widget(apis.APIVersionV2, "unknown-field", `{"nope":1}`))
	reject(ctx, c, widget(apis.APIVersionV1, "bad-mode", `{"mode":"Auto"}`))
	reject(ctx, c, widget(apis.APIVersionV2, "self", `{"dependsOn":["self"]}`))
	reject(ctx, c, widget(apis.APIVersionV2, "ghost", `{"dependsOn":["does-not-exist"]}`))

	section("5. transient backend failure: bounded backoff then converge")
	must(c.Apply(ctx, widget(apis.APIVersionV2, "flaky", `{"replicas":1}`)))
	waitIdle(c)
	act.SetRule("flaky", engine.FailureRule{Kind: engine.FailTransient, Times: 2})
	must(c.Apply(ctx, widget(apis.APIVersionV2, "flaky", `{"replicas":2}`)))
	waitIdle(c)

	section("6. deterministic failure: observable terminal state, no busy loop")
	must(c.Apply(ctx, widget(apis.APIVersionV2, "doomed", `{"replicas":1}`)))
	waitIdle(c)
	act.SetRule("doomed", engine.FailureRule{Kind: engine.FailDeterministic})
	must(c.Apply(ctx, widget(apis.APIVersionV2, "doomed", `{"replicas":2}`)))
	waitIdle(c)

	// Capture identities and side-effect counts before the restart.
	idA := act.SnapshotActual("a").ResourceID
	idLegacy := act.SnapshotActual("legacy").ResourceID
	effectsA := act.SideEffectCount("a")

	section("7. restart: durable control + actual state reloaded in place")
	must(store.SaveToFile(snapPath))
	cancel()
	<-runDone

	// New process lifetime: same durable files, fresh in-memory state.
	store2 := storage.NewMemStore()
	must(store2.LoadFile(snapPath))
	act2, err := engine.NewDurableFakeActuator(actualPath)
	must(err)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c2, err := controller.New(controller.Config{
		Workers: 4, QueueCapacity: 32, Store: store2, Actuator: act2,
		Clock: engine.RealClock{}, Backoff: engine.DefaultBackoff(), Sink: sink,
		Jitter: deterministicHalfJitter, Recovery: controller.DefaultRecoveryConfig(),
	})
	must(err)
	go func() { _ = c2.Run(ctx2) }()
	waitIdle(c2)

	newIDA := act2.SnapshotActual("a")
	newIDLegacy := act2.SnapshotActual("legacy")
	fmt.Printf("a:      identity before=%s after=%v  side effects new-lifetime=%d (before total=%d)\n",
		idA, safeID(newIDA), act2.SideEffectCount("a"), effectsA)
	fmt.Printf("legacy: identity before=%s after=%v  side effects new-lifetime=%d\n",
		idLegacy, safeID(newIDLegacy), act2.SideEffectCount("legacy"))
	if newIDA != nil && newIDA.ResourceID != idA {
		fmt.Println("UNEXPECTED: a identity changed across restart")
	}
	if act2.SideEffectCount("a") != 0 || act2.SideEffectCount("legacy") != 0 {
		fmt.Println("UNEXPECTED: restart re-executed side effects")
	}
	fmt.Println("demo complete")
	fmt.Printf("\ntip: reproduce a crash window with:\n"+
		"  rm -f %s %s\n"+
		"  go run ./cmd/demo -crash AfterEnsure -crash-key a\n"+
		"  go run ./cmd/demo   # recovery run, then rerun to see idempotence\n",
		snapPath, actualPath)
}

func safeID(st *apis.ActualState) string {
	if st == nil {
		return "<missing>"
	}
	return st.ResourceID
}

// runCrashDemo creates one object and kills itself at the named barrier,
// leaving the durable control snapshot and actual backend exactly in the
// crash window. A subsequent plain `go run ./cmd/demo` performs startup
// reconciliation and converges without duplicate side effects.
func runCrashDemo(snapPath, actualPath, crash, crashKey string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewMemStore()
	if _, err := os.Stat(snapPath); err == nil {
		must(store.LoadFile(snapPath))
	}
	act, err := engine.NewDurableFakeActuator(actualPath)
	must(err)
	sink := &engine.WriterSink{W: os.Stdout}
	hook := func(key string, at engine.CrashPoint) bool {
		if key == crashKey && string(at) == crash {
			fmt.Printf(">>> CRASH at %s: action committed durably, control record pending\n", at)
			// Persist the control plane exactly as it stands (finalizer may
			// already be present, actual record must not), then stop.
			if err := store.SaveToFile(snapPath); err != nil {
				fmt.Fprintf(os.Stderr, "snapshot at crash: %v\n", err)
			}
			os.Exit(42)
		}
		return false
	}
	c, err := controller.New(controller.Config{
		Workers: 2, QueueCapacity: 32, Store: store, Actuator: act,
		Clock: engine.RealClock{}, Backoff: engine.DefaultBackoff(), Sink: sink,
		CrashHook: hook, Recovery: controller.DefaultRecoveryConfig(),
	})
	must(err)
	go func() { _ = c.Run(ctx) }()

	must(c.Apply(ctx, widget(apis.APIVersionV2, crashKey, `{"replicas":3,"mode":"On"}`)))

	// Also keep the desired snapshot on disk continuously enough to
	// survive the os.Exit (the hook snapshots too); wait briefly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if act.SnapshotActual(crashKey) != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if act.SnapshotActual(crashKey) == nil {
		fmt.Fprintln(os.Stderr, "crash point never reached (backend missing)")
		os.Exit(1)
	}
	fmt.Printf("durable actual exists with identity %s; run 'go run ./cmd/demo' to recover\n",
		act.SnapshotActual(crashKey).ResourceID)
	time.Sleep(time.Second)
}
func widget(version, name, specJSON string) *apis.RawObject {
	return &apis.RawObject{
		APIVersion: version,
		Kind:       apis.KindWidget,
		Metadata:   apis.ObjectMeta{Name: name},
		Spec:       []byte(specJSON),
	}
}

func section(title string) {
	fmt.Printf("\n==== %s ====\n", title)
}

func waitIdle(c *controller.Controller) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.AwaitIdle(ctx, 5*time.Millisecond); err != nil {
		fmt.Println("await idle timed out")
	}
}

func rv(ctx context.Context, s *storage.MemStore, name string) int64 {
	_, v, err := s.Get(ctx, name)
	if err != nil {
		return -1
	}
	return v
}

func reject(ctx context.Context, c *controller.Controller, o *apis.RawObject) {
	err := c.Apply(ctx, o)
	var ae *apis.AdmissionError
	if errors.As(err, &ae) {
		fmt.Printf("REJECTED input=%s reason=%s message=%s\n", o.Metadata.Name, ae.Reason, ae.Message)
		return
	}
	fmt.Printf("UNEXPECTED acceptance for %s (err=%v)\n", o.Metadata.Name, err)
}

func dump(ctx context.Context, s *storage.MemStore, names ...string) {
	for _, n := range names {
		o, _, err := s.Get(ctx, n)
		if err != nil {
			fmt.Printf("[%s] <gone>\n", n)
			continue
		}
		data, _ := json.MarshalIndent(map[string]any{
			"name":       o.Metadata.Name,
			"apiVersion": o.APIVersion,
			"generation": o.Metadata.Generation,
			"finalizers": o.Metadata.Finalizers,
			"deleting":   o.Metadata.DeletionTimestamp != nil,
			"status":     o.Status,
		}, "  ", "  ")
		fmt.Printf("[%s] %s\n", n, string(data))
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// deterministicHalfJitter keeps demo output reproducible while still
// exercising the jitter code path.
func deterministicHalfJitter() float64 { return 0.5 }
