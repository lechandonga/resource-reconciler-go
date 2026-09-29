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

	"github.com/lechandonga/resource-reconciler-go/pkg/actual"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/controller"
	"github.com/lechandonga/resource-reconciler-go/pkg/engine"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

func main() {
	snapPath := flag.String("snapshot", filepath.Join(os.TempDir(), "reconciler-demo-snapshot.json"), "snapshot file used for the restart step")
	actualDir := flag.String("actual-dir", filepath.Join(os.TempDir(), "reconciler-demo-actual"), "directory backing the durable actual resources")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewMemStore()
	actualStore, err := actual.NewDirStore(ctx, *actualDir)
	must(err)
	act := engine.NewFakeActuatorWithStore(actualStore)
	clk := engine.RealClock{}
	sink := &engine.WriterSink{W: os.Stdout}
	act.SetCrashHook(engine.EnvCrashHook())

	c, err := controller.New(controller.Config{
		Workers:       2,
		QueueCapacity: 32,
		Store:         store,
		Actuator:      act,
		Clock:         clk,
		Backoff:       engine.DefaultBackoff(),
		Sink:          sink,
		Jitter:        deterministicHalfJitter,
	})
	must(err)

	runDone := make(chan struct{})
	go func() { _ = c.Run(ctx); close(runDone) }()
	defer func() { cancel(); <-runDone }()

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
	dump(ctx, store, "a")

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
	dump(ctx, store, "flaky")

	section("6. deterministic failure: observable terminal state, no busy loop")
	must(c.Apply(ctx, widget(apis.APIVersionV2, "doomed", `{"replicas":1}`)))
	waitIdle(c)
	act.SetRule("doomed", engine.FailureRule{Kind: engine.FailDeterministic})
	must(c.Apply(ctx, widget(apis.APIVersionV2, "doomed", `{"replicas":2}`)))
	waitIdle(c)
	ops := len(act.Ops())
	waitIdle(c)
	if len(act.Ops()) != ops {
		fmt.Println("UNEXPECTED: deterministic failure kept hitting backend")
	} else {
		fmt.Printf("deterministic failure stable across extra reconcile (ops=%d)\n", ops)
	}
	dump(ctx, store, "doomed")

	section("7. ordered deletion: delete a blocked while b depends on it")
	must(c.Delete(ctx, "a"))
	waitIdle(c)
	if _, _, err := store.Get(ctx, "a"); errors.Is(err, storage.ErrNotFound) {
		fmt.Println("UNEXPECTED: a deleted while dependent b still exists")
	} else {
		fmt.Println("a correctly kept alive (dependent b still references it)")
	}
	must(c.Delete(ctx, "b"))
	waitIdle(c)
	must(c.Delete(ctx, "a"))
	waitIdle(c)
	if _, _, err := store.Get(ctx, "a"); errors.Is(err, storage.ErrNotFound) {
		fmt.Println("a deleted after its dependent b was removed")
	}

	section("8. restart: snapshot, reload, resume without duplicate side effects")
	must(err)
	_ = os.Remove(*snapPath)
	must(store.SaveToFile(*snapPath))
	effectsBefore := act.SideEffectCount("legacy")
	cancel()
	<-runDone

	store2 := storage.NewMemStore()
	must(store2.LoadFile(*snapPath))
	ctx2, cancel2 := context.WithCancel(context.Background())
	actualStore2, err := actual.NewDirStore(ctx2, *actualDir)
	must(err)
	act2 := engine.NewFakeActuatorWithStore(actualStore2)
	act2.SetCrashHook(engine.EnvCrashHook())
	c2, err := controller.New(controller.Config{
		Workers: 4, QueueCapacity: 32, Store: store2, Actuator: act2,
		Clock: engine.RealClock{}, Backoff: engine.DefaultBackoff(), Sink: sink, Jitter: deterministicHalfJitter,
		RecoveryPageSize: 16,
	})
	must(err)
	go func() { _ = c2.Run(ctx2) }()
	defer func() { cancel2() }()
	waitIdle(c2)
	all, _ := store2.List(ctx2)
	fmt.Printf("after restart: %d objects re-loaded; legacy side effects before=%d after(new backend)=%d\n",
		len(all), effectsBefore, act2.SideEffectCount("legacy"))
	dump(ctx2, store2, "legacy")
	fmt.Printf("durable actual resources kept in %q across the restart\n", *actualDir)
	fmt.Println("demo complete")
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
