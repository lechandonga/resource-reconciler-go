// Command demo runs a small, fully local in-process scenario demonstrating
// declaration and convergence, admission rejection, ordered dependencies,
// bounded backoff, version upgrade and restart. Everything is persisted to a
// temporary directory, so the run is self-contained.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/admission"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/controller"
	"github.com/lechandonga/resource-reconciler-go/pkg/dataplane"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

func main() {
	dir, err := os.MkdirTemp("", "reconciler-demo-")
	must(err)
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "cp.json")
	dpPath := filepath.Join(dir, "dp.json")

	log := func(format string, args ...any) { fmt.Printf("  "+format+"\n", args...) }

	start := func(hook dataplane.Hook) (*controller.Controller, context.CancelFunc) {
		st, err := storage.NewFileStore(dbPath)
		must(err)
		dp, err := dataplane.NewMemClient(dpPath, hook)
		must(err)
		c := controller.New(controller.Config{
			Store:    st,
			DP:       dp,
			Reviewer: admission.NewChain(),
			Workers:  2,
			QueueCap: 16,
			Log:      log,
		})
		ctx, cancel := context.WithCancel(context.Background())
		must(c.Start(ctx))
		return c, cancel
	}

	ctx := context.Background()

	// A flaky data plane: the first two ensure calls fail transiently.
	var flake int32
	hook := func(op string, _ dataplane.InstanceID, _ int64) error {
		if op == "ensure" && atomic.AddInt32(&flake, 1) <= 2 {
			return &dataplane.Failure{Class: dataplane.ClassTransient, Code: "FlakyAPI", Message: "simulated flake"}
		}
		return nil
	}
	c, cancel := start(hook)

	section("1. illegal spec is rejected with a specific reason")
	dec := c.Submit(ctx, []byte(`{"apiVersion":"core.example.com/v1","kind":"Workload","metadata":{"name":"bad"},"spec":{"replicas":99}}`))
	fmt.Printf("  allowed=%v reason=%s message=%s\n", dec.Allowed, dec.Reason, dec.Message)

	section("2. historical v1beta1 manifest is upgraded and converges (with bounded backoff)")
	dec = c.Submit(ctx, []byte(`{
	  "apiVersion":"core.example.com/v1beta1","kind":"Workload",
	  "metadata":{"name":"legacy"},
	  "spec":{"replicaCount":2,"containerImage":"alpine:3.19"}
	}`))
	fmt.Printf("  admitted=%v stored-version=%s\n", dec.Allowed,
		dec.Workload.ObjectMeta.Annotations[v1.AnnStoredAPIVersion])
	wait(c, "legacy", v1.PhaseAvailable)

	section("3. ordered lifecycle: dependent waits for its dependency")
	c.Submit(ctx, []byte(v1Manifest("web", 1, "nginx:1", "db")))
	time.Sleep(30 * time.Millisecond)
	show(c, "web")
	c.Submit(ctx, []byte(v1Manifest("db", 1, "redis:7")))
	wait(c, "web", v1.PhaseAvailable)

	section("4. no-op resubmit does not rewrite the object")
	before := get(c, "db")
	c.Submit(ctx, []byte(v1Manifest("db", 1, "redis:7")))
	after := get(c, "db")
	fmt.Printf("  rv before=%d after=%d (equal means no rewrite)\n",
		before.ObjectMeta.ResourceVersion, after.ObjectMeta.ResourceVersion)

	section("5. simulated restart resumes from persisted state")
	cancel()
	c.Stop()
	c2, cancel2 := start(nil) // healthy data plane after restart
	defer func() { c2.Stop(); cancel2() }()
	wait(c2, "legacy", v1.PhaseAvailable)
	show(c2, "legacy")

	section("6. ordered deletion: db cannot go while web depends on it")
	must(c2.Delete(ctx, "db"))
	time.Sleep(40 * time.Millisecond)
	show(c2, "db")
	must(c2.Delete(ctx, "web"))
	waitGone(c2, "web")
	waitGone(c2, "db")
	fmt.Println("  both objects physically deleted; no data-plane orphans")

	fmt.Println("\nDemo finished. State directory:", dir)
}

func section(title string) { fmt.Printf("\n=== %s ===\n", title) }

func v1Manifest(name string, replicas int, image string, deps ...string) string {
	d := "[]"
	if len(deps) > 0 {
		d = "["
		for i, x := range deps {
			if i > 0 {
				d += ","
			}
			d += `"` + x + `"`
		}
		d += "]"
	}
	return fmt.Sprintf(`{"apiVersion":"core.example.com/v1","kind":"Workload","metadata":{"name":%q},"spec":{"replicas":%d,"image":%q,"dependsOn":%s}}`,
		name, replicas, image, d)
}

func get(c *controller.Controller, name string) *v1.Workload {
	w, err := c.Get(context.Background(), name)
	must(err)
	return w
}

func show(c *controller.Controller, name string) {
	w := get(c, name)
	fmt.Printf("  %s: phase=%s gen=%d observed=%d instances=%d attempts=%d deleting=%t\n",
		name, w.Status.Phase, w.ObjectMeta.Generation, w.Status.ObservedGeneration,
		len(w.Status.Instances), w.Status.Attempts, w.ObjectMeta.DeletionTimestamp != nil)
	for _, cond := range w.Status.Conditions {
		fmt.Printf("    condition %-18s %-5s reason=%s %s\n", cond.Type, cond.Status, cond.Reason, cond.Message)
	}
}

func wait(c *controller.Controller, name string, phase v1.Phase) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := c.WaitForPhase(ctx, name, phase)
	must(err)
	fmt.Printf("  %s reached %s (instances=%d)\n", name, w.Status.Phase, len(w.Status.Instances))
}

func waitGone(c *controller.Controller, name string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.Get(context.Background(), name); err != nil {
			fmt.Printf("  %s physically deleted\n", name)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("demo: " + name + " was not deleted")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
