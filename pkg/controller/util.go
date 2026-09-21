package controller

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/engine"
)

func jsonUnmarshal(data []byte, v any) error {
	if len(data) == 0 {
		data = []byte("null")
	}
	return json.Unmarshal(data, v)
}

func jsonEqual(a, b any) bool {
	return reflect.DeepEqual(a, b)
}

// newClockTicker returns a channel that fires every period on the given
// clock abstraction. For FakeClock tests the controller advances time
// manually; this ticker is driven by a lightweight polling goroutine so
// it respects both real and fake clocks.
func newClockTicker(ctx context.Context, clk engine.Clock, period time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	go func() {
		for {
			if err := clk.Sleep(ctx, period); err != nil {
				return
			}
			select {
			case ch <- clk.Now():
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}
