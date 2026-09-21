package engine

import (
	"testing"
	"time"
)

func TestBackoffIsBoundedAndFirstAttemptImmediate(t *testing.T) {
	p := DefaultBackoff()
	if d := p.Delay(1, nil); d != 0 {
		t.Fatalf("first attempt delay = %v, want 0", d)
	}
	for a := 2; a <= 50; a++ {
		if d := p.Delay(a, nil); d > p.Max {
			t.Fatalf("attempt %d delay %v exceeds max %v", a, d, p.Max)
		}
	}
	if d := p.Delay(50, nil); d != p.Max {
		t.Fatalf("saturated delay = %v, want %v", d, p.Max)
	}
}

func TestBackoffJitterStaysInBand(t *testing.T) {
	p := DefaultBackoff()
	base := p.Delay(3, nil)
	lo := p.Delay(3, func() float64 { return 0 })
	hi := p.Delay(3, func() float64 { return 0.999999 })
	if !(lo <= base && base <= hi) || lo < 0 || hi > p.Max {
		t.Fatalf("jitter out of band: lo=%v base=%v hi=%v", lo, base, hi)
	}
	if lo == hi {
		t.Fatal("jitter had no effect")
	}
}

func TestBackoffDeterminismWithoutJitter(t *testing.T) {
	p := DefaultBackoff()
	if p.Delay(4, nil) != p.Delay(4, nil) {
		t.Fatal("delay nondeterministic without jitter")
	}
	if p.Exhausted(p.MaxAttempts) {
		t.Fatal("the final allowed attempt must not count as exhausted")
	}
	if !p.Exhausted(p.MaxAttempts + 1) {
		t.Fatal("attempt beyond budget must be exhausted")
	}
}

func TestBackoffGrowth(t *testing.T) {
	p := DefaultBackoff()
	prev := time.Duration(0)
	for a := 1; a <= 5; a++ {
		d := p.Delay(a, nil)
		if d < prev {
			t.Fatalf("delay decreased at attempt %d: %v < %v", a, d, prev)
		}
		prev = d
	}
	if p.Delay(2, nil) != p.Base {
		t.Fatalf("second attempt delay = %v, want %v", p.Delay(2, nil), p.Base)
	}
}
