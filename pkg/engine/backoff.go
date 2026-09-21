package engine

import "time"

// BackoffPolicy is a bounded exponential backoff with proportional
// jitter. It never grows without bound: delays saturate at Max.
type BackoffPolicy struct {
	Base        time.Duration
	Max         time.Duration
	Factor      float64
	JitterFrac  float64
	MaxAttempts int
}

// DefaultBackoff returns the production policy.
func DefaultBackoff() BackoffPolicy {
	return BackoffPolicy{
		Base:        10 * time.Millisecond,
		Max:         time.Second,
		Factor:      2,
		JitterFrac:  0.2,
		MaxAttempts: 8,
	}
}

// NoBackoff is for tests that want immediate retries without jitter.
func NoBackoff() BackoffPolicy {
	return BackoffPolicy{Base: time.Millisecond, Max: time.Millisecond, Factor: 1, JitterFrac: 0, MaxAttempts: 8}
}

// Delay computes the delay before the (1-based) attempt. attempt 1 is
// the first try and therefore waits 0. jitter returns a value in [0,1);
// nil disables jitter, keeping tests reproducible. The result is always
// bounded by Max.
func (p BackoffPolicy) Delay(attempt int, jitter func() float64) time.Duration {
	if attempt <= 1 {
		return 0
	}
	base := float64(p.Base)
	for i := 2; i < attempt; i++ {
		base *= p.Factor
		if time.Duration(base) >= p.Max || base >= float64(p.Max) {
			base = float64(p.Max)
			break
		}
	}
	d := base
	if jitter != nil && p.JitterFrac > 0 {
		// multiply by [1-J, 1+J): never negative, stays proportional
		d *= 1 + p.JitterFrac*(2*jitter()-1)
	}
	if d < 0 {
		d = 0
	}
	if d > float64(p.Max) {
		d = float64(p.Max)
	}
	dur := time.Duration(d)
	// Retry delays must be positive when a base is configured, even if
	// jitter scales a tiny base down: a zero delay looks like "settled".
	if dur == 0 && p.Base > 0 && attempt > 1 {
		dur = time.Nanosecond
	}
	return dur
}

// Exhausted reports whether a failure sequence has consumed its retry
// budget. attempt is the count of recorded failures on the current
// desired generation: 1..MaxAttempts may still be retried (with
// backoff); once attempt > MaxAttempts the failure becomes terminal.
// Deterministic failures are surfaced separately and never reach here.
func (p BackoffPolicy) Exhausted(attempt int) bool {
	return p.MaxAttempts > 0 && attempt > p.MaxAttempts
}
