package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
)

// EnvCrashPoint selects, for a controller process started as a child in
// crash-window tests/demos, a single window in which the process must
// die immediately after the durable side effect. EnvCrashKey optionally
// narrows it to one object key.
const (
	EnvCrashPoint = "RECONCILER_CRASH_POINT"
	EnvCrashKey   = "RECONCILER_CRASH_KEY"
)

func sha256Sum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:12])
}

// EnvCrashHook builds a CrashHook from the environment: when point (and
// optionally key) matches, the process exits with a distinct status.
// Returns nil when no crash point is configured.
func EnvCrashHook() CrashHook {
	point := os.Getenv(EnvCrashPoint)
	if point == "" {
		return nil
	}
	key := os.Getenv(EnvCrashKey)
	return func(k, p string) {
		if p != point {
			return
		}
		if key != "" && k != key {
			return
		}
		os.Exit(ExitCrashSimulated)
	}
}

// ExitCrashSimulated is the exit status used by the injected crash hook.
const ExitCrashSimulated = 77
