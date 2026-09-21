package engine

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// Decision is one explainable reconciliation decision. Every reconcile
// emits at least one decision: the input (observed generation/hash),
// the judgement (action + reason) and the resulting state.
type Decision struct {
	At       time.Time `json:"at"`
	Key      string    `json:"key"`
	Input    string    `json:"input"`
	Action   string    `json:"action"`
	Reason   string    `json:"reason"`
	Message  string    `json:"message,omitempty"`
	ResultRV int64     `json:"resultRV,omitempty"`
}

// DecisionSink receives decisions. Implementations must be concurrency safe.
type DecisionSink interface {
	Log(d Decision)
}

// WriterSink writes one JSON object per decision, one per line. Writes
// are serialized, so concurrent workers never interleave log lines.
type WriterSink struct {
	mu sync.Mutex
	W  io.Writer
}

func (s *WriterSink) Log(d Decision) {
	data, err := json.Marshal(d)
	if err != nil {
		// Decisions are diagnostic only; never panic the controller.
		data = []byte(`{"reason":"LogEncodeError"}`)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.W.Write(data)
	_, _ = s.W.Write([]byte{'\n'})
}

// NopSink drops decisions.
type NopSink struct{}

func (NopSink) Log(Decision) {}

// SliceSink records decisions in memory for test assertions.
type SliceSink struct {
	mu        sync.Mutex
	Decisions []Decision
}

func NewSliceSink() *SliceSink { return &SliceSink{} }

func (s *SliceSink) Log(d Decision) {
	s.mu.Lock()
	s.Decisions = append(s.Decisions, d)
	s.mu.Unlock()
}

// Snapshot returns a detached copy of decisions so far.
func (s *SliceSink) Snapshot() []Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Decision(nil), s.Decisions...)
}

// Reasons returns just the reason codes in emission order.
func (s *SliceSink) Reasons() []string {
	d := s.Snapshot()
	out := make([]string, len(d))
	for i, x := range d {
		out[i] = x.Reason
	}
	return out
}
