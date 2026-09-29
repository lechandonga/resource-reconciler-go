// Package apis defines the versioned wire types, admission/defaulting
// rules and status conventions of the declarative resource reconciler.
package apis

import (
	"encoding/json"
	"time"
)

// Supported API versions.
const (
	APIVersionV1 = "reconciler.example.com/v1"
	APIVersionV2 = "reconciler.example.com/v2"
	KindWidget   = "Widget"

	// FinalizerReconciler keeps an object around until its simulated
	// actual resource has been torn down.
	FinalizerReconciler = "reconciler.example.com/widget-actual"
)

// Modes supported by the canonical internal spec.
const (
	ModeOn   = "On"
	ModeOff  = "Off"
	ModeAuto = "Auto" // accepted only by v2
)

// ObjectMeta is a small subset of the usual object metadata.
type ObjectMeta struct {
	Name              string            `json:"name"`
	Labels            map[string]string `json:"labels,omitempty"`
	Generation        int64             `json:"generation"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp,omitempty"`
	Finalizers        []string          `json:"finalizers,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
}

// RawObject is exactly what is persisted: a versioned wire spec plus
// controller-owned status. Old objects are decoded through the versioned
// path and never "upgraded" destructively on the wire.
type RawObject struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Metadata   ObjectMeta      `json:"metadata"`
	Spec       json.RawMessage `json:"spec"`
	Status     *Status         `json:"status,omitempty"`
}

// Status is controller-owned and persisted so that a restarted
// controller resumes from exactly the last observed state.
type Status struct {
	ObservedGeneration int64  `json:"observedGeneration"`
	ObservedVersion    string `json:"observedVersion,omitempty"`
	SpecHash           string `json:"specHash,omitempty"`
	Attempts           int    `json:"attempts"`
	// FatalReason/FatalMessage describe a deterministic failure. They
	// persist across restarts so the failure stays observable and the
	// controller does not keep spinning against an impossible state.
	FatalReason  string           `json:"fatalReason,omitempty"`
	FatalMessage string           `json:"fatalMessage,omitempty"`
	Defaults     *AppliedDefaults `json:"defaults,omitempty"`
	Actual       *ActualState     `json:"actual,omitempty"`
	Conditions   []Condition      `json:"conditions"`
}

// AppliedDefaults is a deterministic record of which default values
// were filled so that repeated reads produce the same conclusion.
type AppliedDefaults struct {
	Fields []string          `json:"fields,omitempty"`
	Values map[string]string `json:"values,omitempty"`
}

// ActualState is the simulated "real world" state owned by the actuator.
type ActualState struct {
	ResourceID   string `json:"resourceID,omitempty"`
	Replicas     int    `json:"replicas"`
	Mode         string `json:"mode"`
	GraceSeconds int    `json:"graceSeconds"`
	Ready        bool   `json:"ready"`
}

// Condition is a status condition carrying a machine-readable reason code.
type Condition struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"` // "True" / "False"
	Reason             string    `json:"reason,omitempty"`
	Message            string    `json:"message,omitempty"`
	ObservedGeneration int64     `json:"observedGeneration"`
	LastTransition     time.Time `json:"lastTransitionTime"`
}

// Condition types.
const (
	CondAdmission   = "Admission"
	CondDependents  = "DependentsBlocked"
	CondProgressing = "Progressing"
	CondReady       = "Ready"
)

// Reason codes. Every non-obvious state the controller can be in has a
// distinct, stable reason code.
const (
	ReasonAccepted             = "Accepted"
	ReasonUnsupportedVersion   = "UnsupportedVersion"
	ReasonUnsupportedKind      = "UnsupportedKind"
	ReasonUnknownField         = "UnknownField"
	ReasonMalformedSpec        = "MalformedSpec"
	ReasonMissingName          = "MissingName"
	ReasonNameInvalid          = "NameInvalid"
	ReasonReplicasOutOfRange   = "ReplicasOutOfRange"
	ReasonUnsupportedMode      = "UnsupportedMode"
	ReasonGraceOutOfRange      = "GraceOutOfRange"
	ReasonSelfDependency       = "SelfDependency"
	ReasonDependencyNotFound   = "DependencyNotFound"
	ReasonCircularDependency   = "CircularDependency"
	ReasonDependencyPending    = "DependencyPending"
	ReasonDependenciesReady    = "DependenciesReady"
	ReasonReady                = "Ready"
	ReasonDeleting             = "Deleting"
	ReasonCreated              = "Created"
	ReasonUpdated              = "Updated"
	ReasonUnchanged            = "Unchanged"
	ReasonDeleted              = "Deleted"
	ReasonTransientError       = "TransientError"
	ReasonDeterministicError   = "DeterministicError"
	ReasonRetryBudgetExhausted = "RetryBudgetExhausted"
	ReasonQueueOverflow        = "QueueOverflow"
	ReasonConflict             = "Conflict"
	// Startup / recovery reconciliation (actual-side ownership audit).
	ReasonOrphanReclaimed      = "OrphanReclaimed"
	ReasonActualKept           = "ActualKept"
	ReasonRecoveryCancelled    = "RecoveryCancelled"
	ReasonRecoveryTimedOut     = "RecoveryTimedOut"
	ReasonActualDriftCorrected = "ActualDriftCorrected"
)

// AdmissionError is returned for specs that must be rejected. The Reason
// code distinguishes rejection causes; callers never have to parse text.
type AdmissionError struct {
	Reason  string
	Message string
}

func (e *AdmissionError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Reason + ": " + e.Message
}

// NewAdmissionError constructs an admission rejection with a reason code.
func NewAdmissionError(reason, format string, args ...any) *AdmissionError {
	return &AdmissionError{Reason: reason, Message: fmtSprintf(format, args...)}
}
