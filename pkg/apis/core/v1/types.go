// Package v1 defines the current stable version of the Workload resource.
package v1

import "time"

const (
	// GroupName is the API group for all Workload versions.
	GroupName = "core.example.com"
	// Version is the stable schema version.
	Version = "v1"
	// Kind identifies the Workload kind.
	Kind = "Workload"

	// TypeMeta APIVersion/Kind values.
	TypeMetaAPIVersion = GroupName + "/" + Version
)

// Phase is the top-level, user-observable lifecycle phase of a Workload.
type Phase string

const (
	PhasePending     Phase = "Pending"
	PhaseProgressing Phase = "Progressing"
	PhaseAvailable   Phase = "Available"
	PhaseDegraded    Phase = "Degraded"
	PhaseFailed      Phase = "Failed"
	PhaseDeleting    Phase = "Deleting"
	PhaseTerminated  Phase = "Terminated"
)

// ConditionType enumerates the state conditions a Workload exposes.
type ConditionType string

const (
	ConditionAdmitted   ConditionType = "Admitted"
	ConditionReconciled ConditionType = "Reconciled"
	ConditionDepsReady  ConditionType = "DependenciesReady"
	ConditionFinalizer  ConditionType = "Finalizer"
)

// ConditionStatus is True/False/Unknown.
type ConditionStatus string

const (
	CondTrue    ConditionStatus = "True"
	CondFalse   ConditionStatus = "False"
	CondUnknown ConditionStatus = "Unknown"
)

// Well-known annotations persisted with every object.
const (
	// AnnStoredAPIVersion records the API version through which the object was admitted.
	AnnStoredAPIVersion = "core.example.com/stored-api-version"
	// AnnDefaultsApplied records the defaulting schema version so that re-reads
	// produce identical results.
	AnnDefaultsApplied = "core.example.com/defaults-version"
	// AnnFinalizer prevents physical deletion until data-plane side effects are gone.
	AnnFinalizer = "core.example.com/workload-finalizer"

	DefaultsSchemaVersion = "v1.20260101"
)

// ObjectMeta carries identity and optimistic-concurrency metadata.
type ObjectMeta struct {
	Name              string            `json:"name"`
	ResourceVersion   int64             `json:"resourceVersion"`
	Generation        int64             `json:"generation"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	CreationTimestamp time.Time         `json:"creationTimestamp,omitempty"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp,omitempty"`
}

// Spec is the v1 desired state.
type Spec struct {
	Replicas  int               `json:"replicas"`
	Image     string            `json:"image"`
	DependsOn []string          `json:"dependsOn,omitempty"`
	Config    map[string]string `json:"config,omitempty"`
}

// ReconcileStatus is the observed status of one data-plane instance.
type ReconcileStatus string

const (
	InstancePresent ReconcileStatus = "Present"
	InstanceAbsent  ReconcileStatus = "Absent"
)

// InstanceStatus describes a single materialized instance.
type InstanceStatus struct {
	ID         string          `json:"id"`
	Status     ReconcileStatus `json:"status"`
	Generation int64           `json:"generation"`
}

// Condition is a point-in-time, reason-coded observation about the object.
type Condition struct {
	Type               ConditionType   `json:"type"`
	Status             ConditionStatus `json:"status"`
	Reason             string          `json:"reason"`
	Message            string          `json:"message,omitempty"`
	ObservedGeneration int64           `json:"observedGeneration"`
	LastTransition     time.Time       `json:"lastTransition"`
}

// Status is the observed state.
type Status struct {
	Phase              Phase `json:"phase"`
	ObservedGeneration int64 `json:"observedGeneration"`
	// RetryGeneration is the generation whose attempts Attempts counts. A
	// spec change (new generation) resets the transient-retry budget.
	RetryGeneration int64            `json:"retryGeneration,omitempty"`
	Instances       []InstanceStatus `json:"instances,omitempty"`
	Conditions      []Condition      `json:"conditions,omitempty"`
	// Attempts counts consecutive transient-failure retries for
	// RetryGeneration (for bounded backoff).
	Attempts int `json:"attempts,omitempty"`
	// LastError records the most recent failure, classified by reason code.
	LastError *ErrorRecord `json:"lastError,omitempty"`
}

// ErrorRecord is a classified, observable failure.
type ErrorRecord struct {
	Reason    string    `json:"reason"`
	Message   string    `json:"message"`
	Transient bool      `json:"transient"`
	Time      time.Time `json:"time"`
}

// Workload is the persisted, internal v1 object.
type Workload struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	ObjectMeta ObjectMeta `json:"metadata"`
	Spec       Spec       `json:"spec"`
	Status     Status     `json:"status"`
}

// WorkloadManifest is what a client submits to admission (versioned raw JSON).
type WorkloadManifest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name        string            `json:"name"`
		Annotations map[string]string `json:"annotations,omitempty"`
	} `json:"metadata"`
	Spec map[string]any `json:"spec"`
}

// Reason codes used by conditions and errors. Stable strings form the
// observable contract of the controller.
const (
	ReasonAdmitted            = "Admitted"
	ReasonInvalidSpec         = "InvalidSpec"
	ReasonReplicasOutOfRange  = "ReplicasOutOfRange"
	ReasonInvalidImage        = "InvalidImage"
	ReasonDuplicateDependency = "DuplicateDependency"
	ReasonEmptyDependency     = "EmptyDependency"
	ReasonInvalidConfigKey    = "InvalidConfigKey"
	ReasonSelfDependency      = "SelfDependency"
	ReasonDependencyCycle     = "DependencyCycle"
	ReasonUnsupportedVersion  = "UnsupportedVersion"
	ReasonUnsupportedField    = "UnsupportedField"
	ReasonUnknownField        = "UnknownField"
	ReasonInvalidKind         = "InvalidKind"
	ReasonMissingName         = "MissingName"
	ReasonInvalidName         = "InvalidName"
	ReasonValidationPending   = "ValidationPending"

	ReasonDependenciesBlocked = "DependenciesBlocked"
	ReasonDependencyNotReady  = "DependencyNotReady"
	ReasonDependencyMissing   = "DependencyMissing"
	ReasonCyclicDependency    = "CyclicDependency"

	ReasonTransientFailure        = "TransientFailure"
	ReasonDeterministicFailure    = "DeterministicFailure"
	ReasonBackoffLimitExceeded    = "BackoffLimitExceeded"
	ReasonReconcileBudgetExceeded = "ReconcileBudgetExceeded"
	ReasonQueueShed               = "QueueShed"

	ReasonConflict         = "Conflict"
	ReasonNoop             = "Noop"
	ReasonApplied          = "Applied"
	ReasonDeleting         = "Deleting"
	ReasonDeleted          = "Deleted"
	ReasonFinalizerBlocked = "FinalizerBlocked"
)
