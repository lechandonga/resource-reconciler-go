// Package validation validates v1 specs and rejects illegal input with
// distinguishable reason codes.
package validation

import (
	"regexp"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/defaults"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

// Failure is a validation rejection carrying a stable reason code.
type Failure struct {
	Reason  string
	Message string
}

func (f *Failure) Error() string { return f.Reason + ": " + f.Message }

var (
	// namePattern: RFC1123-label-like: lower-case alphanumerics or '-',
	// must start and end with an alphanumeric, 1..63 chars.
	namePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	// imagePattern: [registry/]name[:tag][@digest] — keep it deliberately
	// strict: no spaces, a slash-free final segment containing at least one
	// non-empty name; tag, if present, matches [A-Za-z0-9_.-]{1,128}.
	tagPattern       = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	configKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
)

// ValidateName validates the object name in isolation.
func ValidateName(name string) *Failure {
	if name == "" {
		return &Failure{Reason: v1.ReasonMissingName, Message: "metadata.name is required"}
	}
	if !namePattern.MatchString(name) {
		return &Failure{Reason: v1.ReasonInvalidName, Message: "metadata.name must be a lowercase DNS label (1-63 chars, [a-z0-9-])"}
	}
	return nil
}

// ValidateSpec validates an already-defaulted v1 spec in isolation.
// It never mutates the input. Failures are ordered deterministically so the
// same input always yields the same reason.
func ValidateSpec(s v1.Spec) *Failure {
	if s.Replicas < 1 || s.Replicas > defaults.MaxReplicas {
		return &Failure{
			Reason:  v1.ReasonReplicasOutOfRange,
			Message: "spec.replicas must be between 1 and 10",
		}
	}
	if fail := validateImage(s.Image); fail != nil {
		return fail
	}
	seen := map[string]struct{}{}
	for _, dep := range s.DependsOn {
		if dep == "" {
			return &Failure{Reason: v1.ReasonEmptyDependency, Message: "spec.dependsOn contains an empty entry"}
		}
		if !namePattern.MatchString(dep) {
			return &Failure{Reason: v1.ReasonInvalidName, Message: "spec.dependsOn entries must be lowercase DNS labels"}
		}
		if _, dup := seen[dep]; dup {
			return &Failure{Reason: v1.ReasonDuplicateDependency, Message: "spec.dependsOn contains duplicate entry: " + dep}
		}
		seen[dep] = struct{}{}
	}
	// Config keys are iterated in sorted order for deterministic rejection.
	for _, k := range sortedKeys(s.Config) {
		if !configKeyPattern.MatchString(k) {
			return &Failure{Reason: v1.ReasonInvalidConfigKey, Message: "spec.config key is invalid: " + k}
		}
	}
	return nil
}

func validateImage(image string) *Failure {
	if image == "" {
		return &Failure{Reason: v1.ReasonInvalidImage, Message: "spec.image is required"}
	}
	if len(image) > 512 {
		return &Failure{Reason: v1.ReasonInvalidImage, Message: "spec.image too long"}
	}
	for _, ch := range image {
		if ch == ' ' || ch == '\t' || ch == '\n' {
			return &Failure{Reason: v1.ReasonInvalidImage, Message: "spec.image must not contain whitespace"}
		}
	}
	// Split tag from final segment.
	lastSlash := -1
	for i, ch := range image {
		if ch == '/' {
			lastSlash = i
		}
	}
	final := image[lastSlash+1:]
	if final == "" {
		return &Failure{Reason: v1.ReasonInvalidImage, Message: "spec.image missing name"}
	}
	name := final
	tag := ""
	for i := len(final) - 1; i >= 0; i-- {
		if final[i] == ':' {
			name = final[:i]
			tag = final[i+1:]
			break
		}
	}
	if name == "" {
		return &Failure{Reason: v1.ReasonInvalidImage, Message: "spec.image name is empty"}
	}
	if tag != "" && !tagPattern.MatchString(tag) {
		return &Failure{Reason: v1.ReasonInvalidImage, Message: "spec.image tag is invalid: " + tag}
	}
	return nil
}

// sortedKeys returns map keys in ascending order (deterministic iteration).
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// simple insertion sort, maps are small
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return keys
}
