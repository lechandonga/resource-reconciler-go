package apis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

// Field bounds shared by all versions.
const (
	MinReplicas     = 0
	MaxReplicas     = 10
	DefaultReplicas = 1

	DefaultMode = ModeOn

	DefaultGraceSeconds = 30
	MinGraceSeconds     = 0
	MaxGraceSeconds     = 3600

	MaxNameLength = 63
)

var namePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// Versioned wire shapes. Pointers distinguish "absent" (defaulting
// applies) from an explicit zero value (which validates and wins).
type widgetSpecV1 struct {
	Replicas  *int     `json:"replicas"`
	Mode      *string  `json:"mode"`
	DependsOn []string `json:"dependsOn"`
}

type widgetSpecV2 struct {
	Replicas     *int     `json:"replicas"`
	Mode         *string  `json:"mode"`
	DependsOn    []string `json:"dependsOn"`
	GraceSeconds *int     `json:"graceSeconds"`
}

// DecodeSpec strictly decodes the versioned spec of obj, fills defaults
// deterministically and returns the canonical internal spec together with
// a stable record of which defaults were applied. Unknown versions,
// unknown kinds and unknown fields are rejected explicitly with
// AdmissionError instead of being silently ignored.
func DecodeSpec(obj *RawObject) (*WidgetSpec, *AppliedDefaults, error) {
	if obj == nil {
		return nil, nil, NewAdmissionError(ReasonMalformedSpec, "object is nil")
	}
	switch obj.APIVersion {
	case "":
		return nil, nil, NewAdmissionError(ReasonUnsupportedVersion, "apiVersion is required")
	case APIVersionV1, APIVersionV2:
	default:
		return nil, nil, NewAdmissionError(ReasonUnsupportedVersion, "apiVersion %q is not supported", obj.APIVersion)
	}
	if obj.Kind == "" {
		return nil, nil, NewAdmissionError(ReasonUnsupportedKind, "kind is required")
	}
	if obj.Kind != KindWidget {
		return nil, nil, NewAdmissionError(ReasonUnsupportedKind, "kind %q is not supported", obj.Kind)
	}
	if err := validateName(obj.Metadata.Name); err != nil {
		return nil, nil, err
	}

	// Presence map: only keys present in the JSON are decoded, so
	// defaulting can record exactly which fields it filled.
	present := map[string]json.RawMessage{}
	if len(bytes.TrimSpace(obj.Spec)) > 0 {
		if err := json.Unmarshal(obj.Spec, &present); err != nil {
			return nil, nil, NewAdmissionError(ReasonMalformedSpec, "spec is not a JSON object: %v", err)
		}
	}

	applied := &AppliedDefaults{Values: map[string]string{}}
	out := &WidgetSpec{}

	switch obj.APIVersion {
	case APIVersionV1:
		var w widgetSpecV1
		if err := strictDecode(obj.Spec, &w); err != nil {
			return nil, nil, wireError(err)
		}
		if w.Replicas != nil {
			out.Replicas = *w.Replicas
		} else {
			out.Replicas = DefaultReplicas
			recordDefault(applied, "replicas", DefaultReplicas)
		}
		if w.Mode != nil {
			if *w.Mode != ModeOn && *w.Mode != ModeOff {
				return nil, nil, NewAdmissionError(ReasonUnsupportedMode, "mode %q is not supported in %s (valid: %s, %s)", *w.Mode, APIVersionV1, ModeOn, ModeOff)
			}
			out.Mode = *w.Mode
		} else {
			out.Mode = DefaultMode
			recordDefault(applied, "mode", DefaultMode)
		}
		// v1 has no concept of graceSeconds: every historical object
		// deterministically receives the canonical default on read.
		out.GraceSeconds = DefaultGraceSeconds
		recordDefault(applied, "graceSeconds", DefaultGraceSeconds)
		if _, ok := present["dependsOn"]; ok {
			out.DependsOn = append([]string(nil), w.DependsOn...)
		} else {
			recordDefault(applied, "dependsOn", "[]")
		}
	case APIVersionV2:
		var w widgetSpecV2
		if err := strictDecode(obj.Spec, &w); err != nil {
			return nil, nil, wireError(err)
		}
		if w.Replicas != nil {
			out.Replicas = *w.Replicas
		} else {
			out.Replicas = DefaultReplicas
			recordDefault(applied, "replicas", DefaultReplicas)
		}
		if w.Mode != nil {
			if *w.Mode != ModeOn && *w.Mode != ModeOff && *w.Mode != ModeAuto {
				return nil, nil, NewAdmissionError(ReasonUnsupportedMode, "mode %q is not supported (valid: %s, %s, %s)", *w.Mode, ModeOn, ModeOff, ModeAuto)
			}
			out.Mode = *w.Mode
		} else {
			out.Mode = DefaultMode
			recordDefault(applied, "mode", DefaultMode)
		}
		if w.GraceSeconds != nil {
			out.GraceSeconds = *w.GraceSeconds
		} else {
			out.GraceSeconds = DefaultGraceSeconds
			recordDefault(applied, "graceSeconds", DefaultGraceSeconds)
		}
		if _, ok := present["dependsOn"]; ok {
			out.DependsOn = append([]string(nil), w.DependsOn...)
		} else {
			recordDefault(applied, "dependsOn", "[]")
		}
	}

	if out.Replicas < MinReplicas || out.Replicas > MaxReplicas {
		return nil, nil, NewAdmissionError(ReasonReplicasOutOfRange, "replicas must be in [%d,%d], got %d", MinReplicas, MaxReplicas, out.Replicas)
	}
	if out.GraceSeconds < MinGraceSeconds || out.GraceSeconds > MaxGraceSeconds {
		return nil, nil, NewAdmissionError(ReasonGraceOutOfRange, "graceSeconds must be in [%d,%d], got %d", MinGraceSeconds, MaxGraceSeconds, out.GraceSeconds)
	}
	if len(applied.Fields) == 0 {
		applied = nil
	} else {
		sort.Strings(applied.Fields)
	}
	return out, applied, nil
}

func strictDecode(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	// reject trailing tokens (e.g. two concatenated objects)
	if dec.More() {
		return fmt.Errorf("json: trailing data after spec")
	}
	return nil
}

// wireError maps encoding/json errors to distinguishable reason codes.
func wireError(err error) error {
	var typeErr *json.UnmarshalTypeError
	if asType(err, &typeErr) {
		return NewAdmissionError(ReasonMalformedSpec, "field %q has an invalid type: %v", typeErr.Field, err)
	}
	var syntaxErr *json.SyntaxError
	if asSyntax(err, &syntaxErr) {
		return NewAdmissionError(ReasonMalformedSpec, "spec is not valid JSON: %v", err)
	}
	if isUnknownField(err) {
		return NewAdmissionError(ReasonUnknownField, "spec contains an unknown field: %v", stripJSONPrefix(err))
	}
	return NewAdmissionError(ReasonMalformedSpec, "%v", err)
}

// Validate checks invariants spanning multiple objects. graph maps the
// names of every object currently admitted (including the candidate,
// under its own name) to canonical specs. It rejects self dependencies,
// references to missing objects and dependency cycles.
func Validate(name string, spec *WidgetSpec, graph map[string]*WidgetSpec) error {
	for _, dep := range spec.DependsOn {
		if dep == name {
			return NewAdmissionError(ReasonSelfDependency, "object %q may not depend on itself", name)
		}
		if _, ok := graph[dep]; !ok {
			return NewAdmissionError(ReasonDependencyNotFound, "%q depends on unknown object %q", name, dep)
		}
	}
	if cycle := findCycle(name, graph); cycle != nil {
		return NewAdmissionError(ReasonCircularDependency, "dependency cycle: %s", joinCycle(cycle))
	}
	return nil
}

// findCycle runs an iterative-ish DFS limited to nodes reachable from
// start and returns the first cycle as a path, or nil.
func findCycle(start string, graph map[string]*WidgetSpec) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	parent := map[string]string{}

	var dfs func(n string) []string
	dfs = func(n string) []string {
		color[n] = gray
		s := graph[n]
		deps := append([]string(nil), s.DependsOn...)
		sort.Strings(deps) // deterministic cycle reporting
		for _, d := range deps {
			if _, ok := graph[d]; !ok {
				continue
			}
			if color[d] == white {
				parent[d] = n
				if cyc := dfs(d); cyc != nil {
					return cyc
				}
			} else if color[d] == gray {
				// reconstruct cycle d -> ... -> n -> d
				path := []string{d}
				for cur := n; cur != d; cur = parent[cur] {
					path = append(path, cur)
					if _, ok := parent[cur]; !ok {
						break
					}
				}
				for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
					path[i], path[j] = path[j], path[i]
				}
				path = append(path, d)
				return path
			}
		}
		color[n] = black
		return nil
	}
	return dfs(start)
}

func validateName(name string) error {
	if name == "" {
		return NewAdmissionError(ReasonMissingName, "metadata.name is required")
	}
	if len(name) > MaxNameLength {
		return NewAdmissionError(ReasonNameInvalid, "metadata.name must be at most %d characters, got %d", MaxNameLength, len(name))
	}
	if !namePattern.MatchString(name) {
		return NewAdmissionError(ReasonNameInvalid, "metadata.name %q must consist of lowercase alphanumeric characters or '-' and start/end alphanumerically", name)
	}
	return nil
}

func recordDefault(a *AppliedDefaults, field string, value any) {
	a.Fields = append(a.Fields, field)
	switch v := value.(type) {
	case int:
		a.Values[field] = strconv.Itoa(v)
	case string:
		a.Values[field] = v
	}
}

func joinCycle(cycle []string) string {
	out := ""
	for i, n := range cycle {
		if i > 0 {
			out += " -> "
		}
		out += n
	}
	return out
}
