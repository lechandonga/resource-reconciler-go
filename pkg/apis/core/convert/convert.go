// Package convert implements version compatibility: historical manifests are
// read and upgraded to the internal v1 representation; unknown versions and
// unknown/unsupported fields are rejected explicitly rather than silently
// ignored.
package convert

import (
	"encoding/json"
	"fmt"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/defaults"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1beta1"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/validation"
)

// Upgraded is the result of decoding a versioned manifest.
type Upgraded struct {
	Workload *v1.Workload
}

// allowedSpecFields lists, per supported version, exactly the keys accepted
// inside "spec". Any other key is an explicit unknown-field rejection.
var allowedSpecFields = map[string]map[string]struct{}{
	v1.Version: {
		"replicas":  {},
		"image":     {},
		"dependsOn": {},
		"config":    {},
	},
	v1beta1.Version: {
		"replicaCount":   {},
		"containerImage": {},
		"depends":        {},
		"config":         {},
	},
}

var allowedMetadataFields = map[string]struct{}{
	"name":        {},
	"annotations": {},
}

var allowedTopLevelFields = map[string]struct{}{
	"apiVersion": {},
	"kind":       {},
	"metadata":   {},
	"spec":       {},
}

// DecodeManifest parses raw JSON submitted by a client and upgrades it to the
// internal v1 object. Unknown fields, unknown versions, unsupported kinds and
// malformed types are rejected with a *validation.Failure carrying a specific
// reason code.
func DecodeManifest(raw []byte) (*Upgraded, error) {
	var top map[string]any
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "manifest is not valid JSON: " + err.Error()}
	}
	for k := range top {
		if _, ok := allowedTopLevelFields[k]; !ok {
			return nil, &validation.Failure{Reason: v1.ReasonUnknownField, Message: "unknown top-level field: " + k}
		}
	}

	apiVersion, _ := top["apiVersion"].(string)
	if apiVersion == "" {
		return nil, &validation.Failure{Reason: v1.ReasonUnsupportedVersion, Message: "apiVersion is required"}
	}
	version, ok := versionFromAPIVersion(apiVersion)
	if !ok {
		return nil, &validation.Failure{Reason: v1.ReasonUnsupportedVersion, Message: "unsupported apiVersion: " + apiVersion}
	}
	kind, _ := top["kind"].(string)
	if kind == "" {
		return nil, &validation.Failure{Reason: v1.ReasonInvalidKind, Message: "kind is required"}
	}
	if kind != v1.Kind {
		return nil, &validation.Failure{Reason: v1.ReasonInvalidKind, Message: "unsupported kind: " + kind}
	}

	name, annotations, err := decodeMetadata(top["metadata"])
	if err != nil {
		return nil, err
	}

	spec, err := decodeSpec(version, top["spec"])
	if err != nil {
		return nil, err
	}

	w := &v1.Workload{
		APIVersion: v1.TypeMetaAPIVersion,
		Kind:       v1.Kind,
		ObjectMeta: v1.ObjectMeta{
			Name:        name,
			Annotations: annotations,
		},
		Spec: spec,
	}
	if w.ObjectMeta.Annotations == nil {
		w.ObjectMeta.Annotations = map[string]string{}
	}
	// Remember through which version the object was admitted.
	w.ObjectMeta.Annotations[v1.AnnStoredAPIVersion] = version

	// Defaulting happens before validation and is deterministic/idempotent.
	defaults.Apply(w)

	if fail := validation.ValidateName(w.ObjectMeta.Name); fail != nil {
		return nil, fail
	}
	if fail := validation.ValidateSpec(w.Spec); fail != nil {
		return nil, fail
	}
	return &Upgraded{Workload: w}, nil
}

// DecodeStored upgrades an object read back from persistence. Persisted
// objects may carry a historical apiVersion; their spec is decoded by that
// version and defaulted. Unknown versions/fields are rejected explicitly so a
// restart never silently guesses semantics.
func DecodeStored(apiVersion string, raw json.RawMessage) (*v1.Workload, error) {
	var envelope struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name              string            `json:"name"`
			ResourceVersion   int64             `json:"resourceVersion"`
			Generation        int64             `json:"generation"`
			Annotations       map[string]string `json:"annotations"`
			CreationTimestamp string            `json:"creationTimestamp,omitempty"`
			DeletionTimestamp *string           `json:"deletionTimestamp,omitempty"`
		} `json:"metadata"`
		Spec json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("convert: malformed stored object: %w", err)
	}
	if apiVersion == "" {
		apiVersion = envelope.APIVersion
	}
	version, ok := versionFromAPIVersion(apiVersion)
	if !ok {
		return nil, &validation.Failure{Reason: v1.ReasonUnsupportedVersion, Message: "unsupported stored apiVersion: " + apiVersion}
	}
	if envelope.Kind != "" && envelope.Kind != v1.Kind {
		return nil, &validation.Failure{Reason: v1.ReasonInvalidKind, Message: "unsupported stored kind: " + envelope.Kind}
	}

	var generic map[string]any
	if len(envelope.Spec) > 0 {
		if err := json.Unmarshal(envelope.Spec, &generic); err != nil {
			return nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "stored spec is not valid JSON: " + err.Error()}
		}
	}
	spec, err := decodeSpec(version, any(generic))
	if err != nil {
		return nil, err
	}

	meta := v1.ObjectMeta{
		Name:            envelope.Metadata.Name,
		ResourceVersion: envelope.Metadata.ResourceVersion,
		Generation:      envelope.Metadata.Generation,
		Annotations:     envelope.Metadata.Annotations,
	}

	w := &v1.Workload{
		APIVersion: v1.TypeMetaAPIVersion,
		Kind:       v1.Kind,
		ObjectMeta: meta,
		Spec:       spec,
	}
	// Historical objects get their missing default semantics backfilled on
	// every read, deterministically.
	defaults.Apply(w)
	if w.ObjectMeta.Annotations[v1.AnnStoredAPIVersion] == "" {
		w.ObjectMeta.Annotations[v1.AnnStoredAPIVersion] = version
	}
	return w, nil
}

// versionFromAPIVersion maps the wire apiVersion to a supported schema
// version.
func versionFromAPIVersion(apiVersion string) (string, bool) {
	switch apiVersion {
	case v1.TypeMetaAPIVersion:
		return v1.Version, true
	case v1beta1.TypeMetaAPIVersion:
		return v1beta1.Version, true
	default:
		return "", false
	}
}

func decodeMetadata(in any) (string, map[string]string, error) {
	if in == nil {
		return "", nil, &validation.Failure{Reason: v1.ReasonMissingName, Message: "metadata is required"}
	}
	m, ok := in.(map[string]any)
	if !ok {
		return "", nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "metadata must be an object"}
	}
	for k := range m {
		if _, allowed := allowedMetadataFields[k]; !allowed {
			return "", nil, &validation.Failure{Reason: v1.ReasonUnknownField, Message: "unknown metadata field: " + k}
		}
	}
	name, _ := m["name"].(string)
	var annotations map[string]string
	if a, present := m["annotations"]; present && a != nil {
		am, ok := a.(map[string]any)
		if !ok {
			return "", nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "metadata.annotations must be an object"}
		}
		annotations = map[string]string{}
		// Deterministic iteration order.
		keys := make([]string, 0, len(am))
		for k := range am {
			keys = append(keys, k)
		}
		sortStrings(keys)
		for _, k := range keys {
			v, ok := am[k].(string)
			if !ok {
				return "", nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "metadata.annotations values must be strings: " + k}
			}
			annotations[k] = v
		}
	}
	return name, annotations, nil
}

func decodeSpec(version string, in any) (v1.Spec, error) {
	out := v1.Spec{DependsOn: []string{}, Config: map[string]string{}}
	if in == nil {
		// Missing spec: defaults handle every field.
		return out, nil
	}
	m, ok := in.(map[string]any)
	if !ok {
		return out, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "spec must be an object"}
	}
	allowed := allowedSpecFields[version]
	for k := range m {
		if _, known := allowed[k]; !known {
			return out, &validation.Failure{Reason: v1.ReasonUnknownField, Message: "unknown spec field for " + version + ": " + k}
		}
	}
	switch version {
	case v1.Version:
		return decodeV1Spec(m)
	case v1beta1.Version:
		return decodeV1Beta1Spec(m)
	default:
		return out, &validation.Failure{Reason: v1.ReasonUnsupportedVersion, Message: "unsupported version: " + version}
	}
}

func decodeV1Spec(m map[string]any) (v1.Spec, error) {
	s := v1.Spec{DependsOn: []string{}, Config: map[string]string{}}
	if v, present := m["replicas"]; present {
		n, err := asInt(v)
		if err != nil {
			return s, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "spec.replicas must be an integer"}
		}
		s.Replicas = n
	}
	if v, present := m["image"]; present {
		str, ok := v.(string)
		if !ok {
			return s, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "spec.image must be a string"}
		}
		s.Image = str
	}
	if v, present := m["dependsOn"]; present {
		deps, err := asStringList(v, "spec.dependsOn")
		if err != nil {
			return s, err
		}
		s.DependsOn = deps
	}
	if v, present := m["config"]; present {
		cfg, err := asStringMap(v, "spec.config")
		if err != nil {
			return s, err
		}
		s.Config = cfg
	}
	return s, nil
}

func decodeV1Beta1Spec(m map[string]any) (v1.Spec, error) {
	s := v1.Spec{DependsOn: []string{}, Config: map[string]string{}}
	if v, present := m["replicaCount"]; present {
		n, err := asInt(v)
		if err != nil {
			return s, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "spec.replicaCount must be an integer"}
		}
		s.Replicas = n
	}
	if v, present := m["containerImage"]; present {
		str, ok := v.(string)
		if !ok {
			return s, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: "spec.containerImage must be a string"}
		}
		s.Image = str
	}
	if v, present := m["depends"]; present {
		deps, err := asStringList(v, "spec.depends")
		if err != nil {
			return s, err
		}
		s.DependsOn = deps
	}
	if v, present := m["config"]; present {
		cfg, err := asStringMap(v, "spec.config")
		if err != nil {
			return s, err
		}
		s.Config = cfg
	}
	return s, nil
}

func asInt(v any) (int, error) {
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("not a number")
	}
	if f != float64(int64(f)) {
		return 0, fmt.Errorf("not an integer")
	}
	return int(f), nil
}

func asStringList(v any, field string) ([]string, error) {
	if v == nil {
		return []string{}, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: field + " must be an array of strings"}
	}
	out := make([]string, 0, len(arr))
	for i, item := range arr {
		str, ok := item.(string)
		if !ok {
			return nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: fmt.Sprintf("%s[%d] must be a string", field, i)}
		}
		out = append(out, str)
	}
	return out, nil
}

func asStringMap(v any, field string) (map[string]string, error) {
	if v == nil {
		return map[string]string{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: field + " must be an object"}
	}
	out := make(map[string]string, len(m))
	for k, item := range m {
		str, ok := item.(string)
		if !ok {
			return nil, &validation.Failure{Reason: v1.ReasonInvalidSpec, Message: field + "." + k + " must be a string"}
		}
		out[k] = str
	}
	return out, nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
