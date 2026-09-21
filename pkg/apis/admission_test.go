package apis

import (
	"encoding/json"
	"errors"
	"testing"
)

func raw(version, specJSON string) *RawObject {
	return &RawObject{
		APIVersion: version,
		Kind:       KindWidget,
		Metadata:   ObjectMeta{Name: "w1"},
		Spec:       json.RawMessage(specJSON),
	}
}

func admitErr(t *testing.T, obj *RawObject) *AdmissionError {
	t.Helper()
	_, _, err := DecodeSpec(obj)
	var ae *AdmissionError
	if !errors.As(err, &ae) {
		t.Fatalf("expected AdmissionError, got %v", err)
	}
	return ae
}

func TestDecodeRejectsBadVersionsAndFields(t *testing.T) {
	cases := []struct {
		name   string
		obj    *RawObject
		reason string
	}{
		{"unsupported version", raw("reconciler.example.com/v9", `{}`), ReasonUnsupportedVersion},
		{"empty version", raw("", `{}`), ReasonUnsupportedVersion},
		{"bad kind", func() *RawObject { o := raw(APIVersionV2, `{}`); o.Kind = "Gadget"; return o }(), ReasonUnsupportedKind},
		{"unknown field", raw(APIVersionV2, `{"boom":1}`), ReasonUnknownField},
		{"malformed json", raw(APIVersionV2, `{not-json`), ReasonMalformedSpec},
		{"wrong type", raw(APIVersionV2, `{"replicas":"two"}`), ReasonMalformedSpec},
		{"replicas too high", raw(APIVersionV2, `{"replicas":11}`), ReasonReplicasOutOfRange},
		{"replicas negative", raw(APIVersionV2, `{"replicas":-1}`), ReasonReplicasOutOfRange},
		{"grace out of range", raw(APIVersionV2, `{"graceSeconds":9999}`), ReasonGraceOutOfRange},
		{"auto mode in v1", raw(APIVersionV1, `{"mode":"Auto"}`), ReasonUnsupportedMode},
		{"unknown mode v2", raw(APIVersionV2, `{"mode":"Banana"}`), ReasonUnsupportedMode},
		{"missing name", raw(APIVersionV2, `{}`).withName(""), ReasonMissingName},
		{"invalid name", raw(APIVersionV2, `{}`).withName("UPPER"), ReasonNameInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if ae := admitErr(t, tc.obj); ae.Reason != tc.reason {
				t.Fatalf("reason = %q, want %q (%s)", ae.Reason, tc.reason, ae.Message)
			}
		})
	}
}

func TestDefaultsAreStableAcrossRepeatedReads(t *testing.T) {
	obj := raw(APIVersionV2, `{}`)
	s1, d1, err := DecodeSpec(obj)
	if err != nil {
		t.Fatal(err)
	}
	s2, d2, err := DecodeSpec(obj)
	if err != nil {
		t.Fatal(err)
	}
	if !s1.Equal(s2) || s1.SpecHash() != s2.SpecHash() {
		t.Fatal("defaulted specs differ between reads")
	}
	if len(d1.Fields) != len(d2.Fields) || d1.Values["replicas"] != "1" || d1.Values["mode"] != "On" || d1.Values["graceSeconds"] != "30" {
		t.Fatalf("unexpected defaults: %+v", d1)
	}
	if s1.Replicas != 1 || s1.Mode != ModeOn || s1.GraceSeconds != 30 {
		t.Fatalf("unexpected default values: %+v", s1)
	}
}

func TestV1HistoricalObjectGetsGraceDefault(t *testing.T) {
	s, d, err := DecodeSpec(raw(APIVersionV1, `{"replicas":3,"mode":"Off","dependsOn":["a"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if s.GraceSeconds != 30 || s.Replicas != 3 || s.Mode != ModeOff || len(s.DependsOn) != 1 {
		t.Fatalf("bad v1 decode: %+v", s)
	}
	if d.Values["graceSeconds"] != "30" {
		t.Fatalf("grace default not recorded: %+v", d)
	}
	// explicit values must not appear as defaults
	for _, f := range d.Fields {
		if f == "replicas" || f == "mode" {
			t.Fatalf("explicit field recorded as default: %s", f)
		}
	}
}

func TestExplicitZeroIsNotDefaulted(t *testing.T) {
	s, d, err := DecodeSpec(raw(APIVersionV2, `{"replicas":0,"graceSeconds":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Replicas != 0 || s.GraceSeconds != 0 {
		t.Fatalf("explicit zero overridden: %+v", s)
	}
	for _, f := range d.Fields {
		if f == "replicas" || f == "graceSeconds" {
			t.Fatalf("explicit zero recorded as default")
		}
	}
}

func TestValidateGraph(t *testing.T) {
	mk := func(deps ...string) *WidgetSpec { return &WidgetSpec{DependsOn: deps} }

	if err := Validate("a", mk("b"), map[string]*WidgetSpec{"a": mk("b"), "b": mk()}); err != nil {
		t.Fatalf("valid graph rejected: %v", err)
	}
	if ae := admitValidate(t, "a", mk("a"), map[string]*WidgetSpec{"a": mk("a")}); ae.Reason != ReasonSelfDependency {
		t.Fatalf("self dep: %s", ae.Reason)
	}
	if ae := admitValidate(t, "a", mk("b"), map[string]*WidgetSpec{"a": mk("b")}); ae.Reason != ReasonDependencyNotFound {
		t.Fatalf("missing dep: %s", ae.Reason)
	}
	cyc := map[string]*WidgetSpec{
		"a": mk("b"),
		"b": mk("c"),
		"c": mk("a"),
	}
	if ae := admitValidate(t, "a", mk("b"), cyc); ae.Reason != ReasonCircularDependency {
		t.Fatalf("cycle: %s", ae.Reason)
	}
}

func admitValidate(t *testing.T, name string, spec *WidgetSpec, graph map[string]*WidgetSpec) *AdmissionError {
	t.Helper()
	err := Validate(name, spec, graph)
	var ae *AdmissionError
	if !errors.As(err, &ae) {
		t.Fatalf("expected AdmissionError, got %v", err)
	}
	return ae
}

func (o *RawObject) withName(n string) *RawObject { o.Metadata.Name = n; return o }
