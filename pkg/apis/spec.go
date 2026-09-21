package apis

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
)

// WidgetSpec is the version-independent internal representation every
// reconciliation step works against.
type WidgetSpec struct {
	Replicas     int
	Mode         string
	DependsOn    []string
	GraceSeconds int
}

// Equal reports whether two canonical specs describe the same intent.
func (s *WidgetSpec) Equal(other *WidgetSpec) bool {
	if s == nil || other == nil {
		return s == other
	}
	if s.Replicas != other.Replicas || s.Mode != other.Mode ||
		s.GraceSeconds != other.GraceSeconds ||
		len(s.DependsOn) != len(other.DependsOn) {
		return false
	}
	a := append([]string(nil), s.DependsOn...)
	b := append([]string(nil), other.DependsOn...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SpecHash returns a stable hash of the canonical spec. It is used to
// decide whether an object is already at its desired state and to detect
// "the spec changed during reconciliation".
func (s *WidgetSpec) SpecHash() string {
	deps := append([]string(nil), s.DependsOn...)
	sort.Strings(deps)
	var b strings.Builder
	b.WriteString("v1|") // hash format version
	b.WriteString(strconv.Itoa(s.Replicas))
	b.WriteByte('|')
	b.WriteString(s.Mode)
	b.WriteByte('|')
	b.WriteString(strings.Join(deps, ","))
	b.WriteByte('|')
	b.WriteString(strconv.Itoa(s.GraceSeconds))
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:16])
}
