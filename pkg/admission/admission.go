// Package admission runs the admission chain over raw client manifests:
// version decoding/upgrade, deterministic defaulting and validation.
package admission

import (
	"context"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/convert"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/validation"
)

// Decision is the admission verdict.
type Decision struct {
	Allowed bool
	Reason  string
	Message string
	// Workload is populated iff Allowed.
	Workload *v1.Workload
}

// Reviewer admits or rejects raw manifests.
type Reviewer interface {
	Review(ctx context.Context, raw []byte) Decision
}

// Chain is the default Reviewer: decode/upgrade -> default -> validate.
// Defaulting and validation are embedded in convert.DecodeManifest; the
// chain therefore guarantees:
//   - illegal specs are rejected with distinguishable reason codes;
//   - defaulting is deterministic and stable across repeated reads;
//   - unsupported versions/fields are rejected explicitly.
type Chain struct{}

// NewChain builds the default admission chain.
func NewChain() *Chain { return &Chain{} }

func (c *Chain) Review(ctx context.Context, raw []byte) Decision {
	up, err := convert.DecodeManifest(raw)
	if err != nil {
		reason, message := v1.ReasonInvalidSpec, err.Error()
		var f *validation.Failure
		if asValidationFailure(err, &f) {
			reason, message = f.Reason, f.Message
		}
		return Decision{Allowed: false, Reason: reason, Message: message}
	}
	return Decision{
		Allowed:  true,
		Reason:   v1.ReasonAdmitted,
		Message:  "manifest admitted",
		Workload: up.Workload,
	}
}

func asValidationFailure(err error, target **validation.Failure) bool {
	f, ok := err.(*validation.Failure)
	if !ok {
		return false
	}
	*target = f
	return true
}
