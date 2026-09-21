package controller

import "github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"

// specEqual reports whether two specs describe the same desired state.
// DependsOn order is significant (it defines the declared ordering), map
// contents compare by key/value.
func specEqual(a, b v1.Spec) bool {
	if a.Replicas != b.Replicas || a.Image != b.Image {
		return false
	}
	if len(a.DependsOn) != len(b.DependsOn) {
		return false
	}
	for i := range a.DependsOn {
		if a.DependsOn[i] != b.DependsOn[i] {
			return false
		}
	}
	if len(a.Config) != len(b.Config) {
		return false
	}
	for k, v := range a.Config {
		if bv, ok := b.Config[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
