// Package v1beta1 defines the historical, still-readable schema version.
package v1beta1

const (
	// Version is the beta schema version identifier.
	Version = "v1beta1"
	// TypeMetaAPIVersion is the apiVersion string clients used historically.
	TypeMetaAPIVersion = "core.example.com/" + Version
	Kind               = "Workload"
)

// Spec is the v1beta1 desired state as stored by old clients.
// Differences from v1:
//   - ReplicaCount instead of replicas; zero/absent must default deterministically.
//   - ContainerImage instead of image.
//   - Depends is a comma-free []string of dependency names.
//   - Config is a free-form map, forwarded as-is.
type Spec struct {
	ReplicaCount   int               `json:"replicaCount,omitempty"`
	ContainerImage string            `json:"containerImage,omitempty"`
	Depends        []string          `json:"depends,omitempty"`
	Config         map[string]string `json:"config,omitempty"`
}

// Manifest is the wire form historical clients submit.
type Manifest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name        string            `json:"name"`
		Annotations map[string]string `json:"annotations,omitempty"`
	} `json:"metadata"`
	Spec map[string]any `json:"spec"`
}
