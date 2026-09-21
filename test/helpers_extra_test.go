package integration

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func mustMarshal(t *testing.T, w *v1.Workload) []byte {
	t.Helper()
	raw, err := json.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
