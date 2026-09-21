package storage

import (
	"os"
	"strings"
)

func osReadFile(path string) ([]byte, error) { return os.ReadFile(path) }

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

func stringsReplaceAll(s, old, new string) string { return strings.ReplaceAll(s, old, new) }
