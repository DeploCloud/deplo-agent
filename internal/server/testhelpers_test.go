package server

import (
	"os"
	"path/filepath"
	"testing"
)

// Shared by the file/copy tests; it outlived the dev-mode test file it was written in.
func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
