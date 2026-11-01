package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Unit tests never cap this host's own build cgroups; a test that wants the watcher points it at a tree.
func TestMain(m *testing.M) {
	buildStepCgroupDir = ""
	os.Exit(m.Run())
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// dockerOnPath puts a `docker` running body first on PATH, logging each argv as a line; it returns the log.
func dockerOnPath(t *testing.T, body string) func() []string {
	t.Helper()
	bin := t.TempDir()
	logf := filepath.Join(bin, "argv.log")
	script := "#!/bin/sh\necho \"$*\" >> " + logf + "\n" + body + "\n"
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return func() []string {
		b, _ := os.ReadFile(logf)
		return strings.Split(strings.TrimSpace(string(b)), "\n")
	}
}
