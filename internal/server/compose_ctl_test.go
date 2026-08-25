package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A compose stack interpolates ${VAR}; compose resolves the whole file on EVERY
// verb, so a stop without the env-file dies at config time on `${VAR:?...}` and
// the caller falls through to a container name a compose stack never has.
func TestComposeCtlCarriesTheEnvFile(t *testing.T) {
	dir := t.TempDir()
	s := &Service{stackDir: dir}
	const slug = "composectl-fixture"

	joined := strings.Join(s.composeCtl(slug, "stop"), " ")
	if strings.Contains(joined, "--env-file") {
		t.Fatalf("no env-file on disk, none should be passed: %s", joined)
	}

	// Current layout: the stack's own directory, which is also the project dir.
	projectDir := s.filesRoot(slug)
	if err := os.MkdirAll(projectDir, 0o700); err != nil {
		t.Fatal(err)
	}
	envFile := filepath.Join(projectDir, ".env")
	if err := os.WriteFile(envFile, []byte("K=v\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(s.composeCtl(slug, "down", "--remove-orphans"), " ")
	if !strings.Contains(joined, "--env-file "+envFile) {
		t.Fatalf("env-file missing: %s", joined)
	}
	if !strings.Contains(joined, "--project-directory "+projectDir) {
		t.Fatalf("project-directory missing: %s", joined)
	}
	if !strings.HasSuffix(joined, "down --remove-orphans") {
		t.Fatalf("verb must stay last: %s", joined)
	}

	// Pre-project-directory layout: env-file beside the stack file, no project dir.
	if err := os.RemoveAll(projectDir); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(dir, slug+".env")
	if err := os.WriteFile(legacy, []byte("K=v\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(s.composeCtl(slug, "start"), " ")
	if !strings.Contains(joined, "--env-file "+legacy) {
		t.Fatalf("legacy env-file missing: %s", joined)
	}
	if strings.Contains(joined, "--project-directory") {
		t.Fatalf("legacy layout has no project directory: %s", joined)
	}
}

// The fallback names deplo-<slug>, which only a single-image app has, so its
// "No such container" must never be the message a compose stack reports.
func TestStackFailurePrefersCompose(t *testing.T) {
	got := stackFailure(
		dockercli.Result{Code: 1, Stderr: "required variable TORBOX_ACCOUNTS is missing a value"},
		nil,
		dockercli.Result{Code: 1, Stderr: "Error response from daemon: No such container: deplo-test"},
		nil,
	)
	if !strings.Contains(got, "TORBOX_ACCOUNTS") {
		t.Fatalf("compose stderr should win, got %q", got)
	}
}
