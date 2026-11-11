package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// realDockerEnv opts a run into the tests that drive a real Docker daemon. CI sets it; nobody sets it by accident.
const realDockerEnv = "DEPLO_AGENT_TEST_REAL_DOCKER"

// Unit tests never cap this host's own build cgroups; a test that wants the watcher points it at a tree.
// Without the opt-in every docker call of the package is aimed at a socket that does not exist, so a test that
// forgot its fake reaches no daemon: this suite has destroyed live state on a production host before.
func TestMain(m *testing.M) {
	buildStepCgroupDir = ""
	if os.Getenv(realDockerEnv) != "1" {
		os.Setenv("DOCKER_HOST", "unix:///nonexistent/deplo-agent-test.sock")
		os.Unsetenv("DOCKER_CONTEXT")
	}
	os.Exit(m.Run())
}

// needsRealDocker skips a test that drives a real daemon unless the run opted in.
func needsRealDocker(t *testing.T) {
	t.Helper()
	if os.Getenv(realDockerEnv) != "1" {
		t.Skipf("drives a real Docker daemon: set %s=1 on a machine that serves nothing", realDockerEnv)
	}
}

func TestSuite_reachesNoDockerDaemonUnlessAskedTo(t *testing.T) {
	if os.Getenv(realDockerEnv) == "1" {
		t.Skip("this run opted into a real daemon")
	}
	if got := os.Getenv("DOCKER_HOST"); got != "unix:///nonexistent/deplo-agent-test.sock" {
		t.Fatalf("docker calls are aimed at %q", got)
	}
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
