package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// teardownStack removes whatever the real `docker compose up` inside Reroute brought
// up.
func teardownStack(t *testing.T, s *Service, slug string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, _ = dockercli.Run(ctx, 60*time.Second,
			"compose", "-p", "deplo-"+slug, "-f", s.stackPath(slug), "down", "-v", "--remove-orphans")
	})
}

// A reroute with no rendered compose is a no-op failure (mirrors Deploy's
// missing-compose guard): Ok:false with a clear error and no stack file written.
func TestReroute_missingComposeFailsCleanly(t *testing.T) {
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := s.Reroute(ctx, &pb.RerouteRequest{Slug: "deplo-agent-test-reroute"})
	if err != nil {
		t.Fatalf("Reroute rpc error: %v", err)
	}
	if res.GetOk() {
		t.Error("expected Ok:false for a missing compose")
	}
	if res.GetError() == "" {
		t.Error("expected an error message for a missing compose")
	}
	// Nothing should have been written for a rejected request.
	if _, err := os.Stat(s.stackPath("deplo-agent-test-reroute")); err == nil {
		t.Error("stack file written despite missing compose")
	}
}

// Reroute writes the rendered YAML, the 0600 env-file and the compose mount files
// BEFORE it runs `compose up`, so the on-disk artefacts are observable even on a host
// without docker (the compose call then just fails).
func TestReroute_writesStackEnvAndMountFiles(t *testing.T) {
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const slug = "deplo-agent-test-reroute"
	teardownStack(t, s, slug)

	const yaml = "services:\n  web:\n    image: nginx\n"
	_, _ = s.Reroute(ctx, &pb.RerouteRequest{
		Slug:        slug,
		ComposeYaml: yaml,
		Env:         map[string]string{"FOO": "bar", "BAZ": "qux"},
		Mounts: []*pb.MountFile{
			{Path: "nginx.conf", Content: "server {}"},
		},
	})
	// The docker `up` may fail (no docker / no such image); we only assert the
	// files written before it, so the return value is intentionally ignored.

	// Stack file: exact YAML, 0644.
	stackFile := s.stackPath("deplo-agent-test-reroute")
	got, err := os.ReadFile(stackFile)
	if err != nil {
		t.Fatalf("read stack file: %v", err)
	}
	if string(got) != yaml {
		t.Fatalf("stack file = %q, want %q", got, yaml)
	}
	// 0600, like the env file below: for a SINGLE-IMAGE app the control plane bakes the
	// whole environment into this YAML, so it holds exactly the secrets the env file is
	// protected for.
	if info, err := os.Stat(stackFile); err != nil {
		t.Fatalf("stat stack file: %v", err)
	} else if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("stack file perm = %o, want 0600", perm)
	}

	// Env file: rendered KEY=VALUE (sorted), 0600, and inside the stack's OWN directory -
	// the one compose is pointed at with --project-directory, so a relative path the
	// author wrote (`env_file: .env` above all) resolves to this stack's file and never to
	// another tenant's.
	envFile := filepath.Join(
		stackDir,
		"files",
		"deplo-agent-test-reroute",
		".env",
	)
	envGot, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatalf("read env file: %v", err)
	}
	if want := "BAZ=\"qux\"\nFOO=\"bar\"\n"; string(envGot) != want {
		t.Fatalf("env file = %q, want %q", envGot, want)
	}
	if info, err := os.Stat(envFile); err != nil {
		t.Fatalf("stat env file: %v", err)
	} else if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("env file perm = %o, want 0600", perm)
	}

	// Mount file: materialised under files/<slug>/.
	mountFile := filepath.Join(stackDir, "files", "deplo-agent-test-reroute", "nginx.conf")
	mGot, err := os.ReadFile(mountFile)
	if err != nil {
		t.Fatalf("read mount file: %v", err)
	}
	if string(mGot) != "server {}" {
		t.Fatalf("mount file = %q", mGot)
	}
}

// With no env, Reroute writes no env-file (single-image stacks bake env into the
// YAML and send empty env), matching Deploy's single-image path.
func TestReroute_noEnvWritesNoEnvFile(t *testing.T) {
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	teardownStack(t, s, "deplo-agent-test-reroute")

	_, _ = s.Reroute(ctx, &pb.RerouteRequest{
		Slug:        "deplo-agent-test-reroute",
		ComposeYaml: "services:\n  web:\n    image: nginx\n",
	})

	if _, err := os.Stat(filepath.Join(stackDir, "deplo-agent-test-reroute.env")); err == nil {
		t.Error("env file written for an env-less reroute")
	}
}

// ReadStack on a slug with no stack file reports Exists:false (nothing deployed
// yet) rather than an RPC error.
func TestReadStack_missingFileReportsNotExists(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := s.ReadStack(ctx, &pb.StackRef{Slug: "never-deployed"})
	if err != nil {
		t.Fatalf("ReadStack rpc error: %v", err)
	}
	if resp.GetExists() {
		t.Error("expected Exists:false for a missing stack file")
	}
	if resp.GetYaml() != "" {
		t.Errorf("expected empty yaml, got %q", resp.GetYaml())
	}
}

// ReadStack on a present stack file returns its exact contents.
func TestReadStack_presentFileReturnsYaml(t *testing.T) {
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const yaml = "services:\n  web:\n    image: nginx:1.27\n"
	if err := os.WriteFile(s.stackPath("deplo-agent-test-reroute"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	resp, err := s.ReadStack(ctx, &pb.StackRef{Slug: "deplo-agent-test-reroute"})
	if err != nil {
		t.Fatalf("ReadStack rpc error: %v", err)
	}
	if !resp.GetExists() {
		t.Error("expected Exists:true for a present stack file")
	}
	if resp.GetYaml() != yaml {
		t.Fatalf("yaml = %q, want %q", resp.GetYaml(), yaml)
	}
}
