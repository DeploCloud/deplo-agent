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

// DestroyStack's `rm -f` fallback must report Ok based on the docker EXIT CODE,
// not merely the spawn error — otherwise a genuine non-zero removal failure is
// reported as a successful destroy. The common already-gone case (`rm -f` of a
// missing container) is idempotent (exit 0) and must still report Ok:true.
func TestDestroyStack_missingContainerReportsOk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	// No such stack/container exists: compose down has no file, rm -f is
	// idempotent (exit 0) → Ok:true, not a false failure.
	res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: "definitely-not-a-real-stack-xyz"})
	if err != nil {
		t.Fatalf("DestroyStack rpc error: %v", err)
	}
	if !res.GetOk() {
		t.Errorf("destroying a missing stack should be Ok (idempotent), got Ok=false err=%q", res.GetError())
	}
}

// removeStackFiles deletes the compose file + env sidecar; it must be idempotent
// (a missing file is not an error) so it can run on any successful destroy.
func TestRemoveStackFiles_idempotentAndScoped(t *testing.T) {
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")

	yml := filepath.Join(stackDir, "db-keep.yml")
	env := filepath.Join(stackDir, "db-keep.env")
	other := filepath.Join(stackDir, "db-other.yml")
	for _, f := range []string{yml, env, other} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Removing a slug with no files on disk must not panic or error.
	s.removeStackFiles("never-existed")

	s.removeStackFiles("db-keep")
	if _, err := os.Stat(yml); !os.IsNotExist(err) {
		t.Errorf("compose file should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(env); !os.IsNotExist(err) {
		t.Errorf("env file should be removed, stat err=%v", err)
	}
	// A different slug's files are untouched — removal is scoped to the slug.
	if _, err := os.Stat(other); err != nil {
		t.Errorf("an unrelated stack's file must survive, stat err=%v", err)
	}
}

// A removeVolumes destroy of a never-deployed stack reaches the success path
// (compose down on an absent project is a no-op exit 0) and sweeps the on-disk
// compose file, so a deleted database leaves no stack file behind on the host.
func TestDestroyStack_removeVolumesSweepsStackFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")

	slug := "db-sweep-xyz"
	stackFile := filepath.Join(stackDir, slug+".yml")
	// A minimal valid compose so `compose down` accepts the -f file.
	if err := os.WriteFile(stackFile, []byte("services: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true})
	if err != nil {
		t.Fatalf("DestroyStack rpc error: %v", err)
	}
	if !res.GetOk() {
		t.Fatalf("destroy should be Ok, got err=%q", res.GetError())
	}
	if _, err := os.Stat(stackFile); !os.IsNotExist(err) {
		t.Errorf("removeVolumes destroy should delete the stack file, stat err=%v", err)
	}
}

// When a removeVolumes destroy can't run a clean `down -v` (here: a malformed
// compose file makes `compose down` fail), it must fall through to rm -f and
// report Ok:false WITHOUT sweeping the stack file — `rm -f` can't reclaim a named
// volume, so the volume survived and the only on-disk record of its name (the
// compose file) must be kept for a retry. Reporting Ok:true here would have the
// control plane believe a still-present volume was reclaimed.
func TestDestroyStack_removeVolumesDownFailKeepsFileAndReportsNotOk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")

	slug := "db-downfail-xyz"
	stackFile := filepath.Join(stackDir, slug+".yml")
	// Malformed YAML → `compose -f <file> down` exits non-zero, forcing the
	// rm -f fallback. (rm -f of the missing compose-named container is exit 0.)
	if err := os.WriteFile(stackFile, []byte("services: [this is not valid compose\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true})
	if err != nil {
		t.Fatalf("DestroyStack rpc error: %v", err)
	}
	if res.GetOk() {
		t.Errorf("a removeVolumes destroy that failed down -v must report Ok=false (volume not reclaimed)")
	}
	if _, err := os.Stat(stackFile); err != nil {
		t.Errorf("the stack file must be KEPT on the fallback path (needed for retry), stat err=%v", err)
	}
}

// Without removeVolumes the stack file is LEFT in place (the original app-teardown
// behaviour): only the containers come down, the compose file and volumes survive.
func TestDestroyStack_keepsStackFileByDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")

	slug := "app-keepfile-xyz"
	stackFile := filepath.Join(stackDir, slug+".yml")
	if err := os.WriteFile(stackFile, []byte("services: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: slug})
	if err != nil {
		t.Fatalf("DestroyStack rpc error: %v", err)
	}
	if !res.GetOk() {
		t.Fatalf("destroy should be Ok, got err=%q", res.GetError())
	}
	if _, err := os.Stat(stackFile); err != nil {
		t.Errorf("default destroy must leave the stack file in place, stat err=%v", err)
	}
}
