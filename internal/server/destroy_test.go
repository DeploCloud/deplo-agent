package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// DestroyStack's `rm -f` fallback must report Ok based on the docker EXIT CODE, not
// merely the spawn error, otherwise a genuine non-zero removal failure is reported as
// a successful destroy.
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
	// A different slug's files are untouched - removal is scoped to the slug.
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

// When a removeVolumes destroy can't run a clean `down -v` (here: a malformed compose
// file makes `compose down` fail), it must fall through to rm -f and report Ok:false
// WITHOUT sweeping the stack file - `rm -f` can't reclaim a named volume, so the volume
// survived and the only on-disk record of its name (the compose file) must be kept for
// a retry.
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

// A successful teardown sweeps the stack files whether or not volumes went with them.
func TestDestroyStack_sweepsStackFilesOnAnySuccessfulDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")

	slug := "app-keepfile-xyz"
	stackFile := filepath.Join(stackDir, slug+".yml")
	envFile := filepath.Join(stackDir, slug+".env")
	if err := os.WriteFile(stackFile, []byte("services: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(envFile, []byte("FOO=bar\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The app's own config files, and a NEIGHBOUR's - the sweep is scoped to one
	// slug, and proving that is the whole reason the second directory is here.
	filesDir := filepath.Join(stackDir, "files", slug)
	if err := os.MkdirAll(filepath.Join(filesDir, "conf.d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filesDir, "conf.d", "nginx.conf"), []byte("server {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	neighbour := filepath.Join(stackDir, "files", "app-keepfile-other")
	if err := os.MkdirAll(neighbour, 0o755); err != nil {
		t.Fatal(err)
	}

	res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: slug})
	if err != nil {
		t.Fatalf("DestroyStack rpc error: %v", err)
	}
	if !res.GetOk() {
		t.Fatalf("destroy should be Ok, got err=%q", res.GetError())
	}
	if _, err := os.Stat(stackFile); !os.IsNotExist(err) {
		t.Errorf("a successful destroy must sweep the stack file, stat err=%v", err)
	}
	if _, err := os.Stat(envFile); !os.IsNotExist(err) {
		t.Errorf("and the env file with it, stat err=%v", err)
	}
	// The config files go too, subdirectories included: leaving them behind left a
	// deleted app's configuration on a shared host forever.
	if _, err := os.Stat(filesDir); !os.IsNotExist(err) {
		t.Errorf("and the app's files directory, stat err=%v", err)
	}
	if _, err := os.Stat(neighbour); err != nil {
		t.Errorf("another app's files must be untouched, stat err=%v", err)
	}
}

// A destroy may reclaim named volumes the control plane lists explicitly, which is the
// ONLY way to reclaim the data of a stack that was never deployed: with no compose file
// on the host, `down -v` has nothing to read and the volume is left behind with nothing
// able to name it (an imported app, deleted before its first deploy).
func TestDestroyStack_reclaimsNamedVolumesAndOnlyDeploOnes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	// Unique per run: this suite runs on hosts that also run real workloads.
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	mine := "deplo-agenttest-reclaim-" + stamp
	foreign := "agenttest-keepme-" + stamp
	for _, v := range []string{mine, foreign} {
		if res, err := dockercli.Run(ctx, 20*time.Second, "volume", "create", v); err != nil || res.Code != 0 {
			t.Skipf("cannot create a scratch volume: %v", err)
		}
	}
	defer dockercli.Run(ctx, 20*time.Second, "volume", "rm", "-f", foreign) //nolint:errcheck

	s := New(t.TempDir(), t.TempDir(), "/", "")
	res, err := s.DestroyStack(ctx, &pb.StackRef{
		Slug:           "agenttest-never-deployed-" + stamp,
		RemoveVolumes:  true,
		ReclaimVolumes: []string{mine, foreign},
	})
	if err != nil {
		t.Fatalf("DestroyStack rpc error: %v", err)
	}
	_ = res

	exists := func(name string) bool {
		r, err := dockercli.Run(ctx, 20*time.Second, "volume", "inspect", name)
		return err == nil && r.Code == 0
	}
	if exists(mine) {
		t.Errorf("%s should have been reclaimed", mine)
	}
	if !exists(foreign) {
		t.Errorf("%s is not Deplo's to remove and must survive", foreign)
	}
}

// A pull request preview lives on a network of its own, and the row for it goes
// away with the pull request: if the destroy does not take the network too, every
// closed pull request leaves one behind against Docker's address pool.
func TestDestroyStack_removesThePreviewNetwork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	// A slug no real stack could carry, so nothing on a shared host is touched.
	slug := fmt.Sprintf("agenttest-prvnet-%d__pr-1", time.Now().UnixNano()%1_000_000)
	network := "deplo-preview-" + slug
	if r, err := dockercli.Run(ctx, 20*time.Second, "network", "create", network); err != nil || r.Code != 0 {
		t.Skipf("cannot create a scratch network: %v %s", err, r.Stderr)
	}
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")
	// The shape the control plane renders: the stack's own network, declared external.
	yml := "services:\n  app:\n    image: alpine:3.20\n    command: [\"sleep\", \"300\"]\n" +
		"    networks: [deplo]\nnetworks:\n  deplo:\n    name: " + network + "\n    external: true\n"
	if err := os.WriteFile(s.stackPath(slug), []byte(yml), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = dockercli.Run(context.Background(), 60*time.Second, s.composeCtl(slug, "down", "-v")...)
		_, _ = dockercli.Run(context.Background(), 20*time.Second, "network", "rm", network)
	})
	if r, err := dockercli.Run(ctx, 90*time.Second, s.composeCtl(slug, "up", "-d")...); err != nil || r.Code != 0 {
		t.Fatalf("compose up: %v %s", err, r.Stderr)
	}

	res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true})
	if err != nil {
		t.Fatalf("DestroyStack rpc error: %v", err)
	}
	if !res.GetOk() {
		t.Fatalf("destroy should succeed, got err=%q", res.GetError())
	}
	ls, _ := dockercli.Run(ctx, 20*time.Second, "network", "ls", "--format", "{{.Name}}")
	for _, n := range strings.Fields(ls.Stdout) {
		if n == network {
			t.Fatalf("the preview network %s survived the destroy", network)
		}
	}
	if !containsString(Capabilities, "teardown.preview-network") {
		t.Error("Capabilities must advertise \"teardown.preview-network\"")
	}
}

// The file only ever goes on a successful `down`, so a destroy that finds no file
// has nothing left to remove: it must say so, or every retry of it fails forever.
func TestDestroyStack_missingFileWithRemoveVolumesReportsOk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: "agenttest-nofile-xyz__pr-3", RemoveVolumes: true})
	if err != nil {
		t.Fatalf("DestroyStack rpc error: %v", err)
	}
	if !res.GetOk() {
		t.Errorf("no file and no container is a finished destroy, got Ok=false err=%q", res.GetError())
	}
	if !containsString(Capabilities, "teardown.missing-file-ok") {
		t.Error("Capabilities must advertise \"teardown.missing-file-ok\"")
	}
}
