package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// End-to-end (real docker): the warm-up leaves railpack's images in BuildKit's cache, writes no image,
// and runs once per railpack version.
func TestE2E_WarmRailpackFillsTheCacheOnce(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) || !dockercli.ImageExportOptsSupported(ctx) {
		t.Skip("needs docker with the containerd image store")
	}
	orig := railpackWarmDelay
	railpackWarmDelay = 0
	t.Cleanup(func() { railpackWarmDelay = orig })
	// Its own tools dir: the marker lives there, and the shared e2e one already has it.
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	before, _ := dockercli.Run(ctx, 30*time.Second, "image", "ls", "-q")

	s.WarmRailpack(ctx)
	marker := filepath.Join(s.dataBase, "tools", "railpack-warm-"+railpackVersion)
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("no marker after the warm-up: %v", err)
	}
	du, err := dockercli.Run(ctx, 60*time.Second, "buildx", "du", "--verbose")
	if err != nil || !strings.Contains(du.Stdout, "railpack-builder:mise-") {
		t.Fatalf("railpack's builder is not in the build cache: %v\n%s", err, du.Stdout)
	}
	after, _ := dockercli.Run(ctx, 30*time.Second, "image", "ls", "-q")
	if after.Stdout != before.Stdout {
		t.Fatalf("the warm-up wrote an image:\nbefore %s\nafter %s", before.Stdout, after.Stdout)
	}

	// Already warm for this version: nothing runs, even with docker gone.
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	done := make(chan struct{})
	go func() { s.WarmRailpack(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a second warm-up did not return at once")
	}
}
