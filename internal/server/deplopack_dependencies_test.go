package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDependencyResourceRevalidatesAndDeduplicates(t *testing.T) {
	s := &Service{}
	s.dependencies.jobs = map[string]*dependencyJob{}
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	work := func() error { calls.Add(1); close(started); <-release; return nil }
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.dependencyResource(context.Background(), "image", true, work); err != nil {
			t.Error(err)
		}
	}()
	<-started
	s.dependencies.mu.Lock()
	job := s.dependencies.jobs["image"]
	s.dependencies.mu.Unlock()
	if job == nil {
		t.Fatal("missing in-flight job")
	}
	joined := make(chan error, 1)
	go func() {
		joined <- s.dependencyResource(context.Background(), "image", true, func() error {
			calls.Add(1)
			return fmt.Errorf("duplicate in-flight work")
		})
	}()
	select {
	case err := <-joined:
		t.Fatalf("concurrent work did not await the active job: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := s.dependencyResource(context.Background(), "image", true, func() error { calls.Add(1); return nil }); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("successful resources were not revalidated")
	}
}

func TestDependencyPullImageRestoresAndReportsFailure(t *testing.T) {
	dir := t.TempDir()
	state, pulls := filepath.Join(dir, "present"), filepath.Join(dir, "pulls")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1 $2" = "image inspect" ]; then [ -f '%s' ]; exit $?; fi
if [ "$1" = pull ]; then
 echo pull >> '%s'
 [ -f '%s/fail' ] && exit 1
 touch '%s'
 exit 0
fi
exit 1
`, state, pulls, dir, state)
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	for i := 0; i < 2; i++ {
		if err := dependencyPullImage(context.Background(), "linux/amd64", "docker:29-cli"); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ := os.ReadFile(pulls)
	if strings.Count(string(raw), "pull") != 1 {
		t.Fatal("present image was pulled again")
	}
	if err := os.Remove(state); err != nil {
		t.Fatal(err)
	}
	if err := dependencyPullImage(context.Background(), "linux/amd64", "docker:29-cli"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(state); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fail"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if dependencyPullImage(context.Background(), "linux/amd64", "docker:29-cli") == nil {
		t.Fatal("pull failure was ignored")
	}
}

func TestDependencyExportDetectsCorruptBlobs(t *testing.T) {
	root := t.TempDir()
	blobDir := filepath.Join(root, "base", "blobs", "sha256")
	if err := os.MkdirAll(blobDir, 0700); err != nil {
		t.Fatal(err)
	}
	blob := func(data []byte) dependencyDescriptor {
		digest := fmt.Sprintf("%x", sha256.Sum256(data))
		if err := os.WriteFile(filepath.Join(blobDir, digest), data, 0600); err != nil {
			t.Fatal(err)
		}
		return dependencyDescriptor{Digest: "sha256:" + digest, Size: int64(len(data))}
	}
	config := blob([]byte("{}"))
	layer := blob([]byte("layer"))
	manifest, _ := json.Marshal(map[string]any{"schemaVersion": 2, "config": config, "layers": []dependencyDescriptor{layer}})
	descriptor := blob(manifest)
	index, _ := json.Marshal(map[string]any{"schemaVersion": 2, "manifests": []dependencyDescriptor{descriptor}})
	indexPath := filepath.Join(root, "base", "index.json")
	if err := os.WriteFile(indexPath, index, 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateDependencyExport(root, "base"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(blobDir, strings.TrimPrefix(layer.Digest, "sha256:"))
	if err := os.WriteFile(path, []byte("wrong"), 0600); err != nil {
		t.Fatal(err)
	}
	if validateDependencyExport(root, "base") == nil {
		t.Fatal("same-size corruption was accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if validateDependencyExport(root, "base") == nil {
		t.Fatal("missing layer was accepted")
	}
	if err := os.WriteFile(indexPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if validateDependencyExport(root, "base") == nil {
		t.Fatal("invalid index was accepted")
	}
}

func TestPreparedDependencyImagesProtectsCatalog(t *testing.T) {
	s := &Service{}
	s.dependencies.preparations = map[string]*dependencyPreparation{"catalog": {spec: &dependencySpec{Dependencies: []packDependency{{Kind: "docker-image", Target: "host", Image: "example.invalid/tools:1"}}}}}
	images := s.preparedDependencyImages()
	if !images["docker"] || !images["docker.io/library/docker"] || !images["example.invalid/tools"] || images["nginx"] {
		t.Fatalf("unexpected protection: %v", images)
	}
}
