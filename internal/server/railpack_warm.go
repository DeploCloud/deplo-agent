package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// Long enough for an install to finish starting what it starts before 1.8 GB of images compete with it.
var railpackWarmDelay = 90 * time.Second

const railpackWarmMinFree = 10 << 30

// WarmRailpack builds a one-page site with railpack once per railpack version, in the background, so its
// frontend and base images are already in BuildKit's cache when the first app on a new server deploys.
func (s *Service) WarmRailpack(ctx context.Context) {
	marker := filepath.Join(s.dataBase, "tools", "railpack-warm-"+railpackVersion)
	if _, err := os.Stat(marker); err == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(railpackWarmDelay):
	}
	if !dockercli.Available(ctx) || !dockercli.ImageExportOptsSupported(ctx) {
		return
	}
	if free := filesystemFree("/"); free < railpackWarmMinFree {
		log.Printf("deplo-agent: not preparing railpack's images: %d MB free", free>>20)
		return
	}
	started := time.Now()
	if err := s.warmRailpackOnce(ctx); err != nil {
		log.Printf("deplo-agent: preparing railpack's images: %v", err)
		return
	}
	_ = os.WriteFile(marker, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
	log.Printf("deplo-agent: railpack's images are ready for the first build (%s)", time.Since(started).Round(time.Second))
}

func (s *Service) warmRailpackOnce(ctx context.Context) error {
	quiet := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	railpack, err := s.ensureRailpack(ctx, railpackVersion, quiet)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp(s.buildTmpDir, "deplo-railpack-warm-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	site, plan := filepath.Join(dir, "site"), filepath.Join(dir, "railpack-plan.json")
	if err := os.Mkdir(site, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(site, "index.html"), []byte("<!doctype html><title>Deplo</title>\n"), 0o644); err != nil {
		return err
	}
	if code, err := dockercli.SpawnEnv(ctx, 5*time.Minute, func(string) {}, nil, railpack, "prepare", site, "--plan-out", plan); err != nil || code != 0 {
		return fmt.Errorf("railpack prepare: exit %d %v", code, err)
	}
	if err := tuneRailpackPlan(plan); err != nil {
		return err
	}
	// cacheonly: the layers land in the cache and no image is written for cleanup to find.
	code, err := dockercli.StreamEnv(ctx, 20*time.Minute, func(string) {}, []string{"DOCKER_BUILDKIT=1"},
		"build", "--build-arg", "BUILDKIT_SYNTAX="+railpackFrontend, "-f", plan, "--output", "type=cacheonly", site)
	if err == nil && code != 0 {
		err = fmt.Errorf("docker build exited %d", code)
	}
	return err
}

func filesystemFree(path string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0
	}
	return int64(st.Bavail) * int64(st.Bsize)
}
