package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

type dependencyVolume struct {
	Name       string
	Mountpoint string
	Labels     map[string]string
}
type dependencySeedMarker struct {
	Catalog  string `json:"catalog"`
	Platform string `json:"platform"`
}

func inspectDependencyVolume(ctx context.Context, name string) (*dependencyVolume, error) {
	result, err := dockercli.Run(ctx, 30*time.Second, "volume", "inspect", name)
	if err != nil {
		return nil, err
	}
	if result.Code != 0 {
		return nil, nil
	}
	var volumes []dependencyVolume
	if json.Unmarshal([]byte(result.Stdout), &volumes) != nil || len(volumes) != 1 || volumes[0].Name != name || !filepath.IsAbs(volumes[0].Mountpoint) {
		return nil, fmt.Errorf("invalid dependency volume inspection")
	}
	return &volumes[0], nil
}

func dependencyDocker(ctx context.Context, args ...string) error {
	result, err := dockercli.Run(ctx, 2*time.Minute, args...)
	if err != nil {
		return err
	}
	if result.Code != 0 {
		return fmt.Errorf("dependency Docker %s failed: %s", args[0], strings.TrimSpace(result.Stderr))
	}
	return nil
}

func dependencyPullImage(ctx context.Context, platform, image string) error {
	result, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", image)
	if err != nil {
		return err
	}
	if result.Code == 0 {
		log.Printf("DeploPack image already available: %s", image)
		return nil
	}
	code, err := dockercli.Stream(ctx, 20*time.Minute, func(line string) { log.Printf("DeploPack pull %s: %s", image, line) }, "", "pull", "--platform", platform, image)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("dependency image pull failed: %s", image)
	}
	return nil
}

func waitDependencyWorker(ctx context.Context, name string) error {
	for attempt := 0; attempt < 30; attempt++ {
		result, err := dockercli.Run(ctx, 10*time.Second, "exec", name, "buildctl", "debug", "workers")
		if err == nil && result.Code == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("dependency BuildKit did not become ready")
}

func (s *Service) prepareDependencySeed(ctx context.Context, prep *dependencyPreparation) error {
	spec := prep.spec
	volume, err := inspectDependencyVolume(ctx, prep.seed)
	if err != nil {
		return err
	}
	if volume != nil {
		if volume.Labels["deplo.deplopack-seed"] != spec.fingerprint {
			return fmt.Errorf("seed volume is not owned by this catalog")
		}
		var marker dependencySeedMarker
		if data, err := os.ReadFile(filepath.Join(volume.Mountpoint, ".deplopack-seed.json")); err == nil && json.Unmarshal(data, &marker) == nil && marker.Catalog == spec.fingerprint && marker.Platform == spec.Platform {
			log.Printf("DeploPack seed already ready: %s", prep.seed)
			return nil
		}
	}
	kit := "deplo-pack-seed-" + spec.fingerprint[:20]
	dockercli.ForceRemove(kit)
	if volume != nil {
		if err := dependencyDocker(ctx, "volume", "rm", prep.seed); err != nil {
			return err
		}
	}
	if err := dependencyDocker(ctx, "volume", "create", "--label", "deplo.deplopack-seed="+spec.fingerprint, prep.seed); err != nil {
		return err
	}
	ready := false
	defer func() {
		dockercli.ForceRemove(kit)
		if !ready {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = dependencyDocker(cleanup, "volume", "rm", prep.seed)
		}
	}()
	if err := dependencyDocker(ctx, "run", "-d", "--privileged", "--name", kit, "-v", prep.seed+":/var/lib/buildkit", spec.resource("buildkit").Image); err != nil {
		return err
	}
	if err := waitDependencyWorker(ctx, kit); err != nil {
		return err
	}
	if err := os.MkdirAll(s.buildTmpDir, 0700); err != nil {
		return err
	}
	root, err := os.MkdirTemp(s.buildTmpDir, "deplo-pack-seed-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := dependencyDocker(ctx, "cp", root+"/.", kit+":/tmp/deplo-seed"); err != nil {
		return err
	}
	for _, dep := range spec.Dependencies {
		if dep.Target != "buildkit-cache" {
			continue
		}
		// RUN forces lazy image layers to be downloaded and unpacked, not merely resolved.
		if err := os.WriteFile(filepath.Join(root, "Dockerfile"), []byte("FROM "+dep.Image+"\nRUN true\n"), 0600); err != nil {
			return err
		}
		if err := dependencyDocker(ctx, "cp", filepath.Join(root, "Dockerfile"), kit+":/tmp/deplo-seed/Dockerfile"); err != nil {
			return err
		}
		log.Printf("DeploPack seed image start: %s", dep.Image)
		code, err := dockercli.Stream(ctx, 20*time.Minute, func(line string) { log.Printf("DeploPack seed %s: %s", dep.ID, line) }, "", "exec", kit, "buildctl", "build", "--frontend", "dockerfile.v0", "--local", "context=/tmp/deplo-seed", "--local", "dockerfile=/tmp/deplo-seed", "--opt", "platform="+spec.Platform, "--progress", "plain")
		if err != nil {
			return err
		}
		if code != 0 {
			return fmt.Errorf("seed image preparation failed: %s", dep.Image)
		}
	}
	if err := dependencyDocker(ctx, "stop", "-t", "10", kit); err != nil {
		return err
	}
	volume, err = inspectDependencyVolume(ctx, prep.seed)
	if err != nil {
		return err
	}
	if volume == nil {
		return fmt.Errorf("seed volume disappeared")
	}
	if err := writeDependencyMarker(filepath.Join(volume.Mountpoint, ".deplopack-seed.json"), dependencySeedMarker{Catalog: spec.fingerprint, Platform: spec.Platform}); err != nil {
		return err
	}
	ready = true
	return nil
}

func (s *Service) initializeDependencyCache(ctx context.Context, volume, projectID, jobKey string, prep *dependencyPreparation, e *emitter) (bool, error) {
	existing, err := inspectDependencyVolume(ctx, volume)
	if err != nil {
		return false, err
	}
	if existing != nil {
		return false, nil
	}
	if err := dependencyDocker(ctx, "volume", "create", "--label", "deplo.deplopack-cache="+projectID, volume); err != nil {
		return false, err
	}
	initialized := false
	helper := "deplo-pack-copy-" + jobKey
	defer func() {
		dockercli.ForceRemove(helper)
		if !initialized {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = dependencyDocker(cleanup, "volume", "rm", volume)
		}
	}()
	e.log("info", "Copying the prepared DeploPack build environment into the new App cache")
	// Copy independently; linking the seed metadata database would corrupt both caches.
	code, err := dockercli.Stream(ctx, 20*time.Minute, func(line string) { e.log("info", line) }, "", "run", "--rm", "--network", "none", "--name", helper, "-v", prep.seed+":/seed:ro", "-v", volume+":/target", "--entrypoint", "sh", prep.spec.resource("docker-cli").Image, "-c", "cp -a /seed/. /target/")
	if err != nil {
		return false, err
	}
	if code != 0 {
		return false, fmt.Errorf("initial App cache copy failed")
	}
	initialized = true
	return true, nil
}
