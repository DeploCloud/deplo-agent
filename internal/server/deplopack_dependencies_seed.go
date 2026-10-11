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
	Format   string `json:"format"`
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
		if data, err := os.ReadFile(filepath.Join(volume.Mountpoint, ".deplopack-seed.json")); err == nil && json.Unmarshal(data, &marker) == nil && marker.Catalog == spec.fingerprint && marker.Platform == spec.Platform && marker.Format == "buildkit-local-v1" && validateDependencyExports(volume.Mountpoint, spec) == nil {
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
	work := prep.seed + "-work"
	ready, workOwned := false, false
	defer func() {
		dockercli.ForceRemove(kit)
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if workOwned {
			_ = dependencyDocker(cleanup, "volume", "rm", work)
		}
		if !ready {
			_ = dependencyDocker(cleanup, "volume", "rm", prep.seed)
		}
	}()
	existingWork, err := inspectDependencyVolume(ctx, work)
	if err != nil {
		return err
	}
	if existingWork != nil && existingWork.Labels["deplo.deplopack-preparation"] != spec.fingerprint {
		return fmt.Errorf("preparation volume is not owned by this catalog")
	}
	workOwned = true
	if err := dependencyDocker(ctx, "volume", "create", "--label", "deplo.deplopack-preparation="+spec.fingerprint, work); err != nil {
		return err
	}
	if err := dependencyDocker(ctx, "run", "-d", "--privileged", "--name", kit, "-v", work+":/var/lib/buildkit", "-v", prep.seed+":/deplopack-cache", spec.resource("buildkit").Image); err != nil {
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
		code, err := dockercli.Stream(ctx, 20*time.Minute, func(line string) { log.Printf("DeploPack seed %s: %s", dep.ID, line) }, "", "exec", kit, "buildctl", "build", "--frontend", "dockerfile.v0", "--local", "context=/tmp/deplo-seed", "--local", "dockerfile=/tmp/deplo-seed", "--opt", "platform="+spec.Platform, "--progress", "plain", "--export-cache", "type=local,mode=max,dest=/deplopack-cache/"+dep.ID)
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
	if err := validateDependencyExports(volume.Mountpoint, spec); err != nil {
		return err
	}
	if err := writeDependencyMarker(filepath.Join(volume.Mountpoint, ".deplopack-seed.json"), dependencySeedMarker{Catalog: spec.fingerprint, Platform: spec.Platform, Format: "buildkit-local-v1"}); err != nil {
		return err
	}
	ready = true
	return nil
}

func validateDependencyExports(root string, spec *dependencySpec) error {
	for _, dep := range spec.Dependencies {
		if dep.Target != "buildkit-cache" {
			continue
		}
		index := filepath.Join(root, dep.ID, "index.json")
		info, err := os.Lstat(index)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 {
			return fmt.Errorf("dependency cache index missing or invalid: %s", dep.ID)
		}
		data, err := os.ReadFile(index)
		if err != nil {
			return err
		}
		var document struct {
			SchemaVersion int               `json:"schemaVersion"`
			Manifests     []json.RawMessage `json:"manifests"`
		}
		if json.Unmarshal(data, &document) != nil || document.SchemaVersion != 2 || len(document.Manifests) == 0 {
			return fmt.Errorf("invalid OCI cache index: %s", dep.ID)
		}
	}
	return nil
}
