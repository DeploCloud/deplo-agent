package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	log.Printf("DeploPack image missing; restoring: %s", image)
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
		if data, err := os.ReadFile(filepath.Join(volume.Mountpoint, ".deplopack-seed.json")); err == nil && json.Unmarshal(data, &marker) == nil && marker.Catalog == spec.fingerprint && marker.Platform == spec.Platform && marker.Format == "oci-images-v1" && validateDependencyExports(volume.Mountpoint, spec) == nil {
			log.Printf("DeploPack seed already ready: %s", prep.seed)
			return nil
		}
	}
	log.Printf("DeploPack seed missing or invalid; repairing: %s", prep.seed)
	kit := "deplo-pack-seed-" + spec.fingerprint[:20]
	dockercli.ForceRemove(kit)
	if err := dependencyDocker(ctx, "volume", "create", "--label", "deplo.deplopack-seed="+spec.fingerprint, prep.seed); err != nil {
		return err
	}
	work := prep.seed + "-work"
	workOwned := false
	defer func() {
		dockercli.ForceRemove(kit)
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if workOwned {
			_ = dependencyDocker(cleanup, "volume", "rm", work)
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
	if err := dependencyDocker(ctx, "run", "-d", "--privileged", "--name", kit, "-v", work+":/var/lib/buildkit", "-v", prep.seed+":/deplopack-images", spec.resource("buildkit").Image); err != nil {
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
		if volume != nil && validateDependencyExport(volume.Mountpoint, dep.ID) == nil {
			log.Printf("DeploPack seed image reused: %s", dep.Image)
			continue
		}
		if volume != nil {
			if err := os.RemoveAll(filepath.Join(volume.Mountpoint, dep.ID)); err != nil {
				return err
			}
		}
		// The OCI exporter materializes the image blobs without copying BuildKit internals.
		if err := os.WriteFile(filepath.Join(root, "Dockerfile"), []byte("FROM "+dep.Image+"\n"), 0600); err != nil {
			return err
		}
		if err := dependencyDocker(ctx, "cp", filepath.Join(root, "Dockerfile"), kit+":/tmp/deplo-seed/Dockerfile"); err != nil {
			return err
		}
		log.Printf("DeploPack seed image start: %s", dep.Image)
		code, err := dockercli.Stream(ctx, 20*time.Minute, func(line string) { log.Printf("DeploPack seed %s: %s", dep.ID, line) }, "", "exec", kit, "buildctl", "build", "--frontend", "dockerfile.v0", "--local", "context=/tmp/deplo-seed", "--local", "dockerfile=/tmp/deplo-seed", "--opt", "platform="+spec.Platform, "--progress", "plain", "--output", "type=oci,tar=false,dest=/deplopack-images/"+dep.ID)
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
	if err := writeDependencyMarker(filepath.Join(volume.Mountpoint, ".deplopack-seed.json"), dependencySeedMarker{Catalog: spec.fingerprint, Platform: spec.Platform, Format: "oci-images-v1"}); err != nil {
		return err
	}
	return nil
}

func validateDependencyExports(root string, spec *dependencySpec) error {
	for _, dep := range spec.Dependencies {
		if dep.Target != "buildkit-cache" {
			continue
		}
		if err := validateDependencyExport(root, dep.ID); err != nil {
			return err
		}
	}
	return nil
}

type dependencyDescriptor struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

var verifiedDependencyBlobs sync.Map

func validateDependencyExport(root, id string) error {
	root = filepath.Join(root, id)
	data, err := readDependencyJSON(filepath.Join(root, "index.json"))
	if err != nil {
		return err
	}
	var index struct {
		SchemaVersion int                    `json:"schemaVersion"`
		Manifests     []dependencyDescriptor `json:"manifests"`
	}
	if json.Unmarshal(data, &index) != nil || index.SchemaVersion != 2 || len(index.Manifests) != 1 {
		return fmt.Errorf("invalid OCI cache index: %s", id)
	}
	manifestPath, err := validateDependencyBlob(root, index.Manifests[0])
	if err != nil {
		return err
	}
	data, err = readDependencyJSON(manifestPath)
	if err != nil {
		return err
	}
	var manifest struct {
		SchemaVersion int                    `json:"schemaVersion"`
		Config        dependencyDescriptor   `json:"config"`
		Layers        []dependencyDescriptor `json:"layers"`
	}
	if json.Unmarshal(data, &manifest) != nil || manifest.SchemaVersion != 2 {
		return fmt.Errorf("invalid OCI cache manifest: %s", id)
	}
	for _, descriptor := range append(manifest.Layers, manifest.Config) {
		if _, err := validateDependencyBlob(root, descriptor); err != nil {
			return err
		}
	}
	return nil
}

func readDependencyJSON(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return nil, fmt.Errorf("dependency JSON missing or invalid: %s", path)
	}
	return os.ReadFile(path)
}

func validateDependencyBlob(root string, descriptor dependencyDescriptor) (string, error) {
	digest, ok := strings.CutPrefix(descriptor.Digest, "sha256:")
	if !ok || !detectorDigest.MatchString(digest) || descriptor.Size < 0 {
		return "", fmt.Errorf("invalid OCI blob descriptor")
	}
	path := filepath.Join(root, "blobs", "sha256", digest)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != descriptor.Size {
		return "", fmt.Errorf("OCI blob missing or invalid: %s", descriptor.Digest)
	}
	// OCI blobs are immutable; rehash when their file identity or metadata changes.
	if cached, ok := verifiedDependencyBlobs.Load(path); ok {
		previous := cached.(os.FileInfo)
		if os.SameFile(info, previous) && info.Size() == previous.Size() && info.ModTime() == previous.ModTime() {
			return path, nil
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	if fmt.Sprintf("%x", hash.Sum(nil)) != digest {
		return "", fmt.Errorf("OCI blob checksum mismatch: %s", descriptor.Digest)
	}
	verifiedDependencyBlobs.Store(path, info)
	return path, nil
}
