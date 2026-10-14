package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

func composeBaseArgs(project, stackFile, envFile, projectDir string) []string {
	args := []string{"compose", "-p", project, "-f", stackFile}
	if projectDir != "" {
		args = append(args, "--project-directory", projectDir)
	}
	if envFile != "" {
		args = append(args, "--env-file", envFile)
	}
	return args
}

func (s *Service) prepareStackImages(ctx context.Context, req *pb.DeployRequest, base []string, e *emitter) ([]string, bool) {
	env := dockerConfigEnv(req)
	steps := [][]string{
		append(append([]string{}, base...), "pull", "--ignore-buildable", "--quiet"),
		append(append([]string{}, base...), "build",
			"--build-arg", "BUILDKIT_CACHE_MOUNT_NS="+s.cacheNamespace(req.GetSlug())),
	}
	for _, args := range steps {
		code, err := dockercli.StreamEnv(ctx, 15*time.Minute, func(l string) { e.log("info", l) }, env, args...)
		if err != nil {
			e.result(false, "docker "+args[len(base)]+": "+err.Error(), "")
			return nil, false
		}
		if code != 0 {
			e.result(false, fmt.Sprintf("docker compose %s failed (exit %d)", args[len(base)], code), "")
			return nil, false
		}
	}
	images, err := composeImages(ctx, env, base)
	if err != nil {
		e.result(false, err.Error(), "")
		return nil, false
	}
	return images, true
}

func composeImages(ctx context.Context, env, base []string) ([]string, error) {
	res, err := dockercli.RunEnv(ctx, 2*time.Minute, env, append(append([]string{}, base...), "config", "--images")...)
	if err != nil || res.Code != 0 {
		return nil, fmt.Errorf("docker compose config --images: %s", strings.TrimSpace(res.Stderr))
	}
	return splitLines(res.Stdout), nil
}

// admitStackImages is Deploy's image admission for a stack already on disk: present images cost
// one inspect each, and only a missing one is pulled, as `up` itself would have.
func admitStackImages(ctx context.Context, base []string) error {
	images, err := composeImages(ctx, nil, base)
	if err != nil {
		return err
	}
	return admitImages(ctx, images, func() error {
		res, err := dockercli.Run(ctx, 2*time.Minute,
			append(append([]string{}, base...), "pull", "--ignore-buildable", "--policy", "missing", "--quiet")...)
		if err == nil && res.Code != 0 {
			err = fmt.Errorf("docker compose pull: %s", strings.TrimSpace(res.Stderr))
		}
		return err
	})
}

// admitImages refuses an image that brings traefik.* labels. One that cannot be inspected is
// pulled once when pull is set, then refused if it still cannot: an unchecked image never starts.
func admitImages(ctx context.Context, images []string, pull func() error) error {
	missing, err := traefikImageCheck(ctx, images)
	if err != nil || len(missing) == 0 {
		return err
	}
	if pull != nil {
		if err := pull(); err != nil {
			return err
		}
		if missing, err = traefikImageCheck(ctx, missing); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("the image %s could not be inspected on this server, so it was not started", strings.Join(missing, ", "))
	}
	return nil
}

func traefikImageCheck(ctx context.Context, images []string) (missing []string, err error) {
	for _, img := range images {
		res, rerr := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "--format", "{{json .Config.Labels}}", img)
		if rerr != nil || res.Code != 0 {
			missing = append(missing, img)
			continue
		}
		if keys := traefikLabelKeys(res.Stdout); len(keys) > 0 {
			return nil, fmt.Errorf(
				"the image %s carries Traefik labels (%s); Deplo routes apps itself, so an image must not bring its own routing",
				img, strings.Join(keys, ", "))
		}
	}
	return missing, nil
}

func traefikLabelKeys(labelsJSON string) []string {
	var labels map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(labelsJSON)), &labels); err != nil {
		return nil
	}
	var keys []string
	for k := range labels {
		if strings.HasPrefix(strings.ToLower(k), "traefik.") {
			keys = append(keys, k)
		}
	}
	sortStrings(keys)
	return keys
}

func sortStrings(a []string) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}
