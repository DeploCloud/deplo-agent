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
		append(append([]string{}, base...), "build"),
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
	res, err := dockercli.RunEnv(ctx, 2*time.Minute, env, append(append([]string{}, base...), "config", "--images")...)
	if err != nil || res.Code != 0 {
		e.result(false, "docker compose config --images: "+strings.TrimSpace(res.Stderr), "")
		return nil, false
	}
	var images []string
	for _, line := range strings.Split(res.Stdout, "\n") {
		if l := strings.TrimSpace(line); l != "" {
			images = append(images, l)
		}
	}
	return images, true
}

func refuseTraefikImageLabels(ctx context.Context, images []string, e *emitter) bool {
	for _, img := range images {
		res, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "--format", "{{json .Config.Labels}}", img)
		if err != nil || res.Code != 0 {
			continue
		}
		if keys := traefikLabelKeys(res.Stdout); len(keys) > 0 {
			e.result(false, fmt.Sprintf(
				"the image %s carries Traefik labels (%s); Deplo routes apps itself, so an image must not bring its own routing",
				img, strings.Join(keys, ", ")), "")
			return false
		}
	}
	return true
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
