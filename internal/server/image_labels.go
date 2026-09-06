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

// Docker merges an IMAGE's labels into every container it starts, and Traefik
// reads the container's labels: a `LABEL traefik.http.routers.x.rule=Host(...)`
// baked into an image would register a router the control plane never rendered
// and never checked against anybody's domains. Deplo stamps every Traefik label
// itself, so an image carrying one is refused before the stack starts.

// composeBaseArgs is the `docker compose` prefix that names THIS stack.
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

// prepareStackImages pulls and builds every image of the stack so that all of
// them can be inspected BEFORE `up`, and returns their references.
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

// refuseTraefikImageLabels fails the deploy when any of the images carries a
// Traefik label. An image that is not present is left to `up` to complain about.
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

// traefikLabelKeys reads `docker image inspect --format {{json .Config.Labels}}`
// output and returns the label keys Traefik would act on, sorted.
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
