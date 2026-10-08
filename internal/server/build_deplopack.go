package server

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

func (s *Service) buildDeplopack(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	fail := func(err error) bool { e.result(false, "DeploPack: "+err.Error(), ""); return false }
	if spec.GetDeplopackProvider() == "" {
		return fail(fmt.Errorf("select a repository detection"))
	}
	for key, value := range spec.GetDeplopackEnvironment() {
		if !analysisEnvName.MatchString(key) || key == "DEPLOPACK_PROVIDER" || strings.ContainsRune(value, 0) {
			return fail(fmt.Errorf("invalid build configuration"))
		}
		if (key == "DEPLOPACK_CONFIG_FILE" || key == "DEPLOPACK_SHELL_SCRIPT") && !analysisRelative(value) {
			return fail(fmt.Errorf("configuration path escapes the project"))
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)
	builder, err := s.ensureBuilder(ctx, spec.GetDeplopackVersion())
	if err != nil {
		return fail(err)
	}
	builder, err = filepath.Abs(builder)
	if err != nil {
		return fail(err)
	}
	cacheKey := fmt.Sprintf("%x", sha256.Sum256([]byte(req.GetProjectId())))[:20]
	unlock, err := s.lockStackContext(ctx, "deplopack-cache/"+cacheKey)
	if err != nil {
		return fail(err)
	}
	defer unlock()
	jobKey := fmt.Sprintf("%x", sha256.Sum256([]byte(req.GetDeployId()+req.GetImageRef())))[:20]
	image := "deplopack-" + jobKey
	kit := "deplo-buildkit-" + jobKey
	volume := "deplo-deplopack-cache-" + cacheKey
	run := func(args ...string) error {
		result, err := dockercli.RunEnv(ctx, 2*time.Minute, dockerConfigEnv(req), args...)
		if err != nil {
			return err
		}
		if result.Code != 0 {
			return fmt.Errorf("Docker %s failed", args[0])
		}
		return nil
	}
	if req.GetNoBuildCache() {
		existing, err := dockercli.Run(ctx, 30*time.Second, "volume", "inspect", volume)
		if err != nil {
			return fail(err)
		}
		if existing.Code == 0 {
			if err := run("volume", "rm", volume); err != nil {
				return fail(err)
			}
		}
	}
	if err := run("volume", "create", "--label", "deplo.deplopack-cache="+req.GetProjectId(), volume); err != nil {
		return fail(err)
	}
	defer dockercli.ForceRemove(kit)
	if err := run("run", "-d", "--rm", "--privileged", "--name", kit, "-v", volume+":/var/lib/buildkit", "moby/buildkit:latest"); err != nil {
		return fail(err)
	}
	ready := false
	for attempt := 0; attempt < 30; attempt++ {
		probe, err := dockercli.Run(ctx, 10*time.Second, "exec", kit, "buildctl", "debug", "workers")
		if err == nil && probe.Code == 0 {
			ready = true
			break
		}
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	if !ready {
		return fail(fmt.Errorf("BuildKit did not become ready"))
	}
	absolute, err := filepath.Abs(buildDir)
	if err != nil {
		return fail(err)
	}
	if err := os.MkdirAll("/tmp/railpack/mise", 0700); err != nil {
		return fail(err)
	}
	workdir := "/build/" + image
	args := []string{"run", "--rm", "--mount", deplopackMount(absolute, workdir, false), "--mount", deplopackMount(builder, "/usr/local/bin/deplopack-builder", true), "-v", "/var/run/docker.sock:/var/run/docker.sock", "--mount", deplopackMount("/tmp/railpack/mise", "/tmp/railpack/mise", false), "-w", workdir, "-e", "BUILDKIT_HOST=docker-container://" + kit, "-e", "DEPLOPACK_PROVIDER=" + spec.GetDeplopackProvider(), "--entrypoint", "/usr/local/bin/deplopack-builder"}
	env := map[string]string{}
	for _, key := range dropReservedBuildEnv(buildEnvKeys(req.GetEnv())) {
		if !strings.HasPrefix(key, "DEPLOPACK_") && !strings.HasPrefix(key, "RAILPACK_") && key != "HOME" {
			env[key] = req.GetEnv()[key]
		}
	}
	for key, value := range spec.GetDeplopackEnvironment() {
		env[key] = value
	}
	env["PORT"] = fmt.Sprint(buildPort(spec))
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "-e", key)
	}
	if len(dockerConfigEnv(req)) > 0 {
		args = append(args, "--mount", deplopackMount(dockerConfigDir(req.GetDeployId()), "/docker-config", true), "-e", "DOCKER_CONFIG=/docker-config")
	}
	args = append(args, "docker:29-cli")
	e.log("info", "Building with DeploPack "+spec.GetDeplopackVersion()+" ("+spec.GetDeplopackProvider()+")")
	if spec.GetDeplopackPath() != "" {
		e.log("info", "Selected repository file: "+spec.GetDeplopackPath())
	}
	code, err := dockercli.StreamEnv(ctx, 20*time.Minute, func(line string) { e.log("info", line) }, append(envKV(env, keys), dockerConfigEnv(req)...), args...)
	defer func() { _, _ = dockercli.Run(context.Background(), 30*time.Second, "image", "rm", image) }()
	if err != nil {
		return fail(err)
	}
	if code != 0 {
		return fail(fmt.Errorf("builder failed (exit %d)", code))
	}
	if err := run("tag", image, req.GetImageRef()); err != nil {
		return fail(err)
	}
	return s.relabel(ctx, req, e)
}

// Docker's --mount argument is CSV; repository paths can contain commas and quotes.
func deplopackMount(source, destination string, readOnly bool) string {
	var output strings.Builder
	writer := csv.NewWriter(&output)
	fields := []string{"type=bind", "src=" + source, "dst=" + destination}
	if readOnly {
		fields = append(fields, "readonly")
	}
	_ = writer.Write(fields)
	writer.Flush()
	return strings.TrimSuffix(output.String(), "\n")
}
