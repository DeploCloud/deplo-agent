package server

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

type emitter struct {
	send func(*pb.DeployEvent) error
}

func (e *emitter) log(level, text string) {
	_ = e.send(&pb.DeployEvent{Event: &pb.DeployEvent_Log{
		Log: &pb.LogLine{Level: level, Text: text},
	}})
}

func (e *emitter) phase(p pb.DeployPhase) {
	_ = e.send(&pb.DeployEvent{Event: &pb.DeployEvent_Phase{
		Phase: &pb.PhaseChange{Phase: p},
	}})
}

func (e *emitter) result(ready bool, errMsg, commitSha string) {
	_ = e.send(&pb.DeployEvent{Event: &pb.DeployEvent_Result{
		Result: &pb.DeployResult{Ready: ready, Error: errMsg, CommitSha: commitSha},
	}})
}

func (s *Service) runDeploy(ctx context.Context, req *pb.DeployRequest, e *emitter) {
	s.runDeployFrom(ctx, req, "", e)
}

// runDeployFrom deploys, reading an upload's build context from contextFile when set, else from the request.
func (s *Service) runDeployFrom(ctx context.Context, req *pb.DeployRequest, contextFile string, e *emitter) {
	slug := req.GetSlug()
	if err := validateSlug(slug); err != nil {
		e.result(false, err.Error(), "")
		return
	}
	name := "deplo-" + slug
	stackFile := s.stackPath(slug)

	if req.GetBuildOnly() {
		if req.GetSourceKind() == pb.SourceKind_SOURCE_KIND_COMPOSE {
			e.result(false, "build-only is not supported for a compose stack (no single image to move)", "")
			return
		}
		if req.GetBuildKind() == pb.BuildKind_BUILD_KIND_NONE ||
			req.GetBuildKind() == pb.BuildKind_BUILD_KIND_UNSPECIFIED {
			e.result(false, "build-only requires a build method (nothing would be built)", "")
			return
		}
	}

	if err := os.MkdirAll(s.stackDir, 0o755); err != nil {
		e.result(false, "create stack dir: "+err.Error(), "")
		return
	}
	if !req.GetBuildOnly() {
		warning, err := ensureTenantNetworkWarned(ctx, req.GetNetwork())
		if err != nil {
			e.result(false, "ensure network: "+err.Error(), "")
			return
		}
		if warning != "" {
			e.log("warn", warning)
		}
	}
	dropAuth, err := writeDockerConfig(req)
	if err != nil {
		e.result(false, "write registry credentials: "+err.Error(), "")
		return
	}
	defer dropAuth()

	imageRef := req.GetImageRef()
	commitSha := ""

	e.phase(pb.DeployPhase_DEPLOY_PHASE_PREPARING)
	switch req.GetSourceKind() {
	case pb.SourceKind_SOURCE_KIND_IMAGE:
		if req.GetPullImage() {
			e.log("command", "docker pull "+imageRef)
			code, err := dockercli.StreamEnv(ctx, 10*time.Minute, func(l string) { e.log("info", l) },
				dockerConfigEnv(req), "pull", imageRef)
			if err != nil {
				e.result(false, "docker pull: "+err.Error(), "")
				return
			}
			if code != 0 {
				e.result(false, fmt.Sprintf("docker pull failed (exit %d)", code), "")
				return
			}
		}
	case pb.SourceKind_SOURCE_KIND_UPLOAD:
		var src io.Reader = bytes.NewReader(req.GetContextTar())
		if contextFile != "" {
			f, err := os.Open(contextFile)
			if err != nil {
				e.result(false, "materialise context: "+err.Error(), "")
				return
			}
			defer f.Close()
			src = f
		}
		buildDir, cleanup, err := s.materializeUploadFrom(src, slug)
		if err != nil {
			e.result(false, "materialise context: "+err.Error(), "")
			return
		}
		defer cleanup()
		if !s.buildImage(ctx, req, buildDir, e) {
			return
		}
	case pb.SourceKind_SOURCE_KIND_GIT:
		buildDir, sha, cleanup, err := s.materializeGit(ctx, req.GetGit(), slug, e)
		if err != nil {
			e.result(false, "git clone: "+err.Error(), "")
			return
		}
		defer cleanup()
		commitSha = sha
		if sha != "" {
			e.log("info", "Checked out "+shortSha(sha))
		}
		if !s.buildImage(ctx, req, buildDir, e) {
			return
		}
	case pb.SourceKind_SOURCE_KIND_COMPOSE:
		if err := s.writeMountFiles(slug, req.GetMounts(), e); err != nil {
			e.result(false, "write mount files: "+err.Error(), "")
			return
		}
	default:
		e.result(false, "unknown source kind", "")
		return
	}

	if req.GetBuildOnly() {
		e.log("info", "Built "+imageRef+" (build server: nothing is started here)")
		e.result(true, "", commitSha)
		return
	}

	isCompose := req.GetSourceKind() == pb.SourceKind_SOURCE_KIND_COMPOSE

	e.phase(pb.DeployPhase_DEPLOY_PHASE_STARTING)
	if req.GetComposeYaml() == "" {
		e.result(false, "deploy request missing rendered compose", "")
		return
	}
	if err := os.WriteFile(stackFile, []byte(req.GetComposeYaml()), 0o600); err != nil {
		e.result(false, "write stack file: "+err.Error(), "")
		return
	}
	if err := os.Chmod(stackFile, 0o600); err != nil {
		e.result(false, "secure stack file: "+err.Error(), "")
		return
	}

	envFile := ""
	projectDir := ""
	if isCompose {
		var err error
		if envFile, projectDir, err = s.writeComposeEnv(slug, req.GetEnv()); err != nil {
			e.result(false, "write env file: "+err.Error(), "")
			return
		}
	}
	if isCompose {
		images, ok := s.prepareStackImages(ctx, req, composeBaseArgs(name, stackFile, envFile, projectDir), e)
		if !ok || !refuseTraefikImageLabels(ctx, images, e) {
			return
		}
	} else if !refuseTraefikImageLabels(ctx, []string{imageRef}, e) {
		return
	}
	composeArgs := composeUpArgs(name, stackFile, envFile, projectDir, req.GetForceRecreate(), req.GetComposeUpArgs())
	upLog := "docker compose up -d"
	if req.GetForceRecreate() {
		upLog += " --force-recreate"
	}
	if extra := sanitizeComposeArgs(req.GetComposeUpArgs()); len(extra) > 0 {
		upLog += " " + strings.Join(extra, " ")
	}
	e.log("command", upLog)
	code, err := dockercli.StreamEnv(ctx, 15*time.Minute, func(l string) { e.log("info", l) },
		dockerConfigEnv(req), composeArgs...)
	if err != nil {
		e.result(false, "compose up: "+err.Error(), "")
		return
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("docker compose up failed (exit %d)", code), "")
		return
	}

	e.phase(pb.DeployPhase_DEPLOY_PHASE_WAITING)
	timeout := time.Duration(req.GetReadyTimeoutMs()) * time.Millisecond
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if isCompose {
		e.log("info", "Waiting for the stack to become healthy…")
		if waitStackRunning(ctx, slug, timeout) {
			e.result(true, "", commitSha)
			return
		}
		e.result(false, "Stack did not reach a running state", commitSha)
		return
	}
	e.log("info", "Waiting for the container to become healthy…")
	if waitRunning(ctx, name, timeout) {
		e.result(true, "", commitSha)
		return
	}
	e.result(false, "Container did not reach a running state", commitSha)
}

func renderEnvFile(env map[string]string) string {
	return sortedEnvLines(env, func(v string) string {
		v = strings.ReplaceAll(v, "\r\n", " ")
		return strings.ReplaceAll(v, "\n", " ")
	})
}

func renderComposeEnvFile(env map[string]string) string {
	return sortedEnvLines(env, func(v string) string {
		return `"` + dotenvEscape.Replace(v) + `"`
	})
}

var dotenvEscape = strings.NewReplacer(
	`\`, `\\`,
	`"`, `\"`,
	`$`, `\$`,
	"\n", `\n`,
	"\r", `\r`,
)

func sortedEnvLines(env map[string]string, encode func(string) string) string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(encode(env[k]))
		b.WriteByte('\n')
	}
	return b.String()
}

func (s *Service) writeMountFiles(slug string, mounts []*pb.MountFile, e *emitter) error {
	if len(mounts) == 0 {
		return nil
	}
	for _, m := range mounts {
		if _, err := s.writeBytes(slug, m.GetPath(), []byte(m.GetContent())); err != nil {
			if status.Code(err) == codes.InvalidArgument {
				e.log("warn", "Skipping unsafe mount path: "+m.GetPath()+" ("+status.Convert(err).Message()+")")
				continue
			}
			return err
		}
	}
	return nil
}

func waitStackRunning(ctx context.Context, slug string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dockercli.StackRunning(ctx, slug) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
	return false
}

func shortSha(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

func (s *Service) buildImage(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	switch req.GetBuildKind() {
	case pb.BuildKind_BUILD_KIND_DOCKERFILE:
		return s.buildDockerfile(ctx, req, buildDir, e)
	case pb.BuildKind_BUILD_KIND_STATIC:
		return s.buildStatic(ctx, req, buildDir, e)
	case pb.BuildKind_BUILD_KIND_NIXPACKS:
		return s.buildNixpacks(ctx, req, buildDir, e)
	case pb.BuildKind_BUILD_KIND_BUILDPACKS:
		return s.buildBuildpacks(ctx, req, buildDir, e)
	case pb.BuildKind_BUILD_KIND_RAILPACK:
		return s.buildRailpack(ctx, req, buildDir, e)
	default:
		e.result(false, "unsupported build kind", "")
		return false
	}
}

func (s *Service) buildDockerfile(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	df := req.GetDockerfile()
	labels := labelArgs(req)

	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	if df.GetGenerated() {
		dfPath := filepath.Join(buildDir, "Dockerfile")
		if _, err := os.Stat(dfPath); err != nil {
			if err := os.WriteFile(dfPath, []byte(df.GetGeneratedDockerfile()), 0o644); err != nil {
				e.result(false, "write generated Dockerfile: "+err.Error(), "")
				return false
			}
			e.log("info", "No Dockerfile found - using one generated from build settings")
		}
		envKeys := dockerfileBuildEnv(dfPath, req)
		args := appendBuildArgKeys(s.buildArgv(req), envKeys)
		args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
		args = append(args, labels...)
		args = append(args, buildDir)
		return s.runBuild(ctx, req, args, envKV(req.GetEnv(), envKeys), e)
	}

	dockerfilePath, ok := safepath.Join(buildDir, orDefault(df.GetDockerfilePath(), "Dockerfile"))
	if !ok {
		e.result(false, "dockerfile path escapes the build context", "")
		return false
	}
	contextDir, ok := safepath.Join(buildDir, orDefault(df.GetContextPath(), "."))
	if !ok {
		e.result(false, "build context path escapes the build context", "")
		return false
	}
	if cd, err := safepath.Inside(buildDir, contextDir); err == nil {
		contextDir = cd
	}
	if li, lerr := os.Lstat(dockerfilePath); lerr == nil && li.Mode()&os.ModeSymlink != 0 {
		e.result(false, "the Dockerfile path must not be a symlink", "")
		return false
	}
	if dp, ierr := safepath.Inside(buildDir, dockerfilePath); ierr == nil {
		dockerfilePath = dp
	}
	if _, err := os.Stat(dockerfilePath); err != nil {
		e.result(false, fmt.Sprintf("No Dockerfile at %q in the build context", df.GetDockerfilePath()), "")
		return false
	}

	args := s.buildArgv(req, "-f", dockerfilePath)
	if stage := strings.TrimSpace(df.GetTargetStage()); stage != "" {
		args = append(args, "--target", stage)
	}
	envKeys := dockerfileBuildEnv(dockerfilePath, req)
	args = appendBuildArgKeys(args, envKeys)
	args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
	args = append(args, labels...)
	args = append(args, contextDir)
	return s.runBuild(ctx, req, args, envKV(req.GetEnv(), envKeys), e)
}

func dockerfileBuildEnv(dockerfilePath string, req *pb.DeployRequest) []string {
	body, err := os.ReadFile(dockerfilePath)
	if err != nil {
		return nil
	}
	return dockerfileEnvKeys(string(body), req.GetEnv())
}

func (s *Service) writeComposeEnv(slug string, env map[string]string) (string, string, error) {
	projectDir := filepath.Join(s.stackDir, "files", slug)
	if err := os.MkdirAll(projectDir, 0o700); err != nil {
		return "", "", err
	}
	envFile := filepath.Join(projectDir, ".env")
	if li, err := os.Lstat(envFile); err == nil && li.Mode()&os.ModeSymlink != 0 {
		return "", "", fmt.Errorf("refusing to write %s through a symlink", envFile)
	}
	f, err := os.OpenFile(envFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return "", "", err
	}
	_ = f.Chmod(0o600)
	if _, err := f.Write([]byte(renderComposeEnvFile(env))); err != nil {
		f.Close()
		return "", "", err
	}
	if err := f.Close(); err != nil {
		return "", "", err
	}
	_ = os.Remove(s.legacyEnvPath(slug))
	return envFile, projectDir, nil
}

func composeUpArgs(project, stackFile, envFile, projectDir string, forceRecreate bool, extra []string) []string {
	args := []string{"compose", "-p", project, "-f", stackFile}
	if projectDir != "" {
		args = append(args, "--project-directory", projectDir)
	}
	if envFile != "" {
		args = append(args, "--env-file", envFile)
	}
	args = append(args, "up", "-d", "--remove-orphans")
	if forceRecreate {
		args = append(args, "--force-recreate")
	}
	return append(args, sanitizeComposeArgs(extra)...)
}

const composeArgAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:/=,+@-"

var composeArgDenied = map[string]bool{
	"-p":                  true,
	"--project-name":      true,
	"-f":                  true,
	"--file":              true,
	"--env-file":          true,
	"--project-directory": true,
}

func sanitizeComposeArgs(extra []string) []string {
	const maxArgs, maxLen = 24, 128
	if len(extra) == 0 || len(extra) > maxArgs {
		return nil
	}
	for _, a := range extra {
		if a == "" || len(a) > maxLen {
			return nil
		}
		for _, r := range a {
			if !strings.ContainsRune(composeArgAlphabet, r) {
				return nil
			}
		}
		name := a
		if i := strings.IndexByte(a, '='); i >= 0 {
			name = a[:i]
		}
		if composeArgDenied[name] {
			return nil
		}
	}
	return extra
}

func (s *Service) buildArgv(req *pb.DeployRequest, rest ...string) []string {
	args := []string{"build"}
	if req.GetNoBuildCache() {
		args = append(args, "--no-cache")
	}
	args = append(args, "--build-arg", "BUILDKIT_CACHE_MOUNT_NS="+s.cacheNamespace(req.GetSlug()))
	return append(args, rest...)
}

func (s *Service) runBuild(ctx context.Context, req *pb.DeployRequest, args []string, extraEnv []string, e *emitter) bool {
	extraEnv = append(extraEnv, dockerConfigEnv(req)...)
	if dockercli.ImageExportOptsSupported(ctx) {
		extraEnv = append([]string{"DOCKER_BUILDKIT=1"}, extraEnv...)
	}
	e.log("command", "docker "+strings.Join(args, " "))
	code, err := dockercli.StreamEnv(ctx, 15*time.Minute, func(l string) { e.log("info", l) }, extraEnv, args...)
	if err != nil {
		e.result(false, "docker build: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("docker build failed (exit %d)", code), "")
		return false
	}
	return true
}

func (s *Service) materializeUpload(tarBytes []byte, slug string) (string, func(), error) {
	return s.materializeUploadFrom(bytes.NewReader(tarBytes), slug)
}

func (s *Service) materializeUploadFrom(src io.Reader, slug string) (string, func(), error) {
	dir, err := os.MkdirTemp(s.buildTmpDir, "deplo-build-"+slug+"-")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }

	tr := tar.NewReader(src)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			cleanup()
			return "", func() {}, fmt.Errorf("read tar: %w", err)
		}
		if hdr.Typeflag == tar.TypeSymlink || hdr.Typeflag == tar.TypeLink {
			cleanup()
			return "", func() {}, fmt.Errorf("archive contains a link entry (%s), which is not allowed", hdr.Name)
		}
		clean := filepath.Clean("/" + hdr.Name)
		target := filepath.Join(dir, clean)
		if target != dir && !strings.HasPrefix(target, dir+string(os.PathSeparator)) {
			cleanup()
			return "", func() {}, fmt.Errorf("archive entry %q escapes the build dir", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				cleanup()
				return "", func() {}, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				cleanup()
				return "", func() {}, err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				cleanup()
				return "", func() {}, err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				cleanup()
				return "", func() {}, err
			}
			f.Close()
		}
	}
	return dir, cleanup, nil
}

func waitRunning(ctx context.Context, name string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dockercli.IsRunning(ctx, name) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
	return false
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
