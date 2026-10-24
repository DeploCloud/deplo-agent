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
	"google.golang.org/protobuf/proto"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

type emitter struct {
	send func(*pb.DeployEvent) error
	// What auto detection chose to build with, reported on every result after it.
	builtWith string
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
	e.finish(&pb.DeployResult{Ready: ready, Error: errMsg, CommitSha: commitSha})
}

// finish sends the deploy's result.
func (e *emitter) finish(r *pb.DeployResult) {
	r.BuiltWith = e.builtWith
	_ = e.send(&pb.DeployEvent{Event: &pb.DeployEvent_Result{Result: r}})
}

func (s *Service) runDeploy(ctx context.Context, req *pb.DeployRequest, e *emitter) {
	s.runDeployFrom(ctx, req, "", e)
}

// runDeployFrom deploys, reading an upload's build context from contextFile when set, else from the request.
func (s *Service) runDeployFrom(ctx context.Context, req *pb.DeployRequest, contextFile string, e *emitter) {
	started := time.Now()
	slug := req.GetSlug()
	if err := validateSlug(slug); err != nil {
		e.result(false, err.Error(), "")
		return
	}
	// Both reach the build argv and the relabel Dockerfile, where a "-" or a newline would be read as more.
	// A stack built from its source names its images in the compose file instead.
	if k := req.GetSourceKind(); (k == pb.SourceKind_SOURCE_KIND_UPLOAD || k == pb.SourceKind_SOURCE_KIND_GIT) &&
		req.GetBuildKind() != pb.BuildKind_BUILD_KIND_COMPOSE {
		if err := validateImageRef(req.GetImageRef()); err != nil {
			e.result(false, err.Error(), "")
			return
		}
	}
	if p := req.GetProjectId(); p != "" && !servicePattern.MatchString(p) {
		e.result(false, fmt.Sprintf("invalid project id %q", p), "")
		return
	}
	name := "deplo-" + slug
	stackFile := s.stackPath(slug)
	if k := req.GetSourceKind(); req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE &&
		k != pb.SourceKind_SOURCE_KIND_GIT && k != pb.SourceKind_SOURCE_KIND_UPLOAD {
		e.result(false, "a stack built from its source needs a git or upload source", "")
		return
	}

	if req.GetBuildOnly() {
		if isStack(req) {
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
	if !req.GetBuildOnly() && !readsCompose(req) {
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
			if err := pullImage(ctx, req, e); err != nil {
				e.result(false, err.Error(), "")
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
		if !s.fromSource(ctx, req, buildDir, "", e) {
			return
		}
	case pb.SourceKind_SOURCE_KIND_GIT:
		g := req.GetGit()
		if req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE && !readsCompose(req) {
			// The working copy holds the whole repository; the subdir is its project directory.
			g = proto.Clone(g).(*pb.GitSource)
			g.Subdir = ""
		}
		buildDir, sha, cleanup, err := s.materializeGit(ctx, g, slug, e)
		if err != nil {
			e.result(false, "git clone: "+err.Error(), "")
			return
		}
		defer cleanup()
		commitSha = sha
		if sha != "" {
			e.log("info", "Checked out "+shortSha(sha))
		}
		if !s.fromSource(ctx, req, buildDir, commitSha, e) {
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

	isCompose := isStack(req)

	e.phase(pb.DeployPhase_DEPLOY_PHASE_STARTING)
	if req.GetComposeYaml() == "" {
		e.result(false, "deploy request missing rendered compose", "")
		return
	}
	if err := s.checkFilesBinds(slug, req.GetFilesBindSources()); err != nil {
		e.result(false, err.Error(), "")
		return
	}
	var admitErr error
	if isCompose {
		// compose pull/build read the stack from disk; startStack writes it again under the lock.
		envFile, projectDir, ok := s.writeStackFiles(req, e)
		if !ok {
			return
		}
		base := composeBaseArgs(name, stackFile, envFile, projectDir)
		if req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE {
			if err := checkSourceStack(ctx, req, base, s.srcDir(slug)); err != nil {
				e.result(false, err.Error(), "")
				return
			}
			e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)
		}
		images, ok := s.prepareStackImages(ctx, req, base, e)
		if !ok {
			return
		}
		admitErr = admitImages(ctx, images, nil)
	} else {
		var pull func() error
		if req.GetSourceKind() == pb.SourceKind_SOURCE_KIND_IMAGE {
			pull = func() error { return pullImage(ctx, req, e) }
		}
		admitErr = admitImages(ctx, []string{imageRef}, pull)
	}
	if admitErr != nil {
		e.result(false, admitErr.Error(), "")
		return
	}
	if !s.startStack(ctx, req, e) {
		return
	}

	e.phase(pb.DeployPhase_DEPLOY_PHASE_WAITING)
	if req.GetReady() != nil {
		s.waitReady(ctx, req, isCompose, started, commitSha, e)
		return
	}
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

// startStack writes the admitted stack and brings it up under the stack lock, so a Reroute, Stop
// or Destroy of the same app cannot interleave with it. Builds stay outside the lock.
func (s *Service) startStack(ctx context.Context, req *pb.DeployRequest, e *emitter) bool {
	defer s.lockStack(req.GetSlug())()
	envFile, projectDir, ok := s.writeStackFiles(req, e)
	if !ok {
		return false
	}
	if req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE && !s.stripWorkingCopyLinks(req.GetSlug(), e) {
		return false
	}
	composeArgs := composeUpArgs("deplo-"+req.GetSlug(), s.stackPath(req.GetSlug()), envFile, projectDir,
		req.GetForceRecreate(), req.GetComposeUpArgs())
	upLog := "docker compose up -d"
	if req.GetForceRecreate() {
		upLog += " --force-recreate"
	}
	if extra := sanitizeComposeArgs(req.GetComposeUpArgs()); len(extra) > 0 {
		upLog += " " + strings.Join(extra, " ")
	}
	e.log("command", upLog)
	tail := &lineTail{max: 50}
	code, err := dockercli.StreamEnv(ctx, 15*time.Minute, func(l string) { tail.add(l); e.log("info", l) },
		dockerConfigEnv(req), composeArgs...)
	if err != nil {
		e.result(false, "compose up: "+err.Error(), "")
		return false
	}
	if code != 0 {
		msg := fmt.Sprintf("docker compose up failed (exit %d)", code)
		if hint := startHint(0, tail.lines); hint != "" {
			msg += ". " + hint
		}
		e.result(false, msg, "")
		return false
	}
	return true
}

func (s *Service) writeStackFiles(req *pb.DeployRequest, e *emitter) (envFile, projectDir string, ok bool) {
	stackFile := s.stackPath(req.GetSlug())
	if err := os.WriteFile(stackFile, []byte(req.GetComposeYaml()), 0o600); err != nil {
		e.result(false, "write stack file: "+err.Error(), "")
		return "", "", false
	}
	if err := os.Chmod(stackFile, 0o600); err != nil {
		e.result(false, "secure stack file: "+err.Error(), "")
		return "", "", false
	}
	if !isStack(req) {
		return "", "", true
	}
	envFile, projectDir, err := s.writeComposeEnv(req.GetSlug(), req.GetEnv())
	if err != nil {
		e.result(false, "write env file: "+err.Error(), "")
		return "", "", false
	}
	if req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE {
		projectDir, _ = s.sourceProjectDir(req)
	}
	return envFile, projectDir, true
}

// isStack: the deploy runs a compose stack, not one container.
func isStack(req *pb.DeployRequest) bool {
	return req.GetSourceKind() == pb.SourceKind_SOURCE_KIND_COMPOSE || req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE
}

// readsCompose: a compose deploy the control plane has not rendered yet only reads the source's file.
func readsCompose(req *pb.DeployRequest) bool {
	return req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE && req.GetComposeYaml() == ""
}

// fromSource takes the materialised source on: it answers with the compose file the control plane
// asked for, moves a stack's working copy to it, or builds the image. false: the result is sent.
func (s *Service) fromSource(ctx context.Context, req *pb.DeployRequest, dir, commitSha string, e *emitter) bool {
	switch {
	case readsCompose(req):
		redirectToCompose(req, dir, commitSha, e)
		return false
	case req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE:
		return s.updateWorkingCopy(req, dir, e)
	case req.GetBuildKind() == pb.BuildKind_BUILD_KIND_RAILPACK && req.GetBuildSpec().GetAutoMethod():
		return s.buildDetected(ctx, req, dir, commitSha, e)
	}
	return s.buildImage(ctx, req, dir, e)
}

func pullImage(ctx context.Context, req *pb.DeployRequest, e *emitter) error {
	e.log("command", "docker pull "+req.GetImageRef())
	code, err := dockercli.StreamEnv(ctx, 10*time.Minute, func(l string) { e.log("info", l) },
		dockerConfigEnv(req), "pull", req.GetImageRef())
	if err != nil {
		return fmt.Errorf("docker pull: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("docker pull failed (exit %d)", code)
	}
	return nil
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
		return s.buildDockerfile(ctx, req, req.GetDockerfile(), buildDir, e)
	case pb.BuildKind_BUILD_KIND_RAILPACK:
		return s.buildRailpack(ctx, req, buildDir, e)
	default:
		e.result(false, "unsupported build kind", "")
		return false
	}
}

func (s *Service) buildDockerfile(ctx context.Context, req *pb.DeployRequest, df *pb.DockerfileBuild, buildDir string, e *emitter) bool {
	labels := labelArgs(req)

	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	if df.GetGenerated() {
		dfPath := filepath.Join(buildDir, "Dockerfile")
		if _, err := os.Stat(dfPath); err != nil {
			if err := writeBuildFile(dfPath, []byte(df.GetGeneratedDockerfile())); err != nil {
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
	if dockercli.ImageExportOptsSupported(ctx) {
		extraEnv = append([]string{"DOCKER_BUILDKIT=1"}, extraEnv...)
	}
	return s.streamBuild(ctx, req, args, extraEnv, e, "")
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
