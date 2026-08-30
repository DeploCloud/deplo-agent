package server

// https://deplo.build/docs/concepts/what-happens-on-a-deploy

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
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

// emitter funnels DeployEvents back over the gRPC stream. A small indirection so
// the deploy steps below don't each carry the grpc stream type.
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

// runDeploy is the agent-side counterpart of the control plane's runDeployment exec
// body (lib/deploy/build.ts).
func (s *Service) runDeploy(ctx context.Context, req *pb.DeployRequest, e *emitter) {
	slug := req.GetSlug()
	// The slug is written into host paths (stack file, env file, files dir) and
	// docker project names, so it must be a safe token before ANY of that - a
	// `../…` slug is otherwise an arbitrary root-owned write on the shared host.
	if err := validateSlug(slug); err != nil {
		e.result(false, err.Error(), "")
		return
	}
	name := "deplo-" + slug
	stackFile := filepath.Join(s.stackDir, slug+".yml")

	// A BUILD-ONLY deploy compiles for a host it is not: it must actually build something,
	// and that something must be one image the caller can then stream away.
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
	// A BUILD-ONLY deploy compiles for a machine it is not: it writes no stack and
	// brings nothing up here, so it needs no network. Creating one anyway left a
	// build server holding an empty network per Environment it ever built for -
	// permanently, because the live-network list is by NAME and instance-wide, so
	// the copy on the machine that runs the app keeps this one alive too.
	if !req.GetBuildOnly() {
		if err := ensureTenantNetwork(ctx, req.GetNetwork()); err != nil {
			e.result(false, "ensure network: "+err.Error(), "")
			return
		}
	}
	// The team's registry credentials, for every pull below (image ref, compose
	// images, a Dockerfile's base image). No-op when none were sent.
	dropAuth, err := writeDockerConfig(req)
	if err != nil {
		e.result(false, "write registry credentials: "+err.Error(), "")
		return
	}
	defer dropAuth()

	imageRef := req.GetImageRef()
	// commitSha is reported in the terminal result for a GIT source (the agent
	// resolves it after cloning); empty for UPLOAD/IMAGE (the control plane
	// already knows the sha, or there is none).
	commitSha := ""

	// --- Phase: prepare the image (build from context, or pull/run as-is). ---
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
		buildDir, cleanup, err := s.materializeUpload(req.GetContextTar(), slug)
		if err != nil {
			e.result(false, "materialise context: "+err.Error(), "")
			return
		}
		defer cleanup()
		if !s.buildImage(ctx, req, buildDir, e) {
			return // buildImage already emitted the failure result
		}
	case pb.SourceKind_SOURCE_KIND_GIT:
		// Part B (D3): the agent clones the repo ITSELF with a short-lived token,
		// resolves the commit sha, then builds exactly like the UPLOAD path.
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
			return // buildImage already emitted the failure result
		}
	case pb.SourceKind_SOURCE_KIND_DEV_WORKSPACE:
		// Part D: "deploy from dev workspace". The build context is the developer's live tree
		// already on THIS host (<dataBase>/dev/<slug>). No bytes cross the wire - the build
		// stays on the owning host.
		buildDir, cleanup, err := s.materializeDevWorkspace(slug, req.GetDevWorkspaceSubdir(), e)
		if err != nil {
			e.result(false, err.Error(), "")
			return
		}
		defer cleanup()
		if !s.buildImage(ctx, req, buildDir, e) {
			return // buildImage already emitted the failure result
		}
	case pb.SourceKind_SOURCE_KIND_COMPOSE:
		// Part C: a multi-service compose stack.
		if err := s.writeMountFiles(slug, req.GetMounts(), e); err != nil {
			e.result(false, "write mount files: "+err.Error(), "")
			return
		}
	default:
		e.result(false, "unknown source kind", "")
		return
	}

	// A BUILD SERVER stops here. The image is built and tagged; nothing is written to the
	// stack dir and no container of this app runs on this host. `ready` means the image
	// exists, the only claim this host can make.
	if req.GetBuildOnly() {
		e.log("info", "Built "+imageRef+" (build server: nothing is started here)")
		e.result(true, "", commitSha)
		return
	}

	// A multi-service compose stack interpolates `${VAR}` from a --env-file (the control
	// plane did NOT bake env into its YAML, unlike the single-image path), and its
	// containers are compose-prefixed (deplo-<slug>-<service>-N) rather than named
	// deplo-<slug>, so the readiness wait is by label, not by name.
	isCompose := req.GetSourceKind() == pb.SourceKind_SOURCE_KIND_COMPOSE

	// --- Phase: write the rendered stack and bring it up. ---
	e.phase(pb.DeployPhase_DEPLOY_PHASE_STARTING)
	if req.GetComposeYaml() == "" {
		e.result(false, "deploy request missing rendered compose", "")
		return
	}
	// 0600, and an explicit Chmod after it, because this file can hold SECRETS.
	if err := os.WriteFile(stackFile, []byte(req.GetComposeYaml()), 0o600); err != nil {
		e.result(false, "write stack file: "+err.Error(), "")
		return
	}
	if err := os.Chmod(stackFile, 0o600); err != nil {
		e.result(false, "secure stack file: "+err.Error(), "")
		return
	}

	// The single-image stack already bakes env into its `environment:` map (the control
	// plane rendered it that way), so no --env-file is needed there.
	envFile := ""
	projectDir := ""
	if isCompose {
		var err error
		if envFile, projectDir, err = s.writeComposeEnv(slug, req.GetEnv()); err != nil {
			e.result(false, "write env file: "+err.Error(), "")
			return
		}
	}
	composeArgs := composeUpArgs(name, stackFile, envFile, projectDir, req.GetForceRecreate(), req.GetComposeUpArgs())
	upLog := "docker compose up -d"
	if req.GetForceRecreate() {
		upLog += " --force-recreate"
	}
	// Echo the operator's own flags into the build log - that is also how they
	// find out a set was rejected and dropped (it simply isn't here).
	if extra := sanitizeComposeArgs(req.GetComposeUpArgs()); len(extra) > 0 {
		upLog += " " + strings.Join(extra, " ")
	}
	e.log("command", upLog)
	// A multi-service compose stack pulls SEVERAL images here (and an IMAGE deploy with
	// pullImage=false pulls at up time too).
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

	// --- Phase: wait for the stack to report running. ---
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

// renderEnvFile renders a decrypted env map as KEY=VALUE lines for a compose
// --env-file. Mirrors the control plane's renderEnvFile (build.ts): values are literal
// and any newline (which would break the env-file format) collapses to a space.
func renderEnvFile(env map[string]string) string {
	return sortedEnvLines(env, func(v string) string {
		v = strings.ReplaceAll(v, "\r\n", " ")
		return strings.ReplaceAll(v, "\n", " ")
	})
}

// renderComposeEnvFile writes what `docker compose --env-file` reads. Every value is
// DOUBLE-QUOTED and escaped, because compose's dotenv EXPANDS a bare `$VAR` (a
// password came out holding this host's own environment instead) and cannot carry a
// newline at all (a PEM key arrived as one line of spaces).
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

// sortedEnvLines writes one `KEY=<encode(value)>` line per key, keys sorted so the
// file is deterministic.
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

// writeMountFiles materialises a compose stack's template config files under
// <stackDir>/files/<slug>/.
func (s *Service) writeMountFiles(slug string, mounts []*pb.MountFile, e *emitter) error {
	if len(mounts) == 0 {
		return nil
	}
	filesDir := filepath.Join(s.stackDir, "files", slug)
	for _, m := range mounts {
		// safepath.Join strips a leading "./"/"/", rejects any ".." segment, and returns the
		// bare filesDir for an empty/"." path - all three of which are "no file to write
		// here", so skip them rather than write outside or onto the dir itself.
		target, ok := safepath.Join(filesDir, m.GetPath())
		if !ok || target == filesDir {
			e.log("warn", "Skipping unsafe mount path: "+m.GetPath())
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, []byte(m.GetContent()), 0o644); err != nil {
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

// buildImage builds req.image_ref from buildDir using the request's BuildKind. Returns
// false (after emitting a failure result) on any error.
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

// buildDockerfile builds req.image_ref from a Dockerfile in buildDir. Mirrors
// builders.ts' buildFromDockerfile / buildGenerated for the Dockerfile family -
// the most common path.
func (s *Service) buildDockerfile(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	df := req.GetDockerfile()
	labels := labelArgs(req)

	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	// Generated Dockerfile: the control plane rendered the body (single source of
	// truth for framework presets); write it into the context, then build it.
	if df.GetGenerated() {
		dfPath := filepath.Join(buildDir, "Dockerfile")
		if _, err := os.Stat(dfPath); err != nil {
			if err := os.WriteFile(dfPath, []byte(df.GetGeneratedDockerfile()), 0o644); err != nil {
				e.result(false, "write generated Dockerfile: "+err.Error(), "")
				return false
			}
			e.log("info", "No Dockerfile found - using one generated from build settings")
		}
		// Build-time env (build_env.go): forward every env var the Dockerfile declares as an
		// ARG.
		envKeys := dockerfileBuildEnv(dfPath, req)
		args := appendBuildArgKeys(buildArgv(req), envKeys)
		args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
		args = append(args, labels...)
		args = append(args, buildDir)
		return s.runBuild(ctx, req, args, envKV(req.GetEnv(), envKeys), e)
	}

	// Explicit Dockerfile path + context, each re-validated to stay inside the
	// context tree (path arrived off the wire, never trusted).
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
	// realpath guard now that the parent exists.
	if cd, err := safepath.Inside(buildDir, contextDir); err == nil {
		contextDir = cd
	}
	// safepath.Join is lexical only; the Dockerfile lives in a user-controlled git repo,
	// so its path could be a SYMLINK pointing at any host file, making `docker build -f`
	// read (and bake into an image the user pulls) e.g. /root/.ssh/id_rsa.
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

	args := buildArgv(req, "-f", dockerfilePath)
	if stage := strings.TrimSpace(df.GetTargetStage()); stage != "" {
		args = append(args, "--target", stage)
	}
	// Build-time env: an explicit Dockerfile opts into a variable by declaring
	// `ARG NAME` - only declared names are forwarded, so builds stay warning-free.
	envKeys := dockerfileBuildEnv(dockerfilePath, req)
	args = appendBuildArgKeys(args, envKeys)
	args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
	args = append(args, labels...)
	args = append(args, contextDir)
	return s.runBuild(ctx, req, args, envKV(req.GetEnv(), envKeys), e)
}

// dockerfileBuildEnv reads the Dockerfile about to build and returns the env
// keys it declares as ARGs (build_env.go). Unreadable file ⇒ no keys - the
// build itself will surface the real error.
func dockerfileBuildEnv(dockerfilePath string, req *pb.DeployRequest) []string {
	body, err := os.ReadFile(dockerfilePath)
	if err != nil {
		return nil
	}
	return dockerfileEnvKeys(string(body), req.GetEnv())
}

// writeComposeEnv writes a compose stack's 0600 env-file and returns it together with
// the project directory to run the stack from.
func (s *Service) writeComposeEnv(slug string, env map[string]string) (string, string, error) {
	projectDir := filepath.Join(s.stackDir, "files", slug)
	if err := os.MkdirAll(projectDir, 0o700); err != nil {
		return "", "", err
	}
	envFile := filepath.Join(projectDir, ".env")
	if err := os.WriteFile(envFile, []byte(renderComposeEnvFile(env)), 0o600); err != nil {
		return "", "", err
	}
	// The pre-project-directory location. Left behind it would go on being the
	// file a `--env-file` never points at again, holding decrypted secrets.
	_ = os.Remove(filepath.Join(s.stackDir, slug+".env"))
	return envFile, projectDir, nil
}

// composeUpArgs assembles the `docker compose … up` argv that brings a stack up.
// envFile is "" for the single-image path (the control plane baked env into the
// rendered `environment:` map) and a written 0600 file for a compose stack (whose YAML
// interpolates `${VAR}`). projectDir is the stack's OWN directory, passed for a compose
// stack so every relative path the author wrote resolves inside it.
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

// The `docker compose` flags that decide WHICH stack is being brought up.
const composeArgAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:/=,+@-"

var composeArgDenied = map[string]bool{
	"-p":                  true,
	"--project-name":      true,
	"-f":                  true,
	"--file":              true,
	"--env-file":          true,
	"--project-directory": true,
}

// sanitizeComposeArgs vets the operator's extra `compose up` flags before they reach
// the argv. A silently half-applied set is worse than none, and the default bring-up
// still runs.
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
			// An ALLOWLIST, not a ban list: every real `compose up` flag and value is made of
			// these (`--pull`, `always`, `web=3`, `--timeout=60`, `--exit-code-from=web`), and
			// anything else - a space, a quote, `;`, `&`, `|`, `$`, a backtick, a control
			// character - is either a token the control plane failed to split or someone
			// expecting a shell.
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

// buildArgv starts a `docker build` argv, adding --no-cache when the deploy asked to
// skip the build cache (the app's Build cache setting is off, or this is the one build
// that follows a manual "Clear build cache").
func buildArgv(req *pb.DeployRequest, rest ...string) []string {
	args := []string{"build"}
	if req.GetNoBuildCache() {
		args = append(args, "--no-cache")
	}
	return append(args, rest...)
}

// runBuild streams a `docker build`; extraEnv carries build-env VALUES (bare
// `--build-arg KEY` flags in args resolve from it) and may be nil.
func (s *Service) runBuild(ctx context.Context, req *pb.DeployRequest, args []string, extraEnv []string, e *emitter) bool {
	extraEnv = append(extraEnv, dockerConfigEnv(req)...)
	// A containerd image store can only be built into by BuildKit, and the `--output
	// type=image,…` flag imageOutputArgs adds on those hosts is a BuildKit-only flag, so
	// force BuildKit on rather than trust whatever DOCKER_BUILDKIT the agent's unit
	// happened to inherit.
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

// materializeUpload extracts a tar archive (the streamed build context) into a fresh
// temp dir, rejecting any entry that would escape it (absolute paths, "..", and
// symlinks - same threat model as the control plane's extractArchive).
func (s *Service) materializeUpload(tarBytes []byte, slug string) (string, func(), error) {
	dir, err := os.MkdirTemp(s.buildTmpDir, "deplo-build-"+slug+"-")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }

	tr := tar.NewReader(bytes.NewReader(tarBytes))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			cleanup()
			return "", func() {}, fmt.Errorf("read tar: %w", err)
		}
		// Reject symlinks/hardlinks outright - they are the escape vector.
		if hdr.Typeflag == tar.TypeSymlink || hdr.Typeflag == tar.TypeLink {
			cleanup()
			return "", func() {}, fmt.Errorf("archive contains a link entry (%s), which is not allowed", hdr.Name)
		}
		clean := filepath.Clean("/" + hdr.Name) // anchor, strips any leading ..
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
