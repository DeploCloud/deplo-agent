package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// This file ports the control plane's old in-process heavy builders
// (lib/deploy/builders.ts) to the agent: static (nginx), nixpacks, Cloud Native
// Buildpacks (heroku/paketo) and railpack. Each builds req.image_ref from a
// materialised buildDir and ends with the image present in the local Docker store
// carrying the three deplo.* labels, listening on BuildSpec.port — byte-identical
// to the old local path so a project deploys the same image wherever it runs.
//
// The agent runs ON the bare host (ADR-0006), so — unlike the control plane, which
// ran inside a container and had to stage the build dir onto a host-visible volume
// to bind-mount it (builders.ts stageOnHostVolume / dataVolumeHostMountpoint) — the
// builders here bind-mount buildDir DIRECTLY. The whole host-mountpoint dance is
// dropped: the agent's own filesystem IS the host's.

// labelArgs is the three image labels every build method stamps, as repeated
// `--label` argv (mirrors builders.ts labelArgs).
func labelArgs(req *pb.DeployRequest) []string {
	return []string{
		"--label", "deplo.managed=true",
		"--label", "deplo.project=" + req.GetProjectId(),
		"--label", "deplo.slug=" + req.GetSlug(),
	}
}

// buildPort returns the container port a heavy build targets, defaulting to 80
// (nginx) when the spec leaves it 0 — mirrors `build.port || 80` in buildStatic.
func buildPort(spec *pb.BuildSpec) int32 {
	if p := spec.GetPort(); p > 0 {
		return p
	}
	return 80
}

// nginxConf renders the nginx server block the static + nixpacks-static paths
// write, listening on `port` with an SPA fallback when requested. Mirrors the
// conf string in builders.ts buildStatic / nginxWrap.
func nginxConf(port int32, spa bool) string {
	tryFiles := "try_files $uri $uri/ =404;"
	if spa {
		tryFiles = "try_files $uri /index.html;"
	}
	return fmt.Sprintf(`server {
  listen       %d;
  server_name  _;
  root   /usr/share/nginx/html;
  index  index.html;
  gzip on;
  gzip_types text/plain text/css application/javascript application/json image/svg+xml;
  location / {
    %s
  }
}
`, port, tryFiles)
}

// relabel re-stamps the three deplo labels onto an already-built image via a
// metadata-only `docker build` fed through stdin (`docker build -`). Used after
// builders (pack, railpack) that do not apply our labels themselves. Mirrors
// builders.ts relabel — piping real bytes to docker's stdin avoids both the
// /dev/null-context BuildKit error and the literal-\n FROM-parse error.
func (s *Service) relabel(ctx context.Context, req *pb.DeployRequest, e *emitter) bool {
	dockerfile := fmt.Sprintf(
		"FROM %s\nLABEL deplo.managed=true deplo.project=%s deplo.slug=%s\n",
		req.GetImageRef(), req.GetProjectId(), req.GetSlug(),
	)
	e.log("command", "docker build (relabel "+req.GetImageRef()+")")
	code, err := dockercli.Stream(ctx, 60*time.Second, func(l string) { e.log("info", l) },
		dockerfile, "build", "-t", req.GetImageRef(), "-")
	if err != nil {
		e.result(false, "relabel build: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("relabel build failed (exit %d)", code), "")
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// static (nginx) — ports builders.ts buildStatic
// ---------------------------------------------------------------------------

// buildStatic serves a static build output with nginx. With a build command it is
// a two-stage build (Node builder → nginx); without one the already-static output
// dir is copied straight into nginx. Mirrors builders.ts buildStatic exactly.
func (s *Service) buildStatic(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	e.log("info", "Building with Static (nginx)")
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	port := buildPort(spec)
	// Strip only a leading "./" or "/"; "." stays "." (mirrors builders.ts).
	outputDir := strings.TrimPrefix(strings.TrimPrefix(spec.GetOutputDirectory(), "./"), "/")
	if outputDir == "" {
		outputDir = "."
	}
	spa := spec.GetStaticSinglePageApp()

	if err := os.WriteFile(filepath.Join(buildDir, "deplo-nginx.conf"),
		[]byte(nginxConf(port, spa)), 0o644); err != nil {
		e.result(false, "write nginx conf: "+err.Error(), "")
		return false
	}

	buildCmd := strings.TrimSpace(spec.GetBuildCommand())
	var dockerfile string
	if buildCmd != "" {
		// Two-stage: install + build with Node, then serve the output with nginx.
		// The builder stage is Node-based, so only honour runtime_version for Node.
		node := "20"
		if spec.GetRuntimeLanguage() == "node" {
			node = majorVersion(spec.GetRuntimeVersion(), "20")
		}
		install := strings.TrimSpace(spec.GetInstallCommand())
		if install == "" {
			install = "npm ci"
		}
		dockerfile = fmt.Sprintf(`FROM node:%s-alpine AS builder
WORKDIR /app
COPY . .
RUN %s
RUN %s
FROM nginx:alpine
RUN rm -f /etc/nginx/conf.d/default.conf
COPY deplo-nginx.conf /etc/nginx/conf.d/deplo.conf
COPY --from=builder /app/%s/ /usr/share/nginx/html/
EXPOSE %d
CMD ["nginx", "-g", "daemon off;"]
`, node, install, buildCmd, outputDir, port)
	} else {
		// Already-static: copy the output dir straight into nginx.
		dockerfile = fmt.Sprintf(`FROM nginx:alpine
RUN rm -f /etc/nginx/conf.d/default.conf
COPY deplo-nginx.conf /etc/nginx/conf.d/deplo.conf
COPY %s/ /usr/share/nginx/html/
EXPOSE %d
CMD ["nginx", "-g", "daemon off;"]
`, outputDir, port)
	}

	if err := os.WriteFile(filepath.Join(buildDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		e.result(false, "write Dockerfile: "+err.Error(), "")
		return false
	}

	args := append([]string{"build", "-t", req.GetImageRef()}, labelArgs(req)...)
	args = append(args, buildDir)
	return s.runBuild(ctx, args, e)
}

// majorVersion extracts the leading major version digits from a version string
// (e.g. "20.11.0" → "20", "v18" → "18"), falling back to def when none. Mirrors
// the `(nodeVersion || "20").replace(/[^\d.]/g,"").split(".")[0]` in builders.ts.
func majorVersion(v, def string) string {
	cleaned := strings.Map(func(r rune) rune {
		if (r >= '0' && r <= '9') || r == '.' {
			return r
		}
		return -1
	}, v)
	major := strings.SplitN(cleaned, ".", 2)[0]
	if major == "" {
		return def
	}
	return major
}

// ---------------------------------------------------------------------------
// nixpacks — host binary generates a Dockerfile, then docker build
// ---------------------------------------------------------------------------

// buildNixpacks runs the nixpacks binary to generate a Dockerfile from the build
// dir, then `docker build`s it (BuildKit). With a publish dir it builds a staging
// image and nginx-wraps its output (static site). Mirrors builders.ts buildNixpacks.
// The nixpacks binary is lazily installed on first use (ensureNixpacks).
func (s *Service) buildNixpacks(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	e.log("info", "Building with Nixpacks")
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	nixpacks, err := s.ensureNixpacks(ctx, e)
	if err != nil {
		e.result(false, "nixpacks unavailable: "+err.Error(), "")
		return false
	}

	port := buildPort(spec)
	// Phase 1: generate .nixpacks/Dockerfile WITHOUT the daemon (host binary).
	prepArgs := []string{"build", buildDir, "--out", buildDir, "--no-error-without-start",
		"--env", fmt.Sprintf("PORT=%d", port)}
	if c := strings.TrimSpace(spec.GetInstallCommand()); c != "" {
		prepArgs = append(prepArgs, "-i", c)
	}
	if c := strings.TrimSpace(spec.GetBuildCommand()); c != "" {
		prepArgs = append(prepArgs, "-b", c)
	}
	if c := strings.TrimSpace(spec.GetStartCommand()); c != "" {
		prepArgs = append(prepArgs, "-s", c)
	}
	// Pin the runtime via nixpacks' per-language env var when the user set one.
	// Node is the runtime Deplo lets you pin, so an empty/none language defaults to
	// node (a control plane that dropped framework detection may send it blank).
	// nixpacks' `--env NIXPACKS_NODE_VERSION` is the highest-precedence node signal
	// (it beats a repo's engines.node / .nvmrc); node wants a bare major ("22").
	if version := strings.TrimSpace(spec.GetRuntimeVersion()); version != "" {
		lang := strings.ToLower(strings.TrimSpace(spec.GetRuntimeLanguage()))
		if lang == "" || lang == "none" {
			lang = "node"
		}
		if lang == "node" {
			version = majorVersion(version, version)
		}
		prepArgs = append(prepArgs, "--env",
			fmt.Sprintf("NIXPACKS_%s_VERSION=%s", strings.ToUpper(lang), version))
	}

	e.log("command", "nixpacks "+strings.Join(prepArgs, " "))
	code, err := dockercli.Spawn(ctx, 5*time.Minute, func(l string) { e.log("info", l) }, "", nixpacks, prepArgs...)
	if err != nil {
		e.result(false, "nixpacks: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("nixpacks failed (exit %d)", code), "")
		return false
	}

	generated := filepath.Join(buildDir, ".nixpacks", "Dockerfile")
	publishDir := strings.TrimSpace(spec.GetNixpacksPublishDirectory())

	if publishDir == "" {
		// App with a start command: build the generated Dockerfile directly.
		args := []string{"build", "-f", generated, "-t", req.GetImageRef(),
			"--build-arg", fmt.Sprintf("PORT=%d", port)}
		args = append(args, labelArgs(req)...)
		args = append(args, buildDir)
		return s.runBuildKit(ctx, args, e)
	}

	// Static publish dir: build a staging image, then nginx-wrap its output.
	staging := "deplo-nixpacks-staging:" + imageTag(req.GetImageRef())
	stageArgs := []string{"build", "-f", generated, "-t", staging,
		"--build-arg", fmt.Sprintf("PORT=%d", port), buildDir}
	if !s.runBuildKit(ctx, stageArgs, e) {
		return false
	}
	defer func() { _, _ = dockercli.Run(ctx, 30*time.Second, "rmi", staging) }()
	// Strip a leading "./" or "/" but keep a bare leading "." (dot-dirs like .next).
	srcPub := strings.TrimPrefix(strings.TrimPrefix(publishDir, "./"), "/")
	return s.nginxWrap(ctx, req, buildDir, staging, "/app/"+srcPub, e)
}

// nginxWrap builds an nginx image serving files copied out of fromImage at
// srcPath, listening on the spec's port. Mirrors builders.ts nginxWrap.
func (s *Service) nginxWrap(ctx context.Context, req *pb.DeployRequest, buildDir, fromImage, srcPath string, e *emitter) bool {
	spec := req.GetBuildSpec()
	port := buildPort(spec)
	if err := os.WriteFile(filepath.Join(buildDir, "deplo-nginx.conf"),
		[]byte(nginxConf(port, spec.GetStaticSinglePageApp())), 0o644); err != nil {
		e.result(false, "write nginx conf: "+err.Error(), "")
		return false
	}
	wrapper := fmt.Sprintf(`FROM %s AS built
FROM nginx:alpine
RUN rm -f /etc/nginx/conf.d/default.conf
COPY deplo-nginx.conf /etc/nginx/conf.d/deplo.conf
COPY --from=built %s/ /usr/share/nginx/html/
EXPOSE %d
CMD ["nginx", "-g", "daemon off;"]
`, fromImage, srcPath, port)
	wrapperPath := filepath.Join(buildDir, "deplo-static.Dockerfile")
	if err := os.WriteFile(wrapperPath, []byte(wrapper), 0o644); err != nil {
		e.result(false, "write wrapper Dockerfile: "+err.Error(), "")
		return false
	}
	args := []string{"build", "-f", wrapperPath, "-t", req.GetImageRef()}
	args = append(args, labelArgs(req)...)
	args = append(args, buildDir)
	return s.runBuild(ctx, args, e)
}

// ---------------------------------------------------------------------------
// Cloud Native Buildpacks (heroku / paketo) — pack in a container, bind-mounted
// ---------------------------------------------------------------------------

var herokuBuilders = map[string]string{
	"22": "heroku/builder:22",
	"24": "heroku/builder:24",
	"26": "heroku/builder:26",
}

// buildBuildpacks builds with Cloud Native Buildpacks via the buildpacksio/pack
// image, bind-mounting the build dir (the agent is on the host, so buildDir is
// directly mountable — no host-volume staging needed). pack does not stamp our
// labels, so we relabel after. Mirrors builders.ts buildBuildpacks; the flavor
// (heroku|paketo) is the spec's method.
func (s *Service) buildBuildpacks(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	flavor := spec.GetMethod()
	builder := "paketobuildpacks/ubuntu-noble-builder"
	label := "Paketo buildpacks"
	if flavor == "heroku" {
		label = "Heroku buildpacks"
		ver := strings.TrimSpace(spec.GetHerokuVersion())
		if ver == "" {
			ver = "24"
		}
		if b, ok := herokuBuilders[ver]; ok {
			builder = b
		} else {
			builder = "heroku/builder:24"
		}
	}
	e.log("info", "Building with "+label)
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	args := []string{
		"run", "--rm",
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		"-v", buildDir + ":/workspace",
		"buildpacksio/pack", "build", req.GetImageRef(),
		"--builder", builder,
		"--path", "/workspace",
		"--docker-host", "inherit",
		"--pull-policy", "if-not-present",
		"--env", fmt.Sprintf("PORT=%d", buildPort(spec)),
	}
	e.log("command", "docker "+strings.Join(args, " "))
	code, err := dockercli.Stream(ctx, 20*time.Minute, func(l string) { e.log("info", l) }, "", args...)
	if err != nil {
		e.result(false, "pack build: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("pack build failed (exit %d)", code), "")
		return false
	}
	return s.relabel(ctx, req, e)
}

// ---------------------------------------------------------------------------
// railpack — privileged buildkitd container + buildctl + tar load + relabel
// ---------------------------------------------------------------------------

// buildRailpack generates a railpack plan in a throwaway container, then builds it
// with a privileged buildkitd + buildctl, loads the resulting tar, and relabels.
// Mirrors builders.ts buildRailpack. The build dir is bind-mounted directly (agent
// on host). The plan + tar live alongside the build dir under buildTmpDir.
func (s *Service) buildRailpack(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	e.log("info", "Building with Railpack")
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	// Normalise the version to each consumer's grammar: the frontend image tag
	// wants `latest` or `v0.27.2`; install.sh's RAILPACK_VERSION wants a bare
	// `0.27.2` (no "latest" sentinel) — pass nothing to let it auto-resolve latest.
	ver := strings.ToLower(strings.TrimSpace(spec.GetRailpackVersion()))
	pinned := ""
	if ver != "" && ver != "latest" {
		pinned = strings.TrimPrefix(ver, "v")
	}
	frontendTag := "latest"
	if pinned != "" {
		frontendTag = "v" + pinned
	}
	frontend := "ghcr.io/railwayapp/railpack-frontend:" + frontendTag

	slug := req.GetSlug()
	tag := imageTag(req.GetImageRef())
	planDir := filepath.Join(s.buildTmpDir, fmt.Sprintf("deplo-railpack-%s-%s-plan", slug, tag))
	tarPath := filepath.Join(s.buildTmpDir, fmt.Sprintf("deplo-railpack-%s-%s.tar", slug, tag))
	buildkitd := "deplo-buildkitd-" + slug
	if err := os.MkdirAll(planDir, 0o755); err != nil {
		e.result(false, "create railpack plan dir: "+err.Error(), "")
		return false
	}
	defer func() {
		_, _ = dockercli.Run(ctx, 30*time.Second, "rm", "-f", buildkitd)
		_ = os.Remove(tarPath)
		_ = os.RemoveAll(planDir)
	}()

	// Phase A: generate the railpack plan (daemon-free, glibc base).
	planArgs := []string{"run", "--rm",
		"-v", buildDir + ":/app:ro",
		"-v", planDir + ":/out",
		"-w", "/app"}
	if pinned != "" {
		planArgs = append(planArgs, "-e", "RAILPACK_VERSION="+pinned)
	}
	// Node version + build/start overrides ride into the plan through the container
	// ENVIRONMENT (docker `-e KEY=VALUE`, an argv — so a user-supplied command can
	// never break out of the `bash -lc` string), then railpack reads each with a
	// BARE `--env KEY` (it does os.LookupEnv on bare keys). Bare refs for an unset
	// key are harmless no-ops, so they stay constant in the prepare command.
	// RAILPACK_NODE_VERSION is railpack's highest-precedence node signal; node wants
	// a bare major. RAILPACK_BUILD_CMD / RAILPACK_START_CMD override the detected
	// build + start commands.
	// The RAILPACK_* overrides, lifted to function scope: prepare bakes them into
	// the plan (as secrets — see Phase B) and Phase B must hand the same values back
	// to buildctl to satisfy those secret mounts.
	nodeVer := majorVersion(strings.TrimSpace(spec.GetRuntimeVersion()), "")
	buildCmd := strings.TrimSpace(spec.GetBuildCommand())
	startCmd := strings.TrimSpace(spec.GetStartCommand())
	if nodeVer != "" {
		planArgs = append(planArgs, "-e", "RAILPACK_NODE_VERSION="+nodeVer)
	}
	if buildCmd != "" {
		planArgs = append(planArgs, "-e", "RAILPACK_BUILD_CMD="+buildCmd)
	}
	if startCmd != "" {
		planArgs = append(planArgs, "-e", "RAILPACK_START_CMD="+startCmd)
	}
	planArgs = append(planArgs, "debian:bookworm-slim", "bash", "-lc",
		"apt-get update -qq && apt-get install -y -qq curl ca-certificates tar && curl -sSL https://railpack.com/install.sh | bash && railpack prepare /app --env RAILPACK_NODE_VERSION --env RAILPACK_BUILD_CMD --env RAILPACK_START_CMD --plan-out /out/railpack-plan.json --info-out /out/railpack-info.json")
	e.log("command", "docker run (railpack prepare)")
	code, err := dockercli.Stream(ctx, 10*time.Minute, func(l string) { e.log("info", l) }, "", planArgs...)
	if err != nil {
		e.result(false, "railpack prepare: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("railpack prepare failed (exit %d)", code), "")
		return false
	}

	// Phase B: a privileged buildkitd with context+plan mounted, build via buildctl.
	_, _ = dockercli.Run(ctx, 15*time.Second, "rm", "-f", buildkitd)
	runArgs := []string{"run", "-d", "--name", buildkitd, "--privileged",
		"-v", buildDir + ":/context:ro",
		"-v", planDir + ":/plan:ro",
		"moby/buildkit:v0.16.0"}
	if res, err := dockercli.Run(ctx, 60*time.Second, runArgs...); err != nil || res.Code != 0 {
		e.result(false, "start buildkitd: "+errOrCode(err, res.Code, res.Stderr), "")
		return false
	}

	// railpack declared each RAILPACK_* override we passed to `prepare` as a BuildKit
	// SECRET in the plan, and its frontend mounts every plan secret as a REQUIRED env
	// secret on EVERY build step. Raw `buildctl build` must therefore hand each one
	// back or it fails "secret <name>: not found". We forward the VALUES as process
	// env (never on a command line) and reference them by bare `docker exec -e NAME`
	// (docker copies NAME from the caller's env) plus `buildctl --secret
	// id=NAME,env=NAME` (buildctl reads NAME from its own env). The secret NAMES come
	// from the plan, which is generated from the UNTRUSTED user repo (a railpack
	// config may declare arbitrary `secrets:`), so everything is passed as argv
	// tokens via StreamOut — never a shell string — leaving a crafted name like
	// `x; rm -rf /` an inert argument rather than a command on the root-privileged
	// host agent.
	known := map[string]string{
		"RAILPACK_NODE_VERSION": nodeVer,
		"RAILPACK_BUILD_CMD":    buildCmd,
		"RAILPACK_START_CMD":    startCmd,
	}
	secretNames, ok := readPlanSecrets(filepath.Join(planDir, "railpack-plan.json"))
	if !ok {
		// Plan unreadable: fall back to every override name `prepare` references, so
		// a still-required secret is never left unprovided (empty value is fine — a
		// provided-but-empty secret resolves, an absent one is "not found").
		secretNames = []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD", "RAILPACK_START_CMD"}
	}
	// Defence in depth: the plan is untrusted, so drop any name that isn't a plain
	// env identifier before it reaches the buildctl `--secret id=…,env=…` CSV (a
	// comma/space in a name could otherwise smuggle extra CSV attributes). A real
	// railpack secret is always an identifier; a dropped hostile name simply fails
	// its own build with "secret not found".
	secretNames = sanitizeSecretNames(secretNames)
	secretEnv := make([]string, 0, len(secretNames)) // the ONLY place secret VALUES live
	for _, name := range secretNames {
		secretEnv = append(secretEnv, name+"="+known[name]) // unknown ⇒ "" (provided ⇒ never "not found")
	}
	execArgs := railpackBuildctlArgs(buildkitd, frontend, req.GetImageRef(), secretNames)

	// buildctl (inside buildkitd) writes the docker-format image tar to STDOUT and
	// BuildKit progress to stderr. Stream stdout straight into tarPath — no shell
	// redirect — then `docker load` it.
	tarFile, err := os.Create(tarPath)
	if err != nil {
		e.result(false, "create railpack tar: "+err.Error(), "")
		return false
	}
	e.log("command", "buildctl build (railpack frontend "+frontendTag+")")
	code, err = dockercli.StreamOut(ctx, 20*time.Minute, tarFile, func(l string) { e.log("info", l) }, secretEnv, execArgs...)
	_ = tarFile.Close()
	if err != nil {
		e.result(false, "railpack buildctl: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("railpack buildctl failed (exit %d)", code), "")
		return false
	}

	if !s.runLoad(ctx, tarPath, e) {
		return false
	}
	// railpack frontend output carries no labels — re-stamp ours.
	return s.relabel(ctx, req, e)
}

// runBuildKit streams a `docker build` with BuildKit forced on (DOCKER_BUILDKIT=1),
// needed by the nixpacks generated Dockerfile (which uses BuildKit syntax).
func (s *Service) runBuildKit(ctx context.Context, args []string, e *emitter) bool {
	e.log("command", "docker "+strings.Join(args, " "))
	code, err := dockercli.StreamEnv(ctx, 15*time.Minute, func(l string) { e.log("info", l) },
		[]string{"DOCKER_BUILDKIT=1"}, args...)
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

// runLoad streams `docker load -i <tar>` into the deploy log.
func (s *Service) runLoad(ctx context.Context, tarPath string, e *emitter) bool {
	e.log("command", "docker load -i "+tarPath)
	code, err := dockercli.Stream(ctx, 5*time.Minute, func(l string) { e.log("info", l) }, "", "load", "-i", tarPath)
	if err != nil {
		e.result(false, "docker load: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("docker load failed (exit %d)", code), "")
		return false
	}
	return true
}

// imageTag returns the tag portion of an image ref (after the last ':'), or the
// whole ref when untagged. Mirrors `imageRef.split(":").pop()` in builders.ts.
func imageTag(ref string) string {
	if i := strings.LastIndex(ref, ":"); i >= 0 {
		return ref[i+1:]
	}
	return ref
}

// validRailpackSecret matches an environment-variable-style identifier — the only
// shape a legitimate railpack secret name takes.
var validRailpackSecret = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// sanitizeSecretNames keeps only identifier-shaped secret names (the plan is
// generated from an untrusted repo). Order is preserved; the result is a fresh
// slice so the caller's fallback literal is never mutated.
func sanitizeSecretNames(names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		if validRailpackSecret.MatchString(n) {
			out = append(out, n)
		}
	}
	return out
}

// railpackBuildctlArgs assembles the injection-safe argv for railpack's Phase-B
// build: `docker exec [-e NAME ...] <buildkitd> buildctl build ... [--secret
// id=NAME,env=NAME ...] --output type=docker,name=<ref>`. The `-e NAME` flags
// forward each secret's value from the docker client's env into buildctl (whose
// `--secret env=NAME` reads it); the VALUES never appear here. secretNames come
// from an untrusted repo's railpack plan, so they are emitted as discrete argv
// tokens (no shell) — a hostile name stays one inert argument.
func railpackBuildctlArgs(buildkitd, frontend, imageRef string, secretNames []string) []string {
	args := []string{"exec"}
	for _, name := range secretNames {
		args = append(args, "-e", name)
	}
	args = append(args, buildkitd, "buildctl", "build",
		"--frontend=gateway.v0",
		"--opt", "source="+frontend,
		"--local", "context=/context",
		"--local", "dockerfile=/plan",
		"--opt", "filename=railpack-plan.json")
	for _, name := range secretNames {
		args = append(args, "--secret", "id="+name+",env="+name)
	}
	return append(args, "--output", "type=docker,name="+imageRef)
}

// readPlanSecrets returns the `secrets` a railpack plan declares — the RAILPACK_*
// overrides we passed to `prepare`, which railpack mounts as REQUIRED BuildKit env
// secrets on every build step. The bool is false only when the plan can't be read
// or parsed (so the caller falls back); a valid plan that declares no secrets
// returns (nil, true) and the caller correctly passes none.
func readPlanSecrets(planPath string) ([]string, bool) {
	b, err := os.ReadFile(planPath)
	if err != nil {
		return nil, false
	}
	var plan struct {
		Secrets []string `json:"secrets"`
	}
	if err := json.Unmarshal(b, &plan); err != nil {
		return nil, false
	}
	return plan.Secrets, true
}

// errOrCode formats either a spawn error or a non-zero exit with its stderr.
func errOrCode(err error, code int, stderr string) string {
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("exit %d: %s", code, strings.TrimSpace(stderr))
}
