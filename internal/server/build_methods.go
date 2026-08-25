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
// Buildpacks (heroku/paketo) and railpack.

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
// builders (pack, railpack) that do not apply our labels themselves.
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

// reservedBuildEnvKeys names a user build-arg must NEVER supply: each one, once present
// in the build process's environment, redirects or hijacks the ROOT- PRIVILEGED build
// tooling instead of configuring the app being built.
var reservedBuildEnvKeys = map[string]bool{
	"DOCKER_HOST":       true,
	"DOCKER_CONFIG":     true,
	"DOCKER_CONTEXT":    true,
	"DOCKER_CERT_PATH":  true,
	"DOCKER_TLS_VERIFY": true,
	"BUILDKIT_HOST":     true,
	"LD_PRELOAD":        true,
	"LD_LIBRARY_PATH":   true,
	"PATH":              true,
}

// dropReservedBuildEnv removes reservedBuildEnvKeys from a build-env key list so a
// user-supplied var can never reach the privileged build process's environment (envKV →
// cmd.Env) and hijack the build.
func dropReservedBuildEnv(keys []string) []string {
	return filterKeys(keys, func(k string) bool { return !reservedBuildEnvKeys[k] })
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
	// Build-time env (build_env.go): the builder stage declares every resolved var as
	// ARG+ENV so the install/build commands see them (a static site's env is build-time by
	// definition — there is no runtime to inject into).
	envKeys := dropReservedBuildEnv(buildEnvKeys(req.GetEnv()))
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
%sCOPY . .
RUN %s
RUN %s
FROM nginx:alpine
RUN rm -f /etc/nginx/conf.d/default.conf
COPY deplo-nginx.conf /etc/nginx/conf.d/deplo.conf
COPY --from=builder /app/%s/ /usr/share/nginx/html/
EXPOSE %d
CMD ["nginx", "-g", "daemon off;"]
`, node, argEnvLines(envKeys), install, buildCmd, outputDir, port)
	} else {
		// No build command runs, so no build env is consumed — pass none.
		envKeys = nil
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

	args := appendBuildArgKeys(buildArgv(req), envKeys)
	args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
	args = append(args, labelArgs(req)...)
	args = append(args, buildDir)
	return s.runBuild(ctx, args, envKV(req.GetEnv(), envKeys), e)
}

// argEnvLines renders one `ARG KEY` + `ENV KEY=$KEY` pair per line for a
// generated builder stage — single-name forms for classic-builder compatibility.
func argEnvLines(keys []string) string {
	var b strings.Builder
	for _, k := range keys {
		b.WriteString("ARG " + k + "\nENV " + k + "=$" + k + "\n")
	}
	return b.String()
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

// buildNixpacks runs the nixpacks binary to generate a Dockerfile from the build dir,
// then `docker build`s it (BuildKit). The nixpacks binary is lazily installed on first
// use (ensureNixpacks).
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
	// Build-time env (build_env.go). PORT and NIXPACKS_* stay excluded: the prep
	// pins those itself below (spec-derived), and a user var must not silently
	// fight the explicit build settings.
	envKeys := filterKeys(dropReservedBuildEnv(buildEnvKeys(req.GetEnv())), func(k string) bool {
		return k != "PORT" && !strings.HasPrefix(k, "NIXPACKS_")
	})
	// Phase 1: generate .nixpacks/Dockerfile WITHOUT the daemon (host binary). --cache-key
	// pins the id of every BuildKit cache mount nixpacks emits.
	prepArgs := []string{"build", buildDir, "--out", buildDir, "--no-error-without-start",
		"--env", fmt.Sprintf("PORT=%d", port)}
	if !req.GetNoBuildCache() {
		prepArgs = append(prepArgs, "--cache-key", req.GetSlug())
	}
	// Restrict the install phase to the manifests where that is provably safe, so
	// a code change stops rebuilding (and re-exporting) the dependency layer. See
	// nixpacks_install_copy.go for the gate and the escape hatch.
	pureInstall := false
	if files, ok := manifestOnlyInstallFiles(buildDir); ok {
		if cfg, cErr := writeInstallScopeConfig(s.buildTmpDir, req.GetSlug(), files); cErr == nil {
			defer func() { _ = os.Remove(cfg) }()
			prepArgs = append(prepArgs, "--config", cfg)
			pureInstall = true
			e.log("info", "Installing dependencies from the manifests only, so unchanged dependencies stay cached")
		} else {
			e.log("warn", "could not scope the install phase: "+cErr.Error())
		}
	}
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
	// Each user var as a BARE `--env KEY` (nixpacks os.LookupEnvs bare names from its
	// process env — SpawnEnv below): the generated Dockerfile then declares `ARG KEY` +
	// `ENV KEY=$KEY`, so the value is consumed at docker-build time (Phase 2's
	// --build-arg), never baked into the Dockerfile text or the log.
	for _, k := range envKeys {
		prepArgs = append(prepArgs, "--env", k)
	}

	e.log("command", "nixpacks "+strings.Join(prepArgs, " "))
	code, err := dockercli.SpawnEnv(ctx, 5*time.Minute, func(l string) { e.log("info", l) },
		envKV(req.GetEnv(), envKeys), nixpacks, prepArgs...)
	if err != nil {
		e.result(false, "nixpacks: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("nixpacks failed (exit %d)", code), "")
		return false
	}

	// nixpacks also drops a convenience `build.sh` next to the Dockerfile, holding the
	// `docker build … -t <FRESH RANDOM UUID>` it would have run itself.
	if err := os.Remove(filepath.Join(buildDir, ".nixpacks", "build.sh")); err != nil && !os.IsNotExist(err) {
		e.log("warn", "could not remove the generated build.sh: "+err.Error())
	}

	generated := filepath.Join(buildDir, ".nixpacks", "Dockerfile")

	// Nixpacks declares the whole build environment ABOVE the install step, so a RUN whose
	// only input is the lockfile still had every app variable in its cache key: editing a
	// runtime-only secret re-installed and re-exported the whole dependency layer.
	if pureInstall {
		moved, dErr := deferAppEnvBelowInstall(generated, envKeys, buildDir, spec.GetInstallCommand())
		switch {
		case dErr != nil:
			e.log("warn", "could not move the build variables below the install step: "+dErr.Error())
		case len(moved) > 0:
			e.log("info", "Applying the app's variables after the install step, so changing one leaves the installed dependencies cached")
		}
	}

	publishDir := strings.TrimSpace(spec.GetNixpacksPublishDirectory())

	// Phase 2 feeds each declared ARG a value: bare `--build-arg KEY` flags with
	// the values riding the docker client's process env (never argv/logs).
	buildEnv := envKV(req.GetEnv(), envKeys)

	if publishDir == "" {
		// App with a start command: build the generated Dockerfile directly.
		args := buildArgv(req, "-f", generated, "--build-arg", fmt.Sprintf("PORT=%d", port))
		args = appendBuildArgKeys(args, envKeys)
		args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
		args = append(args, labelArgs(req)...)
		args = append(args, buildDir)
		return s.runBuildKit(ctx, 15*time.Minute, args, buildEnv, e)
	}

	// Static publish dir: build a staging image, then nginx-wrap its output.
	staging := "deplo-nixpacks-staging:" + imageTag(req.GetImageRef())
	stageArgs := buildArgv(req, "-f", generated, "--build-arg", fmt.Sprintf("PORT=%d", port))
	stageArgs = appendBuildArgKeys(stageArgs, envKeys)
	stageArgs = append(stageArgs, imageOutputArgs(ctx, staging)...)
	stageArgs = append(stageArgs, buildDir)
	if !s.runBuildKit(ctx, 15*time.Minute, stageArgs, buildEnv, e) {
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
	args := []string{"build", "-f", wrapperPath}
	args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
	args = append(args, labelArgs(req)...)
	args = append(args, buildDir)
	// The wrapper only copies files out of the built image — no build env needed.
	return s.runBuild(ctx, args, nil, e)
}

// ---------------------------------------------------------------------------
// Cloud Native Buildpacks (heroku / paketo) — pack in a container, bind-mounted
// ---------------------------------------------------------------------------

var herokuBuilders = map[string]string{
	"22": "heroku/builder:22",
	"24": "heroku/builder:24",
	"26": "heroku/builder:26",
}

// buildBuildpacks builds with Cloud Native Buildpacks via the buildpacksio/pack image,
// bind-mounting the build dir (the agent is on the host, so buildDir is directly
// mountable — no host-volume staging needed). pack does not stamp our labels, so we
// relabel after.
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

	// Build-time env: pack resolves a bare `--env KEY` from ITS process env — the pack
	// container's — so each key rides in twice: `-e KEY` on the docker run (docker copies
	// the value from the client's process env, via StreamEnv) and `--env KEY` on pack.
	envKeys := filterKeys(dropReservedBuildEnv(buildEnvKeys(req.GetEnv())), func(k string) bool { return k != "PORT" })
	args := []string{
		"run", "--rm",
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		"-v", buildDir + ":/workspace",
	}
	for _, k := range envKeys {
		args = append(args, "-e", k)
	}
	args = append(args,
		"buildpacksio/pack", "build", req.GetImageRef(),
		"--builder", builder,
		"--path", "/workspace",
		"--docker-host", "inherit",
		"--pull-policy", "if-not-present",
		"--env", fmt.Sprintf("PORT=%d", buildPort(spec)),
	)
	for _, k := range envKeys {
		args = append(args, "--env", k)
	}
	e.log("command", "docker "+strings.Join(args, " "))
	code, err := dockercli.StreamEnv(ctx, 20*time.Minute, func(l string) { e.log("info", l) },
		envKV(req.GetEnv(), envKeys), args...)
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
// railpack — host binary generates a plan, then docker build via its frontend
// ---------------------------------------------------------------------------

// buildRailpack generates a railpack plan with the host railpack binary, then hands the
// plan to `docker build` as its Dockerfile with the railpack BuildKit frontend selected
// by BUILDKIT_SYNTAX.
func (s *Service) buildRailpack(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	e.log("info", "Building with Railpack")
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	// One version drives both halves: the CLI that writes the plan and the frontend image
	// that executes it.
	version := railpackVersion
	if v := strings.ToLower(strings.TrimSpace(spec.GetRailpackVersion())); v != "" && v != "latest" {
		version = strings.TrimPrefix(v, "v")
	}
	frontend := "ghcr.io/railwayapp/railpack-frontend:v" + version

	railpack, err := s.ensureRailpack(ctx, version, e)
	if err != nil {
		e.result(false, "railpack unavailable: "+err.Error(), "")
		return false
	}

	planDir := filepath.Join(s.buildTmpDir,
		fmt.Sprintf("deplo-railpack-%s-%s-plan", req.GetSlug(), imageTag(req.GetImageRef())))
	if err := os.MkdirAll(planDir, 0o755); err != nil {
		e.result(false, "create railpack plan dir: "+err.Error(), "")
		return false
	}
	defer func() { _ = os.RemoveAll(planDir) }()
	planPath := filepath.Join(planDir, "railpack-plan.json")

	// Phase A: generate the plan. They are lifted to function scope because prepare bakes
	// them into the plan as secrets (see Phase B) and Phase B must hand the same values
	// back to satisfy those secret mounts.
	nodeVer := majorVersion(strings.TrimSpace(spec.GetRuntimeVersion()), "")
	buildCmd := strings.TrimSpace(spec.GetBuildCommand())
	startCmd := strings.TrimSpace(spec.GetStartCommand())
	// Build-time env (build_env.go): each user var reaches `railpack prepare` the same way
	// the overrides do — its VALUE in the process env, its NAME as a bare `--env KEY`.
	// railpack declares each one a plan SECRET, which its frontend mounts as env on every
	// build step, so the var is present while `npm run build` inlines it without being
	// baked into the image.
	envKeys := filterKeys(dropReservedBuildEnv(buildEnvKeys(req.GetEnv())), func(k string) bool {
		return !strings.HasPrefix(k, "RAILPACK_")
	})
	prepareArgs := []string{"prepare", buildDir,
		"--env", "RAILPACK_NODE_VERSION", "--env", "RAILPACK_BUILD_CMD", "--env", "RAILPACK_START_CMD"}
	for _, k := range envKeys {
		prepareArgs = append(prepareArgs, "--env", k)
	}
	prepareArgs = append(prepareArgs,
		"--plan-out", planPath,
		"--info-out", filepath.Join(planDir, "railpack-info.json"))

	// Only EXPORT an override that was actually set. railpack reads these with
	// os.LookupEnv, so an empty-but-present var still counts as supplied: it declares the
	// name a plan secret and mounts it on every build step for no reason.
	prepareEnv := envKV(req.GetEnv(), envKeys)
	for _, kv := range [][2]string{
		{"RAILPACK_NODE_VERSION", nodeVer},
		{"RAILPACK_BUILD_CMD", buildCmd},
		{"RAILPACK_START_CMD", startCmd},
	} {
		if kv[1] != "" {
			prepareEnv = append(prepareEnv, kv[0]+"="+kv[1])
		}
	}

	e.log("command", "railpack "+strings.Join(prepareArgs, " "))
	code, err := dockercli.SpawnEnv(ctx, 5*time.Minute, func(l string) { e.log("info", l) },
		prepareEnv, railpack, prepareArgs...)
	if err != nil {
		e.result(false, "railpack prepare: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("railpack prepare failed (exit %d)", code), "")
		return false
	}

	// Phase B: build the plan with the railpack frontend. railpack declared each name we
	// passed to `prepare` as a BuildKit SECRET in the plan, and its frontend mounts every
	// plan secret as a REQUIRED env secret on EVERY build step — so the build must hand
	// each one back or it fails "secret <name>: not found".
	known := map[string]string{}
	for _, k := range envKeys {
		known[k] = req.GetEnv()[k]
	}
	known["RAILPACK_NODE_VERSION"] = nodeVer
	known["RAILPACK_BUILD_CMD"] = buildCmd
	known["RAILPACK_START_CMD"] = startCmd
	secretNames, ok := readPlanSecrets(planPath)
	if !ok {
		// Plan unreadable: fall back to every name `prepare` referenced — the three overrides
		// plus each user env key — so a still-required secret is never left unprovided (empty
		// value is fine — a provided-but-empty secret resolves, an absent one is "not
		// found").
		secretNames = append([]string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD", "RAILPACK_START_CMD"}, envKeys...)
	}
	// Defence in depth: the plan is untrusted, so drop any name that isn't a plain env
	// identifier before it reaches the `--secret id=…,env=…` CSV (a comma or space in a
	// name could otherwise smuggle extra CSV attributes).
	secretNames = sanitizeSecretNames(secretNames)
	secretEnv := make([]string, 0, len(secretNames)) // the ONLY place secret VALUES live
	for _, name := range secretNames {
		secretEnv = append(secretEnv, name+"="+known[name]) // unknown ⇒ "" (provided ⇒ never "not found")
	}

	args := railpackBuildArgs(frontend, planPath, buildDir, secretNames,
		imageOutputArgs(ctx, req.GetImageRef()), req.GetNoBuildCache())
	if !s.runBuildKit(ctx, 20*time.Minute, args, secretEnv, e) {
		return false
	}
	// The railpack frontend builds the image config itself and DROPS the `--label` flags
	// buildx forwards — verified against a real build, where the Dockerfile frontend kept
	// all three deplo labels and railpack's kept none.
	return s.relabel(ctx, req, e)
}

// railpackBuildArgs assembles the `docker build` argv that runs a railpack plan: the
// plan file stands in for the Dockerfile and BUILDKIT_SYNTAX selects the railpack
// frontend to interpret it.
func railpackBuildArgs(frontend, planPath, contextDir string, secretNames, output []string, noCache bool) []string {
	args := []string{"build"}
	if noCache {
		args = append(args, "--no-cache")
	}
	args = append(args, "--build-arg", "BUILDKIT_SYNTAX="+frontend, "-f", planPath)
	for _, name := range secretNames {
		args = append(args, "--secret", "id="+name+",env="+name)
	}
	args = append(args, output...)
	return append(args, contextDir)
}

// runBuildKit streams a `docker build` with BuildKit forced on (DOCKER_BUILDKIT=1),
// needed by the nixpacks generated Dockerfile and the railpack plan (both use
// BuildKit-only syntax). extraEnv carries build-env VALUES for bare `--build-arg KEY`
// flags, or a railpack plan's `--secret env=NAME` values (may be nil). timeout is
// explicit because the methods deserve different budgets: railpack kept the 20 minutes
// its old buildctl path had, nixpacks the 15 it always had.
func (s *Service) runBuildKit(ctx context.Context, timeout time.Duration, args []string, extraEnv []string, e *emitter) bool {
	e.log("command", "docker "+strings.Join(args, " "))
	code, err := dockercli.StreamEnv(ctx, timeout, func(l string) { e.log("info", l) },
		append([]string{"DOCKER_BUILDKIT=1"}, extraEnv...), args...)
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

// readPlanSecrets returns the `secrets` a railpack plan declares — the RAILPACK_*
// overrides we passed to `prepare`, which railpack mounts as REQUIRED BuildKit env
// secrets on every build step.
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
