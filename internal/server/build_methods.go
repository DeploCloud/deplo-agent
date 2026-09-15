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

func labelArgs(req *pb.DeployRequest) []string {
	return []string{
		"--label", "deplo.managed=true",
		"--label", "deplo.project=" + req.GetProjectId(),
		"--label", "deplo.slug=" + req.GetSlug(),
	}
}

func buildPort(spec *pb.BuildSpec) int32 {
	if p := spec.GetPort(); p > 0 {
		return p
	}
	return 80
}

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

func dropReservedBuildEnv(keys []string) []string {
	return filterKeys(keys, func(k string) bool { return !reservedBuildEnvKeys[k] })
}

func (s *Service) buildStatic(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	e.log("info", "Building with Static (nginx)")
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	port := buildPort(spec)
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
	envKeys := dropReservedBuildEnv(buildEnvKeys(req.GetEnv()))
	var dockerfile string
	if buildCmd != "" {
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
`, node, buildArgLines(envKeys), install, buildCmd, outputDir, port)
	} else {
		envKeys = nil
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

	args := appendBuildArgKeys(s.buildArgv(req), envKeys)
	args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
	args = append(args, labelArgs(req)...)
	args = append(args, buildDir)
	return s.runBuild(ctx, req, args, envKV(req.GetEnv(), envKeys), e)
}

func buildArgLines(keys []string) string {
	var b strings.Builder
	for _, k := range keys {
		b.WriteString("ARG " + k + "\n")
	}
	return b.String()
}

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
	envKeys := filterKeys(dropReservedBuildEnv(buildEnvKeys(req.GetEnv())), func(k string) bool {
		return k != "PORT" && !strings.HasPrefix(k, "NIXPACKS_")
	})
	planFlags := []string{"--env", fmt.Sprintf("PORT=%d", port)}
	pureInstall := false
	scopeFiles, scoped := manifestOnlyInstallFiles(buildDir)
	if !scoped {
		scopeFiles = nil
	}
	skipInstall, skipBuild := spec.GetSkipInstall(), spec.GetSkipBuild()
	if skipInstall {
		scopeFiles = nil
	}
	if cfg, cErr := writeNixpacksConfig(s.buildTmpDir, req.GetSlug(), scopeFiles, skipInstall, skipBuild); cErr != nil {
		e.log("warn", "could not write the nixpacks config: "+cErr.Error())
	} else if cfg != "" {
		defer func() { _ = os.Remove(cfg) }()
		planFlags = append(planFlags, "--config", cfg)
		if len(scopeFiles) > 0 {
			pureInstall = true
			e.log("info", "Installing dependencies from the manifests only, so unchanged dependencies stay cached")
		}
		if skipInstall {
			e.log("info", "Skipping the install step, as asked")
		}
		if skipBuild {
			e.log("info", "Skipping the build step, as asked")
		}
	}
	if c := strings.TrimSpace(spec.GetInstallCommand()); c != "" && !skipInstall {
		planFlags = append(planFlags, "-i", c)
	}
	if c := strings.TrimSpace(spec.GetBuildCommand()); c != "" && !skipBuild {
		planFlags = append(planFlags, "-b", c)
	}
	if c := strings.TrimSpace(spec.GetStartCommand()); c != "" {
		planFlags = append(planFlags, "-s", c)
	}
	if version := strings.TrimSpace(spec.GetRuntimeVersion()); version != "" {
		if !runtimeVersionRe.MatchString(version) {
			e.result(false, "runtime version must look like 20 or 3.12", "")
			return false
		}
		lang := strings.ToLower(strings.TrimSpace(spec.GetRuntimeLanguage()))
		if lang == "" || lang == "none" {
			lang = "node"
		}
		pin := true
		if lang == "node" {
			version = majorVersion(version, version)
			wrote, wErr := writeNodeVersionPin(buildDir, version)
			if wErr != nil {
				e.log("warn", "could not pin the Node version: "+wErr.Error())
			}
			if !wrote && wErr == nil {
				pin = false
				e.log("info", "This repository pins its own Node version, so that is the one being built")
			}
		}
		if pin {
			planFlags = append(planFlags, "--env",
				fmt.Sprintf("NIXPACKS_%s_VERSION=%s", strings.ToUpper(lang), version))
		}
	}
	for _, k := range envKeys {
		planFlags = append(planFlags, "--env", k)
	}

	prepArgs := append([]string{"build", buildDir, "--out", buildDir, "--no-error-without-start"}, planFlags...)
	if !req.GetNoBuildCache() {
		prepArgs = append(prepArgs, "--cache-key", req.GetSlug())
	}

	spawnEnv := append(envKV(req.GetEnv(), envKeys), dockerConfigEnv(req)...)
	e.log("command", "nixpacks "+strings.Join(prepArgs, " "))
	code, err := dockercli.SpawnEnv(ctx, 5*time.Minute, func(l string) { e.log("info", l) },
		spawnEnv, nixpacks, prepArgs...)
	if err != nil {
		e.result(false, "nixpacks: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("nixpacks failed (exit %d)", code), "")
		return false
	}

	ownVars, vErr := nixpacksOwnVariables(ctx, nixpacks, buildDir, planFlags, spawnEnv,
		append([]string{"PORT"}, envKeys...))
	if vErr != nil {
		e.log("warn", "could not read the nixpacks build variables, the image may miss them: "+vErr.Error())
	}

	if err := os.Remove(filepath.Join(buildDir, ".nixpacks", "build.sh")); err != nil && !os.IsNotExist(err) {
		e.log("warn", "could not remove the generated build.sh: "+err.Error())
	}

	generated := filepath.Join(buildDir, ".nixpacks", "Dockerfile")

	if pureInstall {
		moved, dErr := deferAppEnvBelowInstall(generated, envKeys, buildDir, spec.GetInstallCommand())
		switch {
		case dErr != nil:
			e.log("warn", "could not move the build variables below the install step: "+dErr.Error())
		case len(moved) > 0:
			e.log("info", "Applying the app's variables after the install step, so changing one leaves the installed dependencies cached")
		}
	}

	if _, sErr := stripAppEnvFromDockerfile(generated, envKeys); sErr != nil {
		e.log("warn", "could not drop the app variables from the image config: "+sErr.Error())
	}

	publishDir := strings.TrimSpace(spec.GetNixpacksPublishDirectory())

	buildEnv := envKV(req.GetEnv(), envKeys)

	if publishDir == "" {
		args := s.buildArgv(req, "-f", generated, "--build-arg", fmt.Sprintf("PORT=%d", port))
		args = appendBuildArgValues(args, ownVars)
		args = appendBuildArgKeys(args, envKeys)
		args = append(args, imageOutputArgs(ctx, req.GetImageRef())...)
		args = append(args, labelArgs(req)...)
		args = append(args, buildDir)
		return s.runBuildKit(ctx, req, 15*time.Minute, args, buildEnv, e)
	}

	staging := "deplo-nixpacks-staging:" + imageTag(req.GetImageRef())
	stageArgs := s.buildArgv(req, "-f", generated, "--build-arg", fmt.Sprintf("PORT=%d", port))
	stageArgs = appendBuildArgValues(stageArgs, ownVars)
	stageArgs = appendBuildArgKeys(stageArgs, envKeys)
	stageArgs = append(stageArgs, imageOutputArgs(ctx, staging)...)
	stageArgs = append(stageArgs, buildDir)
	if !s.runBuildKit(ctx, req, 15*time.Minute, stageArgs, buildEnv, e) {
		return false
	}
	defer func() { _, _ = dockercli.Run(ctx, 30*time.Second, "rmi", staging) }()
	srcPub := relativeDir(publishDir)
	return s.nginxWrap(ctx, req, buildDir, staging, "/app/"+srcPub, e)
}

func relativeDir(dir string) string {
	return strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(dir), "./"), "/")
}

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
	return s.runBuild(ctx, req, args, nil, e)
}

var herokuBuilders = map[string]string{
	"22": "heroku/builder:22",
	"24": "heroku/builder:24",
	"26": "heroku/builder:26",
}

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
		append(envKV(req.GetEnv(), envKeys), dockerConfigEnv(req)...), args...)
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

var runtimeVersionRe = regexp.MustCompile(`^[0-9]+(\.[0-9]+){0,2}$`)

var toolVersionRe = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

func (s *Service) buildRailpack(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	e.log("info", "Building with Railpack")
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	version := railpackVersion
	if v := strings.ToLower(strings.TrimSpace(spec.GetRailpackVersion())); v != "" && v != "latest" {
		v = strings.TrimPrefix(v, "v")
		if !toolVersionRe.MatchString(v) {
			e.result(false, "railpack version must look like 1.2.3", "")
			return false
		}
		version = v
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

	nodeVer := majorVersion(strings.TrimSpace(spec.GetRuntimeVersion()), "")
	buildCmd := strings.TrimSpace(spec.GetBuildCommand())
	if spec.GetSkipBuild() {
		buildCmd = ""
	}
	startCmd := strings.TrimSpace(spec.GetStartCommand())
	envKeys := filterKeys(dropReservedBuildEnv(buildEnvKeys(req.GetEnv())), func(k string) bool {
		return !strings.HasPrefix(k, "RAILPACK_")
	})
	spaDir := relativeDir(spec.GetOutputDirectory())
	prepareArgs := []string{"prepare", buildDir,
		"--env", "RAILPACK_NODE_VERSION", "--env", "RAILPACK_BUILD_CMD",
		"--env", "RAILPACK_START_CMD", "--env", "RAILPACK_SPA_OUTPUT_DIR"}
	for _, k := range envKeys {
		prepareArgs = append(prepareArgs, "--env", k)
	}
	prepareArgs = append(prepareArgs,
		"--plan-out", planPath,
		"--info-out", filepath.Join(planDir, "railpack-info.json"))

	if spec.GetSkipInstall() || spec.GetSkipBuild() {
		name, cErr := writeRailpackSkipConfig(buildDir, spec.GetSkipInstall(), spec.GetSkipBuild())
		if cErr != nil {
			e.result(false, "write railpack config: "+cErr.Error(), "")
			return false
		}
		defer func() { _ = os.Remove(filepath.Join(buildDir, name)) }()
		prepareArgs = append(prepareArgs, "--config-file", name)
		if spec.GetSkipInstall() {
			e.log("info", "Skipping the install step, as asked")
		}
		if spec.GetSkipBuild() {
			e.log("info", "Skipping the build step, as asked")
		}
	}

	prepareEnv := envKV(req.GetEnv(), envKeys)
	for _, kv := range [][2]string{
		{"RAILPACK_NODE_VERSION", nodeVer},
		{"RAILPACK_BUILD_CMD", buildCmd},
		{"RAILPACK_START_CMD", startCmd},
		{"RAILPACK_SPA_OUTPUT_DIR", spaDir},
	} {
		if kv[1] != "" {
			prepareEnv = append(prepareEnv, kv[0]+"="+kv[1])
		}
	}

	e.log("command", "railpack "+strings.Join(prepareArgs, " "))
	code, err := dockercli.SpawnEnv(ctx, 5*time.Minute, func(l string) { e.log("info", l) },
		append(prepareEnv, dockerConfigEnv(req)...), railpack, prepareArgs...)
	if err != nil {
		e.result(false, "railpack prepare: "+err.Error(), "")
		return false
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("railpack prepare failed (exit %d)", code), "")
		return false
	}

	known := map[string]string{}
	for _, k := range envKeys {
		known[k] = req.GetEnv()[k]
	}
	known["RAILPACK_NODE_VERSION"] = nodeVer
	known["RAILPACK_BUILD_CMD"] = buildCmd
	known["RAILPACK_START_CMD"] = startCmd
	known["RAILPACK_SPA_OUTPUT_DIR"] = spaDir
	secretNames, ok := readPlanSecrets(planPath)
	if !ok {
		secretNames = append([]string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD",
			"RAILPACK_START_CMD", "RAILPACK_SPA_OUTPUT_DIR"}, envKeys...)
	}
	secretNames = sanitizeSecretNames(secretNames)
	secretEnv := make([]string, 0, len(secretNames))
	for _, name := range secretNames {
		secretEnv = append(secretEnv, name+"="+known[name])
	}

	args := railpackBuildArgs(frontend, planPath, buildDir, secretNames,
		imageOutputArgs(ctx, req.GetImageRef()), req.GetNoBuildCache(), s.cacheNamespace(req.GetSlug()))
	if !s.runBuildKit(ctx, req, 20*time.Minute, args, secretEnv, e) {
		return false
	}
	return s.relabel(ctx, req, e)
}

func railpackBuildArgs(frontend, planPath, contextDir string, secretNames, output []string, noCache bool, cacheNS string) []string {
	args := []string{"build"}
	if noCache {
		args = append(args, "--no-cache")
	}
	args = append(args, "--build-arg", "BUILDKIT_SYNTAX="+frontend, "-f", planPath)
	if cacheNS != "" {
		args = append(args, "--build-arg", "BUILDKIT_CACHE_MOUNT_NS="+cacheNS)
	}
	for _, name := range secretNames {
		args = append(args, "--secret", "id="+name+",env="+name)
	}
	args = append(args, output...)
	return append(args, contextDir)
}

func (s *Service) runBuildKit(ctx context.Context, req *pb.DeployRequest, timeout time.Duration, args []string, extraEnv []string, e *emitter) bool {
	extraEnv = append(extraEnv, dockerConfigEnv(req)...)
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

func imageTag(ref string) string {
	if i := strings.LastIndex(ref, ":"); i >= 0 {
		return ref[i+1:]
	}
	return ref
}

var validRailpackSecret = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func sanitizeSecretNames(names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		if validRailpackSecret.MatchString(n) {
			out = append(out, n)
		}
	}
	return out
}

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
