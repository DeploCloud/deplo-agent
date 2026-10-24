package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
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

// writeBuildFile writes an agent-owned file into a checkout, replacing a symlink the repo put
// there instead of writing through it to wherever it points on the host.
func writeBuildFile(path string, data []byte) error {
	if li, err := os.Lstat(path); err == nil && li.Mode()&os.ModeSymlink != 0 {
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func stripHostLinks(buildDir string, e *emitter) bool {
	removed, err := stripEscapingSymlinks(buildDir)
	if err != nil {
		e.result(false, "check the repository's links: "+err.Error(), "")
		return false
	}
	if len(removed) > 0 {
		e.log("warn", "Ignoring links that point outside the repository: "+strings.Join(removed, ", "))
	}
	return true
}

// stripEscapingSymlinks removes the links under dir that resolve outside it, so a host tool
// reading the tree as root, or a bind mount into it, cannot reach a host file through one. The
// walk goes through an os.Root, so a container rewriting the tree meanwhile cannot steer it out.
func stripEscapingSymlinks(dir string) ([]string, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	var removed []string
	err = fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == "." {
				return err
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink == 0 || resolvesInside(root, p) {
			return nil
		}
		removed = append(removed, p)
		return root.Remove(p)
	})
	return removed, err
}

// resolvesInside: the link leads inside the root, or dangles with a target that would.
func resolvesInside(root *os.Root, p string) bool {
	_, err := root.Stat(p)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return err == nil
	}
	target, err := root.Readlink(p)
	return err == nil && linkStaysInside(p, target, false)
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

func relativeDir(dir string) string {
	return strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(dir), "./"), "/")
}

func (s *Service) buildRailpack(ctx context.Context, req *pb.DeployRequest, buildDir string, e *emitter) bool {
	spec := req.GetBuildSpec()
	e.log("info", "Building with Railpack")
	e.phase(pb.DeployPhase_DEPLOY_PHASE_BUILDING)

	railpack, err := s.ensureRailpack(ctx, railpackVersion, e)
	if err != nil {
		e.result(false, "railpack unavailable: "+err.Error(), "")
		return false
	}
	if !stripHostLinks(buildDir, e) {
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
	infoPath := filepath.Join(planDir, "railpack-info.json")

	known := railpackSettings(spec, buildDir)
	userEnv := req.GetEnv()
	if v := javaFromGradle(buildDir); v != "" && userEnv["RAILPACK_JDK_VERSION"] == "" {
		known["RAILPACK_JDK_VERSION"] = v
		e.log("info", "Using Java "+v+", the version the Gradle build asks for")
	}
	envKeys := filterKeys(buildEnvKeys(userEnv), func(k string) bool { _, ours := known[k]; return !ours })
	prepareArgs := []string{"prepare", buildDir, "--plan-out", planPath, "--info-out", infoPath, "--error-missing-start"}

	if spec.GetStaticSinglePageApp() {
		wrote, sErr := writeStaticfile(buildDir, relativeDir(spec.GetOutputDirectory()))
		if sErr != nil {
			e.result(false, "write the Staticfile: "+sErr.Error(), "")
			return false
		}
		if wrote {
			e.log("info", "Serving the output as a single-page app: unknown paths get index.html")
		}
	}
	if spec.GetSkipInstall() || spec.GetSkipBuild() {
		base := ""
		if slices.Contains(envKeys, "RAILPACK_CONFIG_FILE") {
			base = userEnv["RAILPACK_CONFIG_FILE"]
		}
		name, cErr := writeRailpackSkipConfig(buildDir, base, spec.GetSkipInstall(), spec.GetSkipBuild())
		if cErr != nil {
			e.result(false, "write railpack config: "+cErr.Error(), "")
			return false
		}
		defer func() { _ = os.Remove(filepath.Join(buildDir, name)) }()
		envKeys = filterKeys(envKeys, func(k string) bool { return k != "RAILPACK_CONFIG_FILE" })
		prepareArgs = append(prepareArgs, "--config-file", name)
		if spec.GetSkipInstall() {
			e.log("info", "Skipping the install step, as asked")
		}
		if spec.GetSkipBuild() {
			e.log("info", "Skipping the build step, as asked")
		}
	}

	for _, k := range envKeys {
		known[k] = userEnv[k]
	}
	names := slices.Sorted(maps.Keys(known))
	prepareEnv := make([]string, 0, len(names)+1)
	for _, k := range names {
		prepareArgs = append(prepareArgs, "--env", k)
		prepareEnv = append(prepareEnv, k+"="+known[k])
	}
	// Only prepare's own version lookups read it: never an --env, so never a build secret.
	if tok := req.GetGithubToken(); tok != "" && !slices.Contains(envKeys, "GITHUB_TOKEN") {
		prepareEnv = append(prepareEnv, "GITHUB_TOKEN="+tok)
	}

	prepare := func(args []string) bool {
		e.log("command", "railpack "+strings.Join(args, " "))
		code, err := dockercli.SpawnEnv(ctx, 5*time.Minute, func(l string) { e.log("info", l) },
			append(prepareEnv, dockerConfigEnv(req)...), railpack, args...)
		if err != nil {
			e.result(false, "railpack prepare: "+err.Error(), "")
			return false
		}
		if code != 0 {
			e.result(false, prepareFailure(code, readRailpackInfo(infoPath), req.GetGithubToken() != "", buildDir), "")
			return false
		}
		return true
	}
	if !prepare(prepareArgs) {
		return false
	}
	info := readRailpackInfo(infoPath)
	// A static site's build step is what writes its web server config, so it is never skipped.
	if spec.GetSkipBuild() && slices.Contains(info.DetectedProviders, "staticfile") {
		e.log("info", "A static site has no build command to skip: serving its files as they are")
		if !prepare(slices.DeleteFunc(slices.Clone(prepareArgs), func(a string) bool {
			return a == "--config-file" || a == railpackSkipConfigName
		})) {
			return false
		}
		info = readRailpackInfo(infoPath)
	}

	if fixed, err := fixGradleStart(planPath); err != nil {
		e.result(false, "read the railpack plan: "+err.Error(), "")
		return false
	} else if fixed {
		e.log("info", "Looking for the Gradle jar in build/libs too")
	}

	secretNames, ok := readPlanSecrets(planPath)
	if !ok {
		secretNames = names
	}
	secretNames = sanitizeSecretNames(secretNames)
	secretEnv := make([]string, 0, len(secretNames))
	for _, name := range secretNames {
		secretEnv = append(secretEnv, name+"="+known[name])
	}

	args := railpackBuildArgs(railpackFrontend, planPath, buildDir, secretNames,
		imageOutputArgs(ctx, req.GetImageRef()), req.GetNoBuildCache(), s.cacheNamespace(req.GetSlug()),
		s.secretsHash(secretEnv))
	if !s.runBuildKit(ctx, req, args, secretEnv, e, info.major("node")) {
		return false
	}
	return s.relabel(ctx, req, e)
}

// railpackSettings maps the build spec onto railpack's own variables. Only what the app set
// is declared, so railpack's defaults (and a user's RAILPACK_* variable) apply otherwise.
func railpackSettings(spec *pb.BuildSpec, buildDir string) map[string]string {
	set := map[string]string{}
	put := func(k, v string) {
		if v = strings.TrimSpace(v); v != "" {
			set[k] = v
		}
	}
	put("RAILPACK_NODE_VERSION", majorVersion(strings.TrimSpace(spec.GetRuntimeVersion()), ""))
	if !spec.GetSkipInstall() {
		put("RAILPACK_INSTALL_CMD", spec.GetInstallCommand())
	}
	if !spec.GetSkipBuild() {
		put("RAILPACK_BUILD_CMD", spec.GetBuildCommand())
	}
	put("RAILPACK_START_CMD", spec.GetStartCommand())
	if out := relativeDir(spec.GetOutputDirectory()); out != "" {
		put("RAILPACK_SPA_OUTPUT_DIR", out)
		if _, err := os.Stat(filepath.Join(buildDir, "package.json")); err != nil {
			put("RAILPACK_STATIC_FILE_ROOT", out)
		}
	}
	return set
}

func railpackBuildArgs(frontend, planPath, contextDir string, secretNames, output []string, noCache bool, cacheNS, secretsHash string) []string {
	args := []string{"build"}
	if noCache {
		args = append(args, "--no-cache")
	}
	args = append(args, "--build-arg", "BUILDKIT_SYNTAX="+frontend, "-f", planPath)
	if cacheNS != "" {
		// The railpack frontend namespaces its cache mounts by cache-key only.
		args = append(args, "--build-arg", "BUILDKIT_CACHE_MOUNT_NS="+cacheNS, "--build-arg", "cache-key="+cacheNS)
	}
	if secretsHash != "" {
		args = append(args, "--build-arg", "secrets-hash="+secretsHash)
	}
	for _, name := range secretNames {
		args = append(args, "--secret", "id="+name+",env="+name)
	}
	args = append(args, output...)
	return append(args, contextDir)
}

func (s *Service) runBuildKit(ctx context.Context, req *pb.DeployRequest, args []string, extraEnv []string, e *emitter, nodeMajor string) bool {
	return s.streamBuild(ctx, req, args, append([]string{"DOCKER_BUILDKIT=1"}, extraEnv...), e, nodeMajor)
}

// buildTimeout is the longest any build may run: a Dockerfile, railpack's or a stack's.
const buildTimeout = 45 * time.Minute

// buildRunError is the deploy error for a build that could not finish, a timeout said in words.
func buildRunError(what string, err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf("The build took longer than %d minutes, the most a build may take on this server.",
			int(buildTimeout/time.Minute))
	}
	return what + ": " + err.Error()
}

// streamBuild runs one `docker build` and, when it fails, names a cause its output shows.
func (s *Service) streamBuild(ctx context.Context, req *pb.DeployRequest, args []string, env []string, e *emitter, nodeMajor string) bool {
	env = append(env, dockerConfigEnv(req)...)
	e.log("command", "docker "+strings.Join(args, " "))
	limit, release := capBuildSteps()
	defer release()
	tail := &lineTail{max: 200}
	code, err := dockercli.StreamEnv(ctx, buildTimeout, func(l string) { tail.add(l); e.log("info", l) }, env, args...)
	if err != nil {
		e.result(false, buildRunError("docker build", err), "")
		return false
	}
	if code != 0 {
		e.result(false, buildFailure("docker build", code, tail.lines, limit, req, nodeMajor), "")
		return false
	}
	return true
}

// buildFailure is a failed build's error: the exit code, then a cause its output shows.
func buildFailure(what string, code int, lines []string, limit int64, req *pb.DeployRequest, nodeMajor string) string {
	msg := fmt.Sprintf("%s failed (exit %d)", what, code)
	hint := memoryKilled(lines, limit)
	if hint == "" {
		hint = buildHint(lines, req.GetGithubToken() != "", nodeMajor)
	}
	if hint != "" {
		msg += ". " + hint
	}
	return msg
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
		if validRailpackSecret.MatchString(n) && !reservedEnvName(n) {
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
