package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
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

// stripEscapingSymlinks removes the links under root that resolve outside it, so railpack
// reading the repo as root cannot read a host file through one. Returns what it removed.
func stripEscapingSymlinks(root string) ([]string, error) {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	var removed []string
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == root || d.Type()&fs.ModeSymlink == 0 {
			return err
		}
		target, err := os.Readlink(p)
		if err != nil {
			return err
		}
		escapes := filepath.IsAbs(target) || !within(root, filepath.Join(filepath.Dir(p), target))
		if real, rerr := filepath.EvalSymlinks(p); rerr == nil && !within(realRoot, real) {
			escapes = true
		}
		if !escapes {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		removed = append(removed, rel)
		return os.Remove(p)
	})
	return removed, err
}

func within(base, p string) bool {
	return p == base || strings.HasPrefix(p, base+string(os.PathSeparator))
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

	frontend := "ghcr.io/railwayapp/railpack-frontend:v" + railpackVersion
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

	nodeVer := majorVersion(strings.TrimSpace(spec.GetRuntimeVersion()), "")
	buildCmd := strings.TrimSpace(spec.GetBuildCommand())
	if spec.GetSkipBuild() {
		buildCmd = ""
	}
	startCmd := strings.TrimSpace(spec.GetStartCommand())
	envKeys := filterKeys(buildEnvKeys(req.GetEnv()), func(k string) bool {
		return !slices.Contains(railpackManagedEnv, k)
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
		imageOutputArgs(ctx, req.GetImageRef()), req.GetNoBuildCache(), s.cacheNamespace(req.GetSlug()),
		s.secretsHash(secretEnv))
	if !s.runBuildKit(ctx, req, 20*time.Minute, args, secretEnv, e) {
		return false
	}
	return s.relabel(ctx, req, e)
}

// Deplo sets these from the build spec; a user env var must not override them.
var railpackManagedEnv = []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD", "RAILPACK_START_CMD", "RAILPACK_SPA_OUTPUT_DIR"}

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
