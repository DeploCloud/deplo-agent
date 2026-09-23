package dockercli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// LineFn receives one line of merged stdout+stderr as a build/run stream.
type LineFn func(line string)

// Run executes `docker <args>` to completion, returning combined output and the exit code.
type Result struct {
	Stdout string
	Stderr string
	Code   int
}

// Run runs `docker <args>` with a timeout, capturing output.
func Run(ctx context.Context, timeout time.Duration, args ...string) (Result, error) {
	return capture(ctx, timeout, nil, false, args...)
}

// RunEnv is Run with extra "KEY=VALUE" host-process env layered on (e.g.
func RunEnv(ctx context.Context, timeout time.Duration, extraEnv []string, args ...string) (Result, error) {
	return capture(ctx, timeout, extraEnv, true, args...)
}

func capture(ctx context.Context, timeout time.Duration, extraEnv []string, redact bool, args ...string) (Result, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := Command(cctx, "docker", args...)
	cmd.Env = scopedEnv(extraEnv)
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	res := Result{Stdout: out.String(), Stderr: errb.String()}
	if err == nil {
		return res, nil
	}
	if ee, ok := err.(*exec.ExitError); ok {
		res.Code = ee.ExitCode()
		return res, nil
	}
	label := strings.Join(args, " ")
	if redact {
		label = redactArgs(args)
	}
	return res, fmt.Errorf("docker %s failed: %w (%s)", label, err, errb.String())
}

// Stream runs `docker <args>` and forwards each line of merged stdout+stderr to onLine as it is produced (the live build/clone log), returning the exit code.
func Stream(ctx context.Context, timeout time.Duration, onLine LineFn, input string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args, cleanup := ephemeral(args)
	defer cleanupIfCancelled(cctx, cleanup)
	cmd := Command(cctx, "docker", args...)
	cmd.Env = scopedEnv(nil)
	return streamCmd(cctx, timeout, onLine, input, cmd)
}

// StreamEnv is Stream with extra "KEY=VALUE" environment entries layered on top of the agent's own env (e.g.
func StreamEnv(ctx context.Context, timeout time.Duration, onLine LineFn, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args, cleanup := ephemeral(args)
	defer cleanupIfCancelled(cctx, cleanup)
	cmd := Command(cctx, "docker", args...)
	cmd.Env = scopedEnv(extraEnv)
	return streamCmd(cctx, timeout, onLine, "", cmd)
}

// Spawn runs an ARBITRARY host binary (not docker) and streams its merged output, for build tools that run on the host rather than via the daemon - e.g.
func Spawn(ctx context.Context, timeout time.Duration, onLine LineFn, input, name string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := Command(cctx, name, args...)
	cmd.Env = scopedEnv(nil)
	return streamCmd(cctx, timeout, onLine, input, cmd)
}

// SpawnEnv is Spawn with extra "KEY=VALUE" env entries layered on top of the agent's own env - e.g.
func SpawnEnv(ctx context.Context, timeout time.Duration, onLine LineFn, extraEnv []string, name string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := Command(cctx, name, args...)
	cmd.Env = scopedEnv(extraEnv)
	return streamCmd(cctx, timeout, onLine, "", cmd)
}

// StreamOut runs `docker <args>` (no shell - argv is injection-safe) with extra "KEY=VALUE" env layered on, streaming the child's RAW stdout into dst (e.g.
func StreamOut(ctx context.Context, timeout time.Duration, dst io.Writer, onLine LineFn, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args, cleanup := ephemeral(args)
	defer cleanupIfCancelled(cctx, cleanup)
	cmd := Command(cctx, "docker", args...)
	cmd.Env = scopedEnv(extraEnv)
	label := redactArgs(args)
	cmd.Stdout = dst
	err := RunLines(cctx, cmd, onLine)
	if err != nil && cmd.Process == nil {
		return -1, fmt.Errorf("docker %s: %w", label, err)
	}
	return exitStatus(cctx, err, timeout, "docker "+label)
}

// StreamPipes runs docker with stdout and stderr each copied straight into a writer - no line scanner in the way, so a single line longer than a scanner buffer (a 9 MB JSON dump on stderr) cannot stall the process on a full pipe.
func StreamPipes(ctx context.Context, timeout time.Duration, stdout, stderr io.Writer, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := Command(cctx, "docker", args...)
	cmd.Env = scopedEnv(extraEnv)
	return runPipes(cctx, timeout, cmd, stdout, stderr, redactArgs(args))
}

func runPipes(cctx context.Context, timeout time.Duration, cmd *exec.Cmd, stdout, stderr io.Writer, label string) (int, error) {
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return -1, fmt.Errorf("docker %s: %w", label, err)
	}
	return exitStatus(cctx, cmd.Wait(), timeout, "docker "+label)
}

func streamCmd(cctx context.Context, timeout time.Duration, onLine LineFn, input string, cmd *exec.Cmd) (int, error) {
	label := strings.Join(cmd.Args, " ")
	if input != "" {
		cmd.Stdin = strings.NewReader(input)
	}
	err := RunLines(cctx, cmd, onLine)
	if err != nil && cmd.Process == nil {
		return -1, fmt.Errorf("%s: %w", label, err)
	}
	return exitStatus(cctx, err, timeout, label)
}

func exitStatus(cctx context.Context, err error, timeout time.Duration, label string) (int, error) {
	if err == nil {
		return 0, nil
	}
	if cctx.Err() == context.DeadlineExceeded {
		return -1, fmt.Errorf("%s timed out after %s", label, timeout)
	}
	if cctx.Err() == context.Canceled {
		return -1, fmt.Errorf("%s canceled", label)
	}
	if ee, ok := err.(*exec.ExitError); ok && ee.ProcessState.Exited() {
		return ee.ExitCode(), nil
	}
	return -1, err
}

// PipeOut runs `docker <args>` and copies the child's RAW stdout into `dst` (bytes, not lines) while collecting stderr for diagnostics - for piping a dump tool's output (`docker exec <c> pg_dump …`) straight into the gzip→S3 pipeline with no temp file.
func PipeOut(ctx context.Context, timeout time.Duration, dst io.Writer, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args, cleanup := ephemeral(args)
	defer cleanupIfCancelled(cctx, cleanup)
	cmd := Command(cctx, "docker", args...)
	cmd.Env = scopedEnv(extraEnv)
	var errb strings.Builder
	cmd.Stdout = dst
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		label := redactArgs(args)
		if cctx.Err() == context.DeadlineExceeded {
			return -1, fmt.Errorf("docker %s timed out after %s", label, timeout)
		}
		if ee, ok := err.(*exec.ExitError); ok && ee.ProcessState.Exited() {
			return ee.ExitCode(), fmt.Errorf("docker %s exited %d: %s", label, ee.ExitCode(), strings.TrimSpace(errb.String()))
		}
		return -1, fmt.Errorf("docker %s failed: %w (%s)", label, err, strings.TrimSpace(errb.String()))
	}
	return 0, nil
}

// PipeIn runs `docker <args>` feeding `src` to the child's stdin (the restore direction: a decompressed dump streamed into `docker exec -i <c> psql …`), collecting stderr.
func PipeIn(ctx context.Context, timeout time.Duration, src io.Reader, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args, cleanup := ephemeral(args)
	defer cleanupIfCancelled(cctx, cleanup)
	cmd := Command(cctx, "docker", args...)
	cmd.Env = scopedEnv(extraEnv)
	var errb strings.Builder
	cmd.Stdin = src
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		label := redactArgs(args)
		if cctx.Err() == context.DeadlineExceeded {
			return -1, fmt.Errorf("docker %s timed out after %s", label, timeout)
		}
		if ee, ok := err.(*exec.ExitError); ok && ee.ProcessState.Exited() {
			return ee.ExitCode(), fmt.Errorf("docker %s exited %d: %s", label, ee.ExitCode(), strings.TrimSpace(errb.String()))
		}
		return -1, fmt.Errorf("docker %s failed: %w (%s)", label, err, strings.TrimSpace(errb.String()))
	}
	return 0, nil
}

func redactArgs(args []string) string {
	out := make([]string, len(args))
	maskNext := false
	for i, a := range args {
		if maskNext {
			out[i] = "***"
			maskNext = false
			continue
		}
		if k, _, ok := strings.Cut(a, "="); ok && a != k+"=" && looksSecretKey(k) {
			out[i] = k + "=***"
			continue
		}
		switch a {
		case "-a", "-p", "--password":
			out[i] = a
			maskNext = true
		default:
			out[i] = a
		}
	}
	return strings.Join(out, " ")
}

func looksSecretKey(k string) bool {
	k = strings.ToUpper(k)
	return k == "PGPASSWORD" || k == "MYSQL_PWD" || k == "REDISCLI_AUTH" ||
		k == "MONGODB_PASSWORD" || strings.Contains(k, "PASSWORD") || strings.Contains(k, "SECRET")
}

// Server asks the daemon once for both things Hello reports: its version, and whether it answered at all.
func Server(ctx context.Context) (version string, available bool) {
	res, err := Run(ctx, 5*time.Second, "version", "--format", "{{.Server.Version}}")
	if err != nil || res.Code != 0 {
		return "", false
	}
	return strings.TrimSpace(res.Stdout), true
}

// Available reports whether the Docker daemon is reachable.
func Available(ctx context.Context) bool {
	_, ok := Server(ctx)
	return ok
}

// ServerVersion returns the Docker engine version, or "" if unreachable.
func ServerVersion(ctx context.Context) string {
	v, _ := Server(ctx)
	return v
}

var (
	imageExportMu    sync.Mutex
	imageExportKnown bool
	imageExportOK    bool
)

// ImageExportOptsSupported reports whether `docker build --output type=image,…` is available on this host - i.e.
func ImageExportOptsSupported(ctx context.Context) bool {
	imageExportMu.Lock()
	defer imageExportMu.Unlock()
	if imageExportKnown {
		return imageExportOK
	}
	res, err := Run(ctx, 15*time.Second, "info", "--format", "{{json .DriverStatus}}")
	if err != nil || res.Code != 0 {
		return false
	}
	if !strings.Contains(res.Stdout, "io.containerd.snapshotter.v1") {
		imageExportKnown, imageExportOK = true, false
		return false
	}
	bx, err := Run(ctx, 15*time.Second, "buildx", "version")
	if err != nil {
		return false
	}
	imageExportKnown, imageExportOK = true, bx.Code == 0
	return imageExportOK
}

func resetImageExportProbe() {
	imageExportMu.Lock()
	defer imageExportMu.Unlock()
	imageExportKnown, imageExportOK = false, false
}

// Build-cache size caps.
const (
	// PruneCapModern accepts --max-used-space / --min-free-space.
	PruneCapModern = "modern"
	// PruneCapLegacy accepts only --keep-storage.
	PruneCapLegacy = "legacy"
	// PruneCapNone accepts no size cap at all - prune by age only.
	PruneCapNone = "none"
)

var (
	pruneCapMu    sync.Mutex
	pruneCapKnown bool
	pruneCapMode  string
)

// BuildCachePruneCap reports which size-ceiling flags `docker builder prune` accepts here.
func BuildCachePruneCap(ctx context.Context) string {
	pruneCapMu.Lock()
	defer pruneCapMu.Unlock()
	if pruneCapKnown {
		return pruneCapMode
	}
	res, err := Run(ctx, 15*time.Second, "builder", "prune", "--help")
	if err != nil {
		return PruneCapNone
	}
	help := res.Stdout + res.Stderr
	mode := PruneCapNone
	switch {
	case strings.Contains(help, "--max-used-space"):
		mode = PruneCapModern
	case strings.Contains(help, "--keep-storage"):
		mode = PruneCapLegacy
	}
	pruneCapKnown, pruneCapMode = true, mode
	return mode
}

func resetPruneCapProbe() {
	pruneCapMu.Lock()
	defer pruneCapMu.Unlock()
	pruneCapKnown, pruneCapMode = false, ""
}

// EnsureNetwork creates the named external network if it is missing.
func EnsureNetwork(ctx context.Context, name string) error {
	if res, err := Run(ctx, 10*time.Second, "network", "inspect", name); err == nil && res.Code == 0 {
		return nil
	}
	res, err := Run(ctx, 15*time.Second, "network", "create", name)
	if err != nil {
		return err
	}
	if res.Code != 0 && !strings.Contains(res.Stderr, "already exists") {
		return fmt.Errorf("docker network create %s failed: %s", name, res.Stderr)
	}
	return nil
}

// ConnectNetwork attaches a container to a network.
func ConnectNetwork(ctx context.Context, network, container string) error {
	res, err := Run(ctx, 15*time.Second, "network", "connect", network, container)
	if err != nil {
		return err
	}
	if res.Code == 0 || strings.Contains(res.Stderr, "already exists in network") {
		return nil
	}
	return fmt.Errorf("docker network connect %s %s failed: %s", network, container, res.Stderr)
}

// ListNetworks names every docker network on this host.
func ListNetworks(ctx context.Context) (names []string, ok bool) {
	res, err := Run(ctx, 10*time.Second, "network", "ls", "--format", "{{.Name}}")
	if err != nil || res.Code != 0 {
		return nil, false
	}
	return nonEmptyLines(res.Stdout), true
}

// TenantNetworksOf keeps the tenant networks out of a listing - the ones a recreated Traefik has to be put back on.
func TenantNetworksOf(names []string) []string {
	var out []string
	for _, n := range names {
		if IsTenantNetwork(n) {
			out = append(out, n)
		}
	}
	return out
}

// DeploNetworks lists the tenant networks Deplo manages on this host.
func DeploNetworks(ctx context.Context) []string {
	names, _ := ListNetworks(ctx)
	return TenantNetworksOf(names)
}

// ContainerNetworks is the set of networks a container is attached to.
func ContainerNetworks(ctx context.Context, name string) (on map[string]bool, exists bool) {
	res, err := Run(ctx, 10*time.Second, "inspect", "-f",
		"{{range $k, $_ := .NetworkSettings.Networks}}{{$k}} {{end}}", name)
	if err != nil || res.Code != 0 {
		return nil, false
	}
	on = map[string]bool{}
	for _, n := range strings.Fields(res.Stdout) {
		on[n] = true
	}
	return on, true
}

// IsTenantNetwork reports whether a network name is one Deplo mints for an Environment, a team or a preview - never the platform's own.
func IsTenantNetwork(name string) bool {
	return strings.HasPrefix(name, "deplo-env-") ||
		strings.HasPrefix(name, "deplo-team-") ||
		strings.HasPrefix(name, "deplo-preview-")
}

// CountRunning counts EVERY running container on the host, returning ok=false when the read itself failed - a caller keeping a gauge must not publish a 0 it never measured.
func CountRunning(ctx context.Context) (int, bool) {
	res, err := Run(ctx, 10*time.Second, "ps", "-q")
	if err != nil || res.Code != 0 {
		return 0, false
	}
	return len(nonEmptyLines(res.Stdout)), true
}

// RunningContainers is CountRunning for a one-shot reader: 0 on any failure.
func RunningContainers(ctx context.Context) int {
	n, _ := CountRunning(ctx)
	return n
}

func nonEmptyLines(out string) []string {
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// TraefikRunning reports whether a Traefik reverse proxy container is running on this host.
func TraefikRunning(ctx context.Context) bool {
	res, err := Run(ctx, 5*time.Second, "ps", "--filter", "status=running",
		"--format", "{{.Image}}\t{{.Names}}")
	if err != nil || res.Code != 0 {
		return false
	}
	for _, line := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		low := strings.ToLower(line)
		if strings.Contains(low, "traefik") {
			return true
		}
	}
	return false
}

// IsRunning reports whether a named container is in the running state.
func IsRunning(ctx context.Context, name string) bool {
	res, err := Run(ctx, 5*time.Second, "inspect", "-f", "{{.State.Running}}", name)
	if err != nil || res.Code != 0 {
		return false
	}
	return strings.TrimSpace(res.Stdout) == "true"
}

// State returns (exists, runtimeState) for a container, e.g.
func State(ctx context.Context, name string) (bool, string) {
	res, err := Run(ctx, 5*time.Second, "inspect", "-f", "{{.State.Status}}", name)
	if err != nil || res.Code != 0 {
		return false, ""
	}
	return true, strings.TrimSpace(res.Stdout)
}

// StackRunning reports whether ANY container of a Deplo stack is running, keyed by the deplo.slug label rather than a container name.
func StackRunning(ctx context.Context, slug string) bool {
	res, err := Run(ctx, 5*time.Second, "ps", "-q",
		"--filter", "label=deplo.slug="+slug,
		"--filter", "status=running")
	if err != nil || res.Code != 0 {
		return false
	}
	return strings.TrimSpace(res.Stdout) != ""
}

// NetworkHeadroom reports how close this host is to running out of docker networks, as a warning to print, or "" when there is room.
func NetworkHeadroom(ctx context.Context) string {
	names, ok := ListNetworks(ctx)
	if !ok {
		return ""
	}
	return NetworkHeadroomFor(ctx, len(names))
}

// NetworkHeadroomFor is NetworkHeadroom for a caller that has already listed the networks, so a deploy pays for that listing once.
func NetworkHeadroomFor(ctx context.Context, count int) string {
	ceiling, widened := addressPoolCapacity(ctx), true
	if ceiling == 0 {
		ceiling, widened = 31, false
	}
	if count < ceiling-8 {
		return ""
	}
	if widened {
		return fmt.Sprintf(
			"this server has %d docker networks and its address pools hold about %d, so it "+
				"is near the ceiling. Widen \"default-address-pools\" in "+
				"/etc/docker/daemon.json and restart docker, or the next deploy that needs a "+
				"new network will fail.", count, ceiling)
	}
	return fmt.Sprintf(
		"this server has %d docker networks and no widened address pool, so it is near "+
			"the built-in ceiling of about 31. Set \"default-address-pools\" in "+
			"/etc/docker/daemon.json and restart docker, or the next deploy that needs a "+
			"new network will fail.", count)
}

func addressPoolCapacity(ctx context.Context) int {
	res, err := Run(ctx, 10*time.Second, "info", "--format", "{{json .DefaultAddressPools}}")
	if err != nil || res.Code != 0 {
		return 0
	}
	return parseAddressPools(res.Stdout)
}

func parseAddressPools(out string) int {
	var pools []struct {
		Base string `json:"Base"`
		Size int    `json:"Size"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &pools); err != nil {
		return 0
	}
	total := 0
	for _, p := range pools {
		_, ipnet, err := net.ParseCIDR(strings.TrimSpace(p.Base))
		if err != nil || ipnet == nil {
			continue
		}
		base, _ := ipnet.Mask.Size()
		bits := p.Size - base
		if bits < 0 || bits > 20 {
			bits = 20
		}
		total += 1 << bits
	}
	return total
}

var envAllowedPrefixes = []string{"DOCKER_", "BUILDKIT_", "COMPOSE_", "XDG_", "LC_", "SSL_CERT_"}

var envAllowedNames = map[string]bool{
	"PATH": true, "HOME": true, "USER": true, "LOGNAME": true, "LANG": true,
	"TMPDIR": true, "TZ": true, "TERM": true,
}

func envAllowed(key string) bool {
	if envAllowedNames[key] {
		return true
	}
	for _, p := range envAllowedPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

func scopedEnv(extra []string) []string {
	out := make([]string, 0, len(extra)+16)
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if envAllowed(k) {
			out = append(out, kv)
		}
	}
	return append(out, extra...)
}
