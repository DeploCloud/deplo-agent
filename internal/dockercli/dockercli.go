// Package dockercli is the agent's Docker client: it shells out to the `docker` CLI
// against the host's daemon, exactly as the control plane's lib/infra/docker.ts does
// today. This is the host-coupled half of the platform moved server-side (ADR-0006).
package dockercli

import (
	"bufio"
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

// Run executes `docker <args>` to completion, returning combined output and the
// exit code. A nil error with a non-zero code means docker ran and the command
// failed; a non-nil error means docker itself could not run (spawn/timeout).
type Result struct {
	Stdout string
	Stderr string
	Code   int
}

// Run runs `docker <args>` with a timeout, capturing output.
func Run(ctx context.Context, timeout time.Duration, args ...string) (Result, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	res := Result{Stdout: out.String(), Stderr: errb.String()}
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			res.Code = ee.ExitCode()
			return res, nil // ran, exited non-zero
		}
		// Spawn failure or timeout: docker never produced an exit status.
		return res, fmt.Errorf("docker %s failed: %w (%s)", strings.Join(args, " "), err, errb.String())
	}
	return res, nil
}

// RunEnv is Run with extra "KEY=VALUE" host-process env layered on (e.g. so a `docker
// exec -e REDISCLI_AUTH <c> redis-cli …` can forward the password from the docker
// client's env into the container without the value touching argv).
func RunEnv(ctx context.Context, timeout time.Duration, extraEnv []string, args ...string) (Result, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	res := Result{Stdout: out.String(), Stderr: errb.String()}
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			res.Code = ee.ExitCode()
			return res, nil
		}
		return res, fmt.Errorf("docker %s failed: %w (%s)", redactArgs(args), err, errb.String())
	}
	return res, nil
}

// Stream runs `docker <args>` and forwards each line of merged stdout+stderr to onLine
// as it is produced (the live build/clone log), returning the exit code.
func Stream(ctx context.Context, timeout time.Duration, onLine LineFn, input string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return streamCmd(cctx, timeout, onLine, input, exec.CommandContext(cctx, "docker", args...))
}

// StreamEnv is Stream with extra "KEY=VALUE" environment entries layered on top
// of the agent's own env (e.g. DOCKER_BUILDKIT=1 for the nixpacks generated
// Dockerfile, which uses BuildKit syntax). Mirrors spawnStream's `env` option.
func StreamEnv(ctx context.Context, timeout time.Duration, onLine LineFn, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	cmd.Env = append(os.Environ(), extraEnv...)
	return streamCmd(cctx, timeout, onLine, "", cmd)
}

// Spawn runs an ARBITRARY host binary (not docker) and streams its merged output,
// for build tools that run on the host rather than via the daemon - e.g. the
// nixpacks binary (lazily installed). Same streaming/timeout discipline as Stream.
func Spawn(ctx context.Context, timeout time.Duration, onLine LineFn, input, name string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return streamCmd(cctx, timeout, onLine, input, exec.CommandContext(cctx, name, args...))
}

// SpawnEnv is Spawn with extra "KEY=VALUE" env entries layered on top of the agent's
// own env - e.g. the nixpacks binary resolving bare `--env KEY` refs from its process
// env, so a build-env VALUE never rides argv (which the deploy log echoes).
func SpawnEnv(ctx context.Context, timeout time.Duration, onLine LineFn, extraEnv []string, name string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, name, args...)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	return streamCmd(cctx, timeout, onLine, "", cmd)
}

// StreamOut runs `docker <args>` (no shell - argv is injection-safe) with extra
// "KEY=VALUE" env layered on, streaming the child's RAW stdout into dst (e.g. a build
// image tar written straight to disk) while forwarding each stderr line to onLine (the
// live progress log).
func StreamOut(ctx context.Context, timeout time.Duration, dst io.Writer, onLine LineFn, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	label := redactArgs(args)
	cmd.Stdout = dst
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return -1, err
	}
	if err := cmd.Start(); err != nil {
		return -1, fmt.Errorf("docker %s: %w", label, err)
	}
	sc := bufio.NewScanner(stderr)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		onLine(strings.TrimRight(sc.Text(), "\r"))
	}
	err = cmd.Wait()
	if err != nil {
		// Context first: a timeout/cancel kill surfaces as an ExitError with a
		// negative code, which the ExitError branch would otherwise misreport.
		if cctx.Err() == context.DeadlineExceeded {
			return -1, fmt.Errorf("docker %s timed out after %s", label, timeout)
		}
		if cctx.Err() == context.Canceled {
			return -1, fmt.Errorf("docker %s canceled", label)
		}
		if ee, ok := err.(*exec.ExitError); ok && ee.ProcessState.Exited() {
			return ee.ExitCode(), nil
		}
		return -1, err
	}
	return 0, nil
}

// streamCmd is the shared core of Stream/StreamEnv/Spawn: start an already-built
// command, fan its stdout+stderr into onLine line-by-line, and map the exit /
// timeout / cancellation outcome the same way for every caller.
func streamCmd(cctx context.Context, timeout time.Duration, onLine LineFn, input string, cmd *exec.Cmd) (int, error) {
	label := strings.Join(cmd.Args, " ")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return -1, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return -1, err
	}
	if input != "" {
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return -1, err
		}
		go func() {
			defer stdin.Close()
			io.WriteString(stdin, input)
		}()
	}

	if err := cmd.Start(); err != nil {
		return -1, fmt.Errorf("%s: %w", label, err)
	}

	// Merge both streams line-by-line. A small fan-in over two scanners keeps
	// ordering close to emission order, same as the TS flush().
	done := make(chan struct{}, 2)
	scan := func(r io.Reader) {
		defer func() { done <- struct{}{} }()
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		for sc.Scan() {
			onLine(strings.TrimRight(sc.Text(), "\r"))
		}
	}
	go scan(stdout)
	go scan(stderr)
	<-done
	<-done

	err = cmd.Wait()
	if err != nil {
		// Check the context FIRST: when CommandContext kills the child on timeout or
		// cancellation, Wait() returns an *exec.ExitError with ExitCode()==-1 (SIGKILL,
		// ProcessState.Exited()==false).
		if cctx.Err() == context.DeadlineExceeded {
			return -1, fmt.Errorf("%s timed out after %s", label, timeout)
		}
		if cctx.Err() == context.Canceled {
			return -1, fmt.Errorf("%s canceled", label)
		}
		// A genuine non-zero exit: the process ran and failed (ExitCode>=0).
		if ee, ok := err.(*exec.ExitError); ok && ee.ProcessState.Exited() {
			return ee.ExitCode(), nil
		}
		return -1, err
	}
	return 0, nil
}

// PipeOut runs `docker <args>` and copies the child's RAW stdout into `dst` (bytes, not
// lines) while collecting stderr for diagnostics - for piping a dump tool's output
// (`docker exec <c> pg_dump …`) straight into the gzip→S3 pipeline with no temp file.
func PipeOut(ctx context.Context, timeout time.Duration, dst io.Writer, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
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

// PipeIn runs `docker <args>` feeding `src` to the child's stdin (the restore
// direction: a decompressed dump streamed into `docker exec -i <c> psql …`),
// collecting stderr. Same exit-code/error discipline as PipeOut.
func PipeIn(ctx context.Context, timeout time.Duration, src io.Reader, extraEnv []string, args ...string) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
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

// redactArgs renders an argv for an error message with any secret-bearing token masked,
// so a failed dump/restore (e.g. on bad credentials) never echoes a cleartext password
// into an error string that the control plane logs.
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
	// Trailing component for an `-e KEY=` is the env name; match the well-known
	// DB-credential vars the backup paths use plus a generic PASSWORD substring.
	return k == "PGPASSWORD" || k == "MYSQL_PWD" || k == "REDISCLI_AUTH" ||
		k == "MONGODB_PASSWORD" || strings.Contains(k, "PASSWORD") || strings.Contains(k, "SECRET")
}

// Available reports whether the Docker daemon is reachable. Never errors.
func Available(ctx context.Context) bool {
	res, err := Run(ctx, 5*time.Second, "version", "--format", "{{.Server.Version}}")
	return err == nil && res.Code == 0
}

// ServerVersion returns the Docker engine version, or "" if unreachable.
func ServerVersion(ctx context.Context) string {
	res, err := Run(ctx, 5*time.Second, "version", "--format", "{{.Server.Version}}")
	if err != nil || res.Code != 0 {
		return ""
	}
	return strings.TrimSpace(res.Stdout)
}

// --- build-export capability -----------------------------------------------

// Daemons that keep their images in containerd pay a heavy, invisible tax on every
// build: BuildKit's `image` exporter GZIPs each new layer into the content store before
// unpacking it back into the snapshotter.
var (
	imageExportMu    sync.Mutex
	imageExportKnown bool
	imageExportOK    bool
)

// ImageExportOptsSupported reports whether `docker build --output type=image,…` is
// available on this host - i.e. the daemon keeps images in containerd AND the CLI has
// the buildx plugin that accepts the flag.
func ImageExportOptsSupported(ctx context.Context) bool {
	imageExportMu.Lock()
	defer imageExportMu.Unlock()
	if imageExportKnown {
		return imageExportOK
	}
	// `docker info` renders the storage driver's status as [[key value] …]; the
	// containerd image store announces itself with this driver-type row.
	res, err := Run(ctx, 15*time.Second, "info", "--format", "{{json .DriverStatus}}")
	if err != nil || res.Code != 0 {
		return false // inconclusive - do not cache
	}
	if !strings.Contains(res.Stdout, "io.containerd.snapshotter.v1") {
		imageExportKnown, imageExportOK = true, false
		return false
	}
	bx, err := Run(ctx, 15*time.Second, "buildx", "version")
	if err != nil {
		return false // inconclusive - do not cache
	}
	imageExportKnown, imageExportOK = true, bx.Code == 0
	return imageExportOK
}

// resetImageExportProbe clears the cached probe. Tests only.
func resetImageExportProbe() {
	imageExportMu.Lock()
	defer imageExportMu.Unlock()
	imageExportKnown, imageExportOK = false, false
}

// Build-cache size caps. Docker 29 has dropped the old name entirely, so the flag a
// host accepts has to be asked for rather than assumed - passing the wrong one turns a
// routine sweep into a hard error.
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

// BuildCachePruneCap reports which size-ceiling flags `docker builder prune`
// accepts here. Cached for the life of the agent (the CLI does not change under
// a running agent); an inconclusive probe is not cached.
func BuildCachePruneCap(ctx context.Context) string {
	pruneCapMu.Lock()
	defer pruneCapMu.Unlock()
	if pruneCapKnown {
		return pruneCapMode
	}
	res, err := Run(ctx, 15*time.Second, "builder", "prune", "--help")
	if err != nil {
		return PruneCapNone // inconclusive - do not cache
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

// resetPruneCapProbe clears the cached probe. Tests only.
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
	// Two deploys of the same Environment start at once, both look and both create.
	// The loser is not a failure: the network it wanted now exists.
	if res.Code != 0 && !strings.Contains(res.Stderr, "already exists") {
		return fmt.Errorf("docker network create %s failed: %s", name, res.Stderr)
	}
	return nil
}

// ConnectNetwork attaches a container to a network. Already being on it is success,
// not an error - every deploy re-asserts this and Docker answers with a message
// rather than a code we can distinguish.
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

// DeploNetworks lists the tenant networks Deplo manages on this host - the ones a
// recreated Traefik has to be put back on. The platform's own (`deplo`,
// `deplo-internal`, `deplo-socket`) are declared in Traefik's compose file and are
// deliberately NOT in this list.
func DeploNetworks(ctx context.Context) []string {
	res, err := Run(ctx, 10*time.Second, "network", "ls", "--format", "{{.Name}}")
	if err != nil || res.Code != 0 {
		return nil
	}
	var out []string
	for _, line := range strings.Split(res.Stdout, "\n") {
		n := strings.TrimSpace(line)
		if IsTenantNetwork(n) {
			out = append(out, n)
		}
	}
	return out
}

// IsTenantNetwork reports whether a network name is one Deplo mints for an
// Environment, a team or a preview - never the platform's own.
func IsTenantNetwork(name string) bool {
	return strings.HasPrefix(name, "deplo-env-") ||
		strings.HasPrefix(name, "deplo-team-") ||
		strings.HasPrefix(name, "deplo-preview-")
}

// RunningContainers counts containers in the running state. Best-effort: returns
// 0 on any failure.
func RunningContainers(ctx context.Context) int {
	res, err := Run(ctx, 10*time.Second, "ps", "-q")
	if err != nil || res.Code != 0 {
		return 0
	}
	n := 0
	for _, l := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		if strings.TrimSpace(l) != "" {
			n++
		}
	}
	return n
}

// TraefikRunning reports whether a Traefik reverse proxy container is running on this
// host.
func TraefikRunning(ctx context.Context) bool {
	res, err := Run(ctx, 5*time.Second, "ps", "--filter", "status=running",
		"--format", "{{.Image}}\t{{.Names}}")
	if err != nil || res.Code != 0 {
		return false
	}
	for _, line := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		// Match the image repo (traefik, traefik:v3.7, library/traefik, …) or a
		// container named *traefik* - covers the deplo-traefik instance and a
		// bring-your-own proxy alike.
		low := strings.ToLower(line)
		if strings.Contains(low, "traefik") {
			return true
		}
	}
	return false
}

// IsRunning reports whether a named container is in the running state. Used by
// the deploy readiness wait and Inspect. Never errors (false on any failure).
func IsRunning(ctx context.Context, name string) bool {
	res, err := Run(ctx, 5*time.Second, "inspect", "-f", "{{.State.Running}}", name)
	if err != nil || res.Code != 0 {
		return false
	}
	return strings.TrimSpace(res.Stdout) == "true"
}

// State returns (exists, runtimeState) for a container, e.g. ("running"). exists
// is false when docker has no such container.
func State(ctx context.Context, name string) (bool, string) {
	res, err := Run(ctx, 5*time.Second, "inspect", "-f", "{{.State.Status}}", name)
	if err != nil || res.Code != 0 {
		return false, ""
	}
	return true, strings.TrimSpace(res.Stdout)
}

// StackRunning reports whether ANY container of a Deplo stack is running, keyed by the
// deplo.slug label rather than a container name.
func StackRunning(ctx context.Context, slug string) bool {
	res, err := Run(ctx, 5*time.Second, "ps", "-q",
		"--filter", "label=deplo.slug="+slug,
		"--filter", "status=running")
	if err != nil || res.Code != 0 {
		return false
	}
	return strings.TrimSpace(res.Stdout) != ""
}

// NetworkHeadroom reports how close this host is to running out of docker
// networks, as a warning to print, or "" when there is room.
//
// Docker's built-in pools give about 31 networks. Deplo now spends one per
// Environment, plus each network a compose file declares and one per open preview,
// so a host installed before the installer began widening the pools reaches that
// ceiling quietly - and the first sign is a deploy failing with an address-pool
// error nobody can act on after the fact.
func NetworkHeadroom(ctx context.Context) string {
	res, err := Run(ctx, 10*time.Second, "network", "ls", "-q")
	if err != nil || res.Code != 0 {
		return ""
	}
	count := 0
	for _, l := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		if strings.TrimSpace(l) != "" {
			count++
		}
	}
	// A pool of its own says how many networks fit; without one, docker's built-in
	// pools give about 31 in practice.
	ceiling, widened := addressPoolCapacity(ctx), true
	if ceiling == 0 {
		ceiling, widened = 31, false
	}
	// Warn with a few to spare, so there is time to act before a deploy fails.
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

// addressPoolCapacity is how many networks the daemon's own pools can carve, or 0
// when it has none.
//
// Asked of `docker info`, which reports them WHOLE: reading daemon.json saw
// nothing of a pool passed as a daemon flag, and a pool of any size at all - one
// /24 included - counted as room for thousands.
func addressPoolCapacity(ctx context.Context) int {
	res, err := Run(ctx, 10*time.Second, "info", "--format", "{{json .DefaultAddressPools}}")
	if err != nil || res.Code != 0 {
		return 0
	}
	return parseAddressPools(res.Stdout)
}

// parseAddressPools counts the networks a `DefaultAddressPools` document can carve.
// `null` - the daemon running on its built-in pools - is 0.
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
			// Past a million subnets the host runs out of everything else first, and
			// the shift stays inside an int on every platform.
			bits = 20
		}
		total += 1 << bits
	}
	return total
}
