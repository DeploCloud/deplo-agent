package dockercli

import (
	"context"
	"strings"
	"testing"
	"time"
)

// When the caller's context is cancelled mid-run, CommandContext SIGKILLs the child and
// Wait() returns an *exec.ExitError with ExitCode()==-1.
func TestStream_cancellationReportsClearError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel almost immediately so the child is killed during/just after spawn.
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	// `docker logs -f` follows forever, guaranteeing the cancel lands mid-run on
	// a host with docker. The container name is bogus; either it blocks on
	// follow (cancelled) or docker errors fast (spawn/daemon path).
	_, err := Stream(ctx, 30*time.Second, func(string) {}, "", "logs", "-f", "deplo-nonexistent-cancel-test")
	if err == nil {
		t.Skip("command completed before cancellation (no docker / fast error path)")
	}
	// Accept either the explicit cancellation message OR a docker spawn/daemon error
	// (docker absent) - both are non-"-1"-exit error paths.
	if ctx.Err() == context.Canceled && !strings.Contains(err.Error(), "canceled") &&
		!strings.Contains(err.Error(), "Cannot connect") && !strings.Contains(err.Error(), "docker") {
		t.Fatalf("cancellation should surface a clear error, got: %v", err)
	}
}

// TraefikRunning detects a running Traefik container on the host (what routes
// deploys). Verified against real docker: false with none present, true once a
// throwaway traefik is up. Skips cleanly when docker is unavailable.
func TestTraefikRunning(t *testing.T) {
	ctx := context.Background()
	if !Available(ctx) {
		t.Skip("docker unavailable")
	}
	const name = "deplo-traefik-dockercli-test"
	// Best-effort cleanup of any leftover from a prior run.
	_, _ = Run(ctx, 15*time.Second, "rm", "-f", name)

	// Start a throwaway traefik (no ports, just `version` then sleep so the image
	// name shows in `docker ps`). The detection matches the image substring.
	res, err := Run(ctx, 60*time.Second, "run", "-d", "--name", name,
		"--entrypoint", "sleep", "traefik:v3.7", "30")
	if err != nil || res.Code != 0 {
		t.Skipf("could not start traefik test container (no image/pull): %v %s", err, res.Stderr)
	}
	t.Cleanup(func() { _, _ = Run(context.Background(), 15*time.Second, "rm", "-f", name) })

	if !TraefikRunning(ctx) {
		t.Fatal("TraefikRunning = false with a traefik container running, want true")
	}

	if _, err := Run(ctx, 15*time.Second, "rm", "-f", name); err != nil {
		t.Fatalf("cleanup rm: %v", err)
	}
	// A brief settle, then it should read false again (assuming no OTHER traefik
	// runs on this host; if one does, skip rather than fail).
	if TraefikRunning(ctx) {
		t.Skip("another traefik is running on this host; can't assert the false case")
	}
}

// ImageExportOptsSupported decides whether builds may pass `--output type=image,…` - a
// flag the containerd image store accepts and the classic graphdriver store rejects
// outright, so a wrong answer breaks every build rather than merely slowing one.
func TestImageExportOptsSupported(t *testing.T) {
	ctx := context.Background()
	if !Available(ctx) {
		t.Skip("docker unavailable")
	}
	resetImageExportProbe()
	t.Cleanup(resetImageExportProbe)

	info, err := Run(ctx, 15*time.Second, "info", "--format", "{{json .DriverStatus}}")
	if err != nil || info.Code != 0 {
		t.Skipf("could not read docker info: %v %s", err, info.Stderr)
	}
	containerd := strings.Contains(info.Stdout, "io.containerd.snapshotter.v1")
	bx, err := Run(ctx, 15*time.Second, "buildx", "version")
	if err != nil {
		t.Skip("could not probe buildx")
	}
	want := containerd && bx.Code == 0

	if got := ImageExportOptsSupported(ctx); got != want {
		t.Fatalf("ImageExportOptsSupported = %v; daemon says containerd=%v buildx=%v",
			got, containerd, bx.Code == 0)
	}

	// Sticky: with the probe cached, a context that is already dead must still
	// return the same answer - proof no further docker call is made.
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if got := ImageExportOptsSupported(dead); got != want {
		t.Fatalf("cached probe re-ran (got %v on a cancelled ctx; want %v)", got, want)
	}
}

// An inconclusive probe (docker unreachable) must NOT be cached: a daemon that
// was momentarily down would otherwise be treated as slow for the life of the
// agent, silently costing every later build the compression tax.
func TestImageExportProbeDoesNotCacheInconclusive(t *testing.T) {
	resetImageExportProbe()
	t.Cleanup(resetImageExportProbe)

	// A cancelled context makes `docker info` fail to run at all - the
	// inconclusive path.
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if ImageExportOptsSupported(dead) {
		t.Fatal("an unreachable daemon must report false")
	}
	imageExportMu.Lock()
	known := imageExportKnown
	imageExportMu.Unlock()
	if known {
		t.Fatal("an inconclusive probe must not be cached")
	}
}

// BuildCachePruneCap must name a flag family the local CLI ACTUALLY accepts: buildx
// renamed `--keep-storage` to `--max-used-space`/`--min-free-space`, and Docker 29
// dropped the old name, so guessing turns a routine cleanup sweep into a hard error.
func TestBuildCachePruneCap(t *testing.T) {
	ctx := context.Background()
	if !Available(ctx) {
		t.Skip("docker unavailable")
	}
	resetPruneCapProbe()
	t.Cleanup(resetPruneCapProbe)

	mode := BuildCachePruneCap(ctx)
	var args []string
	switch mode {
	case PruneCapModern:
		// 1 PB ceiling / 1 byte free target: accepted, reclaims nothing.
		args = []string{"builder", "prune", "--force", "--max-used-space", "1000000000000000", "--min-free-space", "1"}
	case PruneCapLegacy:
		args = []string{"builder", "prune", "--force", "--keep-storage", "1000000000000000"}
	case PruneCapNone:
		t.Skip("this CLI takes no size cap; nothing to verify")
	default:
		t.Fatalf("unknown prune-cap mode %q", mode)
	}
	res, err := Run(ctx, 60*time.Second, args...)
	if err != nil {
		t.Fatalf("probe chose %q but %v failed to run: %v", mode, args, err)
	}
	if res.Code != 0 {
		t.Fatalf("probe chose %q but docker rejected %v: %s", mode, args, strings.TrimSpace(res.Stderr))
	}

	// Sticky: a cancelled context must still yield the same answer, proving the
	// probe is not re-run per sweep.
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if got := BuildCachePruneCap(dead); got != mode {
		t.Fatalf("cached probe re-ran: got %q, want %q", got, mode)
	}
}

// redactArgs must mask any secret-bearing token so a failed dump/restore never
// echoes a cleartext password into an error string the control plane logs.
func TestRedactArgs(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		// Inline KEY=VALUE secret env: value hidden, key kept.
		{[]string{"exec", "-e", "PGPASSWORD=hunter2", "c", "pg_dump"}, "exec -e PGPASSWORD=*** c pg_dump"},
		{[]string{"exec", "-e", "MYSQL_PWD=s3cret", "c"}, "exec -e MYSQL_PWD=*** c"},
		// Bare -e NAME (no value) is NOT a secret value - left intact.
		{[]string{"exec", "-e", "PGPASSWORD", "c", "pg_dump"}, "exec -e PGPASSWORD c pg_dump"},
		// Value after a secret flag is masked.
		{[]string{"exec", "c", "redis-cli", "-a", "topsecret", "PING"}, "exec c redis-cli -a *** PING"},
		{[]string{"exec", "c", "mongodump", "-p", "pw", "--archive"}, "exec c mongodump -p *** --archive"},
		// A non-secret env (no PASSWORD/SECRET) is untouched.
		{[]string{"exec", "-e", "PGHOST=db", "c"}, "exec -e PGHOST=db c"},
	}
	for _, tc := range cases {
		if got := redactArgs(tc.in); got != tc.want {
			t.Errorf("redactArgs(%v)\n  got  %q\n  want %q", tc.in, got, tc.want)
		}
	}
}
