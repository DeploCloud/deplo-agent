package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// The panel's stand-in for a replica: one copy, no fixed container name, the probe as its health check.
func probeStack(service, image, command, probeHost string, test string) string {
	return fmt.Sprintf(`services:
  %s:
    image: %s
    command: %s
    restart: unless-stopped
    deploy: {replicas: 1}
    healthcheck: {test: %s, interval: 30s, timeout: 5s, retries: 3, start_period: 180s, start_interval: 1s}
    volumes: ["%s:%s:ro"]
`, service, image, command, test, probeHost, probeDir)
}

func agentProbeTest(port int) string {
	return fmt.Sprintf(`["CMD", "%s/%s", "probe", "%d"]`, probeDir, probeFile, port)
}

// The panel's shell loop (lib/deploy/port-probe.ts), `$$` as compose spells a `$` for the shell.
func shellProbeTest(port int) string {
	cmd := fmt.Sprintf(`for f in /proc/net/tcp /proc/net/tcp6; do [ -r "$$f" ] || continue; `+
		`while read -r _ a _ s _; do [ "$$s" = 0A ] && [ "$${a##*:}" = %04X ] && exit 0; done < "$$f"; done; exit 1`, port)
	return fmt.Sprintf(`["CMD-SHELL", %q]`, cmd)
}

type probeRun struct {
	t    *testing.T
	s    *Service
	slug string
}

func newProbeRun(t *testing.T, slug string) *probeRun {
	t.Helper()
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	agentBinaryAt(t, captureTestBinary(t))
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if err := s.PlaceProbe(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", testNetwork, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", testNetwork)
	})
	r := &probeRun{t, s, slug}
	t.Cleanup(r.remove)
	return r
}

// beside is another App on the same server, so its containers mount the same probe folder.
func (r *probeRun) beside(slug string) *probeRun {
	o := &probeRun{r.t, r.s, slug}
	r.t.Cleanup(o.remove)
	return o
}

func (r *probeRun) remove() {
	ctx := context.Background()
	name := "deplo-" + r.slug
	for _, p := range []string{name, name + standInSuffix} {
		if ids := projectIDs(ctx, p); len(ids) > 0 {
			_, _ = dockercli.Run(ctx, 60*time.Second, append([]string{"rm", "-f"}, ids...)...)
		}
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", p+"_default")
	}
}

func projectIDs(ctx context.Context, project string) []string {
	res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-aq", "--filter", "label=com.docker.compose.project="+project)
	if err != nil {
		return nil
	}
	return splitLines(res.Stdout)
}

func (r *probeRun) probeHost() string { return filepath.Join(r.s.stackDir, filepath.Base(probeDir)) }

func (r *probeRun) deploy(image, live, standIn string, port int32, timeout time.Duration) (*pb.DeployResult, string, time.Duration) {
	r.t.Helper()
	req := &pb.DeployRequest{
		DeployId: fmt.Sprintf("dep_%s_%d", r.slug, time.Now().UnixNano()), Slug: r.slug, ProjectId: "prj_" + r.slug,
		ImageRef: image, SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_NONE, PullImage: true,
		ComposeYaml: live, StandInComposeYaml: standIn, Network: testNetwork, ReadyTimeoutMs: timeout.Milliseconds(),
		Ready: &pb.ReadyCheck{Port: port, PortRequired: true},
	}
	var res *pb.DeployResult
	rec := &e2eBuildRecorder{}
	start := time.Now()
	r.s.runDeploy(context.Background(), req, &emitter{send: func(ev *pb.DeployEvent) error {
		if x := ev.GetResult(); x != nil {
			res = x
		}
		return rec.emitter().send(ev)
	}})
	if res == nil {
		r.t.Fatalf("no result; log:\n%s", rec.joined())
	}
	return res, rec.joined(), time.Since(start)
}

// End-to-end (real docker): an image with no shell deploys beside its live copy, the way every replica of an
// App spread over a cluster does (deplo#281): the stand-in's health check is this binary, not the image's sh.
func TestE2E_ProbeLetsAShellLessImageStartBesideItself(t *testing.T) {
	r := newProbeRun(t, "deplo-agent-test-probe-noshell")
	const image = "traefik/whoami:v1.10.3"
	name := "deplo-" + r.slug
	live := probeStack(name, image, `["--port", "8080"]`, r.probeHost(), agentProbeTest(8080))
	standIn := probeStack(name+standInSuffix, image, `["--port", "8080"]`, r.probeHost(), agentProbeTest(8080))
	if res, log, _ := r.deploy(image, live, "", 8080, 2*time.Minute); !res.GetReady() {
		t.Fatalf("first deploy: %v; log:\n%s", res, log)
	}
	res, log, _ := r.deploy(image, live, standIn, 8080, 2*time.Minute)
	if !res.GetReady() || !strings.Contains(log, "beside the one") || strings.Contains(log, "offline briefly") {
		t.Fatalf("redeploy beside itself: %v; log:\n%s", res, log)
	}
	ctx := context.Background()
	ids := projectIDs(ctx, name)
	if len(ids) != 1 {
		t.Fatalf("live copies: %v", ids)
	}
	if got, _ := dockercli.Run(ctx, 15*time.Second, "inspect", "-f", "{{.State.Health.Status}}", ids[0]); strings.TrimSpace(got.Stdout) != "healthy" {
		t.Fatalf("health = %q", got.Stdout)
	}
	// The probe is no storage: a stand-in that fails must not make the App swap in place.
	if hasStorage(ctx, ids[0]) {
		t.Fatal("the probe's folder counted as the App's storage")
	}
}

// End-to-end (real docker): a container that never listens fails the ready check at the deploy's deadline, with
// the probe as with the shell loop before it: same message, same time.
func TestE2E_ProbeFailsAnAppThatNeverListensAtTheSameDeadline(t *testing.T) {
	r := newProbeRun(t, "deplo-agent-test-probe-silent")
	const image = "busybox:1.37"
	const timeout = 20 * time.Second
	name := "deplo-" + r.slug
	var took [2]time.Duration
	for i, test := range []string{shellProbeTest(3000), agentProbeTest(3000)} {
		live := probeStack(name, image, `["sleep", "3600"]`, r.probeHost(), test)
		res, log, d := r.deploy(image, live, "", 3000, timeout)
		if res.GetReady() || !strings.Contains(res.GetError(), "never passed its health check") {
			t.Fatalf("%s: %v; log:\n%s", test, res, log)
		}
		took[i] = d
		for _, id := range projectIDs(context.Background(), name) {
			_, _ = dockercli.Run(context.Background(), 60*time.Second, "rm", "-f", id)
		}
	}
	for _, d := range took {
		if d < timeout || d > timeout+15*time.Second {
			t.Fatalf("failed after %s and %s, want both at the %s deadline", took[0], took[1], timeout)
		}
	}
}

// End-to-end (real docker): a self-update renames the next release over the agent's binary and the restarted agent
// places it, never writing a file a check may be running. An App deployed before the update and one deployed
// after both keep passing.
func TestE2E_ProbeKeepsPassingAcrossASelfUpdate(t *testing.T) {
	older := newProbeRun(t, "deplo-agent-test-probe-update")
	newer := older.beside("deplo-agent-test-probe-update2")
	ctx := context.Background()
	built, err := captureExe()
	if err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(t.TempDir(), "deplo-agent")
	if err := copyExecutable(built, exe); err != nil {
		t.Fatal(err)
	}
	agentBinaryAt(t, exe)
	if err := older.s.PlaceProbe(); err != nil {
		t.Fatal(err)
	}

	const image = "traefik/whoami:v1.10.3"
	up := func(r *probeRun) string {
		name := "deplo-" + r.slug
		// Checked every second, so the older App runs checks while the binary is swapped and right after.
		stack := strings.Replace(probeStack(name, image, `["--port", "8080"]`, r.probeHost(), agentProbeTest(8080)),
			"interval: 30s", "interval: 1s", 1)
		if res, log, _ := r.deploy(image, stack, "", 8080, 2*time.Minute); !res.GetReady() {
			t.Fatalf("%s: %v; log:\n%s", name, res, log)
		}
		ids := projectIDs(ctx, name)
		if len(ids) != 1 {
			t.Fatalf("%s: containers %v", name, ids)
		}
		return ids[0]
	}
	old := up(older)

	// The next release: the same program, other bytes.
	next, err := os.ReadFile(built)
	if err != nil {
		t.Fatal(err)
	}
	next = append(next, 0)
	stubDownload(t, next, nil)
	captureReexec(t)
	if _, err := older.s.applyUpdate(ctx, exe, &pb.SelfUpdateRequest{
		Version: "999.0.0", Binaries: binariesFor("https://example/deplo-agent", sha256Hex(next)),
	}); err != nil {
		t.Fatal(err)
	}
	swapped := time.Now()
	// What the restarted agent does first.
	if err := older.s.PlaceProbe(); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(older.probeHost(), probeFile)); string(b) != string(next) {
		t.Fatal("the probe folder does not hold the update")
	}

	fresh := up(newer)
	for _, id := range []string{old, fresh} {
		passesChecksSince(t, id, swapped)
	}
}

// passesChecksSince waits for a check of container id that started after since, and fails on any failed check.
func passesChecksSince(t *testing.T, id string, since time.Time) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		res, err := dockercli.Run(context.Background(), 15*time.Second, "inspect", "-f", "{{json .State.Health}}", id)
		if err != nil {
			t.Fatal(err)
		}
		var h struct {
			Status string
			Log    []struct {
				Start    time.Time
				ExitCode int
				Output   string
			}
		}
		if err := json.Unmarshal([]byte(res.Stdout), &h); err != nil {
			t.Fatalf("health of %s: %v: %s", id, err, res.Stdout)
		}
		after := false
		for _, l := range h.Log {
			if l.ExitCode != 0 {
				t.Fatalf("a check of %s failed at %s: exit %d %q", id, l.Start, l.ExitCode, l.Output)
			}
			after = after || l.Start.After(since)
		}
		if after && h.Status == "healthy" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no passing check of %s since the update: %s", id, res.Stdout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
