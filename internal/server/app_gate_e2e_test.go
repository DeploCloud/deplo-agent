package server

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// gateStack is an app whose database keeps its data in a volume and has to be healthy before the app starts (#229).
func gateStack(slug, appBoot string) string {
	return "services:\n" +
		"  db:\n    image: busybox:1.37\n    labels: [\"deplo.slug=" + slug + "\"]\n    restart: unless-stopped\n" +
		"    volumes: [\"data:/data\"]\n    command: [\"sh\", \"-c\", \"touch /data/ready; exec sleep 3600\"]\n" +
		"    healthcheck: {test: [\"CMD\", \"test\", \"-f\", \"/data/ready\"], interval: 1s, timeout: 1s, retries: 1}\n" +
		"  app:\n    image: busybox:1.37\n    labels: [\"deplo.slug=" + slug + "\"]\n    restart: unless-stopped\n" +
		"    depends_on: {db: {condition: service_healthy}}\n" +
		"    command: [\"sh\", \"-c\", " + strconv.Quote(appBoot) + "]\n" +
		"volumes:\n  data: {}\n"
}

type gateRun struct {
	t    *testing.T
	s    *Service
	slug string
}

func newGateRun(t *testing.T, slug string) *gateRun {
	t.Helper()
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	stacks, files := t.TempDir(), t.TempDir()
	s := New(stacks, files, "/", t.TempDir())
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true})
		_, _ = dockercli.Run(ctx, 20*time.Second, "volume", "rm", "-f", "deplo-"+slug+"_data")
		giveBackToTestUser(t, stacks)
		giveBackToTestUser(t, files)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", testNetwork, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", testNetwork)
	})
	return &gateRun{t, s, slug}
}

func (r *gateRun) deploy(appBoot string) (*pb.DeployResult, string) {
	r.t.Helper()
	var res *pb.DeployResult
	rec := &e2eBuildRecorder{}
	r.s.runDeploy(context.Background(), &pb.DeployRequest{
		DeployId: "dep_" + r.slug + strconv.FormatInt(time.Now().UnixNano(), 36), Slug: r.slug, ProjectId: "prj_" + r.slug,
		ImageRef: "busybox:1.37", SourceKind: pb.SourceKind_SOURCE_KIND_COMPOSE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml: gateStack(r.slug, appBoot), Network: testNetwork, ReadyTimeoutMs: 120_000, ForceRecreate: true,
		Ready: &pb.ReadyCheck{Port: 8080, PortRequired: true, Service: "app"},
	}, &emitter{send: func(ev *pb.DeployEvent) error {
		if x := ev.GetResult(); x != nil {
			res = x
		}
		return rec.emitter().send(ev)
	}})
	if res == nil {
		r.t.Fatalf("no result; log:\n%s", rec.joined())
	}
	return res, rec.joined()
}

// The compose project is deplo-<slug>, so its volume is <project>_data.
func (r *gateRun) volume() string { return "deplo-" + r.slug + "_data" }

func (r *gateRun) dbHealth() string {
	got, _ := dockercli.Run(context.Background(), 15*time.Second, "inspect", "-f", "{{.State.Status}} {{.State.Health.Status}}", "deplo-"+r.slug+"-db-1")
	return strings.TrimSpace(got.Stdout)
}

// End-to-end (real docker): a deploy that lands while a backup has the database paused waits for it, then succeeds.
func TestE2E_ADeployDuringABackupsPauseWaitsAndSucceeds(t *testing.T) {
	r := newGateRun(t, "agent-test-gate-deploy")
	const boot = "exec httpd -f -p 8080"
	if res, log := r.deploy(boot); !res.GetReady() {
		t.Fatalf("first deploy: %v; log:\n%s", res, log)
	}
	h, err := r.s.holdVolumes(context.Background(), r.slug, []string{r.volume()}, func(string) {})
	if err != nil || h == nil {
		t.Fatalf("hold: %v (volume %q)", err, r.volume())
	}
	health := make(chan string, 1)
	go func() {
		deadline := time.Now().Add(8 * time.Second)
		seen := r.dbHealth()
		for time.Now().Before(deadline) && !strings.HasSuffix(seen, "unhealthy") {
			time.Sleep(250 * time.Millisecond)
			seen = r.dbHealth()
		}
		health <- seen
		h.release()
	}()
	time.Sleep(500 * time.Millisecond)
	res, log := r.deploy(boot)
	seen := <-health
	if !res.GetReady() {
		t.Fatalf("the deploy during the pause failed: %v (database while paused: %q); log:\n%s", res, seen, log)
	}
	if !strings.Contains(log, "A backup of this app has paused it to copy its data, so the deploy waits until it runs again (10 minutes at most).") ||
		!strings.Contains(log, "for the backup to finish.") {
		t.Fatalf("the log does not say it waited for the backup:\n%s", log)
	}
	if !strings.HasSuffix(seen, "unhealthy") {
		t.Logf("the paused database never read unhealthy (%q): docker skipped its checks while paused", seen)
	}
}

// End-to-end (real docker): a backup that starts while a deploy is under way waits for the deploy before it pauses anything.
func TestE2E_ABackupDuringADeployWaitsForIt(t *testing.T) {
	r := newGateRun(t, "agent-test-gate-backup")
	if res, log := r.deploy("exec httpd -f -p 8080"); !res.GetReady() {
		t.Fatalf("first deploy: %v; log:\n%s", res, log)
	}
	finished := make(chan time.Time, 1)
	go func() {
		res, log := r.deploy("sleep 6; exec httpd -f -p 8080")
		if !res.GetReady() {
			t.Errorf("the slow deploy failed: %v; log:\n%s", res, log)
		}
		finished <- time.Now()
	}()
	time.Sleep(2 * time.Second)
	var logs []string
	h, err := r.s.holdVolumes(context.Background(), r.slug, []string{r.volume()}, func(s string) { logs = append(logs, s) })
	paused := time.Now()
	if err != nil || h == nil {
		t.Fatalf("hold: %v", err)
	}
	h.release()
	done := <-finished
	if paused.Before(done.Add(-time.Second)) {
		t.Fatalf("the backup paused the app at %s, before the deploy ended at %s", paused, done)
	}
	if joined := strings.Join(logs, "\n"); !strings.Contains(joined, "A deploy of this app is under way") || !strings.Contains(joined, "for the deploy to finish.") {
		t.Fatalf("backup log:\n%s", joined)
	}
}
