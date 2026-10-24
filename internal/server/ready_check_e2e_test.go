package server

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// e2eReadyDeploy runs a busybox app through the ready check and returns its result and log.
func e2eReadyDeploy(t *testing.T, slug, command string, ready *pb.ReadyCheck) (*pb.DeployResult, string, time.Duration) {
	t.Helper()
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	name := "deplo-" + slug
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = dockercli.Run(ctx, 60*time.Second, "rm", "-f", name)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", name+"_default")
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", testNetwork, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", testNetwork)
	})
	yaml := "services:\n  " + name + ":\n    image: busybox:1.37\n    container_name: " + name + "\n" +
		"    restart: unless-stopped\n    command: [\"sh\", \"-c\", " + strconv.Quote(command) + "]\n"
	req := &pb.DeployRequest{
		DeployId: "dep_" + slug, Slug: slug, ProjectId: "prj_" + slug, ImageRef: "busybox:1.37",
		SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_NONE, PullImage: true,
		ComposeYaml: yaml, Network: testNetwork, ReadyTimeoutMs: 90_000, Ready: ready,
	}
	var res *pb.DeployResult
	rec := &e2eBuildRecorder{}
	start := time.Now()
	New(t.TempDir(), t.TempDir(), "/", "").runDeploy(ctx, req, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return rec.emitter().send(ev)
	}})
	if res == nil {
		t.Fatalf("no result; log:\n%s", rec.joined())
	}
	return res, rec.joined(), time.Since(start)
}

// End-to-end (real docker): the app listens on 8080 although 3000 is routed; the deploy is up
// and names 8080, read from the container's own sockets.
func TestE2E_ReadyCheckAdoptsTheObservedPort(t *testing.T) {
	res, log, _ := e2eReadyDeploy(t, "deplo-agent-test-ready-port", "exec httpd -f -p 8080",
		&pb.ReadyCheck{Port: 3000, PortRequired: true})
	if !res.GetReady() || res.GetObservedPort() != 8080 || !slices.Equal(res.GetListeningPorts(), []int32{8080}) {
		t.Fatalf("result = %v; log:\n%s", res, log)
	}
}

// End-to-end (real docker): Docker reports a crash-looping container as running; the ready
// check fails it fast, with what it printed.
func TestE2E_ReadyCheckFailsACrashLoop(t *testing.T) {
	res, log, took := e2eReadyDeploy(t, "deplo-agent-test-ready-crash", "echo boom-from-the-app; exit 3",
		&pb.ReadyCheck{Port: 3000, PortRequired: true})
	if res.GetReady() || !strings.Contains(res.GetError(), "(exit code 3)") || !strings.Contains(log, "boom-from-the-app") {
		t.Fatalf("result = %v; log:\n%s", res, log)
	}
	if took > 45*time.Second {
		t.Fatalf("a crash loop took %s to fail", took)
	}
}

// End-to-end (real docker): an app bound to its own loopback is unreachable for Traefik.
func TestE2E_ReadyCheckRefusesALoopbackOnlyApp(t *testing.T) {
	res, log, _ := e2eReadyDeploy(t, "deplo-agent-test-ready-loop", "exec httpd -f -p 127.0.0.1:3000",
		&pb.ReadyCheck{Port: 3000, PortRequired: true})
	if res.GetReady() || !strings.Contains(res.GetError(), "listens only on 127.0.0.1:3000") {
		t.Fatalf("result = %v; log:\n%s", res, log)
	}
}

// End-to-end (real docker): in a stack, an app that crashes until its database answers (here: its
// first three boots) is not a failed deploy; it is up once it stays up.
func TestE2E_ReadyCheckWaitsOutAStackServiceThatCrashesWhileItsDatabaseBoots(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	const slug = "deplo-agent-test-ready-stack"
	stacks, files := t.TempDir(), t.TempDir()
	s := New(stacks, files, "/", "")
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true})
		giveBackToTestUser(t, stacks)
		giveBackToTestUser(t, files)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", testNetwork, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", testNetwork)
	})
	boot := `n=$$(cat /state/boots 2>/dev/null || echo 0); n=$$((n+1)); echo $$n > /state/boots; ` +
		`[ $$n -gt 3 ] || { echo "boot $$n: the database is not answering yet"; exit 1; }; exec httpd -f -p 8080`
	yaml := "services:\n" +
		"  db:\n    image: busybox:1.37\n    labels: [\"deplo.slug=" + slug + "\"]\n    restart: unless-stopped\n" +
		"    command: [\"httpd\", \"-f\", \"-p\", \"5432\"]\n" +
		"  app:\n    image: busybox:1.37\n    labels: [\"deplo.slug=" + slug + "\"]\n    restart: unless-stopped\n" +
		"    volumes: [\"./state:/state\"]\n    command: [\"sh\", \"-c\", " + strconv.Quote(boot) + "]\n"
	var res *pb.DeployResult
	rec := &e2eBuildRecorder{}
	s.runDeploy(ctx, &pb.DeployRequest{
		DeployId: "dep_" + slug, Slug: slug, ProjectId: "prj_" + slug, ImageRef: "busybox:1.37",
		SourceKind: pb.SourceKind_SOURCE_KIND_COMPOSE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml: yaml, Network: testNetwork, ReadyTimeoutMs: 120_000,
		Ready: &pb.ReadyCheck{Port: 8080, PortRequired: true, Service: "app"},
	}, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return rec.emitter().send(ev)
	}})
	if res == nil || !res.GetReady() || !slices.Equal(res.GetListeningPorts(), []int32{8080}) {
		t.Fatalf("result = %v; log:\n%s", res, rec.joined())
	}
	got, _ := dockercli.Run(ctx, 15*time.Second, "inspect", "-f", "{{.RestartCount}}", "deplo-"+slug+"-app-1")
	if n := strings.TrimSpace(got.Stdout); n == "0" || n == "" {
		t.Fatalf("the app never restarted (RestartCount %q): the test proved nothing", n)
	}
}
