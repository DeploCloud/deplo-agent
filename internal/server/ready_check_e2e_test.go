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
