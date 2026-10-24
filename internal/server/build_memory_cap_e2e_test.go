package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// End-to-end (real docker): a RUN step that outgrows the cap is killed with exit code 137 and
// the deploy says how much memory a build may use. The cap is shrunk so the test stays small.
func TestE2E_BuildMemoryCapStopsAGreedyStep(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	prevDir, prevMem := buildStepCgroupDir, hostMemTotal
	buildStepCgroupDir, hostMemTotal = "/sys/fs/cgroup/system.slice", func() int64 { return 128 << 20 }
	t.Cleanup(func() { buildStepCgroupDir, hostMemTotal = prevDir, prevMem })
	if why := buildCapLayout(buildStepCgroupDir); why != "" {
		t.Skipf("no build memory cap on this host: %s", why)
	}

	const image = "deplo-agent-test-memcap:e2e"
	buildDir := t.TempDir()
	mustWrite(t, buildDir+"/Dockerfile", "FROM busybox:1.37\nRUN sleep 2 && head -c 400m /dev/zero | tail > /dev/null\n")
	t.Cleanup(func() { _, _ = dockercli.Run(context.Background(), 60*time.Second, "rmi", "-f", image) })

	var res *pb.DeployResult
	rec := &e2eBuildRecorder{}
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return rec.emitter().send(ev)
	}}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	req := &pb.DeployRequest{
		Slug: "deplo-agent-test-memcap", ProjectId: "prj_deplo-agent-test-memcap", ImageRef: image, NoBuildCache: true,
		BuildKind: pb.BuildKind_BUILD_KIND_DOCKERFILE, Dockerfile: &pb.DockerfileBuild{DockerfilePath: "Dockerfile"},
	}
	if s.buildImage(ctx, req, buildDir, e) {
		t.Fatalf("the greedy step survived the cap; log:\n%s", rec.joined())
	}
	if res == nil || !strings.Contains(res.GetError(), "The build needed more than 0.1 GB of memory") {
		t.Fatalf("result = %v; log:\n%s", res, rec.joined())
	}
}
