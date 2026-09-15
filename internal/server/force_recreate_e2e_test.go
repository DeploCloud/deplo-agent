package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// End-to-end (real docker): what "Rebuild container" is worth.
func TestE2E_ForceRecreateReplacesAnUnchangedContainer(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	const slug = "forcerecreatee2e"
	const name = "deplo-" + slug
	s := New(t.TempDir(), t.TempDir(), "/", "")

	yaml := "services:\n" +
		"  " + name + ":\n" +
		"    image: busybox:latest\n" +
		"    container_name: " + name + "\n" +
		"    command: [\"sleep\", \"600\"]\n"

	req := &pb.DeployRequest{
		DeployId:       "dep_force_e2e",
		Slug:           slug,
		ProjectId:      "prj_" + slug,
		ImageRef:       "busybox:latest",
		SourceKind:     pb.SourceKind_SOURCE_KIND_IMAGE,
		BuildKind:      pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml:    yaml,
		Network:        testNetwork,
		PullImage:      true,
		ReadyTimeoutMs: 60_000,
	}

	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = dockercli.Run(ctx, 60*time.Second, "rm", "-f", name)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", testNetwork, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", testNetwork)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", "deplo-"+slug+"_default")
	})

	deploy := func(step string) string {
		rec := &e2eBuildRecorder{}
		var ok bool
		var failure string
		e := &emitter{send: func(ev *pb.DeployEvent) error {
			if r := ev.GetResult(); r != nil {
				ok, failure = r.GetReady(), r.GetError()
			}
			return rec.emitter().send(ev)
		}}
		s.runDeploy(ctx, req, e)
		if !ok {
			t.Fatalf("%s: deploy failed (%s); log:\n%s", step, failure, rec.joined())
		}
		out, err := dockercli.Run(ctx, 30*time.Second, "inspect", "-f", "{{.Id}}", name)
		if err != nil || out.Code != 0 {
			t.Fatalf("%s: inspect: %v %s", step, err, out.Stderr)
		}
		return strings.TrimSpace(out.Stdout)
	}

	first := deploy("first deploy")
	if first == "" {
		t.Fatal("no container after the first deploy")
	}

	if again := deploy("unchanged redeploy"); again != first {
		t.Fatalf("an unchanged redeploy replaced the container (%s → %s); "+
			"ordinary deploys must not restart a stack that did not change", first, again)
	}

	req.ForceRecreate = true
	forced := deploy("forced rebuild")
	if forced == first {
		t.Fatalf("force_recreate left the SAME container running (%s) - "+
			"Rebuild container would report success without rebuilding anything", first)
	}
}
