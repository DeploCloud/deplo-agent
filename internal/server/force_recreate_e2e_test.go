package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// End-to-end (real docker): what "Rebuild container" is worth. This test pins both
// halves: unchanged deploy keeps the container (that is the desirable default: an
// unchanged reroute must not restart anything), and force_recreate replaces it.
func TestE2E_ForceRecreateReplacesAnUnchangedContainer(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	const slug = "forcerecreatee2e"
	const name = "deplo-" + slug
	s := New(t.TempDir(), t.TempDir(), "/", "")

	// The smallest stack that stays up: no build, no routing, one busybox that
	// sleeps. Byte-identical across all three deploys, which is the whole point.
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
		PullImage:      true,
		ReadyTimeoutMs: 60_000,
	}

	t.Cleanup(func() {
		_, _ = dockercli.Run(context.Background(), 60*time.Second, "rm", "-f", name)
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

	// Unchanged redeploy: compose sees the same config and keeps the container.
	// This is the behaviour every ordinary deploy relies on — assert it so the
	// force below can't be mistaken for "deploys always recreate now".
	if again := deploy("unchanged redeploy"); again != first {
		t.Fatalf("an unchanged redeploy replaced the container (%s → %s); "+
			"ordinary deploys must not restart a stack that did not change", first, again)
	}

	// "Rebuild container": same stack, new container.
	req.ForceRecreate = true
	forced := deploy("forced rebuild")
	if forced == first {
		t.Fatalf("force_recreate left the SAME container running (%s) — "+
			"Rebuild container would report success without rebuilding anything", first)
	}
}
