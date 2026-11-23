package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A container whose image left /data unnamed: its content lands in the named volume, the old one stays.
func TestE2E_AdoptVolumeCarriesTheUnnamedVolume(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	tag := fmt.Sprintf("adopt-e2e-%d", time.Now().UnixNano())
	project, volume := "deplo-"+tag, "deplo-"+tag+"-data"
	run := func(args ...string) dockercli.Result {
		res, err := dockercli.Run(ctx, 2*time.Minute, args...)
		if err != nil {
			t.Fatalf("docker %v: %v", args, err)
		}
		return res
	}
	t.Cleanup(func() {
		ids := strings.Fields(run("ps", "-aq", "--filter", "label=com.docker.compose.project="+project).Stdout)
		for _, id := range ids {
			run("rm", "-f", "-v", id)
		}
		run("volume", "rm", "-f", volume)
	})
	ensureVolumeHelperImage(ctx)
	if res := run("run", "-d", "--label", "com.docker.compose.project="+project, "--label", "com.docker.compose.service=db",
		"-v", "/data", volumeHelperImage, "sh", "-c", "echo kept > /data/row && sleep 600"); res.Code != 0 {
		t.Fatalf("start: %s", res.Stderr)
	}
	time.Sleep(time.Second)
	a := &pb.VolumeAdoption{Service: "db", Target: "/data", Volume: volume}
	real := func(ctx context.Context, timeout time.Duration, args ...string) (dockercli.Result, error) {
		if args[0] == "run" {
			args = volumeHelperRun(ctx, args[1:]...)
		}
		return dockercli.Run(ctx, timeout, args...)
	}
	carried, err := adoptVolume(ctx, real, project, a)
	if err != nil || !carried {
		t.Fatalf("carried=%v err=%v", carried, err)
	}
	if got := run("run", "--rm", "-v", volume+":/v:ro", volumeHelperImage, "cat", "/v/row").Stdout; strings.TrimSpace(got) != "kept" {
		t.Fatalf("named volume holds %q", got)
	}
	if again, err := adoptVolume(ctx, real, project, a); err != nil || again {
		t.Fatalf("second deploy carried again: %v %v", again, err)
	}
}
