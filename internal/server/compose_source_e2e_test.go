package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// End-to-end (real docker): a stack built from its source comes up from the working copy, keeps
// what its services wrote there across a redeploy, and goes with DestroyStack.
func TestE2E_ComposeFromSourceKeepsItsDataUntilDestroyed(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	const slug = "deplo-agent-test-cs"
	const image = "deplo/" + slug + "-web:e2e"
	s := New(t.TempDir(), t.TempDir(), "/", "")
	wc := s.srcDir(slug)
	t.Cleanup(func() {
		ctx := context.Background()
		giveBackToTestUser(t, wc)
		_, _ = s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true})
		_, _ = dockercli.Run(ctx, 60*time.Second, "rmi", "-f", image)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", testNetwork, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", testNetwork)
	})
	yaml := "services:\n  web:\n    build:\n      context: " + wc + "\n    image: " + image + "\n" +
		"    labels: [\"deplo.slug=" + slug + "\", \"deplo.managed=true\"]\n    restart: unless-stopped\n" +
		"    volumes: [\"" + wc + "/data:/data\"]\n"
	src := composeTarball(t, map[string]string{
		"Dockerfile":   "FROM busybox:1.37\nCOPY index.html /www/index.html\nCMD [\"sh\", \"-c\", \"date >> /data/boots && exec httpd -f -p 8080 -h /www\"]\n",
		"index.html":   "<h1>from the repo</h1>\n",
		"compose.yaml": "services: {web: {build: .}}\n",
	})
	deploy := func(step string) *pb.DeployResult {
		var res *pb.DeployResult
		rec := &e2eBuildRecorder{}
		s.runDeploy(ctx, &pb.DeployRequest{
			DeployId: "dep_" + slug + step, Slug: slug, ProjectId: "prj_" + slug, ImageRef: "deplo/" + slug + ":e2e",
			SourceKind: pb.SourceKind_SOURCE_KIND_UPLOAD, BuildKind: pb.BuildKind_BUILD_KIND_COMPOSE, Network: testNetwork,
			ContextTar: src, ComposeYaml: yaml, ReadyTimeoutMs: 120_000,
			Ready: &pb.ReadyCheck{Port: 8080, PortRequired: true, Service: "web"},
		}, &emitter{send: func(ev *pb.DeployEvent) error {
			if r := ev.GetResult(); r != nil {
				res = r
			}
			return rec.emitter().send(ev)
		}})
		if res == nil || !res.GetReady() || !slices.Equal(res.GetListeningPorts(), []int32{8080}) {
			t.Fatalf("%s: result %v; log:\n%s", step, res, rec.joined())
		}
		return res
	}
	deploy("first")
	if b, err := os.ReadFile(filepath.Join(wc, "data", "boots")); err != nil || strings.TrimSpace(string(b)) == "" {
		t.Fatalf("the service wrote nothing into the working copy: %q %v", b, err)
	}
	deploy("second")
	if b, _ := os.ReadFile(filepath.Join(wc, "data", "boots")); strings.TrimSpace(string(b)) == "" {
		t.Fatal("the redeploy lost what the service wrote")
	}
	giveBackToTestUser(t, wc)
	if res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true}); err != nil || !res.GetOk() {
		t.Fatalf("destroy: %v %v", res, err)
	}
	if _, err := os.Stat(wc); !os.IsNotExist(err) {
		t.Fatalf("DestroyStack left the working copy: %v", err)
	}
}

// giveBackToTestUser hands what a container wrote as root back to the test's user: a non-root CI
// runner cannot delete it otherwise, where the agent itself runs as root. A no-op as root.
func giveBackToTestUser(t *testing.T, dir string) {
	t.Helper()
	if _, err := os.Stat(dir); os.Geteuid() == 0 || err != nil {
		return
	}
	owner := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	_, _ = dockercli.Run(context.Background(), 60*time.Second, "run", "--rm", "-v", dir+":/d", "busybox:1.37", "chown", "-R", owner, "/d")
}

// End-to-end (real docker): a service mounting the whole working copy writable while another
// mounts a folder inside it is refused before anything is built or started.
func TestE2E_ComposeFromSourceRefusesANestedWritableBind(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	const slug = "deplo-agent-test-cs-nested"
	s := New(t.TempDir(), t.TempDir(), "/", "")
	wc := s.srcDir(slug)
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true})
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", testNetwork, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", testNetwork)
	})
	yaml := "services:\n  writer:\n    image: busybox:1.37\n    volumes: [\"" + wc + ":/w\"]\n" +
		"  reader:\n    image: busybox:1.37\n    labels: [\"deplo.slug=" + slug + "\"]\n    volumes: [\"" + wc + "/data:/host\"]\n"
	var res *pb.DeployResult
	rec := &e2eBuildRecorder{}
	s.runDeploy(ctx, &pb.DeployRequest{
		DeployId: "dep_" + slug, Slug: slug, ProjectId: "prj_" + slug,
		SourceKind: pb.SourceKind_SOURCE_KIND_UPLOAD, BuildKind: pb.BuildKind_BUILD_KIND_COMPOSE, Network: testNetwork,
		ContextTar: composeTarball(t, map[string]string{"compose.yaml": "services: {}\n"}), ComposeYaml: yaml,
		ReadyTimeoutMs: 60_000, Ready: &pb.ReadyCheck{},
	}, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return rec.emitter().send(ev)
	}})
	if res == nil || res.GetReady() || !strings.Contains(res.GetError(), "service reader's volume (./data) is inside ./, which service writer mounts writable") {
		t.Fatalf("result %v; log:\n%s", res, rec.joined())
	}
	if strings.Contains(rec.joined(), "up -d") {
		t.Fatal("the stack came up anyway")
	}
}
