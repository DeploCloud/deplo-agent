package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// Dockerfile > compose file > Railpack, read from the build folder on every deploy.
func TestDetectBuildMethod(t *testing.T) {
	both := t.TempDir()
	mustWrite(t, filepath.Join(both, "Dockerfile"), "FROM busybox\n")
	mustWrite(t, filepath.Join(both, "compose.yaml"), "services: {}\n")
	compose := t.TempDir()
	mustWrite(t, filepath.Join(compose, "docker-compose.yml"), "services: {}\n")
	named := t.TempDir()
	mustWrite(t, filepath.Join(named, "ops", "stack.yml"), "services: {}\n")
	linked := t.TempDir()
	mustWrite(t, filepath.Join(linked, "docker", "Dockerfile.prod"), "FROM busybox\n")
	if err := os.Symlink("docker/Dockerfile.prod", filepath.Join(linked, "Dockerfile")); err != nil {
		t.Fatal(err)
	}
	escaping := t.TempDir()
	mustWrite(t, filepath.Join(t.TempDir(), "Dockerfile"), "FROM busybox\n")
	if err := os.Symlink("/etc/hostname", filepath.Join(escaping, "Dockerfile")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		dir, composeFile, method, dockerfile string
	}{
		{both, "", "dockerfile", "Dockerfile"},
		{compose, "", "compose", ""},
		{named, "ops/stack.yml", "compose", ""},
		{named, "", "railpack", ""},
		{linked, "", "dockerfile", "docker/Dockerfile.prod"},
		{escaping, "", "railpack", ""},
		{t.TempDir(), "", "railpack", ""},
	} {
		if m, df := detectBuildMethod(c.dir, c.composeFile); m != c.method || df != c.dockerfile {
			t.Errorf("%s (%q): %s %q, want %s %q", c.dir, c.composeFile, m, df, c.method, c.dockerfile)
		}
	}
}

func autoDeploy(t *testing.T, files map[string]string) (*pb.DeployResult, string) {
	t.Helper()
	var res *pb.DeployResult
	var logs []string
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.runDeploy(context.Background(), &pb.DeployRequest{
		DeployId: "dep_auto", Slug: "deplo-agent-test-auto", ProjectId: "prj", ImageRef: "deplo/deplo-agent-test-auto:t",
		SourceKind: pb.SourceKind_SOURCE_KIND_UPLOAD, BuildKind: pb.BuildKind_BUILD_KIND_RAILPACK, BuildOnly: true,
		BuildSpec: &pb.BuildSpec{Method: "auto", AutoMethod: true}, ContextTar: composeTarball(t, files),
	}, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		if l := ev.GetLog(); l != nil {
			logs = append(logs, l.GetText())
		}
		return nil
	}})
	if res == nil {
		t.Fatal("no result")
	}
	return res, strings.Join(logs, "\n")
}

func TestAutoMethodBuildsTheRepoBuildFile(t *testing.T) {
	calls := dockerOnPath(t, `[ "$1" = info ] && exit 1; exit 0`)
	res, logs := autoDeploy(t, map[string]string{"Dockerfile": "FROM busybox\n", "compose.yaml": "services: {}\n"})
	if !res.GetReady() || res.GetBuiltWith() != "dockerfile" || !strings.Contains(logs, "Found a Dockerfile in the root directory: building it") {
		t.Fatalf("result %v\n%s", res, logs)
	}
	if got := strings.Join(calls(), "\n"); !strings.Contains(got, "/Dockerfile ") || !strings.Contains(got, " -f ") {
		t.Fatalf("docker build never read the Dockerfile:\n%s", got)
	}
}

func TestAutoMethodReturnsTheComposeFile(t *testing.T) {
	calls := dockerOnPath(t, "exit 0")
	res, _ := autoDeploy(t, map[string]string{"compose.yml": "services: {web: {build: .}}\n"})
	if res.GetReady() || res.GetError() != "" || res.GetBuiltWith() != "" || res.GetSourceCompose().GetPath() != "compose.yml" {
		t.Fatalf("result %v", res)
	}
	if strings.Contains(strings.Join(calls(), "\n"), "build") {
		t.Fatal("a compose redirect built something")
	}
}

func TestAutoMethodFallsBackToRailpack(t *testing.T) {
	fakeRailpackTools(t, 1, `{"logs":[{"Level":"error","Msg":"No start command detected."}],"success":false}`)
	res, logs := autoDeploy(t, map[string]string{"README.md": "hi\n"})
	if res.GetReady() || res.GetBuiltWith() != "railpack" || !strings.Contains(res.GetError(), "Set a Start command") ||
		!strings.Contains(logs, "Railpack works out the build") {
		t.Fatalf("result %v\n%s", res, logs)
	}
}

func TestCapabilities_advertisesAutoMethod(t *testing.T) {
	if !containsString(Capabilities, "build.auto-method") {
		t.Error(`Capabilities must advertise "build.auto-method"`)
	}
}
