package server

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The generated-Dockerfile path (legacy/auto: no Dockerfile in the repo).
func TestBuildImage_generatedWritesDockerfileIntoContext(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")

	// A context with a source file but NO Dockerfile.
	data := tarball(t,
		[]tar.Header{{Name: "app.js", Typeflag: tar.TypeReg, Mode: 0o644}},
		map[string]string{"app.js": "console.log(1)\n"},
	)
	buildDir, cleanup, err := s.materializeUpload(data, "gen")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	body := "FROM busybox\nCMD [\"true\"]\n"
	req := &pb.DeployRequest{
		Slug:       "gen",
		ProjectId:  "prj_gen",
		ImageRef:   "deplo/gen:test",
		BuildKind:  pb.BuildKind_BUILD_KIND_DOCKERFILE,
		Dockerfile: &pb.DockerfileBuild{Generated: true, GeneratedDockerfile: body},
	}
	// Drains events into nowhere; we only care about the Dockerfile being written.
	e := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	_ = s.buildImage(context.Background(), req, buildDir, e)

	got, err := os.ReadFile(filepath.Join(buildDir, "Dockerfile"))
	if err != nil {
		t.Fatalf("generated Dockerfile not written into the context: %v", err)
	}
	if string(got) != body {
		t.Errorf("generated Dockerfile body = %q, want %q", got, body)
	}
}
