package server

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func tarball(t *testing.T, entries []tar.Header, bodies map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range entries {
		hh := h
		if body, ok := bodies[h.Name]; ok {
			hh.Size = int64(len(body))
		}
		if err := tw.WriteHeader(&hh); err != nil {
			t.Fatal(err)
		}
		if body, ok := bodies[h.Name]; ok {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	tw.Close()
	return buf.Bytes()
}

func TestMaterializeUpload_extractsRegularFiles(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	data := tarball(t,
		[]tar.Header{
			{Name: "Dockerfile", Typeflag: tar.TypeReg, Mode: 0o644},
			{Name: "src/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "src/app.js", Typeflag: tar.TypeReg, Mode: 0o644},
		},
		map[string]string{
			"Dockerfile": "FROM scratch\n",
			"src/app.js": "console.log(1)\n",
		},
	)
	dir, cleanup, err := s.materializeUpload(data, "demo")
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	defer cleanup()

	got, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
	if err != nil || string(got) != "FROM scratch\n" {
		t.Fatalf("Dockerfile content = %q, err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "src", "app.js")); err != nil {
		t.Fatalf("nested file missing: %v", err)
	}
}

func TestMaterializeUpload_rejectsTraversal(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	data := tarball(t,
		[]tar.Header{{Name: "../escape.txt", Typeflag: tar.TypeReg, Mode: 0o644}},
		map[string]string{"../escape.txt": "pwned"},
	)
	dir, cleanup, err := s.materializeUpload(data, "demo")
	if err != nil {
		return
	}
	defer cleanup()
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "escape.txt")); err == nil {
		t.Fatal("traversal escaped the build dir")
	}
}

func TestMaterializeUpload_rejectsSymlink(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	data := tarball(t,
		[]tar.Header{{Name: "evil", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}},
		nil,
	)
	if _, _, err := s.materializeUpload(data, "demo"); err == nil {
		t.Fatal("expected symlink entry to be rejected")
	}
}

// image_ref reaches the build argv and, with project_id, a --label value: a leading "-" is a flag
// and a newline breaks the value, so a build refuses both before anything runs.
func TestRunDeployRefusesAnUnsafeImageRefOrProjectID(t *testing.T) {
	s := newTestService(t)
	for _, req := range []*pb.DeployRequest{
		{ImageRef: "--network=host"},
		{ImageRef: "deplo/app:1\nRUN touch /pwned"},
		{ImageRef: "nginx:latest"},
		{ImageRef: "deplo/app:1", ProjectId: "prj\nLABEL x=y"},
		{ImageRef: "deplo/app:1", ProjectId: "-prj"},
	} {
		req.Slug, req.SourceKind = "deplo-agent-test-ref", pb.SourceKind_SOURCE_KIND_GIT
		var got *pb.DeployResult
		s.runDeploy(context.Background(), req, &emitter{send: func(ev *pb.DeployEvent) error {
			if r := ev.GetResult(); r != nil && got == nil {
				got = r
			}
			return nil
		}})
		if got == nil || got.GetReady() || !strings.Contains(got.GetError(), "unsafe image ref") && !strings.Contains(got.GetError(), "invalid project id") {
			t.Fatalf("%q / %q was not refused up front: %v", req.GetImageRef(), req.GetProjectId(), got)
		}
	}
}
