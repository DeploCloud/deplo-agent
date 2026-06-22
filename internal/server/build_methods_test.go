package server

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// staticBuildDir materialises a one-file context for a static build test.
func staticBuildDir(t *testing.T, s *Service) (string, func()) {
	t.Helper()
	data := tarball(t,
		[]tar.Header{{Name: "index.html", Typeflag: tar.TypeReg, Mode: 0o644}},
		map[string]string{"index.html": "<html></html>\n"},
	)
	dir, cleanup, err := s.materializeUpload(data, "static")
	if err != nil {
		t.Fatal(err)
	}
	return dir, cleanup
}

// readBuiltDockerfile returns the Dockerfile + nginx conf the static builder wrote
// into the context. The build itself needs a daemon (covered by e2e); we assert
// the rendered artifacts, the part that must mirror builders.ts byte-for-byte.
func readBuiltDockerfile(t *testing.T, dir string) (string, string) {
	t.Helper()
	df, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
	if err != nil {
		t.Fatalf("static Dockerfile not written: %v", err)
	}
	conf, err := os.ReadFile(filepath.Join(dir, "deplo-nginx.conf"))
	if err != nil {
		t.Fatalf("nginx conf not written: %v", err)
	}
	return string(df), string(conf)
}

func TestBuildStatic_alreadyStaticCopiesOutputDir(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	dir, cleanup := staticBuildDir(t, s)
	defer cleanup()

	req := &pb.DeployRequest{
		Slug: "static", ProjectId: "prj", ImageRef: "deplo/static:test",
		BuildKind: pb.BuildKind_BUILD_KIND_STATIC,
		BuildSpec: &pb.BuildSpec{Method: "static", Port: 8080, OutputDirectory: "dist"},
	}
	e := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	_ = s.buildStatic(context.Background(), req, dir, e)

	df, conf := readBuiltDockerfile(t, dir)
	// No build command → single-stage: copy the output dir straight into nginx.
	if strings.Contains(df, "AS builder") {
		t.Errorf("expected single-stage Dockerfile, got builder stage:\n%s", df)
	}
	if !strings.Contains(df, "COPY dist/ /usr/share/nginx/html/") {
		t.Errorf("output dir not copied:\n%s", df)
	}
	if !strings.Contains(conf, "listen       8080;") {
		t.Errorf("nginx not listening on the spec port:\n%s", conf)
	}
	// Non-SPA default → directory/404 fallback, not the index.html rewrite.
	if !strings.Contains(conf, "try_files $uri $uri/ =404;") {
		t.Errorf("expected non-SPA try_files:\n%s", conf)
	}
}

func TestBuildStatic_withBuildCommandIsTwoStage(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	dir, cleanup := staticBuildDir(t, s)
	defer cleanup()

	req := &pb.DeployRequest{
		Slug: "static", ProjectId: "prj", ImageRef: "deplo/static:test",
		BuildKind: pb.BuildKind_BUILD_KIND_STATIC,
		BuildSpec: &pb.BuildSpec{
			Method: "static", Port: 80, OutputDirectory: "build",
			InstallCommand: "npm ci", BuildCommand: "npm run build",
			RuntimeLanguage: "node", RuntimeVersion: "18.17.0",
			StaticSinglePageApp: true,
		},
	}
	e := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	_ = s.buildStatic(context.Background(), req, dir, e)

	df, conf := readBuiltDockerfile(t, dir)
	// Two-stage: Node builder then nginx; runtime_version pins the node major.
	if !strings.Contains(df, "FROM node:18-alpine AS builder") {
		t.Errorf("node major not pinned from runtime_version:\n%s", df)
	}
	if !strings.Contains(df, "RUN npm ci") || !strings.Contains(df, "RUN npm run build") {
		t.Errorf("install/build commands missing:\n%s", df)
	}
	if !strings.Contains(df, "COPY --from=builder /app/build/ /usr/share/nginx/html/") {
		t.Errorf("output dir not copied from builder stage:\n%s", df)
	}
	// SPA → history-API fallback to index.html.
	if !strings.Contains(conf, "try_files $uri /index.html;") {
		t.Errorf("expected SPA try_files:\n%s", conf)
	}
}

func TestBuildStatic_nonNodeIgnoresRuntimeVersion(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	dir, cleanup := staticBuildDir(t, s)
	defer cleanup()

	req := &pb.DeployRequest{
		Slug: "static", ProjectId: "prj", ImageRef: "deplo/static:test",
		BuildKind: pb.BuildKind_BUILD_KIND_STATIC,
		BuildSpec: &pb.BuildSpec{
			Method: "static", BuildCommand: "make",
			RuntimeLanguage: "go", RuntimeVersion: "1.22", // not node → ignored
		},
	}
	e := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	_ = s.buildStatic(context.Background(), req, dir, e)

	df, _ := readBuiltDockerfile(t, dir)
	// The builder stage is always Node; a non-node runtime_version must not leak in.
	if !strings.Contains(df, "FROM node:20-alpine AS builder") {
		t.Errorf("expected default node:20 builder for non-node runtime:\n%s", df)
	}
}

func TestMajorVersion(t *testing.T) {
	cases := []struct{ in, def, want string }{
		{"20.11.0", "20", "20"},
		{"v18", "20", "18"},
		{"", "20", "20"},
		{"latest", "20", "20"}, // no digits → default
		{"3.12", "3", "3"},
	}
	for _, c := range cases {
		if got := majorVersion(c.in, c.def); got != c.want {
			t.Errorf("majorVersion(%q,%q) = %q, want %q", c.in, c.def, got, c.want)
		}
	}
}

func TestImageTag(t *testing.T) {
	if got := imageTag("deplo/app:abc123"); got != "abc123" {
		t.Errorf("imageTag tagged = %q, want abc123", got)
	}
	if got := imageTag("deplo/app"); got != "deplo/app" {
		t.Errorf("imageTag untagged = %q, want the whole ref", got)
	}
}

func TestBuildPortDefaultsTo80(t *testing.T) {
	if got := buildPort(&pb.BuildSpec{}); got != 80 {
		t.Errorf("buildPort(0) = %d, want 80", got)
	}
	if got := buildPort(&pb.BuildSpec{Port: 3000}); got != 3000 {
		t.Errorf("buildPort(3000) = %d, want 3000", got)
	}
}

func TestNixpacksDownloadURL_arch(t *testing.T) {
	// The URL builder only runs on linux servers; on this host arch it must yield a
	// musl asset URL for the pinned version (or a clear unsupported-arch error).
	url, err := nixpacksDownloadURL()
	if err != nil {
		// Acceptable on a non-linux/unsupported-arch test host — just assert it's a
		// deliberate error, not a malformed URL.
		if !strings.Contains(err.Error(), "nixpacks auto-install") {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if !strings.Contains(url, "releases/download/v"+nixpacksVersion) ||
		!strings.Contains(url, "linux-musl.tar.gz") {
		t.Errorf("malformed nixpacks URL: %s", url)
	}
}
