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
	if strings.Contains(df, "AS builder") {
		t.Errorf("expected single-stage Dockerfile, got builder stage:\n%s", df)
	}
	if !strings.Contains(df, "COPY dist/ /usr/share/nginx/html/") {
		t.Errorf("output dir not copied:\n%s", df)
	}
	if !strings.Contains(conf, "listen       8080;") {
		t.Errorf("nginx not listening on the spec port:\n%s", conf)
	}
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
	if !strings.Contains(df, "FROM node:18-alpine AS builder") {
		t.Errorf("node major not pinned from runtime_version:\n%s", df)
	}
	if !strings.Contains(df, "RUN npm ci") || !strings.Contains(df, "RUN npm run build") {
		t.Errorf("install/build commands missing:\n%s", df)
	}
	if !strings.Contains(df, "COPY --from=builder /app/build/ /usr/share/nginx/html/") {
		t.Errorf("output dir not copied from builder stage:\n%s", df)
	}
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
			RuntimeLanguage: "go", RuntimeVersion: "1.22",
		},
	}
	e := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	_ = s.buildStatic(context.Background(), req, dir, e)

	df, _ := readBuiltDockerfile(t, dir)
	if !strings.Contains(df, "FROM node:20-alpine AS builder") {
		t.Errorf("expected default node:20 builder for non-node runtime:\n%s", df)
	}
}

func TestMajorVersion(t *testing.T) {
	cases := []struct{ in, def, want string }{
		{"20.11.0", "20", "20"},
		{"v18", "20", "18"},
		{"", "20", "20"},
		{"latest", "20", "20"},
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
	url, err := nixpacksDownloadURL()
	if err != nil {
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

// A checkout's Dockerfile or deplo-nginx.conf may be a symlink to a host path: the root agent must
// replace the link, never write through it.
func TestBuildStaticNeverWritesThroughASymlink(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	host, dir := t.TempDir(), t.TempDir()
	created, existing := filepath.Join(host, "authorized_keys"), filepath.Join(host, "passwd")
	mustWrite(t, existing, "keep\n")
	mustWrite(t, filepath.Join(dir, "index.html"), "<html></html>\n")
	for name, target := range map[string]string{"Dockerfile": created, "deplo-nginx.conf": existing, ".nvmrc": created + ".nvmrc"} {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}

	s := New(t.TempDir(), t.TempDir(), "/", "")
	req := &pb.DeployRequest{
		Slug: "deplo-agent-test-static", ProjectId: "prj", ImageRef: "deplo/deplo-agent-test-static:t",
		BuildKind: pb.BuildKind_BUILD_KIND_STATIC,
		BuildSpec: &pb.BuildSpec{Method: "static", BuildCommand: "true\nssh-ed25519 AAAA evil"},
	}
	_ = s.buildStatic(context.Background(), req, dir, &emitter{send: func(*pb.DeployEvent) error { return nil }})
	if _, err := writeNodeVersionPin(dir, "20"); err != nil {
		t.Fatal(err)
	}

	if b, _ := os.ReadFile(existing); string(b) != "keep\n" {
		t.Fatalf("the symlinked deplo-nginx.conf overwrote its host target: %q", b)
	}
	if left, _ := filepath.Glob(filepath.Join(host, "authorized_keys*")); len(left) != 0 {
		t.Fatalf("a dangling symlink let the agent create a host file: %v", left)
	}
	for _, name := range []string{"Dockerfile", "deplo-nginx.conf", ".nvmrc"} {
		if li, err := os.Lstat(filepath.Join(dir, name)); err != nil || !li.Mode().IsRegular() {
			t.Fatalf("%s should now be a regular file in the checkout: %v %v", name, li, err)
		}
	}
}

// nixpacks writes its output under .nixpacks as root, so no symlink may survive there.
func TestStripSymlinksClearsNixpacksOutput(t *testing.T) {
	host, dir := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(host, "cron"), "keep\n")
	out := filepath.Join(dir, ".nixpacks")
	if err := os.Symlink(host, out); err != nil {
		t.Fatal(err)
	}
	if err := stripSymlinks(out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(out); !os.IsNotExist(err) {
		t.Fatalf("a symlinked .nixpacks survived: %v", err)
	}

	mustWrite(t, filepath.Join(out, "assets", "kept.txt"), "mine\n")
	if err := os.Symlink(filepath.Join(host, "cron"), filepath.Join(out, "Dockerfile")); err != nil {
		t.Fatal(err)
	}
	if err := stripSymlinks(out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(out, "Dockerfile")); !os.IsNotExist(err) {
		t.Fatalf("a symlink inside .nixpacks survived: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "assets", "kept.txt")); err != nil {
		t.Fatalf("a regular file under .nixpacks was removed: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(host, "cron")); string(b) != "keep\n" {
		t.Fatalf("the link target was touched: %q", b)
	}
	if err := stripSymlinks(filepath.Join(dir, "missing")); err != nil {
		t.Fatalf("a missing directory must be a no-op: %v", err)
	}
}

// pack trusts its known builders by exact short name, and a trusted builder runs the repo's
// buildpacks with the docker socket; every builder Deplo picks must take the untrusted phases.
func TestBuildpacksBuilderIsNeverPackTrusted(t *testing.T) {
	out, bin := t.TempDir(), t.TempDir()
	script := "#!/bin/sh\n[ \"$1\" = run ] && printf '%s\\n' \"$@\" >> " + out + "/args\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	packTrusted := map[string]bool{"heroku/builder:22": true, "heroku/builder:24": true,
		"heroku/builder:26": true, "paketobuildpacks/ubuntu-noble-builder": true}

	s := New(t.TempDir(), t.TempDir(), "/", "")
	for _, spec := range []*pb.BuildSpec{{Method: "paketo"}, {Method: "heroku"}, {Method: "heroku", HerokuVersion: "22"},
		{Method: "heroku", HerokuVersion: "26"}, {Method: "heroku", HerokuVersion: "99"}} {
		_ = os.Remove(filepath.Join(out, "args"))
		req := &pb.DeployRequest{Slug: "deplo-agent-test-bp", ProjectId: "prj", ImageRef: "deplo/deplo-agent-test-bp:t", BuildSpec: spec}
		_ = s.buildBuildpacks(context.Background(), req, t.TempDir(), &emitter{send: func(*pb.DeployEvent) error { return nil }})
		b, err := os.ReadFile(filepath.Join(out, "args"))
		if err != nil {
			t.Fatalf("%v: pack never ran: %v", spec, err)
		}
		args := strings.Split(strings.TrimSpace(string(b)), "\n")
		builder := ""
		for i, a := range args {
			if a == "--builder" && i+1 < len(args) {
				builder = args[i+1]
			}
		}
		if builder == "" || packTrusted[builder] || !strings.HasPrefix(builder, "docker.io/") {
			t.Fatalf("%v: builder %q would run trusted, with the docker socket next to the repo's buildpacks", spec, builder)
		}
	}
}

// nixpacks and railpack read the repo as root on the host, so a link that resolves outside the
// checkout (absolute, via .., or through another link) is gone before either runs.
func TestHostToolsNeverSeeALinkOutOfTheCheckout(t *testing.T) {
	out, bin := t.TempDir(), t.TempDir()
	probe := "#!/bin/sh\n[ \"$1\" = --version ] && { echo railpack v" + railpackVersion + "; exit 0; }\n" +
		"for f in Procfile package.json inside; do if [ -L \"$2/$f\" ]; then echo \"$f link\"; " +
		"elif [ -e \"$2/$f\" ]; then echo \"$f file\"; else echo \"$f gone\"; fi; done > " + out + "/seen\nexit 1\n"
	for _, tool := range []string{"nixpacks", "railpack"} {
		if err := os.WriteFile(filepath.Join(bin, tool), []byte(probe), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	s := New(t.TempDir(), t.TempDir(), "/", "")
	for _, kind := range []pb.BuildKind{pb.BuildKind_BUILD_KIND_NIXPACKS, pb.BuildKind_BUILD_KIND_RAILPACK} {
		base := t.TempDir()
		dir := filepath.Join(base, "repo")
		mustWrite(t, filepath.Join(base, "host-package.json"), `{"secret":true}`)
		mustWrite(t, filepath.Join(dir, "sub", "real.txt"), "mine\n")
		for name, target := range map[string]string{
			"Procfile": filepath.Join(base, "host-package.json"), "package.json": "../host-package.json", "inside": "sub/real.txt",
		} {
			if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
		}
		_ = os.Remove(filepath.Join(out, "seen"))
		req := &pb.DeployRequest{Slug: "deplo-agent-test-links", ProjectId: "prj", ImageRef: "deplo/deplo-agent-test-links:t",
			BuildKind: kind, BuildSpec: &pb.BuildSpec{}}
		_ = s.buildImage(context.Background(), req, dir, &emitter{send: func(*pb.DeployEvent) error { return nil }})
		seen, err := os.ReadFile(filepath.Join(out, "seen"))
		if err != nil {
			t.Fatalf("%v: the tool never ran: %v", kind, err)
		}
		if want := "Procfile gone\npackage.json gone\ninside link\n"; string(seen) != want {
			t.Fatalf("%v: the tool saw\n%s\nwant\n%s", kind, seen, want)
		}
	}
}

// A relative link that climbs out through another link is caught by resolving it, not by its text.
func TestStripEscapingSymlinksResolvesChains(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "repo")
	mustWrite(t, filepath.Join(dir, "a", "keep.txt"), "x")
	for link, target := range map[string]string{"s": ".", "a/z": "../s/..", "a/ok": "../s/a/keep.txt", "dangling": "nope"} {
		if err := os.Symlink(target, filepath.Join(dir, link)); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := stripEscapingSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(removed, ",") != "a/z" {
		t.Fatalf("removed %v, want only a/z", removed)
	}
}
