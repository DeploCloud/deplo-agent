package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

func TestPackageManager(t *testing.T) {
	for want, files := range map[string][]string{
		"pnpm":       {"pnpm-lock.yaml", "package-lock.json"},
		"yarn":       {"yarn.lock"},
		"yarn-berry": {"yarn.lock", ".yarnrc.yml"},
		"npm":        {"package-lock.json"},
		"":           {"package.json"},
	} {
		dir := t.TempDir()
		for _, f := range files {
			mustWrite(t, filepath.Join(dir, f), "")
		}
		if got, _ := packageManager(dir, dir); got != want {
			t.Errorf("%v: got %q, want %q", files, got, want)
		}
	}
}

func TestListing(t *testing.T) {
	m := []byte(`{"dependencies":{"express":"^4.17.1","left":"1.3.0"},"devDependencies":{"mocha":"~10.0.0"}}`)
	for pkg, want := range map[string][2]string{
		"express": {"dependencies", "^"},
		"left":    {"dependencies", ""},
		"mocha":   {"devDependencies", "~"},
		"bytes":   {"", ""},
	} {
		if s, p := listing(m, pkg); [2]string{s, p} != want {
			t.Errorf("%s: got %v, want %v", pkg, [2]string{s, p}, want)
		}
	}
}

func TestManagerTool(t *testing.T) {
	for name, tc := range map[string]struct {
		manager, manifest, lock, want string
	}{
		"pnpm pinned by the app": {"pnpm", `{"packageManager":"pnpm@9.12.3+sha512.abc"}`, "lockfileVersion: '9.0'", "pnpm@9.12.3"},
		"pnpm 8 lockfile":        {"pnpm", `{}`, "lockfileVersion: '6.0'\n", "pnpm@8"},
		"pnpm 7 lockfile":        {"pnpm", `{}`, "lockfileVersion: 5.4\n", "pnpm@7"},
		"pnpm default":           {"pnpm", `{}`, "", "pnpm@9"},
		"yarn berry pinned":      {"yarn-berry", `{"packageManager":"yarn@4.5.1"}`, "", "@yarnpkg/cli-dist@4.5.1"},
		"npm ships with node":    {"npm", `{"packageManager":"npm@10.0.0"}`, "", ""},
	} {
		if got := managerTool(tc.manager, []byte(tc.manifest), []byte(tc.lock)); got != tc.want {
			t.Errorf("%s: got %q, want %q", name, got, tc.want)
		}
	}
}

func TestRegistryHost(t *testing.T) {
	if !registryHost("registry.npmjs.org") || registryHost("evil.example") || registryHost("169.254.169.254") {
		t.Fatal("only the registries are allowed")
	}
}

func TestPreparePackageUpdate_refusesBadInput(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	for _, req := range []*pb.PreparePackageUpdateRequest{
		{Package: "minimist; rm -rf /", Version: "1.2.6", Git: &pb.GitSource{Url: "file:///x"}},
		{Package: "minimist", Version: "latest", Git: &pb.GitSource{Url: "file:///x"}},
		{Package: "minimist", Version: "1.2.6"},
	} {
		resp, err := s.PreparePackageUpdate(context.Background(), req)
		if err != nil || resp.Error == "" {
			t.Errorf("%v: want a plain refusal, got %v %v", req, resp, err)
		}
	}
}

// testUser keeps a fixture's files deletable by the test that made them.
func testUser() string { return fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()) }

func gunzip(t *testing.T, b []byte) string {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(r)
	return string(out)
}

// End-to-end: npm in the sealed container moves a direct dependency, then pins an indirect one.
func TestE2E_PreparePackageUpdate_npm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	prev := gitAllowProtocol
	gitAllowProtocol += ":file"
	t.Cleanup(func() { gitAllowProtocol = prev })
	repo := t.TempDir()
	app := filepath.Join(repo, "web")
	if err := os.MkdirAll(app, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(app, "package.json"),
		`{"name":"e2e","version":"1.0.0","dependencies":{"minimist":"^1.2.5","mkdirp":"0.5.5"}}`+"\n")
	if res, err := dockercli.Run(ctx, 5*time.Minute, "run", "--rm", "--user", testUser(), "-e", "HOME=/tmp", "-v", app+":/w", "-w", "/w", pkgUpdateImage,
		"npm", "install", "--package-lock-only", "--ignore-scripts", "--no-audit", "--no-fund"); err != nil || res.Code != 0 {
		t.Skipf("cannot make the lockfile: %v %s", err, res.Stderr)
	}
	git := func(args ...string) string {
		out, err := exec.Command("git", append([]string{"-C", repo, "-c", "user.email=e2e@deplo", "-c", "user.name=e2e"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("add", ".")
	git("commit", "-q", "-m", "init")
	sha := git("rev-parse", "HEAD")

	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	src := &pb.GitSource{Url: "file://" + repo, Commit: sha, Subdir: "web"}
	resp, err := s.PreparePackageUpdate(ctx, &pb.PreparePackageUpdateRequest{Git: src, Package: "minimist", Version: "1.2.6"})
	if err != nil || resp.Error != "" {
		t.Fatalf("direct update: %v %v", err, resp.GetError())
	}
	files := map[string]string{}
	for _, f := range resp.Files {
		files[f.Path] = gunzip(t, f.Gzip)
	}
	// npm keeps the file's own formatting, so compare without spaces.
	pkg := strings.ReplaceAll(files["web/package.json"], " ", "")
	if !strings.Contains(pkg, `"minimist":"^1.2.6"`) || strings.Contains(files["web/package-lock.json"], "minimist-1.2.5.tgz") {
		t.Fatalf("minimist not moved past 1.2.5: %v", files)
	}
	if resp.Manager != "npm" || resp.BaseCommit != sha {
		t.Fatalf("manager/commit: %q %q", resp.Manager, resp.BaseCommit)
	}

	resp, err = s.PreparePackageUpdate(ctx, &pb.PreparePackageUpdateRequest{Git: src, Package: "minimist", Version: "1.2.8", Override: true})
	if err != nil || resp.Error != "" {
		t.Fatalf("override: %v %v", err, resp.GetError())
	}
	for _, f := range resp.Files {
		if f.Path == "web/package.json" && !strings.Contains(gunzip(t, f.Gzip), `"overrides"`) {
			t.Fatalf("no overrides block: %s", gunzip(t, f.Gzip))
		}
	}
}

// End-to-end: pnpm, installed by npm through the same proxy, pins an indirect dependency.
func TestE2E_PreparePackageUpdate_pnpm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	prev := gitAllowProtocol
	gitAllowProtocol += ":file"
	t.Cleanup(func() { gitAllowProtocol = prev })
	repo := t.TempDir()
	mustWrite(t, filepath.Join(repo, "package.json"), `{"name":"e2e","version":"1.0.0","dependencies":{"mkdirp":"0.5.5"}}`+"\n")
	if res, err := dockercli.Run(ctx, 5*time.Minute, "run", "--rm", "--user", testUser(), "-e", "HOME=/tmp", "-v", repo+":/w", "-w", "/w", pkgUpdateImage,
		"sh", "-c", "COREPACK_ENABLE_DOWNLOAD_PROMPT=0 corepack pnpm install --lockfile-only --ignore-scripts"); err != nil || res.Code != 0 {
		t.Skipf("cannot make the pnpm lockfile: %v %s", err, res.Stderr)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.email=e2e@deplo", "-c", "user.name=e2e", "commit", "-q", "-m", "init"}} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	resp, err := s.PreparePackageUpdate(ctx, &pb.PreparePackageUpdateRequest{
		Git: &pb.GitSource{Url: "file://" + repo}, Package: "minimist", Version: "1.2.8", Override: true})
	if err != nil || resp.Error != "" {
		t.Fatalf("pnpm override: %v %v", err, resp.GetError())
	}
	got := map[string]string{}
	for _, f := range resp.Files {
		got[f.Path] = gunzip(t, f.Gzip)
	}
	if resp.Manager != "pnpm" || !strings.Contains(got["package.json"], "overrides") || !strings.Contains(got["pnpm-lock.yaml"], "minimist@1.2.8") {
		t.Fatalf("pnpm did not pin minimist 1.2.8: manager %q, files %v", resp.Manager, len(got))
	}
}

// End-to-end: yarn 1, the copy node ships, pins an indirect dependency through the same proxy.
func TestE2E_PreparePackageUpdate_yarn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	prev := gitAllowProtocol
	gitAllowProtocol += ":file"
	t.Cleanup(func() { gitAllowProtocol = prev })
	repo := t.TempDir()
	// The lockfile starts on minimist 1.2.5, which mkdirp's ^1.2.5 keeps until something moves it.
	mustWrite(t, filepath.Join(repo, "package.json"), `{"name":"e2e","version":"1.0.0","dependencies":{"mkdirp":"0.5.5"},"resolutions":{"minimist":"1.2.5"}}`+"\n")
	if res, err := dockercli.Run(ctx, 5*time.Minute, "run", "--rm", "--user", testUser(), "-e", "HOME=/tmp", "-v", repo+":/w", "-w", "/w", pkgUpdateImage,
		"yarn", "install", "--ignore-scripts", "--non-interactive"); err != nil || res.Code != 0 {
		t.Skipf("cannot make the yarn lockfile: %v %s", err, res.Stderr)
	}
	mustWrite(t, filepath.Join(repo, "package.json"), `{"name":"e2e","version":"1.0.0","dependencies":{"mkdirp":"0.5.5"}}`+"\n")
	_ = os.RemoveAll(filepath.Join(repo, "node_modules"))
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.email=e2e@deplo", "-c", "user.name=e2e", "commit", "-q", "-m", "init"}} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	resp, err := s.PreparePackageUpdate(ctx, &pb.PreparePackageUpdateRequest{
		Git: &pb.GitSource{Url: "file://" + repo}, Package: "minimist", Version: "1.2.8", Override: true})
	if err != nil || resp.Error != "" {
		t.Fatalf("yarn override: %v %v", err, resp.GetError())
	}
	got := map[string]string{}
	for _, f := range resp.Files {
		got[f.Path] = gunzip(t, f.Gzip)
	}
	if resp.Manager != "yarn" || !strings.Contains(got["package.json"], "resolutions") || !strings.Contains(got["yarn.lock"], "minimist-1.2.8.tgz") {
		t.Fatalf("yarn did not pin minimist 1.2.8: manager %q, files %v", resp.Manager, len(got))
	}
}
