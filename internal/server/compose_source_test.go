package server

import (
	"archive/tar"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The compose file is compose's own default first, a named one when set, and only ever a
// regular file inside the build folder.
func TestReadSourceCompose(t *testing.T) {
	dir := t.TempDir()
	if sc, err := readSourceCompose(dir, ""); sc != nil || err != nil {
		t.Fatalf("empty folder = %v, %v", sc, err)
	}
	mustWrite(t, filepath.Join(dir, "docker-compose.yml"), "services: {old: {image: a}}\n")
	mustWrite(t, filepath.Join(dir, "compose.yaml"), "services: {web: {image: b}}\n")
	mustWrite(t, filepath.Join(dir, "deploy", "prod.yml"), "services: {prod: {image: c}}\n")
	if sc, err := readSourceCompose(dir, ""); err != nil || sc.GetPath() != "compose.yaml" || !strings.Contains(sc.GetContent(), "web") {
		t.Fatalf("default = %v, %v", sc, err)
	}
	if sc, err := readSourceCompose(dir, "./deploy/prod.yml"); err != nil || sc.GetPath() != "deploy/prod.yml" {
		t.Fatalf("named = %v, %v", sc, err)
	}

	host := t.TempDir()
	mustWrite(t, filepath.Join(host, "secret.yml"), "token: hunter2\n")
	if err := os.Symlink(filepath.Join(host, "secret.yml"), filepath.Join(dir, "escape.yml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("deploy/prod.yml", filepath.Join(dir, "inside.yml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "folder.yml"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, "big.yml"), strings.Repeat("#", maxSourceCompose+1))
	mustWrite(t, filepath.Join(dir, "latin1.yml"), "services: {caf\xe9: {}}\n")
	for name, want := range map[string]string{
		"escape.yml": "links outside", "../escape.yml": "outside the build folder", "folder.yml": "not a regular file",
		"big.yml": "larger than 512 KiB", "latin1.yml": "not UTF-8", "missing.yml": "there is no missing.yml",
	} {
		if sc, err := readSourceCompose(dir, name); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, %v (want %q)", name, sc, err, want)
		}
	}
	if sc, err := readSourceCompose(dir, "inside.yml"); err != nil || !strings.Contains(sc.GetContent(), "prod") {
		t.Fatalf("a link inside the folder = %v, %v", sc, err)
	}
}

func gitRepo(t *testing.T, files map[string]string) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	allowFileGit(t)
	repo := t.TempDir()
	for name, body := range files {
		mustWrite(t, filepath.Join(repo, name), body)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"add", "-A"},
		{"-c", "user.email=t@example.test", "-c", "user.name=t", "commit", "-q", "-m", "x"}} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	sha, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return repo, strings.TrimSpace(string(sha))
}

// Leg 1: with no render yet, a compose deploy returns the source's compose file at the commit it
// checked out, and touches nothing on the host: no docker call, no stack file, no working copy.
func TestComposeDeployWithoutARenderReturnsTheSourceFile(t *testing.T) {
	repo, sha := gitRepo(t, map[string]string{"api/compose.yml": "services:\n  web:\n    build: .\n", "api/Dockerfile": "FROM busybox\n"})
	calls := dockerOnPath(t, "exit 0")
	s := New(t.TempDir(), t.TempDir(), "/", "")
	var res *pb.DeployResult
	s.runDeploy(context.Background(), &pb.DeployRequest{
		DeployId: "dep_leg1", Slug: "deplo-agent-test-leg1", ProjectId: "prj", ImageRef: "deplo/deplo-agent-test-leg1:t",
		SourceKind: pb.SourceKind_SOURCE_KIND_GIT, BuildKind: pb.BuildKind_BUILD_KIND_COMPOSE, Network: testNetwork,
		Git: &pb.GitSource{Url: "file://" + repo, Subdir: "api"}, BuildSpec: &pb.BuildSpec{Method: "compose"},
	}, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return nil
	}})
	if res == nil || res.GetReady() || res.GetError() != "" || res.GetCommitSha() != sha ||
		res.GetSourceCompose().GetPath() != "compose.yml" || !strings.Contains(res.GetSourceCompose().GetContent(), "build: .") {
		t.Fatalf("result = %v", res)
	}
	if got := calls(); len(got) != 1 || got[0] != "" {
		t.Fatalf("reading the compose file ran docker: %v", got)
	}
	if entries, _ := os.ReadDir(s.stackDir); len(entries) != 0 {
		t.Fatalf("the stack dir was touched: %v", entries)
	}
}

func inode(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Sys().(*syscall.Stat_t).Ino
}

// The working copy moves to each checkout in place: tracked files are rewritten (same inode, so
// a bound file updates), untracked data stays, and a link a container planted cannot redirect a write.
func TestSyncWorkingCopyUpdatesInPlace(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	const slug = "deplo-agent-test-sync"
	first := t.TempDir()
	mustWrite(t, filepath.Join(first, "nginx.conf"), "v1\n")
	mustWrite(t, filepath.Join(first, "conf", "app.ini"), "a=1\n")
	mustWrite(t, filepath.Join(first, "old.txt"), "gone next time\n")
	mustWrite(t, filepath.Join(first, "run.sh"), "#!/bin/sh\n")
	mustWrite(t, filepath.Join(first, ".git", "config"), "[core]\n")
	if err := os.Symlink("conf/app.ini", filepath.Join(first, "app.ini")); err != nil {
		t.Fatal(err)
	}
	if err := s.syncWorkingCopy(slug, first, true); err != nil {
		t.Fatal(err)
	}
	wc := s.srcDir(slug)
	before := inode(t, filepath.Join(wc, "nginx.conf"))
	if _, err := os.Stat(filepath.Join(wc, ".git")); !os.IsNotExist(err) {
		t.Fatalf("the git metadata reached the working copy: %v", err)
	}

	// What the app's services did since: data written, a tracked folder swapped for a link out.
	host := t.TempDir()
	mustWrite(t, filepath.Join(host, "app.ini"), "host file\n")
	mustWrite(t, filepath.Join(wc, "data", "pg", "PG_VERSION"), "16\n")
	if err := os.RemoveAll(filepath.Join(wc, "conf")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(host, filepath.Join(wc, "conf")); err != nil {
		t.Fatal(err)
	}

	second := t.TempDir()
	mustWrite(t, filepath.Join(second, "nginx.conf"), "v2\n")
	mustWrite(t, filepath.Join(second, "conf", "app.ini"), "a=2\n")
	mustWrite(t, filepath.Join(second, "run.sh"), "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(second, "run.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.syncWorkingCopy(slug, second, true); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(wc, "nginx.conf")); string(b) != "v2\n" || inode(t, filepath.Join(wc, "nginx.conf")) != before {
		t.Fatalf("nginx.conf = %q, rewritten in place: %v", b, inode(t, filepath.Join(wc, "nginx.conf")) == before)
	}
	if b, _ := os.ReadFile(filepath.Join(host, "app.ini")); string(b) != "host file\n" {
		t.Fatalf("a planted link redirected a write to the host: %q", b)
	}
	if li, err := os.Lstat(filepath.Join(wc, "conf")); err != nil || !li.IsDir() {
		t.Fatalf("conf should be a folder again: %v %v", li, err)
	}
	if b, _ := os.ReadFile(filepath.Join(wc, "data", "pg", "PG_VERSION")); string(b) != "16\n" {
		t.Fatal("untracked data did not survive the deploy")
	}
	if _, err := os.Lstat(filepath.Join(wc, "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("a file the commit dropped is still there: %v", err)
	}
	if fi, _ := os.Stat(filepath.Join(wc, "run.sh")); fi.Mode().Perm() != 0o755 {
		t.Fatalf("run.sh mode = %v", fi.Mode())
	}

	// An upload extracts over the copy and deletes nothing.
	third := t.TempDir()
	mustWrite(t, filepath.Join(third, "nginx.conf"), "v3\n")
	if err := s.syncWorkingCopy(slug, third, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(wc, "run.sh")); err != nil {
		t.Fatalf("an upload removed a file: %v", err)
	}
	s.removeWorkingCopy(slug)
	if _, err := os.Stat(wc); !os.IsNotExist(err) {
		t.Fatalf("removeWorkingCopy left %s", wc)
	}
}

func composeTarball(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var hdrs []tar.Header
	for name := range files {
		hdrs = append(hdrs, tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644})
	}
	return tarball(t, hdrs, files)
}

// Leg 2: the stack builds and comes up from the working copy, never the throwaway extract.
func TestComposeDeployBuildsFromTheWorkingCopy(t *testing.T) {
	calls := dockerOnPath(t, `case "$*" in
*" config --images"*) echo deplo/deplo-agent-test-leg2-web:t ;;
*" config --format json"*) echo '{"services":{"web":{"build":{"context":"/x"}}}}' ;;
"image inspect "*) echo null ;;
"ps "*) echo 0123abcd ;;
esac
exit 0`)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	const slug = "deplo-agent-test-leg2"
	req := &pb.DeployRequest{
		DeployId: "dep_leg2", Slug: slug, ProjectId: "prj", ImageRef: "deplo/" + slug + ":t",
		SourceKind: pb.SourceKind_SOURCE_KIND_UPLOAD, BuildKind: pb.BuildKind_BUILD_KIND_COMPOSE, Network: testNetwork,
		ContextTar:  composeTarball(t, map[string]string{"compose.yaml": "services: {}\n", "Dockerfile": "FROM busybox\n"}),
		ComposeYaml: "services:\n  web:\n    image: deplo/" + slug + "-web:t\n", ReadyTimeoutMs: 1,
	}
	var res *pb.DeployResult
	s.runDeploy(context.Background(), req, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return nil
	}})
	got := strings.Join(calls(), "\n")
	if res == nil || !res.GetReady() {
		t.Fatalf("result = %v\ncalls:\n%s", res, got)
	}
	wc := s.srcDir(slug)
	for _, want := range []string{"--project-directory " + wc + " --env-file", " build --build-arg BUILDKIT_CACHE_MOUNT_NS=", " up -d --remove-orphans"} {
		if !strings.Contains(got, want) {
			t.Errorf("calls lack %q:\n%s", want, got)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(wc, "Dockerfile")); string(b) != "FROM busybox\n" {
		t.Fatalf("working copy Dockerfile = %q", b)
	}
	s.removeStackFiles(slug)
	if _, err := os.Stat(wc); !os.IsNotExist(err) {
		t.Fatalf("removeStackFiles left the working copy: %v", err)
	}
}

// build_only cannot carry a stack, and a source-built stack needs a source to build.
func TestComposeDeployRefusesWhatItCannotDo(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	for want, req := range map[string]*pb.DeployRequest{
		"build-only is not supported":  {SourceKind: pb.SourceKind_SOURCE_KIND_GIT, BuildKind: pb.BuildKind_BUILD_KIND_COMPOSE, BuildOnly: true},
		"needs a git or upload source": {SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_COMPOSE},
	} {
		req.Slug, req.ImageRef = "deplo-agent-test-refuse", "deplo/deplo-agent-test-refuse:t"
		var res *pb.DeployResult
		s.runDeploy(context.Background(), req, &emitter{send: func(ev *pb.DeployEvent) error {
			if r := ev.GetResult(); r != nil && res == nil {
				res = r
			}
			return nil
		}})
		if res == nil || res.GetReady() || !strings.Contains(res.GetError(), want) {
			t.Errorf("%s: %v", want, res)
		}
	}
}

func TestCapabilities_advertisesComposeSource(t *testing.T) {
	if !containsString(Capabilities, "deploy.compose.source") {
		t.Error(`Capabilities must advertise "deploy.compose.source"`)
	}
}

// Start and Reroute bring a source-built stack's containers up too: a link one of its services
// planted in the working copy is gone first, an inside link stays.
func TestStripSourceLinksBeforeAStart(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if err := s.stripSourceLinks("deplo-agent-test-none"); err != nil {
		t.Fatalf("no working copy is nothing to check: %v", err)
	}
	wc := s.srcDir("deplo-agent-test-links")
	mustWrite(t, filepath.Join(wc, "data", "keep"), "x")
	for link, target := range map[string]string{"etc": "/etc", "alias": "keep"} {
		if err := os.Symlink(target, filepath.Join(wc, "data", link)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.stripSourceLinks("deplo-agent-test-links"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(wc, "data", "etc")); !os.IsNotExist(err) {
		t.Fatalf("a link to /etc survived: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(wc, "data", "alias")); err != nil {
		t.Fatalf("a link inside the copy was removed: %v", err)
	}
}

func hostPathsModel(t *testing.T, raw string) composeHostPaths {
	t.Helper()
	var m composeHostPaths
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// A folder a service mounts writable may hold no path that compose or Docker opens later: the
// container could swap it for a link, and Docker would mount or read wherever it points.
func TestSwappablePath(t *testing.T) {
	const root = "/data/stacks/app.src"
	ok := []string{
		`{"services":{"web":{"build":{"context":"/data/stacks/app.src"},"volumes":[{"type":"bind","source":"/data/stacks/app.src","target":"/app"}]}}}`,
		`{"services":{"db":{"volumes":[{"type":"bind","source":"/data/stacks/app.src/data","target":"/d"}]},"web":{"build":{"context":"/data/stacks/app.src/web"},"volumes":[{"type":"bind","source":"/data/stacks/app.src","target":"/app","read_only":true},{"type":"volume","source":"named","target":"/n"}]}}}`,
		`{"services":{"a":{"volumes":[{"type":"bind","source":"/data/stacks/app.src/data","target":"/d"}]},"b":{"volumes":[{"type":"bind","source":"/data/stacks/app.src/data","target":"/e"}]}}}`,
		`{"services":{"web":{"build":{"context":"/data/stacks/app.src/web","additional_contexts":{"img":"docker-image://busybox"}},"volumes":[{"type":"bind","source":"/data/stacks/app.src/data","target":"/d"}]}}}`,
	}
	for _, raw := range ok {
		if msg := swappablePath(hostPathsModel(t, raw), root); msg != "" {
			t.Errorf("refused %s: %s", raw, msg)
		}
	}
	bad := map[string]string{
		`{"services":{"a":{"volumes":[{"type":"bind","source":"/data/stacks/app.src","target":"/w"}]},"b":{"volumes":[{"type":"bind","source":"/data/stacks/app.src/data","target":"/host"}]}}}`: "service b's volume (./data) is inside ./, which service a mounts writable",
		`{"services":{"web":{"build":{"context":"/data/stacks/app.src/web"},"volumes":[{"type":"bind","source":"/data/stacks/app.src","target":"/repo"}]}}}`:                                     "service web's build context (./web) is inside ./",
		`{"services":{"web":{"build":{"context":"/data/stacks/app.src","dockerfile":"docker/Dockerfile"},"volumes":[{"type":"bind","source":"/data/stacks/app.src","target":"/r"}]}}}`:           "service web's Dockerfile folder (./docker)",
		`{"services":{"web":{"volumes":[{"type":"bind","source":"/data/stacks/app.src","target":"/r"}]}},"secrets":{"tok":{"file":"/data/stacks/app.src/tok.txt"}}}`:                             "secret tok (./tok.txt)",
		`{"services":{"web":{"volumes":[{"type":"bind","source":"/data/stacks/app.src","target":"/r"}]}},"configs":{"cfg":{"file":"/data/stacks/app.src/a.ini"}}}`:                               "config cfg (./a.ini)",
	}
	for raw, want := range bad {
		if msg := swappablePath(hostPathsModel(t, raw), root); !strings.Contains(msg, want) || !strings.Contains(msg, "read-only (:ro)") {
			t.Errorf("%s: %q, want %q", raw, msg, want)
		}
	}
}
