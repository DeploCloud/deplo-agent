package server

import (
	"context"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAuthenticatedURL_injectsToken(t *testing.T) {
	// The token must NOT ride the clone URL (that lands on argv / in
	// /proc/<pid>/cmdline); it is carried as an out-of-band Authorization header.
	clone, display, authHeader := authenticatedURL("https://github.com/acme/app.git", "tok-secret")
	if strings.Contains(clone, "tok-secret") || strings.Contains(clone, "@") {
		t.Fatalf("credentials must not be on the clone URL (argv leak): %s", clone)
	}
	if strings.Contains(display, "tok-secret") {
		t.Fatalf("display URL leaks the token: %s", display)
	}
	want := "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:tok-secret"))
	if authHeader != want {
		t.Fatalf("auth header mismatch: got %q want %q", authHeader, want)
	}
}

func TestAuthenticatedURL_liftsPreAuthenticatedCreds(t *testing.T) {
	// A control-plane GitHub-App URL arrives already authenticated; the creds must
	// be lifted OFF the URL (argv leak) into the header, not left on the URL.
	pre := "https://x-access-token:ghs_existing@github.com/acme/app.git"
	clone, display, authHeader := authenticatedURL(pre, "ignored")
	if strings.Contains(clone, "ghs_existing") || strings.Contains(clone, "@") {
		t.Fatalf("pre-authenticated creds must be stripped from the clone URL: %s", clone)
	}
	if clone != "https://github.com/acme/app.git" {
		t.Fatalf("unexpected clone URL: %s", clone)
	}
	if strings.Contains(display, "ghs_existing") {
		t.Fatalf("display URL leaks the existing token: %s", display)
	}
	want := "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:ghs_existing"))
	if authHeader != want {
		t.Fatalf("auth header mismatch: got %q want %q", authHeader, want)
	}
}

func TestAuthenticatedURL_publicNoToken(t *testing.T) {
	clone, display, authHeader := authenticatedURL("https://github.com/acme/public.git", "")
	if strings.Contains(clone, "@") {
		t.Fatalf("public URL gained credentials: %s", clone)
	}
	if display != "https://github.com/acme/public.git" {
		t.Fatalf("display mismatch: %s", display)
	}
	if authHeader != "" {
		t.Fatalf("public clone must have no auth header: %q", authHeader)
	}
}

func TestSanitizeGitLine_scrubsToken(t *testing.T) {
	line := "Cloning into https://x-access-token:supersecret@github.com/acme/app.git ..."
	got := sanitizeGitLine(line)
	if strings.Contains(got, "supersecret") {
		t.Fatalf("token survived sanitisation: %s", got)
	}
	if !strings.Contains(got, "x-access-token:***@") {
		t.Fatalf("expected masked token marker, got: %s", got)
	}
}

// Two clones of the SAME commit must leave a byte-identical build context, or
// Docker's layer cache can never be hit: BuildKit keys a COPY on the content it
// copies, and git rewrites its index and reflogs on every clone. This is the
// difference between a redeploy that reuses a 1.88 GB dependency layer and one
// that rebuilds and recompresses it.
func TestStripVolatileGitMetadataMakesClonesIdentical(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx := context.Background()

	// An origin with one commit.
	origin := t.TempDir()
	run := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t",
			"GIT_COMMITTER_EMAIL=t@t", "GIT_AUTHOR_DATE=2026-01-01T00:00:00Z",
			"GIT_COMMITTER_DATE=2026-01-01T00:00:00Z")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run(origin, "init", "-q", "-b", "main", ".")
	if err := os.WriteFile(filepath.Join(origin, "app.txt"), []byte("source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(origin, "add", "-A")
	run(origin, "commit", "-qm", "one")

	clone := func(dest string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", "clone", "-q", "--depth", "1",
			"--branch", "main", "--single-branch", "file://"+origin, dest)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("clone: %v: %s", err, out)
		}
	}
	a, b := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")
	clone(a)
	time.Sleep(1100 * time.Millisecond) // guarantee a different clone timestamp
	clone(b)

	// Before: the reflogs alone already differ, which is what defeated the cache.
	if sameTree(t, a, b) {
		t.Skip("this git leaves clones identical already; nothing to prove")
	}

	stripVolatileGitMetadata(a)
	stripVolatileGitMetadata(b)
	if !sameTree(t, a, b) {
		t.Fatal("clones of the same commit still differ after stripping volatile git metadata")
	}

	// And git must still answer the questions a build asks of it.
	sha, err := gitOutput(ctx, a, "rev-parse", "HEAD")
	if err != nil || len(strings.TrimSpace(sha)) != 40 {
		t.Fatalf("rev-parse after strip: %q %v", sha, err)
	}
	if out, err := gitOutput(ctx, a, "log", "-1", "--format=%s"); err != nil || strings.TrimSpace(out) != "one" {
		t.Fatalf("log after strip: %q %v", out, err)
	}
}

// A `.git` FILE (a worktree or submodule pointer) must be left alone — there is
// no index of ours behind it and following it would reach outside the clone.
func TestStripVolatileGitMetadataIgnoresGitFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".git"), []byte("gitdir: /elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stripVolatileGitMetadata(dir) // must not panic, must not remove it
	if _, err := os.Stat(filepath.Join(dir, ".git")); err != nil {
		t.Fatalf("the .git file was disturbed: %v", err)
	}
}

// A clone with no .git at all (already stripped, or a plain directory) is a no-op.
func TestStripVolatileGitMetadataNoGitDir(t *testing.T) {
	stripVolatileGitMetadata(t.TempDir()) // must not panic
}

// sameTree reports whether two directory trees are byte-identical.
func sameTree(t *testing.T, a, b string) bool {
	t.Helper()
	out, err := exec.Command("diff", "-r", a, b).CombinedOutput()
	if err == nil {
		return true
	}
	if _, ok := err.(*exec.ExitError); ok {
		t.Logf("trees differ:\n%s", out)
		return false
	}
	t.Fatalf("diff: %v", err)
	return false
}
