package server

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

// materializeGit clones a git source (PLAN Part B, D3): the agent clones the
// repo ITSELF with a short-lived token the control plane minted, so a remote
// build never ships the whole repo over the wire. It returns the build dir
// (the clone root, or a sub-directory of it), the resolved commit sha, and a
// cleanup func. The token NEVER rides the clone URL on argv (that would land in
// the world-readable /proc/<pid>/cmdline); it is carried as an out-of-band
// http.extraHeader, and is NEVER logged — the emitted command line is sanitised.
func (s *Service) materializeGit(
	ctx context.Context,
	g *pb.GitSource,
	slug string,
	e *emitter,
) (buildDir string, commitSha string, cleanup func(), err error) {
	if g == nil || strings.TrimSpace(g.GetUrl()) == "" {
		return "", "", func() {}, fmt.Errorf("git source missing url")
	}
	dir, mkErr := os.MkdirTemp(s.buildTmpDir, "deplo-git-"+slug+"-")
	if mkErr != nil {
		return "", "", func() {}, mkErr
	}
	cleanup = func() { _ = os.RemoveAll(dir) }

	cloneURL, display, authHeader := authenticatedURL(g.GetUrl(), g.GetToken())

	// Clone shallowly at the requested branch. A shallow single-branch clone is
	// the smallest fetch that still yields a working tree + the tip commit sha.
	args := []string{"clone", "--depth", "1"}
	if b := strings.TrimSpace(g.GetBranch()); b != "" {
		args = append(args, "--branch", b, "--single-branch")
	}
	args = append(args, cloneURL, dir)

	// Log the SANITISED command (the real URL with the token is never emitted).
	branchNote := ""
	if b := strings.TrimSpace(g.GetBranch()); b != "" {
		branchNote = " (" + b + ")"
	}
	e.log("command", "git clone "+display+branchNote)

	if err := runGit(ctx, e, "", authHeader, args...); err != nil {
		cleanup()
		return "", "", func() {}, err
	}

	// Resolve the checked-out commit sha (reported back so the control plane can
	// write it to the Deployment row — proto DeployResult.commit_sha).
	sha, _ := gitOutput(ctx, dir, "rev-parse", "HEAD")
	commitSha = strings.TrimSpace(sha)

	// Apply the optional sub-directory (the project's rootDirectory), validated to
	// stay inside the clone — the subdir arrived off the wire, never trusted.
	buildDir = dir
	if sub := strings.TrimSpace(g.GetSubdir()); sub != "" {
		joined, ok := safepath.Join(dir, sub)
		if !ok {
			cleanup()
			return "", "", func() {}, fmt.Errorf("git subdir %q escapes the clone", sub)
		}
		if real, rErr := safepath.Inside(dir, joined); rErr == nil {
			joined = real
		}
		info, statErr := os.Stat(joined)
		if statErr != nil || !info.IsDir() {
			cleanup()
			return "", "", func() {}, fmt.Errorf("git subdir %q was not found in the repository", sub)
		}
		buildDir = joined
	}
	return buildDir, commitSha, cleanup, nil
}

// authenticatedURL returns (cloneURL, display, authHeader). Credentials are
// NEVER placed on the clone URL — a URL on argv lands in /proc/<pid>/cmdline,
// which is world-readable, so the token would leak. Instead, when the URL
// carries userinfo (a GitHub-App x-access-token URL from the control plane) or a
// bare token is supplied, the credential is returned as an
// "Authorization: Basic <b64>" header value the caller injects out-of-band (git
// http.extraHeader via env), and the clone URL is stripped of any userinfo. The
// display URL likewise never carries credentials.
func authenticatedURL(raw, token string) (cloneURL, display, authHeader string) {
	u, err := url.Parse(raw)
	if err != nil {
		// Not a parseable URL (e.g. scp-like git@host:repo) — pass through. Such
		// forms carry no HTTP userinfo and authenticate over ssh, so no secret
		// reaches argv.
		return raw, raw, ""
	}
	// Strip any credentials from both the clone URL and the display URL.
	user := u.User
	u.User = nil
	cloneURL = u.String()
	display = cloneURL

	if user != nil && user.Username() != "" {
		// Pre-authenticated URL: lift the existing creds off the URL into a header.
		pass, _ := user.Password()
		return cloneURL, display, basicAuthHeader(user.Username(), pass)
	}
	if strings.TrimSpace(token) != "" {
		return cloneURL, display, basicAuthHeader("x-access-token", token)
	}
	return cloneURL, display, ""
}

// basicAuthHeader builds the "Authorization: Basic <b64>" header line for git's
// http.extraHeader, matching git's own userinfo scheme (base64 of "user:pass").
func basicAuthHeader(user, pass string) string {
	return "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// runGit runs a git command, streaming combined output line-by-line as info
// logs (so the operator sees clone progress). Any credential is supplied via
// authHeader (an http.extraHeader line) through git's env-based config, so it
// never appears on argv. dir="" runs in the process cwd (used for the clone
// itself, whose target is an arg).
func runGit(ctx context.Context, e *emitter, dir, authHeader string, args ...string) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	// GIT_TERMINAL_PROMPT=0: never block on an interactive credential prompt (a
	// bad/expired token must fail fast, not hang the deploy).
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if strings.TrimSpace(authHeader) != "" {
		// Inject the credential as an http.extraHeader through git's env-based
		// config (GIT_CONFIG_COUNT/KEY/VALUE) so it authenticates the request
		// WITHOUT ever appearing on argv / in the world-readable /proc/<pid>/cmdline.
		cmd.Env = append(cmd.Env,
			"GIT_CONFIG_COUNT=1",
			"GIT_CONFIG_KEY_0=http.extraHeader",
			"GIT_CONFIG_VALUE_0="+authHeader,
		)
	}
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout // fold stderr into the same stream for ordered logs
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("git: %w", err)
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		e.log("info", sanitizeGitLine(scanner.Text()))
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git %s failed: %w", args[0], err)
	}
	return nil
}

// gitOutput runs a git command and returns its trimmed stdout (no streaming).
func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	return string(out), err
}

// sanitizeGitLine strips anything that looks like an x-access-token credential
// from a log line, as belt-and-suspenders — git should not echo the URL, but a
// defensive scrub guarantees a token never lands in the deployment log.
func sanitizeGitLine(line string) string {
	if i := strings.Index(line, "x-access-token:"); i >= 0 {
		if at := strings.Index(line[i:], "@"); at >= 0 {
			return line[:i] + "x-access-token:***@" + line[i+at+1:]
		}
	}
	return line
}
