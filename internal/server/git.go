package server

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

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

	args := []string{"clone", "--depth", "1"}
	if b := strings.TrimSpace(g.GetBranch()); b != "" {
		args = append(args, "--branch", b, "--single-branch")
	}
	args = append(args, "--", cloneURL, dir)

	branchNote := ""
	if b := strings.TrimSpace(g.GetBranch()); b != "" {
		branchNote = " (" + b + ")"
	}
	e.log("command", "git clone "+display+branchNote)

	if err := runGit(ctx, e, "", authHeader, args...); err != nil {
		cleanup()
		return "", "", func() {}, err
	}

	sha, _ := gitOutput(ctx, dir, "rev-parse", "HEAD")
	commitSha = strings.TrimSpace(sha)

	stripVolatileGitMetadata(dir)

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

var volatileGitPaths = []string{"index", "logs"}

func stripVolatileGitMetadata(root string) {
	gitDir := filepath.Join(root, ".git")
	if fi, err := os.Lstat(gitDir); err != nil || !fi.IsDir() {
		return
	}
	for _, name := range volatileGitPaths {
		_ = os.RemoveAll(filepath.Join(gitDir, name))
	}
}

func authenticatedURL(raw, token string) (cloneURL, display, authHeader string) {
	u, err := url.Parse(raw)
	if err != nil {
		return raw, raw, ""
	}
	user := u.User
	u.User = nil
	cloneURL = u.String()
	display = cloneURL

	if user != nil && user.Username() != "" {
		pass, _ := user.Password()
		return cloneURL, display, basicAuthHeader(user.Username(), pass)
	}
	if strings.TrimSpace(token) != "" {
		return cloneURL, display, basicAuthHeader("x-access-token", token)
	}
	return cloneURL, display, ""
}

func basicAuthHeader(user, pass string) string {
	return "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func runGit(ctx context.Context, e *emitter, dir, authHeader string, args ...string) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if strings.TrimSpace(authHeader) != "" {
		cmd.Env = append(cmd.Env,
			"GIT_CONFIG_COUNT=1",
			"GIT_CONFIG_KEY_0=http.extraHeader",
			"GIT_CONFIG_VALUE_0="+authHeader,
		)
	}
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
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

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	return string(out), err
}

func sanitizeGitLine(line string) string {
	if i := strings.Index(line, "x-access-token:"); i >= 0 {
		if at := strings.Index(line[i:], "@"); at >= 0 {
			return line[:i] + "x-access-token:***@" + line[i+at+1:]
		}
	}
	return line
}
