package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
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

	if commit := strings.TrimSpace(g.GetCommit()); commit != "" {
		if err := fetchCommit(ctx, e, dir, cloneURL, display, authHeader, commit); err != nil {
			cleanup()
			return "", "", func() {}, err
		}
	} else {
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
	}

	sha, _ := gitOutput(ctx, dir, "rev-parse", "HEAD")
	commitSha = strings.TrimSpace(sha)

	if g.GetSubmodules() {
		if err := cloneSubmodules(ctx, e, dir, cloneURL, authHeader, 0); err != nil {
			cleanup()
			return "", "", func() {}, err
		}
	}

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

var fullCommitSha = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// fetchCommit checks out one commit by SHA. A server that refuses an unadvertised object
// (protocol v0, some plain git servers) gets the whole history fetched instead.
func fetchCommit(ctx context.Context, e *emitter, dir, cloneURL, display, authHeader, commit string) error {
	if !fullCommitSha.MatchString(commit) {
		return fmt.Errorf("git commit %q is not a full commit SHA", commit)
	}
	short := commit[:7]
	if err := runGit(ctx, e, dir, "", "init", "-q"); err != nil {
		return err
	}
	e.log("command", "git fetch "+display+" ("+short+")")
	if err := runGit(ctx, e, dir, authHeader, "fetch", "--depth", "1", "--", cloneURL, commit); err != nil {
		e.log("info", "This git server can't fetch a single commit, fetching the whole history instead")
		if err := runGit(ctx, e, dir, authHeader, "fetch", "--tags", "--", cloneURL, "+refs/heads/*:refs/remotes/origin/*"); err != nil {
			return err
		}
	}
	if err := runGit(ctx, e, dir, "", "checkout", "-q", "--detach", commit+"^{commit}"); err != nil {
		return fmt.Errorf("commit %s is no longer in the repository", short)
	}
	return nil
}

const maxSubmoduleDepth = 3

// cloneSubmodules checks out the submodules .gitmodules names, recursively, and only from the
// repository's own host: those URLs are whoever-can-push's, and the credential must reach no other server.
func cloneSubmodules(ctx context.Context, e *emitter, dir, parentURL, authHeader string, depth int) error {
	urls := gitmodules(ctx, dir, "url")
	if len(urls) == 0 {
		return nil
	}
	parent, err := url.Parse(parentURL)
	if err != nil {
		return fmt.Errorf("submodules need the repository's address to be a URL")
	}
	resolved := map[string]string{}
	for name, raw := range urls {
		u, ok := submoduleURL(parent, raw)
		if !ok {
			return fmt.Errorf("submodule %s is not on %s: Deplo clones submodules only from the repository's own server", name, parent.Host)
		}
		resolved[name] = u
		if err := runGit(ctx, e, dir, "", "config", "submodule."+name+".url", u); err != nil {
			return err
		}
	}
	scope := parent.Scheme + "://" + parent.Host + "/"
	e.log("command", "git submodule update --init")
	if err := runGitScoped(ctx, e, dir, scope, authHeader, "submodule", "update", "--init", "--depth", "1"); err != nil {
		e.log("info", "A submodule's commit is not at the tip of its branch, fetching full history instead")
		if err := runGitScoped(ctx, e, dir, scope, authHeader, "submodule", "update", "--init"); err != nil {
			return err
		}
	}
	if depth+1 >= maxSubmoduleDepth {
		return nil
	}
	for name, path := range gitmodules(ctx, dir, "path") {
		sub, ok := safepath.Join(dir, path)
		if _, err := os.Stat(filepath.Join(sub, ".gitmodules")); !ok || err != nil || resolved[name] == "" {
			continue
		}
		if err := cloneSubmodules(ctx, e, sub, resolved[name], authHeader, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// gitmodules maps each submodule's name to one of its .gitmodules keys ("url", "path").
func gitmodules(ctx context.Context, dir, key string) map[string]string {
	out, err := gitOutput(ctx, dir, "config", "-f", ".gitmodules", "--get-regexp", `^submodule\..*\.`+key+`$`)
	if err != nil {
		return nil
	}
	m := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		k, v, ok := strings.Cut(line, " ")
		if ok {
			m[strings.TrimSuffix(strings.TrimPrefix(k, "submodule."), "."+key)] = strings.TrimSpace(v)
		}
	}
	return m
}

var scpLikeURL = regexp.MustCompile(`^[\w.-]+@([\w.-]+):(.+)$`)

// submoduleURL is a submodule's address as https on the parent's own host, or false when it points anywhere else.
func submoduleURL(parent *url.URL, raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "./") || strings.HasPrefix(raw, "../") {
		base := *parent
		base.Path = strings.TrimSuffix(parent.Path, "/") + "/"
		ref, err := url.Parse(raw)
		if err != nil {
			return "", false
		}
		return base.ResolveReference(ref).String(), true
	}
	host, path := "", ""
	if m := scpLikeURL.FindStringSubmatch(raw); m != nil && !strings.Contains(raw, "://") {
		host, path = m[1], m[2]
	} else if u, err := url.Parse(raw); err == nil && (u.Scheme == "https" || u.Scheme == parent.Scheme || u.Scheme == "ssh") {
		host, path = u.Hostname(), u.Path
	} else {
		return "", false
	}
	if !strings.EqualFold(host, parent.Hostname()) || path == "" {
		return "", false
	}
	return parent.Scheme + "://" + parent.Host + "/" + strings.TrimPrefix(path, "/"), true
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

// gitAllowProtocol keeps a clone off file:// and local paths (any git repo on the host) and ext::.
var gitAllowProtocol = "https:http:ssh:git"

func runGit(ctx context.Context, e *emitter, dir, authHeader string, args ...string) error {
	return runGitScoped(ctx, e, dir, "", authHeader, args...)
}

// runGitScoped sends authHeader only to URLs under scope ("" sends it everywhere, for the repository's own clone).
func runGitScoped(ctx context.Context, e *emitter, dir, scope, authHeader string, args ...string) error {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := dockercli.Command(cctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL="+gitAllowProtocol)
	if strings.TrimSpace(authHeader) != "" {
		key := "http.extraHeader"
		if scope != "" {
			key = "http." + scope + ".extraHeader"
		}
		cmd.Env = append(cmd.Env,
			"GIT_CONFIG_COUNT=1",
			"GIT_CONFIG_KEY_0="+key,
			"GIT_CONFIG_VALUE_0="+authHeader,
		)
	}
	err := dockercli.RunLines(cctx, cmd, func(line string) {
		e.log("info", sanitizeGitLine(line))
	})
	if err != nil {
		if cmd.Process == nil {
			return fmt.Errorf("git: %w", err)
		}
		return fmt.Errorf("git %s failed: %w", args[0], err)
	}
	return nil
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := dockercli.Command(cctx, "git", args...)
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
