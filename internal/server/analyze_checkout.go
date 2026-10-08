package server

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

var analysisMedia = []string{".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".ico", ".mp4", ".mov", ".mkv", ".webm"}

func (s *Service) analysisCheckout(ctx context.Context, source *pb.GitSource, env map[string]string) (string, string, func(), error) {
	noop := func() {}
	parsed, err := validateAnalysisSource(source)
	if err != nil {
		return "", "", noop, err
	}
	sub := source.GetSubdir()
	if err := os.MkdirAll(s.buildTmpDir, 0700); err != nil {
		return "", "", noop, err
	}
	root, err := os.MkdirTemp(s.buildTmpDir, "deplo-analysis-")
	if err != nil {
		return "", "", noop, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		_ = os.RemoveAll(root)
		return "", "", noop, err
	}
	cleanup := func() { _ = os.RemoveAll(root) }
	fail := func(err error) (string, string, func(), error) { cleanup(); return "", "", noop, err }
	cloneURL, _, auth := authenticatedURL(source.GetUrl(), source.GetToken())
	if parsed.Scheme == "ssh" {
		cloneURL, auth = source.GetUrl(), ""
	}
	run := func(input string, args ...string) (string, error) {
		return analysisGit(ctx, root, auth, input, args...)
	}
	if _, err := run("", "init", "-q"); err != nil {
		return fail(err)
	}
	if _, err := run("", "remote", "add", "origin", cloneURL); err != nil {
		return fail(err)
	}
	ref := source.GetBranch()
	if ref == "" {
		ref = "HEAD"
	}
	if source.GetCommit() != "" {
		ref = source.GetCommit()
	}
	// Fetch has no checkout, so ignored media blobs stay lazy on supporting servers.
	if _, err := run("", "fetch", "--depth=1", "--filter=blob:none", "origin", ref); err != nil {
		if _, err := run("", "fetch", "--depth=1", "origin", ref); err != nil {
			return fail(err)
		}
	}
	sha, err := run("", "rev-parse", "--verify", "FETCH_HEAD^{commit}")
	if err != nil {
		return fail(err)
	}
	sha = strings.TrimSpace(sha)
	if !fullCommitSha.MatchString(sha) || (source.GetCommit() != "" && sha != source.GetCommit()) {
		return fail(fmt.Errorf("could not verify requested commit"))
	}
	tree, err := run("", "ls-tree", "-r", "-z", sha)
	if err != nil {
		return fail(err)
	}
	patterns := []string{"/*"}
	forced := []string{}
	directories := map[string]bool{}
	links := []string{}
	for _, entry := range strings.Split(tree, "\x00") {
		if entry == "" {
			continue
		}
		header, name, ok := strings.Cut(entry, "\t")
		if !ok || !analysisRelative(name) || strings.ContainsAny(name, "\r\n") {
			return fail(fmt.Errorf("repository contains an unsupported path"))
		}
		if slices.Contains(analysisMedia, strings.ToLower(path.Ext(name))) {
			patterns = append(patterns, "!"+sparseLiteral(name))
		}
		for d := path.Dir(name); d != "."; d = path.Dir(d) {
			directories[d] = true
		}
		if strings.HasPrefix(header, "160000 ") {
			return fail(fmt.Errorf("Git submodules are not supported for analysis"))
		}
		if strings.HasPrefix(header, "120000 ") {
			target, err := run("", "show", sha+":"+name)
			if err != nil {
				return fail(err)
			}
			if path.IsAbs(target) || strings.ContainsAny(target, "\x00\r\n") {
				return fail(fmt.Errorf("repository symlink escapes the checkout"))
			}
			dest := path.Clean(path.Join(path.Dir(name), target))
			if !analysisRelative(dest) {
				return fail(fmt.Errorf("repository symlink escapes the checkout"))
			}
			forced = append(forced, sparseLiteral(name), sparseLiteral(dest), sparseLiteral(dest)+"/**")
			links = append(links, name)
		}
	}
	patterns = append(patterns, forced...)
	for _, key := range []string{"DEPLOPACK_CONFIG_FILE", "DEPLOPACK_SHELL_SCRIPT"} {
		if rel := env[key]; rel != "" {
			if !analysisRelative(rel) {
				return fail(fmt.Errorf("configuration path must be inside the project"))
			}
			patterns = append(patterns, sparseLiteral(path.Join(filepath.ToSlash(sub), rel)))
		}
	}
	if _, err := run(strings.Join(patterns, "\n")+"\n", "sparse-checkout", "set", "--no-cone", "--stdin"); err != nil {
		return fail(err)
	}
	if _, err := run("", "checkout", "-q", "--detach", sha); err != nil {
		return fail(err)
	}
	for d := range directories {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(d)), 0755); err != nil {
			return fail(err)
		}
	}
	for _, link := range links {
		candidate := filepath.Join(root, filepath.FromSlash(link))
		real, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			return fail(fmt.Errorf("repository contains an unresolved symlink"))
		}
		if real != root && !strings.HasPrefix(real, root+string(os.PathSeparator)) {
			return fail(fmt.Errorf("repository symlink escapes the checkout"))
		}
	}
	dir := filepath.Join(root, sub)
	real, err := filepath.EvalSymlinks(dir)
	if err != nil || (real != root && !strings.HasPrefix(real, root+string(os.PathSeparator))) {
		return fail(fmt.Errorf("project directory is outside the checkout or missing"))
	}
	info, err := os.Stat(real)
	if err != nil || !info.IsDir() {
		return fail(fmt.Errorf("project directory was not found"))
	}
	return real, sha, cleanup, nil
}

func validateAnalysisSource(source *pb.GitSource) (*url.URL, error) {
	if source == nil {
		return nil, fmt.Errorf("Git source is required")
	}
	parsed, err := url.Parse(source.GetUrl())
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http" && parsed.Scheme != "ssh" && parsed.Scheme != "git") {
		return nil, fmt.Errorf("Git URL must use HTTP, HTTPS, SSH or Git")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("Git URL must not contain query parameters or a fragment")
	}
	if parsed.User != nil && parsed.Scheme != "ssh" {
		return nil, fmt.Errorf("Git credentials must use the dedicated token field")
	}
	if parsed.Scheme == "ssh" && parsed.User != nil {
		if _, ok := parsed.User.Password(); ok {
			return nil, fmt.Errorf("SSH passwords are not supported")
		}
	}
	if source.GetToken() != "" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("Git tokens require HTTPS")
	}
	if !analysisRelative(source.GetSubdir()) {
		return nil, fmt.Errorf("project directory must be relative and inside the repository")
	}
	if strings.HasPrefix(source.GetBranch(), "-") || strings.ContainsAny(source.GetBranch(), "\r\n\x00") {
		return nil, fmt.Errorf("invalid Git ref")
	}
	if source.GetCommit() != "" && !fullCommitSha.MatchString(source.GetCommit()) {
		return nil, fmt.Errorf("commit must be a full SHA")
	}
	return parsed, nil
}

func analysisRelative(rel string) bool {
	if filepath.IsAbs(rel) || strings.ContainsAny(rel, "\\\x00\r\n") {
		return false
	}
	for _, part := range strings.Split(rel, "/") {
		if part == ".." || part == ".git" {
			return false
		}
	}
	return true
}

func sparseLiteral(name string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "*", "\\*", "?", "\\?", "[", "\\[", "]", "\\]", "!", "\\!", "#", "\\#", " ", "\\ ")
	return "/" + replacer.Replace(name)
}
