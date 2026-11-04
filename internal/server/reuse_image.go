package server

import (
	"context"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// reuseBuiltImage skips the clone and the build when the branch still points at the commit an earlier
// deployment built with the same settings and that image is still here: it becomes this deployment's.
func reuseBuiltImage(ctx context.Context, req *pb.DeployRequest, e *emitter) (string, bool) {
	r := req.GetReuseImage()
	if r == nil || r.GetCommitSha() == "" || req.GetBuildOnly() || isStack(req) ||
		!sameImageRepository(r.GetImageRef(), req.GetImageRef()) {
		return "", false
	}
	g := req.GetGit()
	head := strings.TrimSpace(g.GetCommit())
	if head == "" {
		cloneURL, _, authHeader := authenticatedURL(g.GetUrl(), g.GetToken())
		head = remoteHead(ctx, cloneURL, authHeader, g.GetBranch())
	}
	if head == "" || head != r.GetCommitSha() {
		return "", false
	}
	if res, err := dockercli.Run(ctx, 30*time.Second, "tag", r.GetImageRef(), req.GetImageRef()); err != nil || res.Code != 0 {
		return "", false
	}
	e.log("info", "Commit "+shortSha(head)+" is already built with these settings: reusing that image, nothing to build")
	return head, true
}

// sameImageRepository: both are deplo/ image refs of the same app, so no app can take on another's image.
func sameImageRepository(a, b string) bool {
	if validateImageRef(a) != nil || validateImageRef(b) != nil {
		return false
	}
	ra, _, _ := strings.Cut(a, ":")
	rb, _, _ := strings.Cut(b, ":")
	return ra == rb
}

// remoteHead is the commit the branch, or the remote's default HEAD, points at; "" when it cannot be read.
func remoteHead(ctx context.Context, cloneURL, authHeader, branch string) string {
	ref := "HEAD"
	if b := strings.TrimSpace(branch); b != "" {
		ref = "refs/heads/" + b
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := dockercli.Command(cctx, "git", "ls-remote", "--", cloneURL, ref)
	cmd.Env = gitEnv("", authHeader)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	for _, line := range splitLines(string(out)) {
		if sha, name, ok := strings.Cut(line, "\t"); ok && name == ref && len(sha) >= 40 {
			return sha
		}
	}
	return ""
}
