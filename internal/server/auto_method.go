package server

import (
	"context"
	"os"
	"path/filepath"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

// detectBuildMethod picks how a "Detect automatically" app builds, on every deploy: the build
// folder's Dockerfile, else its compose file, else Railpack. dockerfile is the path to build.
func detectBuildMethod(dir, composeFile string) (method, dockerfile string) {
	if p, ok := dockerfileIn(dir); ok {
		return "dockerfile", p
	}
	names := composeFileNames
	if composeFile != "" {
		names = []string{composeFile}
	}
	for _, name := range names {
		if p, ok := safepath.Join(dir, name); ok {
			if _, err := os.Lstat(p); err == nil {
				return "compose", ""
			}
		}
	}
	return "railpack", ""
}

// dockerfileIn finds dir's Dockerfile, through a link only when it stays inside dir.
func dockerfileIn(dir string) (string, bool) {
	p := filepath.Join(dir, "Dockerfile")
	li, err := os.Lstat(p)
	if err != nil {
		return "", false
	}
	if li.Mode()&os.ModeSymlink == 0 {
		return "Dockerfile", li.Mode().IsRegular()
	}
	real, err := safepath.Inside(dir, p)
	root, rerr := filepath.EvalSymlinks(dir)
	if err != nil || rerr != nil || real == root {
		return "", false
	}
	rel, err := filepath.Rel(root, real)
	if fi, serr := os.Stat(real); err != nil || serr != nil || !fi.Mode().IsRegular() {
		return "", false
	}
	return rel, true
}

// buildDetected builds an auto app the way its source says, and reports which in the result.
func (s *Service) buildDetected(ctx context.Context, req *pb.DeployRequest, dir, commitSha string, e *emitter) bool {
	method, dockerfile := detectBuildMethod(dir, req.GetBuildSpec().GetComposeFile())
	where := "the root directory"
	if sub := req.GetGit().GetSubdir(); req.GetSourceKind() == pb.SourceKind_SOURCE_KIND_GIT && sub != "" {
		where = sub
	}
	switch method {
	case "dockerfile":
		e.builtWith = "dockerfile"
		e.log("info", "Found a Dockerfile in "+where+": building it")
		return s.buildDockerfile(ctx, req, &pb.DockerfileBuild{DockerfilePath: dockerfile, ContextPath: "."}, dir, e)
	case "compose":
		redirectToCompose(req, dir, commitSha, e)
		return false
	}
	e.builtWith = "railpack"
	e.log("info", "No Dockerfile or compose file in "+where+": Railpack works out the build")
	return s.buildRailpack(ctx, req, dir, e)
}
