package server

import (
	"context"
	"errors"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strings"
	"time"
)

const (
	cleanupTimeout             = 30 * time.Minute
	cleanupQueryTimeout        = 60 * time.Second
	cleanupBuilderPruneTimeout = 10 * time.Minute
	cleanupImagePruneTimeout   = 5 * time.Minute
	cleanupRemoveTimeout       = 30 * time.Second

	cleanupMaxItems = 200

	buildkitSentinel = "buildkitd.lock"

	appImageDeployGrace = time.Hour

	leftoverFilesGrace = time.Hour
)

var removeObject = func(ctx context.Context, args ...string) (dockercli.Result, error) {
	return dockercli.Run(ctx, cleanupTimeout, args...)
}
var dockerQuery = func(ctx context.Context, timeout time.Duration, args ...string) (dockercli.Result, error) {
	return dockercli.Run(ctx, timeout, args...)
}
var dockerAvailable = dockercli.Available

type cleanupParams struct {
	dryRun           bool
	minAgeHours      int
	keepImagesPerApp int
	keepPerSlug      map[string]int
	dataDir          string
	stackDir         string
	buildTmpDir      string
	liveSlugs        map[string]bool
	liveNetworks     map[string]bool
	cutoff           time.Time
	appImageCutoff   time.Time
	filesCutoff      time.Time
}

func (p cleanupParams) keepImagesFor(slug string) int {
	if n, ok := p.keepPerSlug[slug]; ok {
		return n
	}
	return p.keepImagesPerApp
}

// DockerCleanup reclaims Docker disk on this host within the allow-listed scopes (the RPC contract is in proto/agent.proto).
func (s *Service) DockerCleanup(ctx context.Context, req *pb.DockerCleanupRequest) (*pb.DockerCleanupResponse, error) {
	if !dockerAvailable(ctx) {
		return nil, status.Error(codes.Unavailable, "docker is not reachable on this host")
	}

	ctx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()

	params := cleanupParams{
		dryRun:           req.GetDryRun(),
		minAgeHours:      int(req.GetMinAgeHours()),
		keepImagesPerApp: int(req.GetKeepImagesPerApp()),
		dataDir:          s.dataDir,
		stackDir:         s.stackDir,
		buildTmpDir:      s.buildTmpDir,
	}
	if live := req.GetLiveSlugs(); len(live) > 0 {
		params.liveSlugs = make(map[string]bool, len(live))
		for _, slug := range live {
			params.liveSlugs[slug] = true
		}
	}
	if live := req.GetLiveNetworks(); len(live) > 0 {
		params.liveNetworks = make(map[string]bool, len(live))
		for _, n := range live {
			params.liveNetworks[n] = true
		}
	}
	if params.minAgeHours < 0 {
		params.minAgeHours = 0
	}
	if params.keepImagesPerApp < 1 {
		params.keepImagesPerApp = 1
	}
	if raw := req.GetKeepPerSlug(); len(raw) > 0 {
		params.keepPerSlug = make(map[string]int, len(raw))
		for slug, n := range raw {
			if n < 1 {
				n = 1
			}
			params.keepPerSlug[slug] = int(n)
		}
	}
	if params.minAgeHours > 0 {
		params.cutoff = time.Now().Add(-time.Duration(params.minAgeHours) * time.Hour)
	}
	params.appImageCutoff = time.Now().Add(-appImageDeployGrace)
	params.filesCutoff = time.Now().Add(-leftoverFilesGrace)

	var idx *containerIndex
	var idxErr error
	var idxBuilt bool
	requireIndex := func() (*containerIndex, error) {
		if !idxBuilt {
			idx, idxErr = buildContainerIndex(ctx)
			idxBuilt = true
		}
		return idx, idxErr
	}

	resp := &pb.DockerCleanupResponse{Ok: true}
	seen := map[pb.CleanupScope]bool{}
	for _, scope := range req.GetScopes() {
		if seen[scope] {
			continue
		}
		seen[scope] = true

		var r *pb.CleanupScopeResult
		switch scope {
		case pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE:
			r = cleanBuildCache(ctx, params)
		case pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES:
			r = cleanDanglingImages(ctx, params)
		case pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE:
			index, err := requireIndex()
			if err != nil {
				r = skippedScope(scope, err)
			} else {
				r = cleanOrphanBuildkitCache(ctx, params, index)
			}
		case pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES:
			index, err := requireIndex()
			if err != nil {
				r = skippedScope(scope, err)
			} else {
				r = cleanUnusedAppImages(ctx, params, index)
			}
		case pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES:
			r = cleanLeftoverAppFiles(params)
		case pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_NETWORKS:
			r = cleanLeftoverNetworks(ctx, params)
		case pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES:
			index, err := requireIndex()
			if err != nil {
				r = skippedScope(scope, err)
			} else {
				r = cleanOrphanVolumes(ctx, params, index)
			}
		case pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES:
			index, err := requireIndex()
			if err != nil {
				r = skippedScope(scope, err)
			} else {
				r = cleanUnusedPulledImages(ctx, params, index)
			}
		default:
			return nil, status.Errorf(codes.InvalidArgument,
				"unknown cleanup scope %q (this agent only implements the allow-listed scopes)", scope.String())
		}

		resp.Results = append(resp.Results, r)
		resp.ReclaimedBytes += r.GetReclaimedBytes()
	}

	items := 0
	for _, r := range resp.GetResults() {
		items += int(r.GetItemsRemoved())
	}
	verb := "removed"
	if params.dryRun {
		verb = "would remove (dry run)"
	}
	log.Printf("deplo-agent: docker cleanup %s %d object(s) across %d scope(s), reclaiming %d bytes",
		verb, items, len(resp.GetResults()), resp.GetReclaimedBytes())
	return resp, nil
}
func skippedScope(scope pb.CleanupScope, err error) *pb.CleanupScopeResult {
	return &pb.CleanupScopeResult{
		Scope:   scope,
		Skipped: true,
		Error:   "skipped: " + err.Error(),
	}
}

type containerIndex struct {
	images  map[string]bool
	volumes map[string]bool
}

func buildContainerIndex(ctx context.Context) (*containerIndex, error) {
	res, err := dockerQuery(ctx, cleanupQueryTimeout, "ps", "-aq")
	if err != nil {
		return nil, err
	}
	if res.Code != 0 {
		return nil, errors.New(dockerErr("ps -aq", res))
	}

	idx := &containerIndex{images: map[string]bool{}, volumes: map[string]bool{}}
	ids := splitLines(res.Stdout)
	if len(ids) == 0 {
		return idx, nil
	}

	args := append([]string{"inspect", "--format",
		`{{.Image}}|{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}},{{end}}{{end}}`}, ids...)
	res, err = dockerQuery(ctx, cleanupQueryTimeout, args...)
	if err != nil {
		return nil, err
	}
	if res.Code != 0 {
		return nil, errors.New(dockerErr("inspect", res))
	}

	for _, line := range splitLines(res.Stdout) {
		image, vols, _ := strings.Cut(line, "|")
		if image = strings.TrimSpace(image); image != "" {
			idx.images[image] = true
		}
		for _, v := range strings.Split(vols, ",") {
			if v = strings.TrimSpace(v); v != "" {
				idx.volumes[v] = true
			}
		}
	}
	return idx, nil
}
