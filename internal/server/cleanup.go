package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
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

type buildCacheRecord struct {
	ID         string `json:"ID"`
	Size       string `json:"Size"`
	InUse      string `json:"InUse"`
	CreatedAt  string `json:"CreatedAt"`
	LastUsedAt string `json:"LastUsedAt"`
}

func cleanBuildCache(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := pruneBuildCache(ctx, p)
	sweepStaleBuildDirs(p, r)
	return r
}

const staleBuildDirAfter = 2 * time.Hour

func sweepStaleBuildDirs(p cleanupParams, r *pb.CleanupScopeResult) {
	if p.buildTmpDir == "" {
		return
	}
	entries, err := os.ReadDir(p.buildTmpDir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-staleBuildDirAfter)
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || !(strings.HasPrefix(name, "deplo-git-") || strings.HasPrefix(name, "deplo-build-")) {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		dir := filepath.Join(p.buildTmpDir, name)
		size := dirSize(dir)
		if !p.dryRun {
			if err := os.RemoveAll(dir); err != nil {
				continue
			}
		}
		r.ReclaimedBytes += size
		addItem(r, name)
		r.ItemsRemoved++
	}
}

func pruneBuildCache(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE}

	var estimate int64
	enumFailure := func() string {
		res, err := dockerQuery(ctx, cleanupQueryTimeout, "system", "df", "-v", "--format", "{{json .BuildCache}}")
		if err != nil {
			return err.Error()
		}
		if res.Code != 0 {
			return dockerErr("system df -v", res)
		}
		var records []buildCacheRecord
		if out := strings.TrimSpace(res.Stdout); out != "" && out != "null" {
			if err := json.Unmarshal([]byte(out), &records); err != nil {
				return "read the build cache: " + err.Error()
			}
		}
		for _, rec := range records {
			if rec.InUse == "true" {
				continue
			}
			at := rec.LastUsedAt
			if at == "" {
				at = rec.CreatedAt
			}
			if !olderThan(at, p.cutoff) {
				continue
			}
			estimate += parseHumanSize(rec.Size)
			addItem(r, rec.ID)
			r.ItemsRemoved++
		}
		return ""
	}()

	if p.dryRun {
		if enumFailure != "" {
			r.Error = enumFailure
		}
		r.ReclaimedBytes = estimate
		return r
	}
	if enumFailure != "" {
		log.Printf("deplo-agent: build-cache enumeration failed (%s); pruning anyway", enumFailure)
	}

	args := []string{"builder", "prune", "--force"}
	if p.minAgeHours > 0 {
		args = append(args, "--filter", "until="+strconv.Itoa(p.minAgeHours)+"h")
	} else {
		args = append(args, "--all")
	}
	cctx, cancel := context.WithTimeout(ctx, cleanupBuilderPruneTimeout)
	defer cancel()
	pres, err := removeObject(cctx, args...)
	if err != nil {
		return failedScope(r, err.Error())
	}
	if pres.Code != 0 {
		return failedScope(r, dockerErr("builder prune", pres))
	}
	total, totalKnown := parsePrunedTotal(pres.Stdout)
	if totalKnown && total == 0 {
		r.ReclaimedBytes = 0
		r.ItemsRemoved = 0
		r.Items = nil
		enforceBuildCacheCeiling(ctx, p, r)
		return r
	}
	if totalKnown {
		r.ReclaimedBytes = total
	} else {
		r.ReclaimedBytes = estimate
	}
	if r.ItemsRemoved == 0 {
		ids := prunedCacheRecordIDs(pres.Stdout)
		for _, id := range ids {
			addItem(r, id)
		}
		r.ItemsRemoved = int32(len(ids))
	}
	enforceBuildCacheCeiling(ctx, p, r)
	return r
}

func enforceBuildCacheCeiling(ctx context.Context, p cleanupParams, r *pb.CleanupScopeResult) {
	if p.dryRun || p.minAgeHours <= 0 {
		return
	}
	capArgs := buildCacheCapArgs(ctx, p.dataDir)
	if len(capArgs) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, cleanupBuilderPruneTimeout)
	defer cancel()
	res, err := removeObject(cctx, append([]string{"builder", "prune", "--force"}, capArgs...)...)
	if err != nil {
		log.Printf("deplo-agent: build-cache ceiling prune failed: %v", err)
		return
	}
	if res.Code != 0 {
		log.Printf("deplo-agent: build-cache ceiling prune failed: %s", dockerErr("builder prune", res))
		return
	}
	freed, known := parsePrunedTotal(res.Stdout)
	if !known || freed <= 0 {
		return
	}
	r.ReclaimedBytes += freed
	for _, id := range prunedCacheRecordIDs(res.Stdout) {
		addItem(r, id)
		r.ItemsRemoved++
	}
}

func cleanDanglingImages(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES}

	var estimate int64
	rawBefore := -1
	enumFailure := func() string {
		res, err := dockerQuery(ctx, cleanupQueryTimeout, "image", "ls", "--filter", "dangling=true", "--quiet")
		if err != nil {
			return err.Error()
		}
		if res.Code != 0 {
			return dockerErr("image ls", res)
		}
		before := uniqueLines(res.Stdout)
		rawBefore = len(before)
		images, err := inspectImages(ctx, before)
		if err != nil {
			return err.Error()
		}
		for _, im := range images {
			if !olderThan(im.created, p.cutoff) {
				continue
			}
			estimate += im.size
			addItem(r, im.id)
			r.ItemsRemoved++
		}
		return ""
	}()

	if p.dryRun {
		if enumFailure != "" {
			r.Error = enumFailure
		}
		r.ReclaimedBytes = estimate
		return r
	}
	if enumFailure != "" {
		log.Printf("deplo-agent: dangling-image enumeration failed (%s); pruning anyway", enumFailure)
	}

	args := []string{"image", "prune", "--force"}
	if p.minAgeHours > 0 {
		args = append(args, "--filter", "until="+strconv.Itoa(p.minAgeHours)+"h")
	}
	cctx, cancel := context.WithTimeout(ctx, cleanupImagePruneTimeout)
	defer cancel()
	pres, err := removeObject(cctx, args...)
	if err != nil {
		return failedScope(r, err.Error())
	}
	if pres.Code != 0 {
		return failedScope(r, dockerErr("image prune", pres))
	}
	total, totalKnown := parsePrunedTotal(pres.Stdout)
	if totalKnown && total == 0 {
		r.ReclaimedBytes = 0
		r.ItemsRemoved = 0
		r.Items = nil
		return r
	}
	if totalKnown {
		r.ReclaimedBytes = total
	} else {
		r.ReclaimedBytes = estimate
	}
	if rawBefore >= 0 {
		if res, err := dockerQuery(ctx, cleanupQueryTimeout, "image", "ls", "--filter", "dangling=true", "--quiet"); err == nil && res.Code == 0 {
			removed := rawBefore - len(uniqueLines(res.Stdout))
			if removed < 0 {
				removed = 0
			}
			r.ItemsRemoved = int32(removed)
			if removed == 0 {
				r.Items = nil
			}
		}
	}
	return r
}

func cleanOrphanBuildkitCache(ctx context.Context, p cleanupParams, idx *containerIndex) *pb.CleanupScopeResult {
	return cleanDanglingVolumes(ctx, p, idx, pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE,
		func(_, mountpoint string) bool {
			_, err := os.Stat(filepath.Join(mountpoint, buildkitSentinel))
			return err == nil
		})
}

var anonymousVolume = regexp.MustCompile(`^[0-9a-f]{64}$`)

func cleanOrphanVolumes(ctx context.Context, p cleanupParams, idx *containerIndex) *pb.CleanupScopeResult {
	return cleanDanglingVolumes(ctx, p, idx, pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES,
		func(name, _ string) bool { return anonymousVolume.MatchString(name) })
}

func cleanDanglingVolumes(ctx context.Context, p cleanupParams, idx *containerIndex, scope pb.CleanupScope, proof func(name, mountpoint string) bool) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: scope}

	res, err := dockerQuery(ctx, cleanupQueryTimeout, "volume", "ls", "--filter", "dangling=true", "--quiet")
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if res.Code != 0 {
		r.Error = dockerErr("volume ls", res)
		return r
	}

	var failures scopeFailures
	for _, name := range uniqueLines(res.Stdout) {
		if idx.volumes[name] {
			continue
		}
		vres, err := dockerQuery(ctx, cleanupQueryTimeout,
			"volume", "inspect", "--format", "{{.Mountpoint}}|{{.CreatedAt}}", name)
		if err != nil || vres.Code != 0 {
			continue
		}
		mountpoint, created, _ := strings.Cut(strings.TrimSpace(vres.Stdout), "|")
		if mountpoint == "" {
			continue
		}
		if !proof(name, mountpoint) {
			continue
		}
		if !olderThan(created, p.cutoff) {
			continue
		}

		size := dirSize(mountpoint)
		if !p.dryRun {
			cctx, cancel := context.WithTimeout(ctx, cleanupRemoveTimeout)
			rres, rerr := removeObject(cctx, "volume", "rm", name)
			cancel()
			if rerr != nil {
				failures.add(name, rerr.Error())
				continue
			}
			if rres.Code != 0 {
				failures.add(name, dockerErr("volume rm", rres))
				continue
			}
		}
		r.ReclaimedBytes += size
		addItem(r, name)
		r.ItemsRemoved++
	}

	r.Error = failures.summary()
	return r
}

func cleanUnusedAppImages(ctx context.Context, p cleanupParams, idx *containerIndex) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES}

	res, err := dockerQuery(ctx, cleanupQueryTimeout,
		"image", "ls", "--filter", "label=deplo.managed=true", "--quiet")
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if res.Code != 0 {
		r.Error = dockerErr("image ls", res)
		return r
	}
	images, err := inspectImages(ctx, uniqueLines(res.Stdout))
	if err != nil {
		r.Error = err.Error()
		return r
	}

	byGroup := map[string][]imageInfo{}
	for _, im := range images {
		if im.slug == "" {
			continue
		}
		key := im.repo + "\x00" + im.slug + "\x00" + im.service
		byGroup[key] = append(byGroup[key], im)
	}

	var failures scopeFailures
	for _, key := range sortedKeys(byGroup) {
		group := byGroup[key]
		sort.SliceStable(group, func(i, j int) bool {
			ti, oki := parseDockerTime(group[i].created)
			tj, okj := parseDockerTime(group[j].created)
			if oki && okj && !ti.Equal(tj) {
				return ti.After(tj)
			}
			return group[i].id < group[j].id
		})

		keep := p.keepImagesFor(group[0].slug)
		if p.liveSlugs != nil && !p.liveSlugs[group[0].slug] {
			keep = 0
		}

		for rank, im := range group {
			if rank < keep {
				continue
			}
			if idx.images[im.id] {
				continue
			}
			if !olderThan(im.created, p.appImageCutoff) {
				continue
			}

			if !p.dryRun {
				cctx, cancel := context.WithTimeout(ctx, cleanupRemoveTimeout)
				rres, rerr := removeObject(cctx, "rmi", im.id)
				cancel()
				if rerr != nil {
					failures.add(im.id, rerr.Error())
					continue
				}
				if rres.Code != 0 {
					failures.add(im.id, dockerErr("rmi", rres))
					continue
				}
			}
			r.ReclaimedBytes += im.size
			addItem(r, im.id)
			r.ItemsRemoved++
		}
	}

	r.Error = failures.summary()
	return r
}

var buildToolingImages = []string{
	"buildpacksio/pack", "heroku/builder", "paketobuildpacks/", "moby/buildkit",
	"ghcr.io/railwayapp/nixpacks", "ghcr.io/railwayapp/railpack", "docker/dockerfile",
}

func isBuildTooling(repo string) bool {
	for _, prefix := range buildToolingImages {
		if strings.HasPrefix(repo, prefix) {
			return true
		}
	}
	return false
}

func cleanUnusedPulledImages(ctx context.Context, p cleanupParams, idx *containerIndex) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES}

	res, err := dockerQuery(ctx, cleanupQueryTimeout,
		"image", "ls", "--filter", "dangling=false", "--quiet")
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if res.Code != 0 {
		r.Error = dockerErr("image ls", res)
		return r
	}
	images, err := inspectImages(ctx, uniqueLines(res.Stdout))
	if err != nil {
		r.Error = err.Error()
		return r
	}
	sort.Slice(images, func(i, j int) bool { return images[i].id < images[j].id })

	var failures scopeFailures
	for _, im := range images {
		if im.managed || idx.images[im.id] || isBuildTooling(im.repo) || len(im.tags) == 0 {
			continue
		}
		if t, ok := parseDockerTime(im.lastTag); !ok || t.IsZero() {
			continue
		}
		if !olderThan(im.lastTag, p.cutoff) || !olderThan(im.lastTag, p.appImageCutoff) {
			continue
		}
		if !p.dryRun {
			failed := false
			for _, tag := range im.tags {
				cctx, cancel := context.WithTimeout(ctx, cleanupRemoveTimeout)
				rres, rerr := removeObject(cctx, "rmi", tag)
				cancel()
				if rerr != nil {
					failures.add(tag, rerr.Error())
					failed = true
					break
				}
				if rres.Code != 0 {
					failures.add(tag, dockerErr("rmi", rres))
					failed = true
					break
				}
			}
			if failed {
				continue
			}
		}
		r.ReclaimedBytes += im.size
		addItem(r, im.id)
		r.ItemsRemoved++
	}

	r.Error = failures.summary()
	return r
}

type imageInfo struct {
	id      string
	slug    string
	service string
	repo    string
	created string
	size    int64
	managed bool
	lastTag string
	tags    []string
}

func inspectImages(ctx context.Context, ids []string) ([]imageInfo, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := append([]string{"image", "inspect", "--format",
		`{{.Id}}|{{with (index .Config "Labels")}}{{index . "deplo.slug"}}|{{index . "deplo.service"}}{{else}}|{{end}}|{{.Created}}|{{.Size}}|` +
			`{{if .RepoTags}}{{index .RepoTags 0}}{{else if .RepoDigests}}{{index .RepoDigests 0}}{{end}}|` +
			`{{with (index .Config "Labels")}}{{index . "deplo.managed"}}{{end}}|` +
			`{{with (index . "Metadata")}}{{with (index . "LastTagTime")}}{{json .}}{{end}}{{end}}|{{join .RepoTags ","}}`}, ids...)
	res, err := dockerQuery(ctx, cleanupQueryTimeout, args...)
	if err != nil {
		return nil, err
	}

	label := func(s string) string {
		s = strings.TrimSpace(s)
		if s == "<no value>" {
			return ""
		}
		return s
	}
	var out []imageInfo
	for _, line := range splitLines(res.Stdout) {
		parts := strings.Split(line, "|")
		if len(parts) < 5 {
			continue
		}
		size, err := strconv.ParseInt(strings.TrimSpace(parts[4]), 10, 64)
		if err != nil {
			continue
		}
		repo := ""
		if len(parts) > 5 {
			repo = repoOf(strings.TrimSpace(parts[5]))
		}
		im := imageInfo{
			id:      strings.TrimSpace(parts[0]),
			slug:    label(parts[1]),
			service: label(parts[2]),
			repo:    repo,
			created: strings.TrimSpace(parts[3]),
			size:    size,
		}
		if len(parts) > 6 {
			im.managed = label(parts[6]) == "true"
		}
		if len(parts) > 7 {
			im.lastTag = strings.Trim(strings.TrimSpace(parts[7]), `"`)
		}
		if len(parts) > 8 {
			for _, t := range strings.Split(parts[8], ",") {
				if t = strings.TrimSpace(t); t != "" {
					im.tags = append(im.tags, t)
				}
			}
		}
		out = append(out, im)
	}
	if res.Code != 0 && len(out) == 0 {
		return nil, errors.New(dockerErr("image inspect", res))
	}
	return out, nil
}

func repoOf(ref string) string {
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		ref = ref[:i]
	}
	return strings.TrimSpace(ref)
}

type scopeFailures struct {
	msgs []string
	n    int
}

func (f *scopeFailures) add(object, msg string) {
	f.n++
	if len(f.msgs) < 3 {
		f.msgs = append(f.msgs, object+": "+msg)
	}
}

func (f *scopeFailures) summary() string {
	if f.n == 0 {
		return ""
	}
	s := strings.Join(f.msgs, "; ")
	if extra := f.n - len(f.msgs); extra > 0 {
		s += fmt.Sprintf(" (and %d more)", extra)
	}
	return s
}

func failedScope(r *pb.CleanupScopeResult, msg string) *pb.CleanupScopeResult {
	r.ReclaimedBytes = 0
	r.ItemsRemoved = 0
	r.Items = nil
	r.Error = msg
	return r
}

func addItem(r *pb.CleanupScopeResult, id string) {
	if len(r.Items) < cleanupMaxItems {
		r.Items = append(r.Items, id)
	}
}

func olderThan(ts string, cutoff time.Time) bool {
	if cutoff.IsZero() {
		return true
	}
	t, ok := parseDockerTime(ts)
	if !ok {
		return false
	}
	return t.Before(cutoff)
}

var dockerTimeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999 -0700 MST",
	"2006-01-02 15:04:05 -0700 MST",
}

func parseDockerTime(ts string) (time.Time, bool) {
	ts = strings.TrimSpace(ts)
	if ts == "" {
		return time.Time{}, false
	}
	for _, layout := range dockerTimeLayouts {
		if t, err := time.Parse(layout, ts); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func parseHumanSize(s string) int64 {
	s = strings.TrimSpace(s)
	end := 0
	for end < len(s) && (s[end] == '.' || (s[end] >= '0' && s[end] <= '9')) {
		end++
	}
	if end == 0 {
		return 0
	}
	n, err := strconv.ParseFloat(s[:end], 64)
	if err != nil {
		return 0
	}
	mult, ok := sizeUnits[strings.TrimSpace(s[end:])]
	if !ok {
		return 0
	}
	return int64(math.Round(n * mult))
}

var sizeUnits = map[string]float64{
	"":    1,
	"B":   1,
	"kB":  1e3,
	"KB":  1e3,
	"MB":  1e6,
	"GB":  1e9,
	"TB":  1e12,
	"PB":  1e15,
	"KiB": 1 << 10,
	"MiB": 1 << 20,
	"GiB": 1 << 30,
	"TiB": 1 << 40,
}

func parsePrunedTotal(out string) (int64, bool) {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		for _, prefix := range []string{"Total reclaimed space:", "Total:"} {
			v, ok := strings.CutPrefix(line, prefix)
			if !ok {
				continue
			}
			v = strings.TrimSpace(v)
			if v != "" && v[0] >= '0' && v[0] <= '9' {
				return parseHumanSize(v), true
			}
		}
	}
	return 0, false
}

var cacheRecordID = regexp.MustCompile(`^[a-z0-9]{12,}$`)

func prunedCacheRecordIDs(out string) []string {
	var ids []string
	for _, line := range splitLines(out) {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if cacheRecordID.MatchString(fields[0]) {
			ids = append(ids, fields[0])
		}
	}
	return ids
}

func dirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512
			return nil
		}
		if !d.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func splitLines(out string) []string {
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func uniqueLines(out string) []string {
	seen := map[string]bool{}
	var lines []string
	for _, l := range splitLines(out) {
		if !seen[l] {
			seen[l] = true
			lines = append(lines, l)
		}
	}
	return lines
}

func sortedKeys(m map[string][]imageInfo) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func dockerErr(what string, res dockercli.Result) string {
	msg := strings.TrimSpace(res.Stderr)
	if msg == "" {
		msg = strings.TrimSpace(res.Stdout)
	}
	if msg == "" {
		return fmt.Sprintf("docker %s exited %d", what, res.Code)
	}
	return fmt.Sprintf("docker %s: %s", what, msg)
}

func cleanLeftoverAppFiles(p cleanupParams) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES}
	if len(p.liveSlugs) == 0 {
		return skippedScope(r.Scope, errors.New(
			"the control plane sent no list of live stacks, and an empty list is not a reason to delete every app's files"))
	}
	if p.stackDir == "" {
		return skippedScope(r.Scope, errors.New("this agent has no stack directory configured"))
	}

	root := filepath.Join(p.stackDir, "files")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return r
		}
		r.Error = err.Error()
		return r
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		slug := e.Name()
		if p.liveSlugs[slug] {
			continue
		}
		if validateSlug(slug) != nil {
			continue
		}
		dir := filepath.Join(root, slug)
		info, err := e.Info()
		if err != nil || info.ModTime().After(p.filesCutoff) {
			continue
		}
		size := dirSize(dir)
		if !p.dryRun {
			if err := os.RemoveAll(dir); err != nil {
				if r.Error == "" {
					r.Error = fmt.Sprintf("remove %s: %v", slug, err)
				}
				continue
			}
		}
		r.ItemsRemoved++
		r.ReclaimedBytes += size
		if len(r.Items) < cleanupMaxItems {
			r.Items = append(r.Items, slug)
		}
	}
	return r
}

func cleanLeftoverNetworks(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_NETWORKS}
	if len(p.liveNetworks) == 0 && len(p.liveSlugs) == 0 {
		return skippedScope(r.Scope, errors.New(
			"the control plane sent no list of live networks, and an empty list is not a reason to remove every app's network"))
	}
	res, err := dockerQuery(ctx, cleanupQueryTimeout, "network", "ls", "--format", "{{.Name}}")
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if res.Code != 0 {
		r.Error = dockerErr("network ls", res)
		return r
	}
	names := splitLines(res.Stdout)
	sort.Strings(names)
	for _, name := range names {
		if !leftoverNetworkCandidate(name, p) {
			continue
		}
		attached, created, ok := networkState(ctx, name)
		if !ok || attached > 0 {
			continue
		}
		if created.After(p.filesCutoff) {
			continue
		}
		if p.dryRun {
			r.ItemsRemoved++
			addItem(r, name)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, cleanupRemoveTimeout)
		_, _ = removeObject(cctx, "network", "disconnect", "-f", name, traefikContainer)
		rm, err := removeObject(cctx, "network", "rm", name)
		cancel()
		if err != nil || rm.Code != 0 {
			if r.Error == "" {
				r.Error = fmt.Sprintf("remove %s: %s", name, dockerErr("network rm", rm))
			}
			continue
		}
		r.ItemsRemoved++
		addItem(r, name)
	}
	return r
}

func leftoverNetworkCandidate(name string, p cleanupParams) bool {
	if dockercli.IsTenantNetwork(name) {
		return len(p.liveNetworks) > 0 && !p.liveNetworks[name]
	}
	if len(p.liveSlugs) == 0 {
		return false
	}
	project, _, ok := strings.Cut(name, "_")
	if !ok {
		return false
	}
	slug, ok := strings.CutPrefix(project, "deplo-")
	if !ok || validateSlug(slug) != nil {
		return false
	}
	return !p.liveSlugs[slug]
}

func attachedExcludingProxy(names string) int {
	n := 0
	for _, c := range strings.Fields(names) {
		if c != traefikContainer {
			n++
		}
	}
	return n
}

func networkState(ctx context.Context, name string) (attached int, created time.Time, ok bool) {
	res, err := dockerQuery(ctx, cleanupQueryTimeout,
		"network", "inspect", "-f",
		"{{range .Containers}}{{.Name}} {{end}}|{{json .Created}}", name)
	if err != nil || res.Code != 0 {
		return 0, time.Time{}, false
	}
	part := strings.SplitN(strings.TrimSpace(res.Stdout), "|", 2)
	if len(part) != 2 {
		return 0, time.Time{}, false
	}
	n := attachedExcludingProxy(part[0])
	t, err := time.Parse(time.RFC3339Nano, strings.Trim(part[1], `"`))
	if err != nil {
		return 0, time.Time{}, false
	}
	return n, t, true
}
