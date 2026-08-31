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

// cleanup.go implements DockerCleanup - reclaiming Docker disk on the host. THE PROOF
// IS NEVER A LABEL. If the index cannot be built, the scopes that rest on it are
// SKIPPED, never guessed at.

const (
	// The whole sweep's budget. Generous: a full host can hold tens of GB across
	// dozens of objects, and every removal is a separate docker call. Mirrors the
	// long-op budgets elsewhere (backupStepTimeout, volumeCopyTimeout).
	cleanupTimeout = 30 * time.Minute
	// One enumeration call (`docker ps`, `image ls`, `system df`, an inspect).
	cleanupQueryTimeout = 60 * time.Second
	// `docker builder prune` walks the whole BuildKit cache; `docker image prune`
	// walks the layer store. Both are slow on a full host and neither is
	// interruptible, so they get their own budgets inside the sweep's.
	cleanupBuilderPruneTimeout = 10 * time.Minute
	cleanupImagePruneTimeout   = 5 * time.Minute
	// One `docker volume rm` / `docker rmi`.
	cleanupRemoveTimeout = 30 * time.Second

	// CleanupScopeResult.items is a UI affordance, not a ledger - items_removed is
	// the authoritative count. Bound the list so a host with thousands of dangling
	// layers cannot blow up the response.
	cleanupMaxItems = 200

	// The file moby/buildkit's daemon holds open in its state dir (/var/lib/buildkit,
	// which the image declares as a VOLUME).
	buildkitSentinel = "buildkitd.lock"

	// How fresh an app image must be to be untouchable by UNUSED_APP_IMAGES, REGARDLESS of
	// the policy's min_age_hours.
	appImageDeployGrace = time.Hour

	// How recently a files/<slug> directory may have changed and still be spared. The
	// live-slug list is a snapshot taken before the sweep dialled this host, so a stack
	// created in between is missing from it while its directory is already on disk.
	leftoverFilesGrace = time.Hour
)

// removeObject is the ONE host-mutating docker call in this file: every prune and every
// `rm` goes through it, and nothing else in here can delete anything.
var removeObject = func(ctx context.Context, args ...string) (dockercli.Result, error) {
	return dockercli.Run(ctx, cleanupTimeout, args...)
}

// dockerQuery is the READ-ONLY half: enumeration only, it never mutates the host.
// A seam as well, so the tests can drive the handler against a synthetic host -
// the safety properties have to be provable with no Docker daemon in the loop.
var dockerQuery = func(ctx context.Context, timeout time.Duration, args ...string) (dockercli.Result, error) {
	return dockercli.Run(ctx, timeout, args...)
}

// dockerAvailable is dockercli.Available, seamed for the same reason.
var dockerAvailable = dockercli.Available

// cleanupParams is the request, normalised once: the scopes read it, never the
// raw proto.
type cleanupParams struct {
	dryRun           bool
	minAgeHours      int
	keepImagesPerApp int
	// keepPerSlug overrides keepImagesPerApp for the slugs it names - an app's own
	// rollback depth. Absent slug => the scalar. Normalised with the same floor of
	// 1, so a scope reading it can never keep zero images of an app.
	keepPerSlug map[string]int
	// dataDir is the filesystem the build-cache ceiling is derived from - the
	// one the cache actually lands on (build_cache_cap.go).
	dataDir string
	// stackDir is where the rendered stacks and their files/<slug> directories
	// live - the only thing LEFTOVER_APP_FILES looks at.
	stackDir string
	// liveSlugs is every stack the control plane still knows about, instance-wide.
	// Nil/empty means it could not tell us, which SKIPS the scope that reads it.
	liveSlugs map[string]bool
	// liveNetworks is every tenant network the control plane still knows about, same
	// contract and same fail-closed rule as liveSlugs.
	liveNetworks map[string]bool
	// cutoff is the newest a CACHE-type object (build cache, dangling image, orphan
	// buildkit volume) may be to qualify. ZERO means "no age filter".
	cutoff time.Time
	// appImageCutoff is the newest an APP image may be to qualify, always
	// appImageDeployGrace ago, never the policy cutoff. See the constant's comment.
	appImageCutoff time.Time
	// filesCutoff is the newest a files/<slug> directory may be to qualify. Same
	// shape and the same reason as appImageCutoff: a directory being written right
	// now belongs to a stack the control plane's list may predate.
	filesCutoff time.Time
}

// keepImagesFor is how many of an app's newest images survive: its own entry when the
// control plane sent one, the host-wide scalar otherwise.
func (p cleanupParams) keepImagesFor(slug string) int {
	if n, ok := p.keepPerSlug[slug]; ok {
		return n
	}
	return p.keepImagesPerApp
}

// DockerCleanup reclaims Docker disk on this host within the allow-listed scopes (see
// the RPC contract in proto/agent.proto and the file comment above).
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
		// Always keep the current tag, even when no container references it: a
		// stopped app must stay redeployable without a rebuild from source.
		params.keepImagesPerApp = 1
	}
	// Same floor per slug, applied here rather than at every read, so no scope can
	// be handed a zero. An empty map stays nil - keepImagesFor then answers the
	// scalar for every app, which is exactly the pre-rollback behaviour.
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

	// The reverse index costs one inspect over every container on the host, and two
	// scopes need it, so build it at most once, and only if a scope actually asks.
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
			continue // a repeated scope is a caller bug, not a reason to prune twice
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
			// No container index here: the proof is the control plane's own list of
			// live stacks, and an absent list skips the scope rather than guessing.
			r = cleanLeftoverAppFiles(params)
		case pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_NETWORKS:
			r = cleanLeftoverNetworks(ctx, params)
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
	// Log every host-mutating outcome (and the dry runs, so a "why did it delete
	// that?" can be reconstructed from the agent's journal alone).
	verb := "removed"
	if params.dryRun {
		verb = "would remove (dry run)"
	}
	log.Printf("deplo-agent: docker cleanup %s %d object(s) across %d scope(s), reclaiming %d bytes",
		verb, items, len(resp.GetResults()), resp.GetReclaimedBytes())
	return resp, nil
}

// skippedScope reports a scope the agent DECLINED rather than failed: it could not
// build the container-reference index this scope's safety rests on, so it refused
// to guess. The sweep continues; the other scopes still run.
func skippedScope(scope pb.CleanupScope, err error) *pb.CleanupScopeResult {
	return &pb.CleanupScopeResult{
		Scope:   scope,
		Skipped: true,
		Error:   "skipped: " + err.Error(),
	}
}

// ---------------------------------------------------------------------------
// The container-reference reverse index - the ownership test everything rests on
// ---------------------------------------------------------------------------

// containerIndex is every image and every volume referenced by ANY container on
// this host, running OR exited. Membership here means "in use"; the absence of a
// label means nothing at all.
type containerIndex struct {
	images  map[string]bool // full sha256 image ids (`docker inspect` .Image)
	volumes map[string]bool // volume names, from each container's volume mounts
}

// buildContainerIndex reads the whole host in two calls. `docker ps -aq` is what makes
// it safe: -a includes EXITED containers, and a stopped Deplo app is a live app whose
// image and volumes must survive.
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
		return idx, nil // a host with no containers at all: an empty index is complete
	}

	args := append([]string{"inspect", "--format",
		`{{.Image}}|{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}},{{end}}{{end}}`}, ids...)
	res, err = dockerQuery(ctx, cleanupQueryTimeout, args...)
	if err != nil {
		return nil, err
	}
	if res.Code != 0 {
		// docker inspect exits non-zero when ANY id is unknown (e.g. a container that
		// vanished mid-sweep) while still printing the rest. We refuse the partial
		// result on purpose: see the "partial index is dangerous" note above.
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

// ---------------------------------------------------------------------------
// Scope: build cache - `docker builder prune`
// ---------------------------------------------------------------------------

// buildCacheRecord is one row of `docker system df -v`'s BuildCache array. Docker
// renders every field as a STRING here, booleans included.
type buildCacheRecord struct {
	ID         string `json:"ID"`
	Size       string `json:"Size"`
	InUse      string `json:"InUse"`
	CreatedAt  string `json:"CreatedAt"`
	LastUsedAt string `json:"LastUsedAt"`
}

// cleanBuildCache reclaims the daemon's own BuildKit cache.
func cleanBuildCache(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
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
				continue // a build is holding it right now
			}
			// Age off last use where docker knows it, creation otherwise - the same
			// choice `--filter until=` makes.
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
		// A dry run IS the enumeration; without it there is nothing to answer with.
		if enumFailure != "" {
			r.Error = enumFailure
		}
		r.ReclaimedBytes = estimate
		return r
	}
	if enumFailure != "" {
		// The preview failed; the prune is still safe (docker's filter decides) and
		// still owed. Items stay empty - docker's total below is the honest number.
		log.Printf("deplo-agent: build-cache enumeration failed (%s); pruning anyway", enumFailure)
	}

	args := []string{"builder", "prune", "--force"}
	if p.minAgeHours > 0 {
		args = append(args, "--filter", "until="+strconv.Itoa(p.minAgeHours)+"h")
	} else {
		// With no age filter, sweep the whole cache including the records docker
		// would otherwise hold back. `--all` is safe HERE in a way `image prune -a`
		// never is: a cache record is derived data, an image is not.
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
		// Docker freed nothing, so our enumerated candidates were NOT removed (its
		// filter disagreed, or another sweep beat us to them). Zero the whole line -
		// count and list included, or the history reports removals that never were.
		r.ReclaimedBytes = 0
		r.ItemsRemoved = 0
		r.Items = nil
		// The ceiling still runs: "the age filter found nothing to drop" is the
		// EXACT state a growing cache is in on a busy host, so returning here would
		// skip the bound precisely when it is needed.
		enforceBuildCacheCeiling(ctx, p, r)
		return r
	}
	// Docker prints the total it actually freed; that beats our estimate. The
	// estimate is the fallback for an output shape we cannot parse, never a made-up
	// number, just the same sum the dry run reported.
	if totalKnown {
		r.ReclaimedBytes = total
	} else {
		r.ReclaimedBytes = estimate
	}
	if r.ItemsRemoved == 0 {
		// Bytes were freed but our enumeration saw no candidate (it failed, or its
		// parse disagreed with docker's filter): recover the records from the prune's
		// own output so the count and the bytes tell one story.
		ids := prunedCacheRecordIDs(pres.Stdout)
		for _, id := range ids {
			addItem(r, id)
		}
		r.ItemsRemoved = int32(len(ids))
	}
	enforceBuildCacheCeiling(ctx, p, r)
	return r
}

// enforceBuildCacheCeiling caps the total size of the BuildKit cache, on top of
// whatever the age filter just took (build_cache_cap.go explains why an age filter
// alone is not a bound).
func enforceBuildCacheCeiling(ctx context.Context, p cleanupParams, r *pb.CleanupScopeResult) {
	// A dry run must not mutate, and the `--all` branch already took everything.
	if p.dryRun || p.minAgeHours <= 0 {
		return
	}
	capArgs := buildCacheCapArgs(ctx, p.dataDir)
	if len(capArgs) == 0 {
		return // this host's CLI takes no size flags, or the disk is unmeasurable
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
		return // already under the ceiling - the normal case, and a no-op
	}
	r.ReclaimedBytes += freed
	for _, id := range prunedCacheRecordIDs(res.Stdout) {
		addItem(r, id)
		r.ItemsRemoved++
	}
}

// ---------------------------------------------------------------------------
// Scope: dangling images - `docker image prune` (NEVER -a)
// ---------------------------------------------------------------------------

// cleanDanglingImages removes untagged layers. Safe because a container - running or
// STOPPED - still pins its image, so docker will not prune an image any app could come
// back to. It never passes `-a`/`--all`.
func cleanDanglingImages(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES}

	var estimate int64
	// The RAW dangling count before the prune (unfiltered, not just our candidates):
	// one half of the post-prune diff that makes items_removed an observation instead
	// of a prediction. -1 = the pre-list failed, no diff possible.
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
		// Nothing was actually freed - see the build-cache scope for why the whole
		// line zeroes rather than reporting the un-removed candidates.
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
	// items_removed as an OBSERVATION: re-list the dangling set and count what
	// disappeared.
	if rawBefore >= 0 {
		if res, err := dockerQuery(ctx, cleanupQueryTimeout, "image", "ls", "--filter", "dangling=true", "--quiet"); err == nil && res.Code == 0 {
			removed := rawBefore - len(uniqueLines(res.Stdout))
			if removed < 0 {
				removed = 0 // a concurrent build minted new dangling layers mid-sweep
			}
			r.ItemsRemoved = int32(removed)
			if removed == 0 {
				r.Items = nil
			}
		}
	}
	return r
}

// ---------------------------------------------------------------------------
// Scope: orphaned buildkit caches - dangling volumes carrying the sentinel
// ---------------------------------------------------------------------------

// cleanOrphanBuildkitCache removes the anonymous volumes the railpack builder leaks:
// moby/buildkit declares VOLUME /var/lib/buildkit, so every buildkitd the build path
// starts gets an anonymous volume, and (before the `docker rm -f -v` fix in
// build_methods.go) it was orphaned when the container was removed.
func cleanOrphanBuildkitCache(ctx context.Context, p cleanupParams, idx *containerIndex) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE}

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
			// docker called it dangling but a container still lists it. Trust the
			// index, not the filter.
			continue
		}
		vres, err := dockerQuery(ctx, cleanupQueryTimeout,
			"volume", "inspect", "--format", "{{.Mountpoint}}|{{.CreatedAt}}", name)
		if err != nil || vres.Code != 0 {
			continue // gone mid-sweep, or a driver that cannot tell us: not a candidate
		}
		mountpoint, created, _ := strings.Cut(strings.TrimSpace(vres.Stdout), "|")
		if mountpoint == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(mountpoint, buildkitSentinel)); err != nil {
			continue // THE proof. No sentinel, no removal - whatever else it looks like.
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

// ---------------------------------------------------------------------------
// Scope: unused app images - an explicit `docker rmi` per image, never a prune
// ---------------------------------------------------------------------------

// cleanUnusedAppImages removes old `deplo/<slug>:<deployment>` images. N is the app's
// own keep_per_slug entry when the control plane sent one - that is the app's rollback
// depth - and keep_images_per_app otherwise.
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

	// Rank within the group's WHOLE image set, in-use ones included: "keep the newest N of
	// this app" has to mean the newest N that exist, or a redeploy that leaves the
	// previous image running would let us delete every older generation at once.
	byGroup := map[string][]imageInfo{}
	for _, im := range images {
		if im.slug == "" {
			// No deplo.slug: we cannot say which app it belongs to, so we cannot apply
			// keep-N to it. Leave it alone.
			continue
		}
		// The REPOSITORY is part of the group, because the labels are not proof of
		// whose image this is: a tenant can pull one carrying `deplo.managed=true
		// deplo.slug=<victim>` and a Created of its choosing, and ranked with the
		// victim's it pushes their rollback images past keep-N. Deplo names every
		// image it builds (`deplo/<slug>`, or the stack's own project prefix), so a
		// foreign one lands in a group of its own and reaches nobody else's.
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
				return ti.After(tj) // newest first
			}
			return group[i].id < group[j].id // stable, deterministic tiebreak
		})

		// How deep this app keeps its history. A compose stack's services each keep the APP's
		// number, which is what the scalar did before there was a per-app one.
		keep := p.keepImagesFor(group[0].slug)

		for rank, im := range group {
			if rank < keep {
				continue // (d) among the newest kept for this app
			}
			if idx.images[im.id] {
				continue // (a) a container, perhaps a stopped one, still needs it
			}
			if !olderThan(im.created, p.appImageCutoff) {
				continue // (c) inside the deploy grace - possibly racing its own start
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
			// docker prints no total for `rmi`, so this is the image's own reported
			// size - layers shared with a kept image inflate it, exactly as
			// `docker system df` inflates them. A real number, not an exact one.
			r.ReclaimedBytes += im.size
			addItem(r, im.id)
			r.ItemsRemoved++
		}
	}

	r.Error = failures.summary()
	return r
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// imageInfo is the five things the allow-list needs about an image.
type imageInfo struct {
	id      string // FULL sha256 - the form the container index is keyed by
	slug    string // deplo.slug label, "" when absent
	service string // deplo.service label (compose-built images), "" when absent
	repo    string // repository of its first tag/digest, "" when it has neither
	created string
	size    int64 // bytes
}

// inspectImages reads those five fields for a batch of ids in ONE docker call. `image
// ls` cannot give us any of them properly: it prints the SHORT id, no labels, and a
// human-rounded size.
func inspectImages(ctx context.Context, ids []string) ([]imageInfo, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := append([]string{"image", "inspect", "--format",
		`{{.Id}}|{{index .Config.Labels "deplo.slug"}}|{{index .Config.Labels "deplo.service"}}|{{.Created}}|{{.Size}}|` +
			`{{if .RepoTags}}{{index .RepoTags 0}}{{else if .RepoDigests}}{{index .RepoDigests 0}}{{end}}`}, ids...)
	res, err := dockerQuery(ctx, cleanupQueryTimeout, args...)
	if err != nil {
		return nil, err
	}

	// text/template prints `<no value>` for an absent label.
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
		out = append(out, imageInfo{
			id:      strings.TrimSpace(parts[0]),
			slug:    label(parts[1]),
			service: label(parts[2]),
			repo:    repo,
			created: strings.TrimSpace(parts[3]),
			size:    size,
		})
	}
	if res.Code != 0 && len(out) == 0 {
		return nil, errors.New(dockerErr("image inspect", res))
	}
	return out, nil
}

// repoOf strips the tag or digest off an image reference, so every generation of one
// app shares a repository. A registry host may carry a port, so only a colon AFTER
// the last slash is a tag.
func repoOf(ref string) string {
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		ref = ref[:i]
	}
	return strings.TrimSpace(ref)
}

// scopeFailures collects the per-object failures of a scope that removes objects
// one at a time. A failed `rmi`/`volume rm` is skipped and reported, never fatal.
type scopeFailures struct {
	msgs []string
	n    int
}

func (f *scopeFailures) add(object, msg string) {
	f.n++
	if len(f.msgs) < 3 { // enough to diagnose; the count carries the rest
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

// failedScope zeroes a scope's result and records why. Called when the removal
// itself failed: nothing was reclaimed, so nothing may be reported as reclaimed -
// the enumerated candidates must not be passed off as removals.
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

// olderThan reports whether a docker timestamp is strictly before the cutoff. A
// timestamp we cannot parse NEVER qualifies while a filter is set: better to leave an
// object behind than to delete one whose age we do not know.
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

// dockerTimeLayouts are the timestamp shapes docker emits across the commands this
// file reads: RFC3339(Nano) from `image inspect` / `volume inspect`, and the Go
// default time rendering from `system df -v`'s JSON.
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

// parseHumanSize turns docker's rendered sizes ("8.19kB", "3.89GB", "1.5GiB", "0B")
// back into bytes. `system df` prints only these, so a size we cannot parse counts
// as 0 - an under-report, never an over-report.
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
	// Round, don't truncate: 8.19kB is 8190 bytes, and float multiplication lands on
	// 8189.999…, which int64() would silently shave a byte off.
	return int64(math.Round(n * mult))
}

// Docker renders decimal units (kB = 1000) but accepts binary ones in places, so
// both are understood.
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

// parsePrunedTotal reads the total docker itself reports after a prune ("Total
// reclaimed space: 449.4MB" from `image prune`, "Total: 449.4MB" from the buildx
// `builder prune`).
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

// pickReclaimed is parsePrunedTotal with the pre-flight estimate as the fallback
// for output shapes with no recognisable total.
func pickReclaimed(out string, estimate int64) int64 {
	if n, ok := parsePrunedTotal(out); ok {
		return n
	}
	return estimate
}

// cacheRecordID is what a BuildKit cache-record id (25-char base36) or a legacy
// builder cache id (hex) looks like as the first token of a prune output line.
// Headers ("ID  RECLAIMABLE …"), totals and warnings never match.
var cacheRecordID = regexp.MustCompile(`^[a-z0-9]{12,}$`)

// prunedCacheRecordIDs recovers the record ids a `builder prune` printed - classic
// docker prints one bare id per line, buildx one table row per record, so a sweep
// whose own enumeration failed (or found nothing) can still report what was actually
// pruned.
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

// dirSize sums the disk a directory tree actually occupies, the way `du` does -
// ALLOCATED BLOCKS, not apparent size, so a sparse buildkit store reports what
// removing it really gives back.
func dirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable subtree: skip it, don't fail the measurement
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512 // the unit `stat` reports blocks in, always
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

// uniqueLines is splitLines without duplicates - `docker image ls -q` repeats an id
// once per tag it carries.
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

// sortedKeys keeps the sweep deterministic: Go map iteration is randomised, and a
// cleanup that removes a different set on every run is impossible to reason about.
func sortedKeys(m map[string][]imageInfo) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// dockerErr renders a non-zero docker exit for a CleanupScopeResult.error the
// operator will actually read.
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

// cleanLeftoverAppFiles removes `<stack-dir>/files/<slug>` directories that belong to
// no stack any more - the config files an App leaves behind when it is deleted, and the
// only thing this file removes that no rebuild can recreate.
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
			return r // no files root: nothing has ever been written here
		}
		r.Error = err.Error()
		return r
	}
	// Deterministic order, like every other scope: ReadDir already sorts by name,
	// so the only thing left is to skip what is not ours to judge.
	for _, e := range entries {
		if !e.IsDir() {
			continue // a stray file at the root is not a stack's directory
		}
		slug := e.Name()
		if p.liveSlugs[slug] {
			continue
		}
		// Refuse to act on a name the agent would refuse anywhere else: a directory
		// that cannot be a slug was not written by a deploy, so it is not ours.
		if validateSlug(slug) != nil {
			continue
		}
		dir := filepath.Join(root, slug)
		info, err := e.Info()
		if err != nil || info.ModTime().After(p.filesCutoff) {
			continue // unknown age fails closed, same rule as the image scopes
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

// cleanLeftoverNetworks removes the tenant networks of Environments and previews that
// are gone. It reclaims no bytes - it reclaims ADDRESS SPACE, which is the scarce
// thing: Docker's default pool tops out at ~31 networks per host.
func cleanLeftoverNetworks(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_NETWORKS}
	if len(p.liveNetworks) == 0 {
		return skippedScope(r.Scope, errors.New(
			"the control plane sent no list of live networks, and an empty list is not a reason to remove every app's network"))
	}
	res, err := dockercli.Run(ctx, 20*time.Second, "network", "ls", "--format", "{{.Name}}")
	if err != nil {
		r.Error = err.Error()
		return r
	}
	if res.Code != 0 {
		r.Error = dockerErr("network ls", res)
		return r
	}
	names := strings.Split(res.Stdout, "\n")
	sort.Strings(names)
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		// Only ever ours, and only ever a TENANT one: the platform's own networks are
		// not in this namespace and could never be candidates.
		if !dockercli.IsTenantNetwork(name) || p.liveNetworks[name] {
			continue
		}
		attached, created, ok := networkState(ctx, name)
		if !ok || attached > 0 {
			continue // unknown state fails closed; an attached network is in use
		}
		// A network created moments ago belongs to a deploy the control plane's list
		// may predate. Same grace, and the same reason, as the files scope. BEFORE the
		// dry-run branch, or the confirm dialog would promise removals the real sweep
		// then skips.
		if created.After(p.filesCutoff) {
			continue
		}
		if p.dryRun {
			r.ItemsRemoved++
			if len(r.Items) < cleanupMaxItems {
				r.Items = append(r.Items, name)
			}
			continue
		}
		// Traefik is on every tenant network because a deploy put it there, and it
		// never leaves. Left counted, no tenant network is EVER a candidate and this
		// scope reclaims nothing; left attached, `docker network rm` refuses outright.
		// So it is taken off first, and only once nothing else is on the network.
		_, _ = dockercli.Run(ctx, 20*time.Second,
			"network", "disconnect", "-f", name, traefikContainer)
		rm, err := dockercli.Run(ctx, 20*time.Second, "network", "rm", name)
		if err != nil || rm.Code != 0 {
			if r.Error == "" {
				r.Error = fmt.Sprintf("remove %s: %s", name, dockerErr("network rm", rm))
			}
			continue
		}
		r.ItemsRemoved++
		if len(r.Items) < cleanupMaxItems {
			r.Items = append(r.Items, name)
		}
	}
	return r
}

// networkState reports how many containers are attached to a network and when it was
// created. ok=false when docker could not answer - which makes the caller skip it.
// attachedExcludingProxy counts the containers on a network that are not Traefik.
// Counting Traefik is what made every tenant network look permanently in use.
func attachedExcludingProxy(names string) int {
	n := 0
	for _, c := range strings.Fields(names) {
		if c != traefikContainer {
			n++
		}
	}
	return n
}

// networkState reports how many containers OTHER THAN THE PROXY are attached, and
// when the network was created. Traefik is excluded on purpose: a deploy attaches it
// to every tenant network and nothing detaches it, so counting it would make an
// emptied network look busy forever.
func networkState(ctx context.Context, name string) (attached int, created time.Time, ok bool) {
	// `{{.Created}}` renders Go's DEFAULT time layout ("2026-08-30 18:50:12 +0200
	// CEST"), which is not RFC3339 - parsing it as RFC3339 failed for every network,
	// so every one of them fell to the fail-closed branch and this scope reclaimed
	// nothing at all. `json` marshals the same value as RFC3339Nano.
	res, err := dockercli.Run(ctx, 10*time.Second,
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
