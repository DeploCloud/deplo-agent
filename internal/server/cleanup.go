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

// cleanup.go implements DockerCleanup — reclaiming Docker disk on the host. It is
// the most dangerous surface the agent has, so it is built as a strict ALLOW-LIST:
// nothing is removed unless the agent can PROVE nothing references it.
//
// THE PROOF IS NEVER A LABEL. A real app container can carry no `deplo.*` label at
// all (`deplo-myapp-web-1`, a compose-stack app, carries only the compose labels),
// so "keep what is labelled ours" would delete other people's objects and "delete
// what is labelled ours" would miss ours. The proof is a container-reference
// REVERSE INDEX over `docker ps -aq` — RUNNING AND EXITED — plus, for buildkit
// volumes, an on-disk sentinel file. If the index cannot be built, the scopes that
// rest on it are SKIPPED, never guessed at.
//
// And that is why `docker system prune`, `container prune`, `volume prune` and
// `network prune` appear nowhere in this file and must never be added. On a Deplo
// host a STOPPED app is a LIVE app: StopStack is `docker compose stop` and
// StartStack is `docker compose start`, so the container, its volumes and its
// networks all have to survive a stop. `container prune` makes every stopped app
// permanently unstartable; `volume prune` deletes dangling anonymous volumes that
// hold live database files; `network prune` deletes the networks of stopped stacks
// that `compose start` will not recreate. Each of those verbs turns a disk-reclaim
// button into a data-loss button. cleanup_test.go fences the argv this file can
// emit against exactly those four.

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

	// CleanupScopeResult.items is a UI affordance, not a ledger — items_removed is
	// the authoritative count. Bound the list so a host with thousands of dangling
	// layers cannot blow up the response.
	cleanupMaxItems = 200

	// The file moby/buildkit's daemon holds open in its state dir (/var/lib/buildkit,
	// which the image declares as a VOLUME). Its presence at a volume's mountpoint is
	// the ONLY thing that proves a dangling volume is an orphaned buildkit store and
	// not, say, a database's data volume — which is exactly what several dangling
	// volumes on a real host turn out to be.
	buildkitSentinel = "buildkitd.lock"

	// How fresh an app image must be to be untouchable by UNUSED_APP_IMAGES,
	// REGARDLESS of the policy's min_age_hours. App-image retention is count-based
	// (keep_images_per_app), NOT age-based: gating it on min_age let a host that
	// redeploys many times a day fill its disk with superseded-but-tagged images
	// none of which ever aged into eligibility (min_age defaults to a day and can be
	// set to a year). The grace window only shields a build racing its own deploy —
	// an image whose container has not started yet — and one hour is far beyond any
	// build→run gap while being far below any real redeploy cadence worth keeping.
	appImageDeployGrace = time.Hour
)

// removeObject is the ONE host-mutating docker call in this file: every prune and
// every `rm` goes through it, and nothing else in here can delete anything. It is
// a package-level var so cleanup_test.go can swap it, assert the EXACT argv the
// handler would run, and delete nothing — and so the regression fence can prove no
// argv this file emits is ever a container/volume/network/system prune. Same seam
// discipline as selfupdate.go's `downloadFile` / `reexec`.
//
// The per-call budget rides in on ctx (each caller wraps it with the scope's
// timeout); cleanupTimeout is the ceiling dockercli enforces if one ever forgot.
var removeObject = func(ctx context.Context, args ...string) (dockercli.Result, error) {
	return dockercli.Run(ctx, cleanupTimeout, args...)
}

// dockerQuery is the READ-ONLY half: enumeration only, it never mutates the host.
// A seam as well, so the tests can drive the handler against a synthetic host —
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
	// cutoff is the newest a CACHE-type object (build cache, dangling image, orphan
	// buildkit volume) may be to qualify. ZERO means "no age filter".
	cutoff time.Time
	// appImageCutoff is the newest an APP image may be to qualify — always
	// appImageDeployGrace ago, never the policy cutoff. See the constant's comment.
	appImageCutoff time.Time
}

// DockerCleanup reclaims Docker disk on this host within the allow-listed scopes
// (see the RPC contract in proto/agent.proto and the file comment above).
//
// Per-scope failures are non-fatal — they land in CleanupScopeResult.error and the
// sweep carries on, because a host with a broken image store should still get its
// build cache back. A gRPC error is reserved for the two things that are not a
// result at all: Docker being unreachable (the sweep cannot start — UNAVAILABLE,
// the same split labelcheck.go draws) and a scope this agent does not define (a
// contract violation — INVALID_ARGUMENT; a control plane must never be told "done"
// about a scope we silently ignored).
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
	}
	if params.minAgeHours < 0 {
		params.minAgeHours = 0
	}
	if params.keepImagesPerApp < 1 {
		// Always keep the current tag, even when no container references it: a
		// stopped app must stay redeployable without a rebuild from source.
		params.keepImagesPerApp = 1
	}
	if params.minAgeHours > 0 {
		params.cutoff = time.Now().Add(-time.Duration(params.minAgeHours) * time.Hour)
	}
	params.appImageCutoff = time.Now().Add(-appImageDeployGrace)

	// The reverse index costs one inspect over every container on the host, and two
	// scopes need it — so build it at most once, and only if a scope actually asks.
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
// The container-reference reverse index — the ownership test everything rests on
// ---------------------------------------------------------------------------

// containerIndex is every image and every volume referenced by ANY container on
// this host, running OR exited. Membership here means "in use"; the absence of a
// label means nothing at all.
type containerIndex struct {
	images  map[string]bool // full sha256 image ids (`docker inspect` .Image)
	volumes map[string]bool // volume names, from each container's volume mounts
}

// buildContainerIndex reads the whole host in two calls. `docker ps -aq` is what
// makes it safe: -a includes EXITED containers, and a stopped Deplo app is a live
// app whose image and volumes must survive.
//
// A PARTIAL index is a DANGEROUS index — a row we failed to read makes a live
// object look orphaned — so any failure is an error, and the caller skips the
// scopes that depend on it rather than deleting on incomplete evidence.
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
// Scope: build cache — `docker builder prune`
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

// cleanBuildCache reclaims the daemon's own BuildKit cache. This is the one scope
// that touches no Deplo object whatsoever — a cache record is pure derived data,
// and the worst a wrong answer here can do is make the next build slower.
//
// It enumerates first because dry_run has to report what a prune WOULD take and
// docker offers no --dry-run of its own — but on a REAL run the enumeration is
// only the preview, never the gate: the prune always runs and docker's own
// `until=` filter decides. Gating the prune on our own candidate count is exactly
// what let a loaded host keep its cache forever (its `system df -v` timed out, or
// our timestamp parse disagreed with docker's), reported as a clean success.
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
			// Age off last use where docker knows it, creation otherwise — the same
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
		// still owed. Items stay empty — docker's total below is the honest number.
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
		// filter disagreed, or another sweep beat us to them). Zero the whole line —
		// count and list included — or the history reports removals that never were.
		r.ReclaimedBytes = 0
		r.ItemsRemoved = 0
		r.Items = nil
		return r
	}
	// Docker prints the total it actually freed; that beats our estimate. The
	// estimate is the fallback for an output shape we cannot parse — never a made-up
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
	return r
}

// ---------------------------------------------------------------------------
// Scope: dangling images — `docker image prune` (NEVER -a)
// ---------------------------------------------------------------------------

// cleanDanglingImages removes untagged layers. Safe because a container — running
// or STOPPED — still pins its image, so docker will not prune an image any app
// could come back to.
//
// It never passes `-a`/`--all`. `image prune -a` removes every image no container
// currently references, which on a Deplo host includes an app image the user has
// never started and every base image kept for an offline rebuild — and nothing in
// Deplo pushes to a registry, so a wrongly-removed app image is recoverable only by
// a full rebuild from source. Removing app images is a separate, opt-in, allow-listed
// scope (UNUSED_APP_IMAGES) that decides image by image, never `-a`.
//
// Like the build-cache scope, the enumeration is the dry run's answer and the wet
// run's preview — never the gate. The prune always runs; docker's filter decides.
func cleanDanglingImages(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := &pb.CleanupScopeResult{Scope: pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES}

	var estimate int64
	// The RAW dangling count before the prune (unfiltered — not just our candidates):
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
		// Nothing was actually freed — see the build-cache scope for why the whole
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
	// disappeared. Docker's filter — not our timestamp parse — decided what went, so
	// the diff is the honest count (an image whose age we couldn't read still gets
	// counted once docker removes it). Best effort: if either list failed, the
	// enumeration's candidate count stands.
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
// Scope: orphaned buildkit caches — dangling volumes carrying the sentinel
// ---------------------------------------------------------------------------

// cleanOrphanBuildkitCache removes the anonymous volumes the railpack builder
// leaks: moby/buildkit declares VOLUME /var/lib/buildkit, so every buildkitd the
// build path starts gets an anonymous volume, and (before the `docker rm -f -v` fix
// in build_methods.go) it was orphaned when the container was removed. On a busy
// host these are the single biggest reclaim — gigabytes each.
//
// TWO independent proofs are required before a volume is touched, because a
// dangling volume on a Deplo host may well hold a live database's data files:
//
//  1. no container — running or EXITED — references it (the reverse index, checked
//     ourselves rather than trusted from docker's `dangling=true` filter), and
//  2. `<mountpoint>/buildkitd.lock` EXISTS.
//
// The sentinel is the load-bearing half. It is what a buildkit state dir has and a
// database volume does not, and it is checked on the host's filesystem, which is
// the agent's job and no one else's (ADR-0006). Name and label are irrelevant.
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
			continue // THE proof. No sentinel, no removal — whatever else it looks like.
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
// Scope: unused app images — an explicit `docker rmi` per image, never a prune
// ---------------------------------------------------------------------------

// cleanUnusedAppImages removes old `deplo/<slug>:<deployment>` images. This is the
// only scope that can destroy something a rebuild is the sole recovery for (Deplo
// pushes to no registry), so every image must clear FOUR independent tests:
//
//	a. no container — running or EXITED — references it (the reverse index);
//	b. it carries deplo.managed=true (the `--filter label=` below);
//	c. it is older than the fixed appImageDeployGrace — NOT min_age_hours. App
//	   retention is count-based; the policy age floor is a cache knob. Gating on
//	   min_age let a host that redeploys many times a day pile up superseded
//	   1-2GB images that never aged into eligibility and saturate its disk while
//	   every sweep reported success/0.
//	d. it is not among the newest keep_images_per_app images of its group — the
//	   deplo.slug, subdivided by the deplo.service image label when present
//	   (a compose stack builds one image per service under the same slug;
//	   ranking them together would keep one service's image and eat the rest's).
//
// (b) is the one label test in this file, and it is not an ownership test: it only
// NARROWS what we will even consider, so a mislabelled object is left alone rather
// than deleted. The keep/delete decision is (a), the index.
//
// Removal is one explicit `docker rmi <id>` per image — never `image prune -a`,
// which would let docker decide. A single failure is recorded and skipped; it never
// aborts the run. No `-f`: forcing would untag an image under some other repo name
// we never reasoned about.
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

	// Rank within the group's WHOLE image set, in-use ones included: "keep the newest
	// N of this app" has to mean the newest N that exist, or a redeploy that leaves
	// the previous image running would let us delete every older generation at once.
	// The group is the slug, split by the deplo.service image label when present —
	// each built service of a compose stack keeps its own newest N.
	byGroup := map[string][]imageInfo{}
	for _, im := range images {
		if im.slug == "" {
			// No deplo.slug: we cannot say which app it belongs to, so we cannot apply
			// keep-N to it. Leave it alone.
			continue
		}
		key := im.slug + "\x00" + im.service
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

		for rank, im := range group {
			if rank < p.keepImagesPerApp {
				continue // (d) among the newest kept for this app
			}
			if idx.images[im.id] {
				continue // (a) a container — perhaps a stopped one — still needs it
			}
			if !olderThan(im.created, p.appImageCutoff) {
				continue // (c) inside the deploy grace — possibly racing its own start
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
			// size — layers shared with a kept image inflate it, exactly as
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
	id      string // FULL sha256 — the form the container index is keyed by
	slug    string // deplo.slug label, "" when absent
	service string // deplo.service label (compose-built images), "" when absent
	created string
	size    int64 // bytes
}

// inspectImages reads those five fields for a batch of ids in ONE docker call.
// `image ls` cannot give us any of them properly: it prints the SHORT id, no
// labels, and a human-rounded size.
//
// Unlike the container index, a PARTIAL read here is SAFE: an image we failed to
// read simply is not a candidate, so the failure mode is "delete less". We
// therefore parse whatever docker printed and only fail when it printed nothing —
// which keeps an image removed by a concurrent deploy from failing the whole scope.
func inspectImages(ctx context.Context, ids []string) ([]imageInfo, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := append([]string{"image", "inspect", "--format",
		`{{.Id}}|{{index .Config.Labels "deplo.slug"}}|{{index .Config.Labels "deplo.service"}}|{{.Created}}|{{.Size}}`}, ids...)
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
		if len(parts) != 5 {
			continue
		}
		size, err := strconv.ParseInt(strings.TrimSpace(parts[4]), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, imageInfo{
			id:      strings.TrimSpace(parts[0]),
			slug:    label(parts[1]),
			service: label(parts[2]),
			created: strings.TrimSpace(parts[3]),
			size:    size,
		})
	}
	if res.Code != 0 && len(out) == 0 {
		return nil, errors.New(dockerErr("image inspect", res))
	}
	return out, nil
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
// itself failed: nothing was reclaimed, so nothing may be reported as reclaimed —
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
// ZERO cutoff means "no age filter" — everything qualifies. A timestamp we cannot
// parse NEVER qualifies while a filter is set: better to leave an object behind
// than to delete one whose age we do not know.
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
// as 0 — an under-report, never an over-report.
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
// reclaimed space: 449.4MB" from `image prune`, "Total:  449.4MB" from the buildx
// `builder prune`). The second return is whether a total was recognised at all.
//
// A parsed total of ZERO is AUTHORITATIVE, not a parse failure: docker printing
// "Total reclaimed space: 0B" means the prune freed nothing, and reporting an
// estimate instead is how a sweep that reclaimed nothing was once recorded as
// having reclaimed a gigabyte (the history's phantom bytes). The digit check is
// what separates "docker said 0" from "docker said something we cannot read".
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

// prunedCacheRecordIDs recovers the record ids a `builder prune` printed —
// classic docker prints one bare id per line, buildx one table row per record —
// so a sweep whose own enumeration failed (or found nothing) can still report
// what was actually pruned. Best effort: unrecognisable output yields nothing;
// this is a report, never a gate.
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

// dirSize sums the disk a directory tree actually occupies, the way `du` does —
// ALLOCATED BLOCKS, not apparent size — so a sparse buildkit store reports what
// removing it really gives back. Unreadable entries are skipped: this number is a
// report, never a gate on whether we delete.
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

// uniqueLines is splitLines without duplicates — `docker image ls -q` repeats an id
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
