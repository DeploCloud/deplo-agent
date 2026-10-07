package server

import (
	"context"
	"encoding/json"
	"errors"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type buildCacheRecord struct {
	ID         string `json:"ID"`
	Size       string `json:"Size"`
	InUse      string `json:"InUse"`
	CreatedAt  string `json:"CreatedAt"`
	LastUsedAt string `json:"LastUsedAt"`
}

func readBuildCacheRecords(ctx context.Context) ([]buildCacheRecord, error) {
	res, err := dockerQuery(ctx, cleanupQueryTimeout, "system", "df", "-v", "--format", "{{json .BuildCache}}")
	if err != nil {
		return nil, err
	}
	if res.Code != 0 {
		return nil, errors.New(dockerErr("system df -v", res))
	}
	var records []buildCacheRecord
	if out := strings.TrimSpace(res.Stdout); out != "" && out != "null" {
		if err := json.Unmarshal([]byte(out), &records); err != nil {
			return nil, errors.New("read the build cache: " + err.Error())
		}
	}
	return records, nil
}
func cleanBuildCache(ctx context.Context, p cleanupParams) *pb.CleanupScopeResult {
	r := pruneBuildCache(ctx, p)
	sweepStaleBuildDirs(p, r)
	return r
}

const staleBuildDirAfter = 2 * time.Hour

// Never evict cache a build running right now may still be reaching for.
const buildCacheCeilingMinHours = 1

var staleBuildDirPrefixes = []string{"deplo-git-", "deplo-build-", "deplo-dockercfg-"}

func isStaleBuildDir(name string) bool {
	for _, prefix := range staleBuildDirPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
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
		if !e.IsDir() || !isStaleBuildDir(name) {
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
		records, err := readBuildCacheRecords(ctx)
		if err != nil {
			return err.Error()
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

// The daemon's own space flags are no-ops once the ceiling sits above what
// docker calls reclaimable, so the age filter is what actually evicts.
func enforceBuildCacheCeiling(ctx context.Context, p cleanupParams, r *pb.CleanupScopeResult) {
	if p.dryRun || p.minAgeHours <= 0 {
		return
	}
	ceiling := buildCacheCeiling(p.dataDir)
	if ceiling <= 0 {
		return
	}
	// A cache list we cannot read leaves the ceiling unknown, not breached:
	// pruneBuildCache already logged the same failure.
	records, err := readBuildCacheRecords(ctx)
	if err != nil {
		return
	}
	hours, over := ceilingEvictionHours(records, ceiling, time.Now())
	if !over {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, cleanupBuilderPruneTimeout)
	defer cancel()
	res, err := removeObject(cctx, "builder", "prune", "--force", "--all",
		"--filter", "until="+strconv.Itoa(hours)+"h")
	if err != nil {
		addScopeError(r, "build-cache ceiling: "+err.Error())
		return
	}
	if res.Code != 0 {
		addScopeError(r, "build-cache ceiling: "+dockerErr("builder prune", res))
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

// ceilingEvictionHours is the `until=` age that frees enough of the OLDEST
// cache to land under the ceiling. over is false when nothing has to go.
func ceilingEvictionHours(records []buildCacheRecord, ceiling int64, now time.Time) (hours int, over bool) {
	type aged struct {
		age  time.Duration
		size int64
	}
	var total int64
	dated := make([]aged, 0, len(records))
	for _, rec := range records {
		size := parseHumanSize(rec.Size)
		total += size
		at := rec.LastUsedAt
		if at == "" {
			at = rec.CreatedAt
		}
		t, ok := parseDockerTime(at)
		if !ok {
			continue
		}
		dated = append(dated, aged{now.Sub(t), size})
	}
	if total <= ceiling {
		return 0, false
	}
	sort.Slice(dated, func(i, j int) bool { return dated[i].age > dated[j].age })
	need := total - ceiling
	var freed int64
	for _, a := range dated {
		freed += a.size
		if freed >= need {
			return max(int(a.age.Hours()), buildCacheCeilingMinHours), true
		}
	}
	return buildCacheCeilingMinHours, true
}
