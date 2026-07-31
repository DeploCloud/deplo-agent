package server

import (
	"context"
	"strconv"
	"syscall"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A ceiling on the BuildKit cache, so "builds are fast" can never become "the
// disk filled up overnight".
//
// The age filter the cleanup already applies ("drop what nothing has used in N
// hours") is good hygiene but it is not a bound: a host running twenty apps that
// each deploy daily keeps every one of their caches forever, because none of
// them is ever stale. Nothing else bounds it either — the agent's builds go to
// the daemon's shared BuildKit cache, whose own garbage collection is whatever
// default the host's dockerd happens to ship with, unconfigured and unverifiable
// from here. Depending on that is exactly the kind of assumption that ends in a
// full disk, and a full disk on this platform does not degrade gracefully: it
// fails every build on the host.
//
// So the sweep states its own ceiling, derived from the filesystem rather than
// asked of the operator (there is no new setting, and nothing to get wrong at
// first run):
//
//   - the cache may hold at most a TENTH of the filesystem, and
//   - pruning targets leaving a tenth of it free.
//
// Both are clamped: small enough on a big disk that the cache never becomes the
// biggest thing on it, large enough on a small VPS to still hold a Nixpacks nix
// layer (~400 MB) and a package-manager cache mount (~1 GB) for a couple of
// apps, which is where nearly all of the speed comes from.
//
// BuildKit evicts least-recently-used first, so what a cap gives up is the cache
// of whatever was deployed longest ago — the cheapest possible thing to lose.
const (
	buildCacheCapFraction = 10       // a tenth of the filesystem
	buildCacheCapMin      = 2 << 30  // 2 GiB — enough to stay useful
	buildCacheCapMax      = 50 << 30 // 50 GiB — never more, however big the disk
	buildCacheFreeFloor   = 2 << 30  // keep at least this much free
	buildCacheFreeCeiling = 20 << 30 // asking for more free space than this is pointless
)

// buildCacheCapArgs returns the `docker builder prune` flags that cap the cache
// on this host, or nil when the host's CLI takes no size flags (then the age
// filter alone decides, exactly as before). dataDir is the filesystem the agent
// already measures for disk metrics — the one the cache actually lands on.
func buildCacheCapArgs(ctx context.Context, dataDir string) []string {
	total := filesystemBytes(dataDir)
	if total <= 0 {
		return nil // cannot measure the disk ⇒ do not invent a ceiling
	}
	maxUsed, minFree := buildCacheCap(total)
	switch dockercli.BuildCachePruneCap(ctx) {
	case dockercli.PruneCapModern:
		return []string{
			"--max-used-space", strconv.FormatInt(maxUsed, 10),
			"--min-free-space", strconv.FormatInt(minFree, 10),
		}
	case dockercli.PruneCapLegacy:
		// The old flag only expresses the cache ceiling; there is no disk floor.
		return []string{"--keep-storage", strconv.FormatInt(maxUsed, 10)}
	default:
		return nil
	}
}

// buildCacheCap turns a filesystem size into (max cache bytes, min free bytes).
// Pure, so the policy is testable without a disk.
func buildCacheCap(totalBytes int64) (maxUsed, minFree int64) {
	tenth := totalBytes / buildCacheCapFraction
	return clampInt64(tenth, buildCacheCapMin, buildCacheCapMax),
		clampInt64(tenth, buildCacheFreeFloor, buildCacheFreeCeiling)
}

func clampInt64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// filesystemBytes returns the total size of the filesystem holding path, or 0
// when it cannot be measured.
func filesystemBytes(path string) int64 {
	if path == "" {
		path = "/"
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0
	}
	return int64(st.Blocks) * int64(st.Bsize)
}
