package server

import (
	"context"
	"strconv"
	"syscall"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const (
	buildCacheCapFraction = 10
	buildCacheCapMin      = 2 << 30
	buildCacheCapMax      = 50 << 30
	buildCacheFreeFloor   = 2 << 30
	buildCacheFreeCeiling = 20 << 30
)

func buildCacheCapArgs(ctx context.Context, dataDir string) []string {
	total := filesystemBytes(dataDir)
	if total <= 0 {
		return nil
	}
	maxUsed, minFree := buildCacheCap(total)
	switch dockercli.BuildCachePruneCap(ctx) {
	case dockercli.PruneCapModern:
		return []string{
			"--max-used-space", strconv.FormatInt(maxUsed, 10),
			"--min-free-space", strconv.FormatInt(minFree, 10),
		}
	case dockercli.PruneCapLegacy:
		return []string{"--keep-storage", strconv.FormatInt(maxUsed, 10)}
	default:
		return nil
	}
}

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
