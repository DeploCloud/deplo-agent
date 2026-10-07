package server

import (
	"syscall"
)

const (
	buildCacheCapFraction = 10
	buildCacheCapMin      = 2 << 30
	buildCacheCapMax      = 50 << 30
)

// buildCacheCeiling is the most build cache this host may hold: a tenth of the
// filesystem, bounded. 0 when the filesystem cannot be measured.
func buildCacheCeiling(dataDir string) int64 {
	return buildCacheCeilingFor(filesystemBytes(dataDir))
}

func buildCacheCeilingFor(totalBytes int64) int64 {
	if totalBytes <= 0 {
		return 0
	}
	return clampInt64(totalBytes/buildCacheCapFraction, buildCacheCapMin, buildCacheCapMax)
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
