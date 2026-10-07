package server

import (
	"context"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

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
		func(name, mountpoint string) bool {
			return anonymousVolume.MatchString(name) ||
				(strings.HasPrefix(name, deploVolumePrefix) && isEmptyDir(mountpoint))
		})
}

const deploVolumePrefix = "deplo-"

// isEmptyDir is the proof that removing a named volume loses nothing.
func isEmptyDir(path string) bool {
	entries, err := os.ReadDir(path)
	return err == nil && len(entries) == 0
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
