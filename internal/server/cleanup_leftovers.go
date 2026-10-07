package server

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

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
