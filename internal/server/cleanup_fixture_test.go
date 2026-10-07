package server

import (
	"context"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type hostFixture struct {
	containers      []string
	inspectRows     []string
	buildCacheJSON  string
	danglingImages  []string
	managedImages   []string
	imageRows       map[string]string
	danglingVolumes []string
	volumeMounts    map[string]string
	volumeCreated   string
	taggedImages    []string
	networks        []string
	networkStates   map[string]string

	psFails      bool
	dfFails      bool
	ceilingFrees string

	mu       sync.Mutex
	removals [][]string
}

func okResult(stdout string) dockercli.Result { return dockercli.Result{Stdout: stdout} }
func (h *hostFixture) query(args []string) (dockercli.Result, error) {
	key := strings.Join(args, " ")
	switch {
	case key == "ps -aq":
		if h.psFails {
			return dockercli.Result{Code: 1, Stderr: "Cannot connect to the Docker daemon"}, nil
		}
		return okResult(strings.Join(h.containers, "\n")), nil
	case args[0] == "inspect":
		return okResult(strings.Join(h.inspectRows, "\n")), nil
	case args[0] == "system" && args[1] == "df":
		if h.dfFails {
			return dockercli.Result{Code: -1, Stderr: "signal: killed"}, nil
		}
		return okResult(h.buildCacheJSON), nil
	case strings.HasPrefix(key, "image ls --filter dangling=true"):
		return okResult(strings.Join(h.danglingImages, "\n")), nil
	case strings.HasPrefix(key, "image ls --filter label=deplo.managed=true"):
		return okResult(strings.Join(h.managedImages, "\n")), nil
	case strings.HasPrefix(key, "image ls --filter dangling=false"):
		return okResult(strings.Join(h.taggedImages, "\n")), nil
	case strings.HasPrefix(key, "network ls"):
		return okResult(strings.Join(h.networks, "\n")), nil
	case args[0] == "network" && args[1] == "inspect":
		state, ok := h.networkStates[args[len(args)-1]]
		if !ok {
			return dockercli.Result{Code: 1, Stderr: "no such network"}, nil
		}
		return okResult(state), nil
	case args[0] == "image" && args[1] == "inspect":
		var rows []string
		for _, id := range args[4:] {
			if row, ok := h.imageRows[id]; ok {
				rows = append(rows, row)
			}
		}
		return okResult(strings.Join(rows, "\n")), nil
	case strings.HasPrefix(key, "volume ls"):
		return okResult(strings.Join(h.danglingVolumes, "\n")), nil
	case args[0] == "volume" && args[1] == "inspect":
		name := args[len(args)-1]
		mount, ok := h.volumeMounts[name]
		if !ok {
			return dockercli.Result{Code: 1, Stderr: "no such volume: " + name}, nil
		}
		return okResult(mount + "|" + h.volumeCreated), nil
	}
	return dockercli.Result{Code: 1, Stderr: "fixture: unexpected query: " + key}, nil
}
func (h *hostFixture) install(t *testing.T) {
	t.Helper()
	origQuery, origRemove, origAvail := dockerQuery, removeObject, dockerAvailable
	dockerQuery = func(_ context.Context, _ time.Duration, args ...string) (dockercli.Result, error) {
		return h.query(args)
	}
	removeObject = func(_ context.Context, args ...string) (dockercli.Result, error) {
		h.mu.Lock()
		h.removals = append(h.removals, append([]string(nil), args...))
		h.mu.Unlock()
		switch {
		case args[0] == "builder" && args[1] == "prune":
			if isCeilingPrune(args) {
				h.mu.Lock()
				out := h.ceilingFrees
				h.mu.Unlock()
				if out == "" {
					out = "Total:\t0B\n"
				}
				return okResult(out), nil
			}
			return okResult("pu0aq3k0be2nxyf87qw0jbh08\nvhcz1lchp7f0nrnu29jj0oy1n\n\nTotal:\t1.5GB\n"), nil
		case args[0] == "image" && args[1] == "prune":
			h.mu.Lock()
			h.danglingImages = nil
			h.mu.Unlock()
			return okResult("Deleted Images:\ndeleted: sha256:ddd\ndeleted: sha256:ddd-layer\n\nTotal reclaimed space: 1.5GB\n"), nil
		}
		return okResult("Total reclaimed space: 1.5GB\n"), nil
	}
	dockerAvailable = func(context.Context) bool { return true }
	t.Cleanup(func() { dockerQuery, removeObject, dockerAvailable = origQuery, origRemove, origAvail })
}
func isCeilingPrune(args []string) bool {
	all, filter := false, false
	for _, a := range args {
		switch a {
		case "--all":
			all = true
		case "--filter":
			filter = true
		}
	}
	return all && filter
}
func (h *hostFixture) argv() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.removals))
	for _, a := range h.removals {
		out = append(out, strings.Join(a, " "))
	}
	return out
}
func newFixture(t *testing.T) *hostFixture {
	t.Helper()
	now := time.Now()
	old := now.Add(-30 * 24 * time.Hour)
	oldRFC := old.Format(time.RFC3339Nano)
	olderRFC := old.Add(-24 * time.Hour).Format(time.RFC3339Nano)
	dfTime := old.UTC().Format("2006-01-02 15:04:05.000000000 -0700 MST")

	orphanMount := filepath.Join(t.TempDir(), "_data")
	if err := os.MkdirAll(orphanMount, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanMount, buildkitSentinel), []byte("lock"), 0o644); err != nil {
		t.Fatal(err)
	}
	dataMount := filepath.Join(t.TempDir(), "_data")
	if err := os.MkdirAll(dataMount, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataMount, "collection-0.wt"), []byte("user data"), 0o644); err != nil {
		t.Fatal(err)
	}

	return &hostFixture{
		containers: []string{"c1", "c2"},
		inspectRows: []string{
			"sha256:aaa|app-data,",
			"sha256:bbb|",
		},
		buildCacheJSON: `[
			{"ID":"cache-idle","Size":"3.6GB","InUse":"false","CreatedAt":"` + dfTime + `","LastUsedAt":""},
			{"ID":"cache-live","Size":"1.2GB","InUse":"true","CreatedAt":"` + dfTime + `","LastUsedAt":""}
		]`,
		danglingImages: []string{"ddd1111"},
		managedImages:  []string{"aaa1111", "ccc1111", "eee1111"},
		imageRows: map[string]string{
			"ddd1111": "sha256:ddd|<no value>|<no value>|" + oldRFC + "|500000000",
			"aaa1111": "sha256:aaa|web|<no value>|" + now.Format(time.RFC3339Nano) + "|1000000000",
			"ccc1111": "sha256:ccc|web|<no value>|" + oldRFC + "|900000000",
			"eee1111": "sha256:eee|web|<no value>|" + olderRFC + "|800000000",
		},
		danglingVolumes: []string{"orphan-buildkit", "mongo-data"},
		volumeMounts: map[string]string{
			"orphan-buildkit": orphanMount,
			"mongo-data":      dataMount,
		},
		volumeCreated: oldRFC,
	}
}
func allScopes() []pb.CleanupScope {
	return []pb.CleanupScope{
		pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE,
		pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES,
		pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE,
		pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES,
	}
}
func everyScope() []pb.CleanupScope {
	return append(allScopes(),
		pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES,
		pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_NETWORKS,
		pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES,
		pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES,
	)
}
func newService(t *testing.T) *Service {
	t.Helper()
	return New(t.TempDir(), t.TempDir(), "/", "")
}
func resultFor(t *testing.T, resp *pb.DockerCleanupResponse, scope pb.CleanupScope) *pb.CleanupScopeResult {
	t.Helper()
	for _, r := range resp.GetResults() {
		if r.GetScope() == scope {
			return r
		}
	}
	t.Fatalf("no result for scope %s", scope)
	return nil
}
func containsString(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
