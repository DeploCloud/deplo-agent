package server

import (
	"context"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The sentinel IS the proof.
func TestDockerCleanup_orphanBuildkit_onlyWithSentinel(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE},
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	got := h.argv()
	if len(got) != 1 || got[0] != "volume rm orphan-buildkit" {
		t.Fatalf("argv = %q, want [volume rm orphan-buildkit]", got)
	}
	for _, a := range got {
		if strings.Contains(a, "mongo-data") {
			t.Fatal("removed a dangling volume with no buildkitd.lock sentinel - that is user data")
		}
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE)
	if r.GetItemsRemoved() != 1 {
		t.Errorf("items_removed = %d, want 1", r.GetItemsRemoved())
	}
	if r.GetReclaimedBytes() <= 0 {
		t.Errorf("reclaimed_bytes = %d, want the measured size of the volume's mountpoint", r.GetReclaimedBytes())
	}
}

// A dangling volume that a container, even an EXITED one, still references is never removed, even with the sentinel present: the reverse index outranks docker's dangling filter.
func TestDockerCleanup_orphanBuildkit_skipsIndexedVolume(t *testing.T) {
	h := newFixture(t)
	h.inspectRows = append(h.inspectRows, "sha256:ccc|orphan-buildkit,")
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE},
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("argv = %q, want none - a container still references that volume", got)
	}
}

// Only a dangling ANONYMOUS volume is a candidate: a named one, however dangling, is somebody's data and stays.
func TestDockerCleanup_orphanVolumes_anonymousOnly(t *testing.T) {
	h := newFixture(t)
	anon := strings.Repeat("ab", 32)
	anonMount := filepath.Join(t.TempDir(), "_data")
	if err := os.MkdirAll(anonMount, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(anonMount, "PG_VERSION"), []byte("18"), 0o644); err != nil {
		t.Fatal(err)
	}
	held := strings.Repeat("cd", 32)
	h.danglingVolumes = append(h.danglingVolumes, anon, held)
	h.volumeMounts[anon] = anonMount
	h.volumeMounts[held] = anonMount
	h.inspectRows = append(h.inspectRows, "sha256:fff|"+held+",")
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES)
	if got := h.argv(); len(got) != 1 || got[0] != "volume rm "+anon {
		t.Fatalf("want exactly the anonymous dangling volume removed, got %v", got)
	}
	if r.GetItemsRemoved() != 1 || r.GetReclaimedBytes() == 0 {
		t.Fatalf("result: %+v", r)
	}
}

// A build directory a dead agent left behind is swept with the build cache once it is old enough to belong to nobody; a fresh one may be a build in flight.
// A NAMED volume goes only when it is provably empty, and only when it is Deplo's: the names are deterministic, so a prefix match on its own would reach another team's data, and an empty volume that belongs to somebody else's tool is still not ours to take.
func TestDockerCleanup_orphanVolumes_namedOnlyWhenEmptyAndOurs(t *testing.T) {
	h := newFixture(t)
	empty := filepath.Join(t.TempDir(), "_data")
	full := filepath.Join(t.TempDir(), "_data")
	foreign := filepath.Join(t.TempDir(), "_data")
	for _, d := range []string{empty, full, foreign} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(full, "PG_VERSION"), []byte("18"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.danglingVolumes = append(h.danglingVolumes,
		"deplo-shop_web-cache", "deplo-shop_db-data", "coder-ab12-home")
	h.volumeMounts["deplo-shop_web-cache"] = empty
	h.volumeMounts["deplo-shop_db-data"] = full
	h.volumeMounts["coder-ab12-home"] = foreign
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := h.argv(); len(got) != 1 || got[0] != "volume rm deplo-shop_web-cache" {
		t.Fatalf("want only the empty Deplo volume removed, got %v", got)
	}
	if r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES); r.GetItemsRemoved() != 1 {
		t.Fatalf("result: %+v", r)
	}
}
