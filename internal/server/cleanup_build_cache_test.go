package server

import (
	"context"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func cacheRec(size string, age time.Duration, now time.Time) buildCacheRecord {
	return buildCacheRecord{
		ID:         "rec",
		Size:       size,
		LastUsedAt: now.Add(-age).Format(time.RFC3339Nano),
	}
}

// The shape that defeated the old ceiling: a cache well over the limit whose RECLAIMABLE part is under it, which is all docker's own space flags ever look at - they free 0 bytes and the cache keeps growing.
func TestCeilingEvictsOldestUntilUnderTheLimit(t *testing.T) {
	now := time.Now()
	const gb = int64(1e9)
	records := []buildCacheRecord{
		cacheRec("10GB", 100*time.Hour, now),
		cacheRec("10GB", 50*time.Hour, now),
		cacheRec("10GB", 10*time.Hour, now),
		cacheRec("7.14GB", time.Hour, now),
	}

	hours, over := ceilingEvictionHours(records, 25*gb, now)
	if !over {
		t.Fatal("a 37.14 GB cache under a 25 GB ceiling has to evict")
	}
	if hours != 50 {
		t.Fatalf("until=%dh; want 50h - the two oldest records cover the 12.14 GB overflow", hours)
	}
}

func TestCeilingLeavesACacheUnderTheLimitAlone(t *testing.T) {
	now := time.Now()
	records := []buildCacheRecord{cacheRec("5GB", 400*time.Hour, now)}
	if hours, over := ceilingEvictionHours(records, int64(10e9), now); over {
		t.Fatalf("pruned (until=%dh) a cache already under the ceiling", hours)
	}
}

// Never 0h: that would evict the cache of a build running right now, and a converging sweep beats a destructive one.
func TestCeilingFloorsTheAgeAtOneHour(t *testing.T) {
	now := time.Now()
	records := []buildCacheRecord{cacheRec("20GB", 20*time.Minute, now)}
	hours, over := ceilingEvictionHours(records, int64(1e9), now)
	if !over || hours != buildCacheCeilingMinHours {
		t.Fatalf("until=%dh over=%v; want %dh", hours, over, buildCacheCeilingMinHours)
	}
}

// An undatable record still counts toward the total, so the ceiling fires; it just cannot steer the age.
func TestCeilingFiresOnRecordsWithNoUsableDate(t *testing.T) {
	now := time.Now()
	records := []buildCacheRecord{{ID: "x", Size: "30GB", CreatedAt: "not a date"}}
	hours, over := ceilingEvictionHours(records, int64(1e9), now)
	if !over || hours != buildCacheCeilingMinHours {
		t.Fatalf("until=%dh over=%v; want %dh", hours, over, buildCacheCeilingMinHours)
	}
}

func TestCeilingIgnoresAnEmptyCache(t *testing.T) {
	if _, over := ceilingEvictionHours(nil, int64(1e9), time.Now()); over {
		t.Fatal("an empty cache has nothing to evict")
	}
}

// The build-cache scope prunes the daemon's BuildKit cache and nothing else.
func TestDockerCleanup_buildCacheArgv(t *testing.T) {
	for _, tc := range []struct {
		name        string
		minAgeHours int32
		want        string
	}{
		{"with an age filter", 168, "builder prune --force --filter until=168h"},
		{"without one", 0, "builder prune --force --all"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFixture(t)
			h.install(t)
			resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
				Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE},
				MinAgeHours: tc.minAgeHours,
			})
			if err != nil {
				t.Fatalf("DockerCleanup: %v", err)
			}
			got := h.argv()
			if len(got) == 0 || got[0] != tc.want {
				t.Fatalf("argv = %q, want the first command to be %q", got, tc.want)
			}
			for _, cmd := range got[1:] {
				if !isCeilingPrune(strings.Fields(cmd)) {
					t.Errorf("argv = %q: only the ceiling may add a second prune", got)
				}
			}
			if tc.minAgeHours == 0 && len(got) > 1 {
				t.Errorf("argv = %q: `--all` already takes everything; no ceiling needed", got)
			}
			r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE)
			if r.GetItemsRemoved() != 1 || r.GetItems()[0] != "cache-idle" {
				t.Errorf("items = %v (removed %d), want only cache-idle", r.GetItems(), r.GetItemsRemoved())
			}
		})
	}
}

// A failed enumeration on a loaded host (`docker system df -v` signal-killed at its timeout) must not abort the wet sweep - the prune is still safe and still owed.
func TestDockerCleanup_buildCache_enumerationFailureStillPrunes(t *testing.T) {
	h := newFixture(t)
	h.dfFails = true
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE},
		MinAgeHours: 168,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) == 0 || got[0] != "builder prune --force --filter until=168h" {
		t.Fatalf("argv = %q, want the prune despite the failed enumeration", got)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE)
	if r.GetError() != "" {
		t.Errorf("error = %q; a pruned scope with a failed preview is a success", r.GetError())
	}
	if r.GetReclaimedBytes() != 1500000000 {
		t.Errorf("reclaimed_bytes = %d, want docker's own total", r.GetReclaimedBytes())
	}

	h2 := newFixture(t)
	h2.dfFails = true
	h2.install(t)
	resp2, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE},
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("DockerCleanup (dry): %v", err)
	}
	if got := h2.argv(); len(got) != 0 {
		t.Fatalf("dry run touched the host: %q", got)
	}
	if r2 := resultFor(t, resp2, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE); r2.GetError() == "" {
		t.Error("a dry run that could not enumerate must say so")
	}
}

// The size ceiling is what stops "builds are fast" from becoming "the disk filled up": the age filter drops nothing on a host whose apps all deploy daily, because no cache is ever idle long enough to qualify. 60GB clears any host's ceiling, which is a tenth of the disk capped at 50GB.
func TestDockerCleanup_buildCacheCeiling_prunesWhatTheAgeFilterCannot(t *testing.T) {
	h := newFixture(t)
	h.buildCacheJSON = `[{"ID":"cache-live","Size":"60GB","InUse":"true","CreatedAt":"","LastUsedAt":"` +
		time.Now().Add(-100*time.Hour).Format(time.RFC3339Nano) + `"}]`
	h.ceilingFrees = "ceil1record0000000000000\n\nTotal:\t2.5GB\n"
	h.install(t)
	orig := removeObject
	removeObject = func(ctx context.Context, args ...string) (dockercli.Result, error) {
		if args[0] == "builder" && args[1] == "prune" && !isCeilingPrune(args) {
			h.mu.Lock()
			h.removals = append(h.removals, append([]string(nil), args...))
			h.mu.Unlock()
			return okResult("Total:\t0B\n"), nil
		}
		return orig(ctx, args...)
	}

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}

	got := h.argv()
	if len(got) != 2 {
		t.Fatalf("argv = %q, want the age prune AND the ceiling prune", got)
	}
	if !isCeilingPrune(strings.Fields(got[1])) {
		t.Fatalf("second command %q is not the ceiling prune", got[1])
	}
	if !strings.Contains(got[1], "--filter until=100h") {
		t.Fatalf("ceiling prune %q must evict back to the oldest record that covers the overflow", got[1])
	}

	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE)
	if r.GetReclaimedBytes() != 2500000000 {
		t.Errorf("reclaimed = %d, want the ceiling's 2.5GB even though the age filter freed nothing",
			r.GetReclaimedBytes())
	}
	if r.GetItemsRemoved() != 1 || len(r.GetItems()) != 1 {
		t.Errorf("items = %v (removed %d), want the one record the ceiling evicted",
			r.GetItems(), r.GetItemsRemoved())
	}
}

// A ceiling that cannot run has to say so IN the run: for as long as the failure only reached the host log, a broken ceiling and a ceiling with nothing to do looked identical in the UI.
func TestDockerCleanup_buildCacheCeiling_failureReachesTheRun(t *testing.T) {
	h := newFixture(t)
	h.buildCacheJSON = `[{"ID":"cache-live","Size":"60GB","InUse":"true","CreatedAt":"","LastUsedAt":"` +
		time.Now().Add(-100*time.Hour).Format(time.RFC3339Nano) + `"}]`
	h.install(t)
	orig := removeObject
	removeObject = func(ctx context.Context, args ...string) (dockercli.Result, error) {
		if isCeilingPrune(args) {
			return dockercli.Result{Code: 1, Stderr: "no space left on device"}, nil
		}
		return orig(ctx, args...)
	}

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE)
	if !strings.Contains(r.GetError(), "no space left on device") {
		t.Fatalf("error = %q; want the ceiling's own failure", r.GetError())
	}
	if r.GetReclaimedBytes() == 0 {
		t.Error("the age prune's total must survive the ceiling's failure")
	}
}
func TestDockerCleanup_buildCache_sweepsStaleBuildDirs(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	tmp := t.TempDir()
	stale := filepath.Join(tmp, "deplo-git-web-123")
	dockercfg := filepath.Join(tmp, "deplo-dockercfg-dpl_abc123")
	live := filepath.Join(tmp, "deplo-build-web-456")
	other := filepath.Join(tmp, "somebody-else")
	for _, d := range []string{stale, dockercfg, live, other} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "f"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-3 * time.Hour)
	for _, d := range []string{stale, dockercfg, other} {
		if err := os.Chtimes(d, past, past); err != nil {
			t.Fatal(err)
		}
	}
	s := New(t.TempDir(), tmp, "/", "")
	resp, err := s.DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE)
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale build dir should be gone, stat err=%v", err)
	}
	if _, err := os.Stat(dockercfg); !os.IsNotExist(err) {
		t.Fatalf("stale registry-auth dir should be gone, stat err=%v", err)
	}
	for _, d := range []string{live, other} {
		if _, err := os.Stat(d); err != nil {
			t.Fatalf("%s should survive: %v", d, err)
		}
	}
	found := false
	for _, it := range r.GetItems() {
		if it == "deplo-git-web-123" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the swept directory is not in the result: %+v", r)
	}
}
