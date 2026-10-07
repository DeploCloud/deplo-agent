package server

import (
	"context"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
)

// The prune scopes must PRUNE even when their own enumeration finds no candidate: the enumeration is the preview, docker's own `until=` filter is the decision.
func TestDockerCleanup_pruneScopes_runEvenWithZeroCandidates(t *testing.T) {
	h := newFixture(t)
	h.buildCacheJSON = `[]`
	h.danglingImages = nil
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{
			pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE,
			pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES,
		},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	got := h.argv()
	want := []string{"builder prune --force --filter until=24h", "image prune --force --filter until=24h"}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q - zero own-candidates must not skip the prune", got, w)
		}
	}
	for _, r := range resp.GetResults() {
		if r.GetReclaimedBytes() != 1500000000 {
			t.Errorf("scope %s reclaimed_bytes = %d, want docker's own total (1500000000)",
				r.GetScope(), r.GetReclaimedBytes())
		}
	}
	bc := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE)
	if bc.GetItemsRemoved() != 2 || len(bc.GetItems()) != 2 {
		t.Errorf("build cache items = %d (%v), want the 2 record ids from docker's output",
			bc.GetItemsRemoved(), bc.GetItems())
	}
	di := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES)
	if di.GetItemsRemoved() != 0 || len(di.GetItems()) != 0 {
		t.Errorf("dangling items = %d (%v), want 0 - the post-prune diff saw nothing disappear",
			di.GetItemsRemoved(), di.GetItems())
	}
}

// When docker's printed total is 0B, NOTHING was freed - the enumerated candidates were not removed, and reporting them (count, list, bytes) would be the phantom the history used to carry.
func TestDockerCleanup_pruneScopes_zeroTotalZeroesTheLine(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	removeObject = func(_ context.Context, args ...string) (dockercli.Result, error) {
		h.mu.Lock()
		h.removals = append(h.removals, append([]string(nil), args...))
		h.mu.Unlock()
		return okResult("Total reclaimed space: 0B\n"), nil
	}

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{
			pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE,
			pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES,
		},
		MinAgeHours: 168,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	got := h.argv()
	for _, w := range []string{
		"builder prune --force --filter until=168h",
		"image prune --force --filter until=168h",
	} {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q - both prunes must be attempted", got, w)
		}
	}
	for _, r := range resp.GetResults() {
		if r.GetReclaimedBytes() != 0 || r.GetItemsRemoved() != 0 || len(r.GetItems()) != 0 {
			t.Errorf("scope %s = %d bytes / %d items %v, want an all-zero line - docker freed nothing",
				r.GetScope(), r.GetReclaimedBytes(), r.GetItemsRemoved(), r.GetItems())
		}
	}
}

// dry_run enumerates and removes NOTHING, removeObject is never called, while still populating every result field, which is what the confirm dialog renders.
func TestDockerCleanup_dryRun_removesNothing(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           allScopes(),
		DryRun:           true,
		MinAgeHours:      168,
		KeepImagesPerApp: 1,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("dry run must not touch the host, but ran: %q", got)
	}
	if len(resp.GetResults()) != len(allScopes()) {
		t.Fatalf("results = %d, want one per scope", len(resp.GetResults()))
	}
	var total int64
	for _, r := range resp.GetResults() {
		if r.GetItemsRemoved() == 0 {
			t.Errorf("scope %s reported nothing; a dry run must report what it WOULD remove", r.GetScope())
		}
		if len(r.GetItems()) == 0 {
			t.Errorf("scope %s listed no items", r.GetScope())
		}
		total += r.GetReclaimedBytes()
	}
	if resp.GetReclaimedBytes() != total {
		t.Errorf("reclaimed_bytes = %d, want the sum of the scopes (%d)", resp.GetReclaimedBytes(), total)
	}
	if !resp.GetOk() {
		t.Error("ok = false; a dry run over a healthy host succeeds")
	}
}

// THE FENCE.
func TestDockerCleanup_neverEmitsAForbiddenPrune(t *testing.T) {
	forbidden := []string{"system prune", "container prune", "volume prune", "network prune"}

	for _, dryRun := range []bool{false, true} {
		for _, minAge := range []int32{0, 1, 168} {
			for _, keep := range []int32{0, 1, 5} {
				h := newFixture(t)
				h.install(t)
				if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
					Scopes:           everyScope(),
					DryRun:           dryRun,
					MinAgeHours:      minAge,
					KeepImagesPerApp: keep,
					LiveSlugs:        []string{"web"},
					LiveNetworks:     []string{"deplo-env-environ_x"},
				}); err != nil {
					t.Fatalf("DockerCleanup(dry=%v age=%d keep=%d): %v", dryRun, minAge, keep, err)
				}
				for _, argv := range h.argv() {
					for _, verb := range forbidden {
						if strings.Contains(argv, verb) {
							t.Fatalf("docker %q is FORBIDDEN, but the handler emitted: docker %s", verb, argv)
						}
					}
				}
			}
		}
	}
}

// Without the container-reference index the agent cannot prove what is unreferenced, so it SKIPS the scopes that rest on it rather than guessing, and the scopes that do not need it still run.
func TestDockerCleanup_skipsIndexScopesWhenIndexFails(t *testing.T) {
	h := newFixture(t)
	h.psFails = true
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: allScopes(),
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if !resp.GetOk() {
		t.Error("ok = false; a skipped scope is not a failed sweep")
	}
	for _, scope := range []pb.CleanupScope{
		pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE,
		pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES,
	} {
		r := resultFor(t, resp, scope)
		if !r.GetSkipped() {
			t.Errorf("scope %s ran without the index it depends on", scope)
		}
		if r.GetItemsRemoved() != 0 {
			t.Errorf("scope %s removed %d items with no index", scope, r.GetItemsRemoved())
		}
	}
	got := h.argv()
	want := []string{"builder prune --force --all", "image prune --force"}
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
	for _, a := range got {
		if strings.HasPrefix(a, "rmi ") || strings.HasPrefix(a, "volume rm ") {
			t.Fatalf("removed an object by id with no container-reference index: %q", a)
		}
	}
}

// Docker unreachable => the sweep cannot start at all.
func TestDockerCleanup_unavailableWhenDockerIsDown(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	dockerAvailable = func(context.Context) bool { return false }

	_, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{Scopes: allScopes()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", status.Code(err))
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("touched the host with docker down: %q", got)
	}
}

// A scope this agent does not define is a contract violation, not a result: the control plane must never be told "done" about something we silently ignored.
func TestDockerCleanup_rejectsUnknownScope(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	_, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNSPECIFIED},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("touched the host on an invalid request: %q", got)
	}
}

// An empty scope list is a no-op, not an error: the control plane owns the default set, and "nothing selected" must never become "everything".
func TestDockerCleanup_noScopesIsANoOp(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if !resp.GetOk() || len(resp.GetResults()) != 0 || resp.GetReclaimedBytes() != 0 {
		t.Fatalf("resp = %+v, want an empty ok response", resp)
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("touched the host with no scopes selected: %q", got)
	}
}

// A failed removal is skipped and reported, never fatal: the rest of the sweep runs.
func TestDockerCleanup_removalFailureIsNonFatal(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	removeObject = func(_ context.Context, args ...string) (dockercli.Result, error) {
		h.mu.Lock()
		h.removals = append(h.removals, append([]string(nil), args...))
		h.mu.Unlock()
		return dockercli.Result{Code: 1, Stderr: "image is being used by stopped container abc"}, nil
	}

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if !resp.GetOk() {
		t.Error("ok = false; a failed rmi is a per-scope error, not a failed sweep")
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES)
	if r.GetError() == "" {
		t.Error("the failed removals were not reported")
	}
	if len(h.argv()) != 2 {
		t.Errorf("argv = %q, want both removals attempted", h.argv())
	}
	if r.GetItemsRemoved() != 0 || r.GetReclaimedBytes() != 0 {
		t.Errorf("counted %d items / %d bytes that were never removed", r.GetItemsRemoved(), r.GetReclaimedBytes())
	}
}
