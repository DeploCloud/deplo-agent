package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// These tests drive DockerCleanup against a SYNTHETIC host: the read-only seam
// (dockerQuery) answers from a fixture and the mutating seam (removeObject) records
// the argv instead of running it. Nothing is ever deleted, no Docker daemon is
// needed, and the safety properties — which objects the handler would touch, and
// which verbs it can never emit — are asserted on the exact command line.

// ---------------------------------------------------------------------------
// The synthetic host
// ---------------------------------------------------------------------------

// hostFixture is the state of the fake host: what each enumeration call answers.
type hostFixture struct {
	containers      []string          // `docker ps -aq`
	inspectRows     []string          // `docker inspect` rows: "<imageID>|<vol>,<vol>,"
	buildCacheJSON  string            // `docker system df -v --format {{json .BuildCache}}`
	danglingImages  []string          // `docker image ls --filter dangling=true -q` (short ids)
	managedImages   []string          // `docker image ls --filter label=deplo.managed=true -q`
	imageRows       map[string]string // short id -> "<fullID>|<slug>|<service>|<created>|<sizeBytes>"
	danglingVolumes []string          // `docker volume ls --filter dangling=true -q`
	volumeMounts    map[string]string // volume name -> mountpoint on disk
	volumeCreated   string            // every volume's CreatedAt (RFC3339)

	// psFails forces `docker ps -aq` to fail, so the container-reference index
	// cannot be built.
	psFails bool
	// dfFails forces `docker system df -v` to fail — the loaded-host case where the
	// build-cache enumeration times out while the prune itself would still work.
	dfFails bool

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
	case args[0] == "image" && args[1] == "inspect":
		var rows []string
		for _, id := range args[4:] { // image inspect --format <fmt> <ids...>
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

// install swaps the three package-level seams for this fixture and restores them on
// cleanup. removeObject RECORDS and returns success — it never touches the host.
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
		return okResult("Total reclaimed space: 1.5GB\n"), nil
	}
	dockerAvailable = func(context.Context) bool { return true }
	t.Cleanup(func() { dockerQuery, removeObject, dockerAvailable = origQuery, origRemove, origAvail })
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

// newFixture builds a host with something to reclaim in every scope:
//
//   - a running container pinning image sha256:aaa… and volume `app-data`;
//   - an exited container pinning image sha256:bbb… — the case a naive `container
//     prune` / label test gets wrong;
//   - build cache: one idle record, one in use;
//   - two dangling volumes, only ONE of which carries the buildkitd.lock sentinel;
//   - three deplo.managed images of slug "web": the newest (in use), an older idle
//     one, and an oldest idle one.
func newFixture(t *testing.T) *hostFixture {
	t.Helper()
	now := time.Now()
	old := now.Add(-30 * 24 * time.Hour)
	oldRFC := old.Format(time.RFC3339Nano)
	olderRFC := old.Add(-24 * time.Hour).Format(time.RFC3339Nano)
	dfTime := old.UTC().Format("2006-01-02 15:04:05.000000000 -0700 MST")

	// A real buildkit orphan: a dangling volume whose mountpoint holds the sentinel.
	orphanMount := filepath.Join(t.TempDir(), "_data")
	if err := os.MkdirAll(orphanMount, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanMount, buildkitSentinel), []byte("lock"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A dangling volume with NO sentinel — on a real host this is where a stopped
	// MongoDB's WiredTiger files live. It must never be removed.
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
			"sha256:aaa|app-data,", // running app
			"sha256:bbb|",          // EXITED app — still pins its image
		},
		buildCacheJSON: `[
			{"ID":"cache-idle","Size":"3.6GB","InUse":"false","CreatedAt":"` + dfTime + `","LastUsedAt":""},
			{"ID":"cache-live","Size":"1.2GB","InUse":"true","CreatedAt":"` + dfTime + `","LastUsedAt":""}
		]`,
		danglingImages: []string{"ddd1111"},
		managedImages:  []string{"aaa1111", "ccc1111", "eee1111"},
		imageRows: map[string]string{
			"ddd1111": "sha256:ddd|<no value>|<no value>|" + oldRFC + "|500000000",
			"aaa1111": "sha256:aaa|web|<no value>|" + now.Format(time.RFC3339Nano) + "|1000000000", // newest, IN USE
			"ccc1111": "sha256:ccc|web|<no value>|" + oldRFC + "|900000000",                        // idle, older
			"eee1111": "sha256:eee|web|<no value>|" + olderRFC + "|800000000",                      // idle, oldest
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

// ---------------------------------------------------------------------------
// Per-scope argv
// ---------------------------------------------------------------------------

// The build-cache scope prunes the daemon's BuildKit cache and nothing else. With
// an age filter it passes docker's own `until=`; with none it may sweep `--all`,
// which is safe for derived cache data (and only there).
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
			if got := h.argv(); len(got) != 1 || got[0] != tc.want {
				t.Fatalf("argv = %q, want [%q]", got, tc.want)
			}
			// Only the idle record is a candidate; the in-use one is docker's to keep.
			r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE)
			if r.GetItemsRemoved() != 1 || r.GetItems()[0] != "cache-idle" {
				t.Errorf("items = %v (removed %d), want only cache-idle", r.GetItems(), r.GetItemsRemoved())
			}
		})
	}
}

// The dangling-images scope prunes untagged layers. It must NEVER pass -a/--all:
// that removes app images nothing is currently running, and Deplo pushes to no
// registry, so the only recovery would be a full rebuild.
func TestDockerCleanup_danglingImagesArgv_neverAll(t *testing.T) {
	for _, tc := range []struct {
		name        string
		minAgeHours int32
		want        string
	}{
		{"with an age filter", 168, "image prune --force --filter until=168h"},
		{"without one", 0, "image prune --force"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFixture(t)
			h.install(t)
			if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
				Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_DANGLING_IMAGES},
				MinAgeHours: tc.minAgeHours,
			}); err != nil {
				t.Fatalf("DockerCleanup: %v", err)
			}
			got := h.argv()
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("argv = %q, want [%q]", got, tc.want)
			}
			for _, a := range got {
				if strings.Contains(a, " -a") || strings.Contains(a, "--all") {
					t.Fatalf("image prune must never be -a/--all, got %q", a)
				}
			}
		})
	}
}

// The sentinel IS the proof. A dangling volume without `buildkitd.lock` at its
// mountpoint is never removed, however dangling docker thinks it is — on a real
// host that volume holds a stopped database's data files.
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
			t.Fatal("removed a dangling volume with no buildkitd.lock sentinel — that is user data")
		}
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE)
	if r.GetItemsRemoved() != 1 {
		t.Errorf("items_removed = %d, want 1", r.GetItemsRemoved())
	}
	// The sentinel volume's bytes are measured off the real directory, never invented.
	if r.GetReclaimedBytes() <= 0 {
		t.Errorf("reclaimed_bytes = %d, want the measured size of the volume's mountpoint", r.GetReclaimedBytes())
	}
}

// A dangling volume that a container — even an EXITED one — still references is
// never removed, even with the sentinel present: the reverse index outranks docker's
// dangling filter.
func TestDockerCleanup_orphanBuildkit_skipsIndexedVolume(t *testing.T) {
	h := newFixture(t)
	h.inspectRows = append(h.inspectRows, "sha256:ccc|orphan-buildkit,") // an exited container claims it
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE},
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("argv = %q, want none — a container still references that volume", got)
	}
}

// The unused-app-images allow-list: delete BY ID, one rmi each, keeping the newest
// keep_images_per_app of the slug and anything a container (running or exited) still
// references. The id on the wire is the FULL sha256 the index is keyed by, never the
// short id `image ls` printed.
func TestDockerCleanup_unusedAppImages_allowList(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
		MinAgeHours:      168,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	// slug "web" has three images: sha256:aaa (newest, in use, rank 0 => kept twice
	// over), sha256:ccc (rank 1) and sha256:eee (rank 2) — both idle and old.
	want := []string{"rmi sha256:ccc", "rmi sha256:eee"}
	got := h.argv()
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
	for _, a := range got {
		if strings.Contains(a, "sha256:aaa") {
			t.Fatal("removed the image a running container is using")
		}
		if strings.Contains(a, "-f") || strings.Contains(a, "--force") {
			t.Fatalf("rmi must not force: %q", a)
		}
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES)
	if r.GetItemsRemoved() != 2 {
		t.Errorf("items_removed = %d, want 2", r.GetItemsRemoved())
	}
	if r.GetReclaimedBytes() != 900000000+800000000 {
		t.Errorf("reclaimed_bytes = %d, want the two removed images' sizes", r.GetReclaimedBytes())
	}
}

// keep_images_per_app ranks within the slug's WHOLE image set, in-use images
// included — so keeping 2 keeps the running one plus the next newest.
func TestDockerCleanup_unusedAppImages_keepsN(t *testing.T) {
	h := newFixture(t)
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 2,
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 1 || got[0] != "rmi sha256:eee" {
		t.Fatalf("argv = %q, want [rmi sha256:eee]", got)
	}
}

// An image with no deplo.slug cannot be reasoned about (which app is it? which of
// its generations is current?), so the allow-list leaves it alone entirely.
func TestDockerCleanup_unusedAppImages_skipsUnslugged(t *testing.T) {
	h := newFixture(t)
	h.managedImages = []string{"ddd1111"} // labelled deplo.managed=true, but no slug
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("argv = %q, want none — an image with no deplo.slug is not a candidate", got)
	}
}

// THE REGRESSION that saturated real hosts: an app redeployed many times a day
// piles up superseded-but-tagged images, all younger than min_age_hours — and the
// old age gate meant none was EVER a candidate, so every sweep "succeeded" with 0
// bytes while the disk filled. App images are count-based now: min_age must not
// shield them, and only the fixed one-hour deploy grace does.
func TestDockerCleanup_unusedAppImages_minAgeDoesNotShield(t *testing.T) {
	h := newFixture(t)
	now := time.Now()
	// Today's churn: the running image, a 30-minute-old build (inside the deploy
	// grace) and a five-hour-old superseded one — nothing near 168h old.
	h.imageRows["aaa1111"] = "sha256:aaa|web|<no value>|" + now.Format(time.RFC3339Nano) + "|1000000000"
	h.imageRows["eee1111"] = "sha256:eee|web|<no value>|" + now.Add(-30*time.Minute).Format(time.RFC3339Nano) + "|800000000"
	h.imageRows["ccc1111"] = "sha256:ccc|web|<no value>|" + now.Add(-5*time.Hour).Format(time.RFC3339Nano) + "|900000000"
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
		MinAgeHours:      168, // must be irrelevant to this scope
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	// Rank: aaa (newest, kept + in use), eee (30min — beyond keep but inside the
	// grace, kept), ccc (5h — beyond keep, unreferenced, past the grace: removed).
	if got := h.argv(); len(got) != 1 || got[0] != "rmi sha256:ccc" {
		t.Fatalf("argv = %q, want [rmi sha256:ccc] — min_age shielded a superseded image (or the grace didn't)", got)
	}
}

// Compose stacks build one image per service under the SAME deplo.slug; the
// deplo.service image label splits them so "keep the newest N" holds per service.
// Ranked together, keep=1 would keep one service's image and eat the others'.
func TestDockerCleanup_unusedAppImages_composeServicesRankApart(t *testing.T) {
	h := newFixture(t)
	now := time.Now()
	newer := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	older := now.Add(-8 * time.Hour).Format(time.RFC3339Nano)
	h.managedImages = []string{"web1111", "web2222", "api1111", "api2222"}
	h.imageRows = map[string]string{
		"web1111": "sha256:web1|shop|web|" + newer + "|100000000",
		"web2222": "sha256:web2|shop|web|" + older + "|100000000",
		"api1111": "sha256:api1|shop|api|" + newer + "|100000000",
		"api2222": "sha256:api2|shop|api|" + older + "|100000000",
	}
	h.install(t)

	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:           []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_APP_IMAGES},
		KeepImagesPerApp: 1,
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	got := h.argv()
	want := []string{"rmi sha256:web2", "rmi sha256:api2"}
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q — services of one slug must rank separately", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
}

// The prune scopes must PRUNE even when their own enumeration finds no candidate:
// the enumeration is the preview, docker's own `until=` filter is the decision.
// Gating on the pre-count is how a host kept 20GB docker itself called reclaimable
// while every sweep reported success — our parse and docker's filter disagree, and
// the short-circuit let ours win.
func TestDockerCleanup_pruneScopes_runEvenWithZeroCandidates(t *testing.T) {
	h := newFixture(t)
	h.buildCacheJSON = `[]` // nothing our enumeration would pick
	h.danglingImages = nil  // ditto
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
	if len(got) != len(want) {
		t.Fatalf("argv = %q, want %q — zero own-candidates must not skip the prune", got, want)
	}
	for _, w := range want {
		if !containsString(got, w) {
			t.Fatalf("argv = %q, missing %q", got, w)
		}
	}
	// Docker's own printed total (the fixture's "1.5GB") is the reported number —
	// never our zero estimate.
	for _, r := range resp.GetResults() {
		if r.GetReclaimedBytes() != 1500000000 {
			t.Errorf("scope %s reclaimed_bytes = %d, want docker's own total (1500000000)",
				r.GetScope(), r.GetReclaimedBytes())
		}
	}
}

// A failed enumeration on a loaded host (`docker system df -v` signal-killed at
// its timeout) must not abort the wet sweep — the prune is still safe and still
// owed. The dry run, whose whole answer IS the enumeration, keeps failing loudly.
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
	if got := h.argv(); len(got) != 1 || got[0] != "builder prune --force --filter until=168h" {
		t.Fatalf("argv = %q, want the prune despite the failed enumeration", got)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_BUILD_CACHE)
	if r.GetError() != "" {
		t.Errorf("error = %q; a pruned scope with a failed preview is a success", r.GetError())
	}
	if r.GetReclaimedBytes() != 1500000000 {
		t.Errorf("reclaimed_bytes = %d, want docker's own total", r.GetReclaimedBytes())
	}

	// Dry run: no enumeration, no answer — and never a prune.
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

// ---------------------------------------------------------------------------
// Dry run
// ---------------------------------------------------------------------------

// dry_run enumerates and removes NOTHING — removeObject is never called — while
// still populating every result field, which is what the confirm dialog renders.
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
	if len(resp.GetResults()) != 4 {
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

// ---------------------------------------------------------------------------
// The regression fence
// ---------------------------------------------------------------------------

// THE FENCE. No argv this handler can emit — under any scope, any age filter, any
// keep count — may be a container/volume/network/system prune. Each of those would
// turn disk reclaim into data loss on a Deplo host: a stopped app is a live app
// (StopStack is `compose stop`), its networks are not recreated by `compose start`,
// and a dangling volume may hold a database's files. If a future scope makes this
// test fail, the scope is wrong — not the test.
func TestDockerCleanup_neverEmitsAForbiddenPrune(t *testing.T) {
	forbidden := []string{"system prune", "container prune", "volume prune", "network prune"}

	for _, dryRun := range []bool{false, true} {
		for _, minAge := range []int32{0, 1, 168} {
			for _, keep := range []int32{0, 1, 5} {
				h := newFixture(t)
				h.install(t)
				if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
					Scopes:           allScopes(),
					DryRun:           dryRun,
					MinAgeHours:      minAge,
					KeepImagesPerApp: keep,
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

// ---------------------------------------------------------------------------
// Degraded hosts
// ---------------------------------------------------------------------------

// Without the container-reference index the agent cannot prove what is unreferenced,
// so it SKIPS the scopes that rest on it rather than guessing — and the scopes that
// do not need it still run. Skipping is not failing: the sweep is still ok.
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
	// The two scopes that need no index still run — docker's own prunes already
	// honour container references, stopped ones included. Nothing was removed BY ID,
	// which is the part that would have needed the evidence we could not gather.
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

// Docker unreachable => the sweep cannot start at all. UNAVAILABLE, the same split
// labelcheck.go draws between "docker could not run" and "the answer is no".
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

// A scope this agent does not define is a contract violation, not a result: the
// control plane must never be told "done" about something we silently ignored.
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

// An empty scope list is a no-op, not an error: the control plane owns the default
// set, and "nothing selected" must never become "everything".
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
	// Both rmis were attempted, and neither is counted as reclaimed.
	if len(h.argv()) != 2 {
		t.Errorf("argv = %q, want both removals attempted", h.argv())
	}
	if r.GetItemsRemoved() != 0 || r.GetReclaimedBytes() != 0 {
		t.Errorf("counted %d items / %d bytes that were never removed", r.GetItemsRemoved(), r.GetReclaimedBytes())
	}
}

// ---------------------------------------------------------------------------
// Unit: the size/time parsers the byte counts rest on
// ---------------------------------------------------------------------------

func TestParseHumanSize(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"0B", 0},
		{"8.19kB", 8190},
		{"276kB", 276000},
		{"449.4MB", 449400000},
		{"3.89GB", 3890000000},
		{"1.5GiB", 1610612736},
		{"", 0},
		{"garbage", 0},
	} {
		if got := parseHumanSize(tc.in); got != tc.want {
			t.Errorf("parseHumanSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// Docker's own printed total is authoritative INCLUDING ZERO: "Total reclaimed
// space: 0B" means the prune freed nothing, and falling back to the pre-flight
// estimate there is how the history once recorded a gigabyte that was never freed.
// The estimate is only for output shapes with no recognisable total at all.
func TestPickReclaimed_zeroTotalIsAuthoritative(t *testing.T) {
	for _, tc := range []struct {
		name     string
		out      string
		estimate int64
		want     int64
	}{
		{"image prune says 0B", "Deleted Images:\nuntagged: x\nTotal reclaimed space: 0B\n", 999, 0},
		{"buildx says 0B", "Total:\t0B\n", 999, 0},
		{"real total wins over estimate", "Total reclaimed space: 449.4MB\n", 1, 449400000},
		{"no total line -> estimate", "nothing to see here\n", 777, 777},
		{"unreadable total -> estimate", "Total: garbage\n", 555, 555},
		{"empty output -> estimate", "", 42, 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickReclaimed(tc.out, tc.estimate); got != tc.want {
				t.Errorf("pickReclaimed(%q, %d) = %d, want %d", tc.out, tc.estimate, got, tc.want)
			}
		})
	}
}

// An object whose age we cannot read is never a candidate while an age filter is
// set — we would rather leave it behind than delete something we know nothing about.
func TestOlderThan_unparseableNeverQualifies(t *testing.T) {
	cutoff := time.Now().Add(-24 * time.Hour)
	if olderThan("not a timestamp", cutoff) {
		t.Error("an unreadable timestamp must not qualify under an age filter")
	}
	if !olderThan("not a timestamp", time.Time{}) {
		t.Error("with no age filter, everything qualifies")
	}
	old := time.Now().Add(-48 * time.Hour).Format(time.RFC3339Nano)
	if !olderThan(old, cutoff) {
		t.Errorf("%q is older than the cutoff", old)
	}
}

func containsString(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
