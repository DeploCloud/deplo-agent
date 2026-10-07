package server

import (
	"context"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func seedFilesDir(t *testing.T, stackDir, slug, name, body string) string {
	t.Helper()
	dir := filepath.Join(stackDir, "files", slug)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}
	return dir
}
func TestDockerCleanup_leftoverAppFiles_removesOnlyTheUnknown(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")

	live := seedFilesDir(t, stackDir, "web", "nginx.conf", "server {}\n")
	preview := seedFilesDir(t, stackDir, "web__pr-7", "nginx.conf", "server {}\n")
	db := seedFilesDir(t, stackDir, "db-shop", "postgresql.conf", "shared_buffers=1GB\n")
	dead := seedFilesDir(t, stackDir, "garage-s3-1", "garage.toml", "secret = 1\n")

	resp, err := s.DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:    []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES},
		LiveSlugs: []string{"web", "web__pr-7", "db-shop"},
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	for _, keep := range []string{live, preview, db} {
		if _, err := os.Stat(keep); err != nil {
			t.Errorf("a live stack's files must survive: %s (%v)", keep, err)
		}
	}
	if _, err := os.Stat(dead); !os.IsNotExist(err) {
		t.Errorf("the deleted app's files must be gone, stat err=%v", err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES)
	if r.GetItemsRemoved() != 1 || len(r.GetItems()) != 1 || r.GetItems()[0] != "garage-s3-1" {
		t.Errorf("result = %d %q, want the one leftover slug", r.GetItemsRemoved(), r.GetItems())
	}
	if r.GetReclaimedBytes() <= 0 {
		t.Errorf("reclaimed_bytes = %d, want the bytes the directory occupied", r.GetReclaimedBytes())
	}
	if got := h.argv(); len(got) != 0 {
		t.Errorf("this scope must run no docker command, got %q", got)
	}
}

// An empty live list is a control plane that could not tell us, not a host with nothing on it.
func TestDockerCleanup_leftoverAppFiles_emptyListSkipsInsteadOfWiping(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")
	dir := seedFilesDir(t, stackDir, "web", "nginx.conf", "server {}\n")

	resp, err := s.DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES},
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("nothing may be removed without a live list: %v", err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES)
	if !r.GetSkipped() {
		t.Errorf("the scope must report itself SKIPPED, got %+v", r)
	}
}

// A directory written moments ago belongs to a stack the caller's snapshot may predate - the same grace the app-image scope gives a build racing its deploy.
func TestDockerCleanup_leftoverAppFiles_sparesAFreshDirectory(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")

	fresh := filepath.Join(stackDir, "files", "just-created")
	if err := os.MkdirAll(fresh, 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := s.DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:    []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES},
		LiveSlugs: []string{"web"},
	}); err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("a directory inside the grace window must be spared: %v", err)
	}
}

// dry_run is what the confirm dialog calls: it must count and name the same directories and remove none of them.
func TestDockerCleanup_leftoverAppFiles_dryRunRemovesNothing(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	stackDir := t.TempDir()
	s := New(stackDir, t.TempDir(), "/", "")
	dead := seedFilesDir(t, stackDir, "anytype-1", "config.yml", "x: 1\n")

	resp, err := s.DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:    []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES},
		LiveSlugs: []string{"web"},
		DryRun:    true,
	})
	if err != nil {
		t.Fatalf("DockerCleanup: %v", err)
	}
	if _, err := os.Stat(dead); err != nil {
		t.Fatalf("dry_run must remove nothing: %v", err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_APP_FILES)
	if r.GetItemsRemoved() != 1 {
		t.Errorf("dry_run must still report the candidate, got %d", r.GetItemsRemoved())
	}
}

// A stack's own `deplo-<slug>_<key>` network is litter once the slug is gone, judged against live_slugs; a tenant network still needs live_networks; anything attached, fresh, live or not ours stays.
func TestDockerCleanup_leftoverNetworks_projectNetworksOfDeadSlugs(t *testing.T) {
	h := newFixture(t)
	old := `"` + time.Now().Add(-48*time.Hour).Format(time.RFC3339Nano) + `"`
	fresh := `"` + time.Now().Format(time.RFC3339Nano) + `"`
	h.networks = []string{
		"deplo-minecraft_default",
		"deplo-garage_default",
		"deplo-b4_backend",
		"deplo-new_default",
		"deplo-env-environ_1",
		"deplo-env-environ_2",
		"deplo",
		"traefik_deplo-socket",
		"bridge",
	}
	h.networkStates = map[string]string{
		"deplo-minecraft_default": "deplo-traefik |" + old,
		"deplo-garage_default":    "|" + old,
		"deplo-b4_backend":        "deplo-b4-web-1 |" + old,
		"deplo-new_default":       "|" + fresh,
		"deplo-env-environ_1":     "deplo-traefik |" + old,
		"deplo-env-environ_2":     "|" + old,
		"deplo":                   "|" + old,
		"traefik_deplo-socket":    "|" + old,
		"bridge":                  "|" + old,
	}
	h.install(t)
	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:       []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_NETWORKS},
		LiveSlugs:    []string{"garage"},
		LiveNetworks: []string{"deplo-env-environ_2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_NETWORKS)
	want := []string{
		"network disconnect -f deplo-env-environ_1 deplo-traefik",
		"network rm deplo-env-environ_1",
		"network disconnect -f deplo-minecraft_default deplo-traefik",
		"network rm deplo-minecraft_default",
	}
	if got := h.argv(); strings.Join(got, ";") != strings.Join(want, ";") {
		t.Fatalf("removals:\n got %v\nwant %v", got, want)
	}
	if r.GetItemsRemoved() != 2 {
		t.Fatalf("result: %+v", r)
	}

	h2 := newFixture(t)
	h2.networks, h2.networkStates = h.networks, h.networkStates
	h2.install(t)
	if _, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:    []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_LEFTOVER_NETWORKS},
		LiveSlugs: []string{"garage"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := h2.argv(); len(got) != 2 || got[1] != "network rm deplo-minecraft_default" {
		t.Fatalf("slugs only: %v", got)
	}
}
