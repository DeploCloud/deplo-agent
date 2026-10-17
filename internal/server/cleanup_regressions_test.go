package server

import (
	"context"
	"fmt"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDockerCleanupRegression_modernScopeReclaimsBuildkit(t *testing.T) {
	h := newFixture(t)
	h.install(t)
	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := h.argv(); len(got) != 1 || got[0] != "volume rm orphan-buildkit" {
		t.Fatalf("orphan build cache was not reclaimed: %v", got)
	}
	if !resp.GetOk() {
		t.Fatal(resp.GetError())
	}
}

func TestDockerCleanupRegression_preservesLocalWorkspaceImage(t *testing.T) {
	h := newFixture(t)
	old := time.Now().Add(-72 * time.Hour).Format(time.RFC3339Nano)
	h.taggedImages = []string{"workspace"}
	h.imageRows["workspace"] = "sha256:workspace|||" + old + "|7000000000|neoncoder-acp-workspace:local||" + old + "|neoncoder-acp-workspace:local|"
	h.install(t)
	_, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES},
		MinAgeHours: 24,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := h.argv(); len(got) != 0 {
		t.Fatalf("local-only workspace image was deleted: %v", got)
	}
}

func TestDockerCleanupRegression_scopeFailureIsNotSuccess(t *testing.T) {
	h := newFixture(t)
	h.psFails = true
	h.install(t)
	resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
		Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetOk() || !strings.Contains(resp.GetError(), "Docker daemon") {
		t.Fatalf("cleanup reported success despite a failed scope: %+v", resp)
	}
}

func TestDockerCleanupRegression_volumeSafetyAndAge(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		referenced, fresh, dry bool
		want                   int32
	}{
		{"old unused cache", false, false, false, 1},
		{"stopped container still references cache", true, false, false, 0},
		{"fresh cache", false, true, false, 0},
		{"preview only", false, false, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFixture(t)
			if tc.referenced {
				h.inspectRows = append(h.inspectRows, "sha256:stopped|orphan-buildkit,")
			}
			if tc.fresh {
				h.volumeCreated = time.Now().Format(time.RFC3339Nano)
			}
			h.install(t)
			resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
				Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES},
				MinAgeHours: 24, DryRun: tc.dry,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := resultFor(t, resp, pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES).GetItemsRemoved(); got != tc.want {
				t.Fatalf("removed=%d want=%d", got, tc.want)
			}
			if tc.dry && len(h.argv()) != 0 {
				t.Fatalf("preview deleted data: %v", h.argv())
			}
			for _, cmd := range h.argv() {
				if strings.Contains(cmd, "mongo-data") {
					t.Fatal("persistent data removed")
				}
			}
		})
	}
}

func TestDockerCleanupRegression_manyCacheVolumesAndMixedScopes(t *testing.T) {
	for _, dry := range []bool{true, false} {
		h := newFixture(t)
		mount := h.volumeMounts["orphan-buildkit"]
		h.danglingVolumes = []string{"mongo-data"}
		for i := 0; i < 250; i++ {
			name := fmt.Sprintf("buildx_buildkit_test_%03d_state", i)
			h.danglingVolumes = append(h.danglingVolumes, name)
			h.volumeMounts[name] = mount
		}
		h.install(t)
		resp, err := newService(t).DockerCleanup(context.Background(), &pb.DockerCleanupRequest{
			Scopes:      []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_VOLUMES, pb.CleanupScope_CLEANUP_SCOPE_ORPHAN_BUILDKIT_CACHE},
			MinAgeHours: 24, DryRun: dry,
		})
		if err != nil {
			t.Fatal(err)
		}
		var count int32
		for _, r := range resp.GetResults() {
			count += r.GetItemsRemoved()
			if len(r.GetItems()) > cleanupMaxItems {
				t.Fatal("unbounded report")
			}
		}
		if count != 250 {
			t.Fatalf("removed=%d, want 250 without duplicate counting", count)
		}
		if dry && len(h.argv()) > 0 {
			t.Fatal("dry run mutated volumes")
		}
		if !dry && len(h.argv()) != 250 {
			t.Fatalf("removal calls=%d", len(h.argv()))
		}
	}
}

func TestDockerCleanupRegression_buildkitMarkerMustBeRegular(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		root := t.TempDir()
		marker := filepath.Join(root, buildkitSentinel)
		var err error
		if symlink {
			err = os.Symlink(filepath.Join(t.TempDir(), "missing"), marker)
		} else {
			err = os.Mkdir(marker, 0700)
		}
		if err != nil {
			t.Fatal(err)
		}
		if isBuildkitCache(root) {
			t.Fatal("directory or symlink is not proof of a build cache")
		}
	}
}

func TestDockerCleanupRegression_pulledImageRequiresAllRepositories(t *testing.T) {
	digest := "nginx@sha256:" + strings.Repeat("a", 64)
	for _, tc := range []struct {
		name          string
		tags, digests []string
		want          bool
	}{
		{"pulled", []string{"nginx:alpine"}, []string{digest}, true},
		{"local only", []string{"neoncoder-acp-workspace:local"}, nil, false},
		{"local alias", []string{"nginx:alpine", "local-backup:keep"}, []string{digest}, false},
		{"missing tag", nil, []string{digest}, false},
		{"build tooling", []string{"moby/buildkit:latest"}, []string{"moby/buildkit@sha256:" + strings.Repeat("a", 64)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			im := imageInfo{tags: tc.tags, digests: tc.digests}
			if im.recoverableFromRegistry() != tc.want {
				t.Fatalf("unexpected eligibility for %s", tc.name)
			}
		})
	}
}
