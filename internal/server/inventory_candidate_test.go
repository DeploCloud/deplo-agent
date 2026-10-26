package server

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const candidateID = "sha256:" + "c0ffee00" + "c0ffee00" + "c0ffee00" + "c0ffee00" + "c0ffee00" + "c0ffee00" + "c0ffee00" + "c0ffee00"

var candidateRef = "nginx@sha256:" + strings.Repeat("d", 64)

// candidateDocker answers like workloadDocker, plus a registry whose newer build appears on pull.
func candidateDocker(marker string) string {
	return `case "$*" in
  "pull --quiet"*) : > ` + marker + ` ;;
  "image inspect -f {{.Id}}"*) [ -e ` + marker + ` ] && echo ` + candidateID + ` || exit 1 ;;
  "image rm"*) rm -f ` + marker + ` ;;
esac
` + workloadDocker
}

func candidateService(t *testing.T, pulledBefore bool) (*Service, func() []string) {
	t.Helper()
	s := inventoryService(t, passThroughSystemdRun)
	marker := filepath.Join(t.TempDir(), "pulled")
	if pulledBefore {
		mustWrite(t, marker, "")
	}
	return s, dockerOnPath(t, candidateDocker(marker))
}

func TestRepository(t *testing.T) {
	for ref, want := range map[string]string{
		"nginx:1.27-alpine":                       "docker.io/library/nginx",
		"nginx@sha256:" + strings.Repeat("d", 64): "docker.io/library/nginx",
		"crazymax/diun:latest":                    "docker.io/crazymax/diun",
		"docker.io/library/redis:8-alpine":        "docker.io/library/redis",
		"ghcr.io/acme/tool:1":                     "ghcr.io/acme/tool",
		"localhost:5000/app:2":                    "localhost:5000/app",
		"registry.local:5000/team/app":            "registry.local:5000/team/app",
	} {
		if got := repository(ref); got != want {
			t.Errorf("repository(%q) = %q, want %q", ref, got, want)
		}
	}
}

func TestInventoryCandidate_readsAPulledBuildAndDropsIt(t *testing.T) {
	s, argv := candidateService(t, false)
	resp, err := s.InventoryCandidate(context.Background(), &pb.InventoryCandidateRequest{ImageRef: candidateRef, CurrentImageId: testImageID})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetResult().GetStatus() != pb.InventoryStatus_INVENTORY_STATUS_OK || resp.GetImageId() != candidateID {
		t.Fatalf("want OK for %s, got %v %q: %s", candidateID, resp.GetResult().GetStatus(), resp.GetImageId(), resp.GetResult().GetError())
	}
	if len(resp.GetResult().GetPackages()) != 2 {
		t.Fatalf("packages: %v", resp.GetResult().GetPackages())
	}
	calls := argv()
	if !slices.Contains(calls, "pull --quiet "+candidateRef) {
		t.Errorf("no pull by digest in %v", calls)
	}
	if !slices.Contains(calls, "image rm "+candidateRef) {
		t.Errorf("a build pulled only to be read must go again: %v", calls)
	}
}

func TestInventoryCandidate_keepsABuildThatWasThere(t *testing.T) {
	s, argv := candidateService(t, true)
	if _, err := s.InventoryCandidate(context.Background(), &pb.InventoryCandidateRequest{ImageRef: candidateRef, CurrentImageId: testImageID}); err != nil {
		t.Fatal(err)
	}
	for _, c := range argv() {
		if strings.HasPrefix(c, "image rm") {
			t.Fatalf("removed an image that was on the host before: %q", c)
		}
	}
}

func TestInventoryCandidate_guards(t *testing.T) {
	ctx := context.Background()
	s, _ := candidateService(t, false)
	for name, req := range map[string]*pb.InventoryCandidateRequest{
		"a tag, not a digest":       {ImageRef: "nginx:latest", CurrentImageId: testImageID},
		"Deplo's own namespace":     {ImageRef: "deplo/web@sha256:" + strings.Repeat("d", 64), CurrentImageId: testImageID},
		"another repository":        {ImageRef: "evil/thing@sha256:" + strings.Repeat("d", 64), CurrentImageId: testImageID},
		"an image no workload runs": {ImageRef: candidateRef, CurrentImageId: "sha256:" + strings.Repeat("b", 64)},
	} {
		if _, err := s.InventoryCandidate(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: want InvalidArgument, got %v", name, err)
		}
	}

	want := func(name string, st pb.InventoryStatus) {
		t.Helper()
		resp, err := s.InventoryCandidate(ctx, &pb.InventoryCandidateRequest{ImageRef: candidateRef, CurrentImageId: testImageID})
		if err != nil || resp.GetResult().GetStatus() != st {
			t.Errorf("%s: want %v, got %v (%v)", name, st, resp.GetResult().GetStatus(), err)
		}
	}
	s.inventoryMu.Lock()
	want("concurrent inventory", pb.InventoryStatus_INVENTORY_STATUS_BUSY)
	s.inventoryMu.Unlock()

	s.deploys["dep"] = newInflight(func() {})
	want("deploy in flight", pb.InventoryStatus_INVENTORY_STATUS_BUSY)
	delete(s.deploys, "dep")

	inventoryFree = func(string) int64 { return 2500 }
	want("low disk before the pull", pb.InventoryStatus_INVENTORY_STATUS_SKIPPED_LOW_DISK)
}
