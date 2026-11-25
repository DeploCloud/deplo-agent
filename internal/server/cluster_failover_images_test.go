package server

import (
	"context"
	"strings"
	"sync"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/room"
)

// failoverHost is a member whose docker holds `held` and pulls into it, with free of total bytes on its disk.
func failoverHost(t *testing.T, held []string, free, total int64) (*Service, *[]string) {
	t.Helper()
	var mu sync.Mutex
	have := map[string]bool{}
	for _, r := range held {
		have[r] = true
	}
	var pulled []string
	s := &Service{agentDir: t.TempDir()}
	s.failoverRootOnce.Do(func() { s.failoverRoot = "/docker" })
	s.imageOps = clusterImageOps{has: func(_ context.Context, ref string) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return have[ref], nil
	}}
	pull, size, stat := failoverPull, imageBytes, room.Stat
	failoverPull = func(_ context.Context, ref string) error {
		mu.Lock()
		defer mu.Unlock()
		pulled, have[ref] = append(pulled, ref), true
		return nil
	}
	imageBytes = func(context.Context, string) int64 { return 7 }
	room.Stat = func(string) (int64, int64, bool) { return free, total, true }
	t.Cleanup(func() { failoverPull, imageBytes, room.Stat = pull, size, stat })
	return s, &pulled
}

func holdAll(t *testing.T, s *Service, ims ...*pb.FailoverImage) map[string]*pb.FailoverImage {
	t.Helper()
	r, err := s.HoldFailoverImages(context.Background(), &pb.HoldFailoverImagesRequest{Images: ims})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*pb.FailoverImage{}
	for _, im := range r.GetImages() {
		out[im.GetImageRef()] = im
	}
	return out
}

func pullsSettled(t *testing.T, s *Service) {
	t.Helper()
	eventually(t, "the pulls settle", func() bool {
		s.failover.mu.Lock()
		defer s.failover.mu.Unlock()
		return !s.failover.running
	})
}

// c41: fise's new server pulled its 3.3 GB of registry images at the failover. A member now holds them ahead.
func TestHoldFailoverImages_pullsWhatIsMissingAheadAndSaysItIsHeld(t *testing.T) {
	s, pulled := failoverHost(t, []string{"postgres:17"}, 80, 100)
	want := []*pb.FailoverImage{{ImageRef: "postgres:17"}, {ImageRef: "ghcr.io/acme/web:2", Bytes: 5}}
	holdAll(t, s, want...)
	pullsSettled(t, s)
	got := holdAll(t, s, want...)
	if !got["postgres:17"].GetHeld() || !got["ghcr.io/acme/web:2"].GetHeld() || got["ghcr.io/acme/web:2"].GetBytes() != 7 {
		t.Fatalf("after the pulls: %v", got)
	}
	pullsSettled(t, s)
	if len(*pulled) != 1 || (*pulled)[0] != "ghcr.io/acme/web:2" {
		t.Fatalf("pulled %v, want only the image it lacked, once", *pulled)
	}
}

func TestHoldFailoverImages_pullsNothingThatLeavesLessThanATenthFree(t *testing.T) {
	s, pulled := failoverHost(t, nil, 15, 100)
	holdAll(t, s, &pb.FailoverImage{ImageRef: "ghcr.io/acme/web:2", Bytes: 10})
	pullsSettled(t, s)
	got := holdAll(t, s, &pb.FailoverImage{ImageRef: "ghcr.io/acme/web:2", Bytes: 10})
	if im := got["ghcr.io/acme/web:2"]; im.GetHeld() || !strings.Contains(im.GetError(), room.Marker) {
		t.Fatalf("a pull past a tenth free: %v", im)
	}
	if len(*pulled) != 0 {
		t.Fatalf("pulled %v onto a disk without room", *pulled)
	}
}

func TestCleanup_sparesTheImagesHeldForAFailoverAfterARestart(t *testing.T) {
	s, _ := failoverHost(t, nil, 80, 100)
	holdAll(t, s, &pb.FailoverImage{ImageRef: "docker.io/library/postgres:17"}, &pb.FailoverImage{ImageRef: "ghcr.io/acme/web"},
		&pb.FailoverImage{ImageRef: "ghcr.io/acme/api@sha256:" + strings.Repeat("a", 64)})
	pullsSettled(t, s)
	restarted := &Service{agentDir: s.agentDir}
	kept := restarted.failoverKept()
	for _, im := range []imageInfo{
		{tags: []string{"postgres:17"}},
		{tags: []string{"ghcr.io/acme/web:latest"}},
		{digests: []string{"ghcr.io/acme/api@sha256:" + strings.Repeat("a", 64)}},
	} {
		if !keptForFailover(im, kept) {
			t.Fatalf("cleanup would remove %+v, held for a failover", im)
		}
	}
	if keptForFailover(imageInfo{tags: []string{"postgres:16"}}, kept) {
		t.Fatal("an image no failover needs is kept")
	}
}
