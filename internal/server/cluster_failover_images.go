package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/room"
)

// A member holds the registry images its cluster's workloads run, so a failover onto it starts from what it has
// (c41: fise's 3.3 GB were pulled at the failover, 76 s).

const (
	failoverImagesFile  = "failover-images.json"
	failoverPullTimeout = 30 * time.Minute
	// failoverHeldFor is how long a look that found an image held stands before docker is asked again.
	failoverHeldFor = 5 * time.Minute
)

type failoverImages struct {
	mu      sync.Mutex
	want    []*pb.FailoverImage
	state   map[string]*failoverState
	running bool
	dirty   bool
	loaded  bool
}

type failoverState struct {
	held   bool
	bytes  int64
	err    string
	lookAt time.Time
	// missingSince is when this member found no other one holding the image, waiting for the first to download it.
	missingSince time.Time
}

// failoverPull downloads one image from its registry, registryPull when nil; a var so tests download nothing.
var failoverPull func(ctx context.Context, ref string) error

// imageBytes is an image's size on this server; a var so tests ask no docker.
var imageBytes = func(ctx context.Context, ref string) int64 {
	res, err := dockercli.Run(ctx, imageRemoveTimeout, "image", "inspect", "--format", "{{.Size}}", ref)
	if err != nil || res.Code != 0 {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(res.Stdout), 10, 64)
	return n
}

// HoldFailoverImages replaces the images this member keeps ready; see the RPC's contract in proto/agent.proto.
func (s *Service) HoldFailoverImages(_ context.Context, req *pb.HoldFailoverImagesRequest) (*pb.HoldFailoverImagesResponse, error) {
	if len(req.GetImages()) > maxExpected {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d images", maxExpected)
	}
	var want []*pb.FailoverImage
	seen := map[string]bool{}
	for _, im := range req.GetImages() {
		ref := im.GetImageRef()
		if err := validateClusterImageRef(ref); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !seen[ref] {
			seen[ref] = true
			want = append(want, &pb.FailoverImage{ImageRef: ref, Bytes: max(im.GetBytes(), 0)})
		}
	}
	f := &s.failover
	f.mu.Lock()
	defer f.mu.Unlock()
	f.want, f.loaded, f.dirty = want, true, true
	if f.state == nil {
		f.state = map[string]*failoverState{}
	}
	refs := make([]string, len(want))
	for i, im := range want {
		refs[i] = im.ImageRef
	}
	if raw, err := json.Marshal(refs); err == nil {
		_ = os.WriteFile(filepath.Join(s.agentDir, failoverImagesFile), raw, 0o600)
	}
	if !f.running {
		f.running = true
		go s.pullFailoverImages()
	}
	out := &pb.HoldFailoverImagesResponse{}
	for _, im := range want {
		r := &pb.FailoverImage{ImageRef: im.ImageRef}
		if st := f.state[im.ImageRef]; st != nil {
			r.Held, r.Bytes, r.Error = st.held, st.bytes, st.err
		}
		out.Images = append(out.Images, r)
	}
	return out, nil
}

// pullFailoverImages brings in what is missing, one image at a time, until no call changed the list meanwhile.
func (s *Service) pullFailoverImages() {
	f := &s.failover
	for {
		f.mu.Lock()
		if !f.dirty {
			f.running = false
			f.mu.Unlock()
			return
		}
		f.dirty = false
		want := append([]*pb.FailoverImage(nil), f.want...)
		f.mu.Unlock()
		for _, im := range want {
			s.holdFailoverImage(im)
		}
	}
}

func (s *Service) holdFailoverImage(im *pb.FailoverImage) {
	f := &s.failover
	ref := im.GetImageRef()
	f.mu.Lock()
	st := f.state[ref]
	if st == nil {
		st = &failoverState{}
		f.state[ref] = st
	}
	fresh := st.held && time.Since(st.lookAt) < failoverHeldFor
	f.mu.Unlock()
	if fresh {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), failoverPullTimeout)
	defer cancel()
	held, _ := s.images().has(ctx, ref)
	next := failoverState{held: held, lookAt: time.Now()}
	if !held {
		if err := room.Check(s.dockerRootPath(ctx), im.GetBytes()); err != nil {
			next.err = err.Error()
		} else if from, err := s.fetchFromMembers(ctx, ref); err == nil {
			next.held = true
			fmt.Fprintf(os.Stderr, "deplo-agent: took %s from the cluster's member %s, not its registry\n", ref, from[:min(12, len(from))])
		} else if wait := s.mayDownload(st, next.lookAt); wait != "" {
			next.err, next.missingSince = wait, st.missingSince
		} else if err := s.downloadFailoverImage(ctx, ref); err != nil {
			next.err = "could not pull it: " + err.Error()
		} else {
			next.held = true
		}
	}
	if next.held {
		next.bytes = imageBytes(ctx, ref)
	}
	f.mu.Lock()
	*st = next
	f.mu.Unlock()
}

// mayDownload says why this member does not download an image no other member holds yet: the cluster's first
// member does, and the others take it from there; past pullerGrace this one does too. The caller owns st.
func (s *Service) mayDownload(st *failoverState, now time.Time) string {
	if _, first := s.clusterPeers(); first {
		return ""
	}
	s.failover.mu.Lock()
	defer s.failover.mu.Unlock()
	if st.missingSince.IsZero() {
		st.missingSince = now
	}
	if now.Sub(st.missingSince) >= pullerGrace {
		return ""
	}
	return "waiting for the cluster's first member to download it, then it comes from there"
}

func (s *Service) downloadFailoverImage(ctx context.Context, ref string) error {
	if failoverPull != nil {
		return failoverPull(ctx, ref)
	}
	return registryPull(ctx, s, ref)
}

// dockerRootPath is the disk Docker keeps its images on, looked up once.
func (s *Service) dockerRootPath(ctx context.Context) string {
	s.failoverRootOnce.Do(func() { s.failoverRoot = dockerRootDir(ctx) })
	if s.failoverRoot == "" {
		return "/var/lib/docker"
	}
	return s.failoverRoot
}

// failoverKept is every image this member holds for a failover, by every name docker may give it; after a restart,
// the list the last call left on disk.
func (s *Service) failoverKept() map[string]bool {
	f := &s.failover
	f.mu.Lock()
	defer f.mu.Unlock()
	var refs []string
	if f.loaded {
		for _, im := range f.want {
			refs = append(refs, im.ImageRef)
		}
	} else if raw, err := os.ReadFile(filepath.Join(s.agentDir, failoverImagesFile)); err == nil {
		_ = json.Unmarshal(raw, &refs)
	}
	kept := map[string]bool{}
	for _, r := range refs {
		kept[imageName(r)] = true
	}
	return kept
}

// imageName is a reference as docker lists it: no default registry or library, and latest when no tag says.
func imageName(ref string) string {
	ref = strings.TrimPrefix(strings.TrimPrefix(ref, "docker.io/"), "library/")
	if strings.Contains(ref, "@") {
		return ref
	}
	if i := strings.LastIndex(ref, ":"); i < 0 || strings.Contains(ref[i:], "/") {
		ref += ":latest"
	}
	return ref
}

// keptForFailover says whether cleanup must spare an image: one of its names or digests is held for a failover.
func keptForFailover(im imageInfo, kept map[string]bool) bool {
	for _, n := range append(append([]string(nil), im.tags...), im.digests...) {
		if kept[imageName(n)] {
			return true
		}
	}
	return false
}
