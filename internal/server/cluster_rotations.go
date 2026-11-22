package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// The key rotations Deplo announced (ADR-0050) are kept on disk: a server back while Deplo is down still knows a
// peer that rotated while it was off.
const maxRotations = 2 * maxMeshPeers

type rotationsFile struct {
	Clusters map[string][]wire.Rotation `json:"clusters"`
}

type clusterRotations struct {
	s  *Service
	mu sync.Mutex
	st rotationsFile
}

func (s *Service) rotations() *clusterRotations {
	s.rotOnce.Do(func() {
		r := &clusterRotations{s: s}
		if s.agentDir != "" {
			if raw, err := os.ReadFile(r.file()); err == nil {
				_ = json.Unmarshal(raw, &r.st)
			}
		}
		s.rot = r
	})
	return s.rot
}

func (r *clusterRotations) file() string {
	return filepath.Join(r.s.agentDir, "cluster-rotations.json")
}

// all is every rotation this server holds, whichever cluster announced it: a certificate names one server only.
func (r *clusterRotations) all() []wire.Rotation {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.st.Clusters))
	for id := range r.st.Clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out []wire.Rotation
	for _, id := range ids {
		out = append(out, r.st.Clusters[id]...)
	}
	return out
}

// SetClusterRotations keeps a cluster's key rotations; see the RPC's contract in proto/agent.proto.
func (s *Service) SetClusterRotations(_ context.Context, req *pb.SetClusterRotationsRequest) (*pb.SetClusterRotationsResponse, error) {
	if !fwdID.MatchString(req.GetClusterId()) {
		return nil, status.Error(codes.InvalidArgument, "a cluster id is required")
	}
	if len(req.GetRotations()) > maxRotations {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d rotations", maxRotations)
	}
	var list []wire.Rotation
	for _, x := range req.GetRotations() {
		rot := wire.Rotation{From: strings.ToLower(x.GetFrom()), To: strings.ToLower(x.GetTo())}
		if !validFingerprint(rot.From) || !validFingerprint(rot.To) || rot.From == rot.To {
			return nil, status.Errorf(codes.InvalidArgument, "%q to %q is not a key rotation", x.GetFrom(), x.GetTo())
		}
		list = append(list, rot)
	}
	r := s.rotations()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.st.Clusters == nil {
		r.st.Clusters = map[string][]wire.Rotation{}
	}
	if len(list) == 0 {
		delete(r.st.Clusters, req.GetClusterId())
	} else {
		r.st.Clusters[req.GetClusterId()] = list
	}
	raw, _ := json.Marshal(r.st)
	if err := writeFileAtomic(r.file(), raw, 0o600); err != nil {
		return nil, status.Errorf(codes.Internal, "remember the key rotations: %v", err)
	}
	return &pb.SetClusterRotationsResponse{Rotations: int32(len(list))}, nil
}

// meshName is the name this server's network knows a party by: the one asked for, or another certificate of the
// same server. Routing only: the stream's own handshake still pins who answers.
func (s *Service) meshName(has func(string) bool, fp string) (string, bool) {
	if has(fp) {
		return fp, true
	}
	rs := s.rotations().all()
	for _, other := range append(wire.Successors(rs, fp), wire.Predecessors(rs, fp)...) {
		if has(other) {
			return other, true
		}
	}
	return "", false
}
