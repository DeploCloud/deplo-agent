package server

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/yamux"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/mesh"
)

// A backup from one member to another's store goes over the cluster's private network; Deplo relays it only when
// no path between them answers (deplo ADR-0047).
const (
	pathDirect = "direct"
	pathRelay  = "relay"
	pathNone   = "none"
)

// expectedBackup is one backup Deplo told this member to keep: from that certificate, in that store.
type expectedBackup struct {
	from, root string
	until      time.Time
}

// ExpectClusterBackup notes one backup another member is about to send; see the RPC's contract in proto/agent.proto.
func (s *Service) ExpectClusterBackup(_ context.Context, req *pb.ExpectClusterBackupRequest) (*pb.ExpectClusterBackupResponse, error) {
	from, key := strings.ToLower(req.GetSourceFingerprint()), req.GetStore().GetObjectKey()
	if !validFingerprint(from) {
		return nil, status.Error(codes.InvalidArgument, "the source is not a certificate fingerprint")
	}
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "the backup has no object key")
	}
	if _, err := s.resolveStoreRoot(req.GetStore().GetRoot(), false); err != nil {
		return nil, err
	}
	m := s.clusterMember()
	m.mu.Lock()
	member := m.st.Member
	m.mu.Unlock()
	if !member {
		return nil, status.Error(codes.FailedPrecondition, "this server agent is not a cluster member")
	}
	now := time.Now()
	s.expectedMu.Lock()
	defer s.expectedMu.Unlock()
	if s.expectedBackups == nil {
		s.expectedBackups = map[string]expectedBackup{}
	}
	for k, e := range s.expectedBackups {
		if now.After(e.until) {
			delete(s.expectedBackups, k)
		}
	}
	if _, known := s.expectedBackups[key]; !known && len(s.expectedBackups) >= maxExpected {
		return nil, status.Error(codes.ResourceExhausted, "this server already waits for too many backups")
	}
	s.expectedBackups[key] = expectedBackup{from: from, root: req.GetStore().GetRoot(), until: now.Add(leftTimeout)}
	return &pb.ExpectClusterBackupResponse{}, nil
}

// takeExpectedBackup uses up the entry for key when that member is the one Deplo named, and says its store.
func (s *Service) takeExpectedBackup(from wire.Party, key string) (string, bool) {
	s.expectedMu.Lock()
	defer s.expectedMu.Unlock()
	e, known := s.expectedBackups[key]
	if !known || !from.Is(e.from) || time.Now().After(e.until) {
		return "", false
	}
	delete(s.expectedBackups, key)
	return e.root, true
}

// backupToMember is the Backup RPC under member_fingerprint: the archive goes to that member, and the result says how.
func (s *Service) backupToMember(ctx context.Context, req *pb.BackupRequest, e *bkEmitter) {
	res, path, err := s.sendToMember(ctx, req, req.GetMemberFingerprint())
	if err != nil {
		res = &pb.BackupResult{Error: err.Error()}
	}
	res.ObjectKey, res.Path = req.GetStore().GetObjectKey(), path
	_ = e.emit(&pb.BackupEvent{Event: &pb.BackupEvent_Result{Result: res}})
}

// copyPath says how a stream to or from a member travels: over a relay when one carries the stream or its tunnel.
func (s *Service) copyPath(c net.Conn, fingerprint string) string {
	if tc, ok := c.(*tls.Conn); ok {
		if _, relayed := tc.NetConn().(*yamux.Stream); relayed {
			return pathRelay
		}
	}
	if s.meshPath(fingerprint) == mesh.PathRelay {
		return pathRelay
	}
	return pathDirect
}

// meshPath is how the private network reaches that member now; empty when it does not know it.
func (s *Service) meshPath(fingerprint string) string {
	n := s.network()
	n.mu.Lock()
	m := n.m
	n.mu.Unlock()
	if m == nil {
		return ""
	}
	for _, st := range m.Status() {
		if strings.EqualFold(st.Fingerprint, fingerprint) {
			return st.Path
		}
	}
	return ""
}
