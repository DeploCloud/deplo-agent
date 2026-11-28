package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/pace"
)

// A restore from a backup kept on another member reads it from that member over the cluster's private network;
// Deplo relays it only when no path between them answers (deplo ADR-0043). Stream: "FETCH <key>", answered SEND
// or ERR, then the archive in frames and "SUM <bytes> <sha256>", as a left save is sent.
const fetchVerb = "FETCH"

// ExpectClusterFetch notes one backup another member is about to read; see the RPC's contract in proto/agent.proto.
func (s *Service) ExpectClusterFetch(_ context.Context, req *pb.ExpectClusterFetchRequest) (*pb.ExpectClusterFetchResponse, error) {
	reader, key := strings.ToLower(req.GetReaderFingerprint()), req.GetStore().GetObjectKey()
	if !validFingerprint(reader) {
		return nil, status.Error(codes.InvalidArgument, "the reader is not a certificate fingerprint")
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
	if s.expectedFetches == nil {
		s.expectedFetches = map[string]expectedBackup{}
	}
	for k, e := range s.expectedFetches {
		if now.After(e.until) {
			delete(s.expectedFetches, k)
		}
	}
	if _, known := s.expectedFetches[key]; !known && len(s.expectedFetches) >= maxExpected {
		return nil, status.Error(codes.ResourceExhausted, "this server already waits for too many reads")
	}
	s.expectedFetches[key] = expectedBackup{from: reader, root: req.GetStore().GetRoot(), until: now.Add(leftTimeout)}
	return &pb.ExpectClusterFetchResponse{}, nil
}

func (s *Service) takeExpectedFetch(from wire.Party, key string) (string, bool) {
	s.expectedMu.Lock()
	defer s.expectedMu.Unlock()
	e, known := s.expectedFetches[key]
	if !known || !from.Is(e.from) || time.Now().After(e.until) {
		return "", false
	}
	delete(s.expectedFetches, key)
	return e.root, true
}

// serveFetch sends one backup of this host's store to the member Deplo named for it.
func (s *Service) serveFetch(from wire.Party, c net.Conn, key string) {
	root, ok := s.takeExpectedFetch(from, key)
	if !ok {
		_ = wire.WriteHeader(c, wire.VerbErr, "this server was not told to send that")
		return
	}
	dir, err := s.resolveStoreRoot(root, false)
	if err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, oneLine(statusMessage(err)))
		return
	}
	f, err := storeOpen(dir, key)
	if err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, oneLine(statusMessage(err)))
		return
	}
	defer f.Close()
	_ = c.SetDeadline(time.Now().Add(leftTimeout))
	if wire.WriteHeader(c, leftSend, "") != nil {
		return
	}
	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(frameWriter{c}, sum), pace.Reader(f))
	if err != nil {
		return
	}
	if _, err := c.Write([]byte{0, 0, 0, 0}); err != nil {
		return
	}
	_ = wire.WriteHeader(c, leftSum, fmt.Sprintf("%d %s", n, hex.EncodeToString(sum.Sum(nil))))
}

// restoreFromMember is the Restore RPC under member_fingerprint: the artifact is read from that member's store.
func (s *Service) restoreFromMember(ctx context.Context, req *pb.RestoreRequest, stream pb.Agent_RestoreServer) {
	path := pathNone
	e := &rsEmitter{send: func(ev *pb.RestoreEvent) error {
		if r := ev.GetResult(); r != nil {
			r.Path = path
		}
		return stream.Send(ev)
	}}
	if req.GetAgeIdentity() == "" {
		e.result(false, "restoring from a server store needs its recovery key")
		return
	}
	r, done, got, err := s.openFromMember(ctx, req.GetMemberFingerprint(), req.GetStore().GetObjectKey())
	path = got
	if err != nil {
		e.result(false, err.Error())
		return
	}
	defer done()
	src := &artifactSource{
		stream:         r,
		identity:       req.GetAgeIdentity(),
		expectedSha256: req.GetExpectedSha256(),
		label:          "the server that holds the backups",
	}
	s.restoreArtifact(ctx, req.GetKind(), req.GetDatabase(), req.GetProject(), src, e)
}

// openFromMember opens one backup of that member's store over the cluster's network: an age file that reads to its
// end only when it is whole. The path is "none" while that member answered nothing.
func (s *Service) openFromMember(ctx context.Context, fingerprint, key string) (io.Reader, func(), string, error) {
	target := wire.Party{Fingerprint: strings.ToLower(fingerprint), Role: wire.RoleMember}
	m := s.clusterMember()
	m.mu.Lock()
	party := m.party
	m.mu.Unlock()
	if party == nil || !target.Valid() {
		return nil, nil, pathNone, errors.New("this server cannot reach the server that holds the backups")
	}
	conn, err := party.Dial(ctx, target)
	if err != nil {
		return nil, nil, pathNone, fmt.Errorf("reach the server that holds the backups: %w", err)
	}
	_ = conn.SetDeadline(time.Now().Add(leftTimeout))
	if err := wire.WriteHeader(conn, fetchVerb, key); err != nil {
		conn.Close()
		return nil, nil, pathNone, fmt.Errorf("reach the server that holds the backups: %w", err)
	}
	verb, arg, err := wire.ReadHeader(conn)
	if err != nil {
		conn.Close()
		return nil, nil, pathNone, fmt.Errorf("the server that holds the backups did not answer: %w", err)
	}
	path := s.copyPath(conn, target.Fingerprint)
	if verb != leftSend {
		conn.Close()
		return nil, nil, path, fmt.Errorf("the server that holds the backups refused it: %s", arg)
	}
	return &leftReader{c: conn, sum: sha256.New()}, func() { conn.Close() }, path, nil
}
