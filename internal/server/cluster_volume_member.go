package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// A move copies a volume member to member over the cluster's private network (deplo ADR-0047). Stream:
// "VOLUME <name>", answered SEND or ERR, the gzipped tar in frames (an empty one ends it), "SUM <bytes> <sha256>",
// answered OK <bytes> <sha256> or ERR.
const volumeVerb = "VOLUME"

type expectedVolume struct {
	from  string
	wipe  bool
	until time.Time
}

// ExpectClusterVolume notes one volume another member is about to send; see the RPC's contract in proto/agent.proto.
func (s *Service) ExpectClusterVolume(_ context.Context, req *pb.ExpectClusterVolumeRequest) (*pb.ExpectClusterVolumeResponse, error) {
	vol, from := req.GetVolumeName(), strings.ToLower(req.GetSourceFingerprint())
	if err := validateVolumeName(vol); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !validFingerprint(from) {
		return nil, status.Error(codes.InvalidArgument, "the source is not a certificate fingerprint")
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
	if s.expectedVolumes == nil {
		s.expectedVolumes = map[string]expectedVolume{}
	}
	for k, e := range s.expectedVolumes {
		if now.After(e.until) {
			delete(s.expectedVolumes, k)
		}
	}
	if _, known := s.expectedVolumes[vol]; !known && len(s.expectedVolumes) >= maxExpected {
		return nil, status.Error(codes.ResourceExhausted, "this server already waits for too many volumes")
	}
	s.expectedVolumes[vol] = expectedVolume{from: from, wipe: req.GetWipeFirst(), until: now.Add(volumeCopyTimeout)}
	return &pb.ExpectClusterVolumeResponse{}, nil
}

func (s *Service) takeExpectedVolume(from wire.Party, vol string) (wipe, ok bool) {
	s.expectedMu.Lock()
	defer s.expectedMu.Unlock()
	e, known := s.expectedVolumes[vol]
	if !known || !from.Is(e.from) || time.Now().After(e.until) {
		return false, false
	}
	delete(s.expectedVolumes, vol)
	return e.wipe, true
}

// frames reads one framed stream and ends cleanly only at a SUM naming exactly what arrived.
type frames struct {
	c   io.Reader
	n   int64
	sum hash.Hash
	buf []byte
}

func (f *frames) next() ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(f.c, head[:]); err != nil {
		return nil, errors.New("the volume was cut short")
	}
	n := binary.BigEndian.Uint32(head[:])
	if n > storeChunkBytes {
		return nil, errors.New("the volume is not framed as expected")
	}
	if n == 0 {
		verb, arg, err := wire.ReadHeader(f.c)
		if err != nil || verb != leftSum || arg != fmt.Sprintf("%d %s", f.n, hex.EncodeToString(f.sum.Sum(nil))) {
			return nil, errors.New("what arrived is not what was sent")
		}
		return nil, io.EOF
	}
	if cap(f.buf) < int(n) {
		f.buf = make([]byte, n)
	}
	b := f.buf[:n]
	if _, err := io.ReadFull(f.c, b); err != nil {
		return nil, errors.New("the volume was cut short")
	}
	f.n += int64(n)
	f.sum.Write(b)
	return b, nil
}

// receiveVolume takes a volume only for an entry Deplo handed this member, from that member.
func (s *Service) receiveVolume(from wire.Party, c net.Conn, vol string) {
	wipe, ok := s.takeExpectedVolume(from, vol)
	if !ok {
		_ = wire.WriteHeader(c, wire.VerbErr, "this server was not told to take that")
		return
	}
	_ = c.SetDeadline(time.Now().Add(volumeCopyTimeout))
	if wire.WriteHeader(c, leftSend, "") != nil {
		return
	}
	f := &frames{c: c, sum: sha256.New()}
	res := importVolumeFrom(context.Background(), &pb.VolumeChunk_Header{VolumeName: vol, WipeFirst: wipe}, f.next)
	if !res.GetOk() {
		_ = wire.WriteHeader(c, wire.VerbErr, oneLine(res.GetError()))
		return
	}
	_ = wire.WriteHeader(c, wire.VerbOK, fmt.Sprintf("%d %s", res.GetBytesWritten(), res.GetSha256()))
}

// SendClusterVolume copies a volume to another member; see the RPC's contract in proto/agent.proto.
func (s *Service) SendClusterVolume(ctx context.Context, req *pb.SendClusterVolumeRequest) (*pb.SendClusterVolumeResponse, error) {
	vol := req.GetVolumeName()
	if err := validateVolumeName(vol); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	target := wire.Party{Fingerprint: strings.ToLower(req.GetTargetFingerprint()), Role: wire.RoleMember}
	if !target.Valid() {
		return nil, status.Error(codes.InvalidArgument, "the target is not a certificate fingerprint")
	}
	if err := assertVolumeExists(ctx, vol); err != nil {
		if status.Code(err) == codes.NotFound {
			return &pb.SendClusterVolumeResponse{Missing: true}, nil
		}
		return nil, err
	}
	m := s.clusterMember()
	m.mu.Lock()
	party := m.party
	m.mu.Unlock()
	none := &pb.SendClusterVolumeResponse{Path: pathNone}
	if party == nil {
		return none, nil
	}
	conn, err := party.Dial(ctx, target)
	if err != nil {
		return none, nil
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(volumeCopyTimeout))
	if wire.WriteHeader(conn, volumeVerb, vol) != nil {
		return none, nil
	}
	verb, arg, err := wire.ReadHeader(conn)
	if err != nil {
		return none, nil
	}
	path := s.copyPath(conn, target.Fingerprint)
	if verb != leftSend {
		return nil, status.Errorf(codes.FailedPrecondition, "the server it moves to refused it: %s", arg)
	}
	sum, n := sha256.New(), int64(0)
	err = exportVolumeTo(ctx, vol, func(b []byte) error {
		sum.Write(b)
		n += int64(len(b))
		_, err := frameWriter{conn}.Write(b)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("send volume %q: %w", vol, err)
	}
	want := fmt.Sprintf("%d %s", n, hex.EncodeToString(sum.Sum(nil)))
	if _, err := conn.Write([]byte{0, 0, 0, 0}); err != nil {
		return nil, fmt.Errorf("send volume %q: %w", vol, err)
	}
	if err := wire.WriteHeader(conn, leftSum, want); err != nil {
		return nil, fmt.Errorf("send volume %q: %w", vol, err)
	}
	verb, arg, err = wire.ReadHeader(conn)
	switch {
	case err != nil:
		return nil, fmt.Errorf("the server it moves to did not confirm %q: %w", vol, err)
	case verb != wire.VerbOK:
		return nil, fmt.Errorf("the server it moves to could not take %q: %s", vol, arg)
	case arg != want:
		return nil, fmt.Errorf("the server it moves to wrote something else than %q", vol)
	}
	return &pb.SendClusterVolumeResponse{Bytes: n, Sha256: hex.EncodeToString(sum.Sum(nil)), Path: path}, nil
}
