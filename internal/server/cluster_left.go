package server

import (
	"bytes"
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

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// What a member had left of a workload goes to the member whose disk holds the team's backups, over the relay
// and mutual TLS end to end (ADR-0044). Stream: "LEFT <key>", answered SEND or ERR, then the encrypted archive
// in frames (4-byte length, an empty one ends it), "SUM <bytes> <sha256>", answered OK or ERR.
const (
	leftVerb = "LEFT"
	leftSend = "SEND"
	leftSum  = "SUM"
)

// ageMagic opens every age file: the receiving member keeps nothing else, so it never holds an archive in the clear.
var ageMagic = []byte("age-encryption.org/v1\n")

// accepts says whether a member may send this one that file: only what the leader of a cluster this agent
// holds a live lease of named, for that sender's certificate.
func (g *leaseGuard) accepts(from, key string) (lease.Store, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	for id, c := range g.clusters {
		if c == nil || c.ranOut(now) {
			continue
		}
		for _, st := range g.stores[id] {
			if key != "" && st.ObjectKey == key && st.From == from {
				return st, true
			}
		}
	}
	return lease.Store{}, false
}

type frameWriter struct{ w io.Writer }

func (f frameWriter) Write(p []byte) (int, error) {
	for done := 0; done < len(p); {
		n := min(len(p)-done, storeChunkBytes)
		var head [4]byte
		binary.BigEndian.PutUint32(head[:], uint32(n))
		if _, err := f.w.Write(append(head[:], p[done:done+n]...)); err != nil {
			return done, err
		}
		done += n
	}
	return len(p), nil
}

// leftReader reads the frames of one archive and ends cleanly only when what arrived is an age file of the
// size and digest its sender says it wrote: anything else fails the write, and nothing is kept.
type leftReader struct {
	c    io.Reader
	left uint32
	n    int64
	sum  hash.Hash
	head []byte
}

func (r *leftReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		var head [4]byte
		if _, err := io.ReadFull(r.c, head[:]); err != nil {
			return 0, errors.New("the archive was cut short")
		}
		if r.left = binary.BigEndian.Uint32(head[:]); r.left > storeChunkBytes {
			return 0, errors.New("the archive is not framed as expected")
		}
		if r.left == 0 {
			return 0, r.end()
		}
	}
	n, err := r.c.Read(p[:min(len(p), int(r.left))])
	r.left -= uint32(n)
	r.n += int64(n)
	r.sum.Write(p[:n])
	if len(r.head) < len(ageMagic) {
		r.head = append(r.head, p[:min(n, len(ageMagic)-len(r.head))]...)
		if !bytes.HasPrefix(ageMagic, r.head) {
			return 0, errors.New("the archive is not encrypted")
		}
	}
	if err == io.EOF {
		err = errors.New("the archive was cut short")
	}
	return n, err
}

func (r *leftReader) end() error {
	verb, arg, err := wire.ReadHeader(r.c)
	if err != nil || verb != leftSum {
		return errors.New("the archive was cut short")
	}
	if len(r.head) < len(ageMagic) {
		return errors.New("the archive is not encrypted")
	}
	if arg != fmt.Sprintf("%d %s", r.n, hex.EncodeToString(r.sum.Sum(nil))) {
		return errors.New("what arrived is not what was sent")
	}
	return io.EOF
}

// receiveLeft keeps an archive another member sends, in this host's backup store.
func (s *Service) receiveLeft(from wire.Party, c net.Conn, key string) {
	st, ok := s.leaseGuard().accepts(from.Fingerprint, key)
	if !ok {
		_ = wire.WriteHeader(c, wire.VerbErr, "this server was not told to keep that")
		return
	}
	root, err := s.resolveStoreRoot(st.Root, false)
	if err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, oneLine(statusMessage(err)))
		return
	}
	_ = c.SetDeadline(time.Now().Add(leftTimeout))
	if wire.WriteHeader(c, leftSend, "") != nil {
		return
	}
	n, sum, err := storeWrite(root, key, &leftReader{c: c, sum: sha256.New()}, true)
	if err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, oneLine(err.Error()))
		return
	}
	_ = wire.WriteHeader(c, wire.VerbOK, fmt.Sprintf("%d %s", n, sum))
}

// sendLeft streams one archive to the member whose disk its destination is on. It is saved only once
// that member says it holds the very bytes this one produced.
func (s *Service) sendLeft(ctx context.Context, req *pb.BackupRequest, to string) (*pb.BackupResult, error) {
	target := wire.Party{Fingerprint: strings.ToLower(to), Role: wire.RoleMember}
	m := s.clusterMember()
	m.mu.Lock()
	party := m.party
	m.mu.Unlock()
	if party == nil || !target.Valid() {
		return nil, errors.New("this server cannot reach the server that holds the backups")
	}
	conn, err := party.Dial(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("reach the server that holds the backups: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(leftTimeout))
	if err := wire.WriteHeader(conn, leftVerb, req.GetStore().GetObjectKey()); err != nil {
		return nil, fmt.Errorf("reach the server that holds the backups: %w", err)
	}
	// Asked before the volume is read: a member that was not told to keep it gets no byte of it.
	verb, arg, err := wire.ReadHeader(conn)
	if err != nil {
		return nil, fmt.Errorf("the server that holds the backups did not answer: %w", err)
	}
	if verb != leftSend {
		return nil, fmt.Errorf("the server that holds the backups refused it: %s", arg)
	}
	req.StreamOut = true
	res, err := runLeftBackup(s, ctx, req, frameWriter{conn})
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte{0, 0, 0, 0}); err != nil {
		return nil, fmt.Errorf("send the archive: %w", err)
	}
	want := fmt.Sprintf("%d %s", res.GetSizeBytes(), res.GetSha256())
	if err := wire.WriteHeader(conn, leftSum, want); err != nil {
		return nil, fmt.Errorf("send the archive: %w", err)
	}
	verb, arg, err = wire.ReadHeader(conn)
	switch {
	case err != nil:
		return nil, fmt.Errorf("the server that holds the backups did not confirm it: %w", err)
	case verb != wire.VerbOK:
		return nil, fmt.Errorf("the server that holds the backups could not keep it: %s", arg)
	case arg != want:
		return nil, errors.New("the server that holds the backups stored something else than what was sent")
	}
	return res, nil
}
