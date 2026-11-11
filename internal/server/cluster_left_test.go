package server

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testrelay"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

const (
	leftSlug = "deplo-agent-test-left"
	leftVol  = "deplo-" + leftSlug + "_data"
	leftKey  = "deplo/team_a/database/db_1/left-20260301T133205Z-srv_a.tar.gz.age"
)

// leftPair is two members of one cluster behind one relay: a held a database that moved, b's disk is where
// the team's backups go.
type leftPair struct {
	a, b     *Service
	fpA, fpB string
	root     string
	argv     func() []string
	identity string
	keep     lease.Keep
	entry    lease.Entry
}

func newLeftPair(t *testing.T) *leftPair {
	t.Helper()
	stubBridge(t, "")
	data := t.TempDir()
	mustWrite(t, filepath.Join(data, "PG_VERSION"), "16\n")
	p := &leftPair{argv: dockerOnPath(t, `case "$*" in *"tar -C /v"*) exec tar -C `+data+` -cf - . ;; esac`)}
	ca := testpki.New(t)
	mk := func() (*Service, string) {
		dir, err := os.MkdirTemp("", "ag")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		id := ca.Provision(t, dir, "member")
		base := t.TempDir()
		s := New(filepath.Join(base, "stacks"), t.TempDir(), "/", base)
		s.SetAgentDir(dir)
		t.Cleanup(func() { s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{}) })
		return s, id.Fingerprint
	}
	p.a, p.fpA = mk()
	p.b, p.fpB = mk()
	var err error
	if p.root, err = p.b.resolveStoreRoot("", true); err != nil {
		t.Fatal(err)
	}
	relay := testrelay.New(t, ca.Leaf(t, "relay"),
		wire.Party{Fingerprint: p.fpA, Role: wire.RoleMember}, wire.Party{Fingerprint: p.fpB, Role: wire.RoleMember})
	for _, s := range []*Service{p.a, p.b} {
		if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{relay.URL}}); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "both members to reach the relay", func() bool { return relay.Connected() == 2 })
	mustWrite(t, p.a.stackPath(leftSlug), "services: {db: {image: postgres}}")
	recipient, identity := testKeypair(t)
	p.identity = identity
	p.keep = lease.Keep{Slug: leftSlug, Ref: "dst_1", StoreMember: "srv_b", StoreFingerprint: p.fpB, FailedOverAt: "2026-03-01T14:32:05+01:00",
		Request: json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT","ageRecipient":"` + recipient + `",
		"store":{"objectKey":"deplo/team_a/database/db_1/left"},"project":{"slug":"` + leftSlug + `","volumeNames":["` + leftVol + `"]}}`)}
	p.entry = lease.Entry{Kind: "database", ID: "db_1", Member: "srv_b", Slug: leftSlug, SingleWriter: true}
	return p
}

// told is b's leader saying what b accepts, on a lease that is alive.
func (p *leftPair) told(stores ...lease.Store) {
	p.b.leaseGuard().Renew(lease.Order{Cluster: "clu_1", Member: "srv_b", Epoch: 1, Stores: stores})
}

func (p *leftPair) save() (*lease.Saved, error) {
	return p.a.keepLeft(context.Background(), p.entry, p.keep, "srv_a", func() bool { return true })
}

// stored is every file under b's store but its own marker.
func (p *leftPair) stored(t *testing.T) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(p.root, func(path string, d os.DirEntry, _ error) error {
		if d != nil && !d.IsDir() && d.Name() != storeSentinel {
			rel, _ := filepath.Rel(p.root, path)
			out = append(out, rel)
		}
		return nil
	})
	return out
}

func TestLeft_aMemberSendsWhatItHeldToTheMemberWhoseDiskHoldsTheBackups(t *testing.T) {
	p := newLeftPair(t)
	p.told(lease.Store{From: p.fpA, ObjectKey: leftKey})
	// A start order in between names no archive: it takes nothing back.
	p.b.leaseGuard().Renew(lease.Order{Cluster: "clu_1", Member: "srv_b", Epoch: 1, Start: &lease.Start{Slug: "web"}})
	saved, err := p.save()
	if err != nil {
		t.Fatal(err)
	}
	if got := p.stored(t); !slices.Equal(got, []string{leftKey}) {
		t.Fatalf("b's store holds %v", got)
	}
	raw, _ := os.ReadFile(filepath.Join(p.root, leftKey))
	sum := sha256.Sum256(raw)
	if saved.ObjectKey != leftKey || saved.Ref != "dst_1" || saved.SizeBytes != int64(len(raw)) || saved.Sha256 != hex.EncodeToString(sum[:]) || saved.DecryptedSizeBytes == 0 {
		t.Fatalf("saved %+v, %d bytes on b's disk", saved, len(raw))
	}
	// b holds ciphertext only: the age wrap happened on a.
	if bytes.Contains(raw, []byte("PG_VERSION")) || !bytes.HasPrefix(raw, []byte("age-encryption.org/v1\n")) {
		t.Fatal("b's disk holds the archive in the clear")
	}
	rd, err := openArtifactReader(bytes.NewReader(raw), p.identity)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for tr := tar.NewReader(rd); ; {
		h, err := tr.Next()
		if err != nil {
			break
		}
		names = append(names, h.Name)
	}
	if !slices.Contains(names, "volumes/"+leftVol+"/PG_VERSION") {
		t.Fatalf("the archive holds %v", names)
	}
	// Nothing went before the archive was confirmed, and a's own disk keeps no copy of it.
	if all := strings.Join(p.argv(), "\n"); strings.Contains(all, "down") || strings.Contains(all, "volume rm") {
		t.Fatalf("a removed something while saving: %q", all)
	}
	if _, err := os.Stat(filepath.Join(p.a.managedStoreRoot(), leftKey)); err == nil {
		t.Fatal("a wrote the archive to its own disk")
	}
}

func TestLeft_aSecondTryWritesOverTheFirst(t *testing.T) {
	p := newLeftPair(t)
	p.told(lease.Store{From: p.fpA, ObjectKey: leftKey})
	first, err := p.save()
	if err != nil {
		t.Fatal(err)
	}
	// The agent stopped before it wrote the archive down: the next pass saves again.
	second, err := p.save()
	if err != nil {
		t.Fatal(err)
	}
	if first.ObjectKey != second.ObjectKey {
		t.Fatalf("two names for one failover: %q then %q", first.ObjectKey, second.ObjectKey)
	}
	if got := p.stored(t); !slices.Equal(got, []string{leftKey}) {
		t.Fatalf("b's store holds %v", got)
	}
}

func TestLeft_isRefusedUnlessThisMembersLeaderNamedThatSenderAndThatFile(t *testing.T) {
	other := strings.Repeat("ab", 32)
	for name, tell := range map[string]func(p *leftPair){
		"a leader that said nothing": func(p *leftPair) {},
		"another member's file":      func(p *leftPair) { p.told(lease.Store{From: other, ObjectKey: leftKey}) },
		"another workload's file": func(p *leftPair) {
			p.told(lease.Store{From: p.fpA, ObjectKey: "deplo/team_b/database/db_9/left-20260301T133205Z-srv_a.tar.gz.age"})
		},
		"a word since taken back":        func(p *leftPair) { p.told(lease.Store{From: p.fpA, ObjectKey: leftKey}); p.told() },
		"a member that left the cluster": func(p *leftPair) { p.told(lease.Store{From: p.fpA, ObjectKey: leftKey}); p.b.leaseGuard().Forget() },
		"a lease that ran out": func(p *leftPair) {
			p.told(lease.Store{From: p.fpA, ObjectKey: leftKey})
			g := p.b.leaseGuard()
			g.mu.Lock()
			g.now = func() time.Time { return time.Now().Add(lease.FenceAfter + time.Second) }
			g.mu.Unlock()
		},
	} {
		t.Run(name, func(t *testing.T) {
			stubTries(t)
			p := newLeftPair(t)
			tell(p)
			saved, err := p.save()
			if err == nil || saved != nil {
				t.Fatalf("saved %+v", saved)
			}
			if got := p.stored(t); len(got) != 0 {
				t.Fatalf("b's store holds %v", got)
			}
			if all := strings.Join(p.argv(), "\n"); strings.Contains(all, "tar -C /v") {
				t.Fatalf("a read the volume for a member that refused it: %q", all)
			}
		})
	}
}

func stubTries(t *testing.T) {
	t.Helper()
	tries, wait := leftTries, leftWait
	leftTries, leftWait = 1, time.Millisecond
	t.Cleanup(func() { leftTries, leftWait = tries, wait })
}

func TestLeft_aMemberNeverSendsToAServerItsLeaderGaveNoCertificateFor(t *testing.T) {
	stubTries(t)
	p := newLeftPair(t)
	p.told(lease.Store{From: p.fpA, ObjectKey: leftKey})
	// The store is on a server of another cluster: the leader names it and hands no way to reach it.
	p.keep.StoreMember, p.keep.StoreFingerprint = "srv_z", ""
	if saved, err := p.save(); err == nil || saved != nil {
		t.Fatalf("saved %+v", saved)
	}
	if got := p.stored(t); len(got) != 0 {
		t.Fatalf("b's store holds %v", got)
	}
	if _, err := os.Stat(filepath.Join(p.a.managedStoreRoot(), leftKey)); err == nil {
		t.Fatal("a wrote another server's backup to its own disk")
	}
}

func TestLeft_anArchiveCutShortOrSentInTheClearIsNotKept(t *testing.T) {
	for name, write := range map[string]func(out io.Writer) (*pb.BackupResult, error){
		"cut short": func(out io.Writer) (*pb.BackupResult, error) {
			out.Write([]byte("age-encryption.org/v1\nhalf of it"))
			return nil, errors.New("the volume could not be read")
		},
		"in the clear": func(out io.Writer) (*pb.BackupResult, error) {
			body := []byte("PG_VERSION 16, readable by whoever holds the disk")
			out.Write(body)
			sum := sha256.Sum256(body)
			return &pb.BackupResult{Ok: true, SizeBytes: int64(len(body)), Sha256: hex.EncodeToString(sum[:])}, nil
		},
		"not what was sent": func(out io.Writer) (*pb.BackupResult, error) {
			body := []byte("age-encryption.org/v1\nciphertext")
			out.Write(body)
			return &pb.BackupResult{Ok: true, SizeBytes: int64(len(body)), Sha256: strings.Repeat("0", 64)}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := newLeftPair(t)
			p.told(lease.Store{From: p.fpA, ObjectKey: leftKey})
			stubLeft(t, func(_ *pb.BackupRequest, out io.Writer) (*pb.BackupResult, error) { return write(out) })
			if saved, err := p.save(); err == nil || saved != nil {
				t.Fatalf("saved %+v", saved)
			}
			// b drops what it had received once it sees the stream end.
			eventually(t, "b's store to hold nothing", func() bool { return len(p.stored(t)) == 0 })
		})
	}
}
