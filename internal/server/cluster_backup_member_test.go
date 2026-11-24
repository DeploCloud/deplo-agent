package server

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"testing"

	"github.com/hashicorp/yamux"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// memberBackup is a's backup of what it holds, sent to b's store under leftKey the way Deplo asks for one.
func (p *leftPair) memberBackup(t *testing.T) *pb.BackupResult {
	t.Helper()
	req := &pb.BackupRequest{}
	if err := bundleJSON.Unmarshal(p.keep.Request, req); err != nil {
		t.Fatal(err)
	}
	req.Store.ObjectKey, req.MemberFingerprint = leftKey, p.fpB
	var res *pb.BackupResult
	p.a.backupToMember(context.Background(), req, &bkEmitter{send: func(ev *pb.BackupEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return nil
	}})
	if res == nil {
		t.Fatal("no result")
	}
	return res
}

// eu, 24 Nov 2026: a 600 MB backup from eu-3 to eu-4 went through Deplo and crossed the lab's uplink twice.
func TestBackupToMember_goesStraightToTheMemberDeploToldAndSaysHow(t *testing.T) {
	p := newLeftPair(t)
	if res := p.memberBackup(t); res.GetOk() || len(p.stored(t)) != 0 {
		t.Fatalf("b kept a backup nobody told it of: %+v %v", res, p.stored(t))
	}
	if _, err := p.b.ExpectClusterBackup(context.Background(), &pb.ExpectClusterBackupRequest{
		Store: &pb.StoreTarget{ObjectKey: leftKey}, SourceFingerprint: p.fpA,
	}); err != nil {
		t.Fatal(err)
	}
	res := p.memberBackup(t)
	if !res.GetOk() || res.GetObjectKey() != leftKey || res.GetSizeBytes() == 0 || res.GetSha256() == "" {
		t.Fatalf("the backup ended %+v", res)
	}
	// This pair meets only through a relay, and says so.
	if res.GetPath() != pathRelay {
		t.Fatalf("path %q", res.GetPath())
	}
	if got := p.stored(t); len(got) != 1 || got[0] != leftKey {
		t.Fatalf("b's store holds %v", got)
	}
	if again := p.memberBackup(t); again.GetOk() {
		t.Fatal("Deplo's word was used twice")
	}
}

func TestBackupToMember_saysNoneWhenNoPathReachesTheMember(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	var res *pb.BackupResult
	s.backupToMember(context.Background(), &pb.BackupRequest{
		MemberFingerprint: strings.Repeat("ab", 32), Store: &pb.StoreTarget{ObjectKey: leftKey},
	}, &bkEmitter{send: func(ev *pb.BackupEvent) error { res = ev.GetResult(); return nil }})
	if res.GetOk() || res.GetPath() != pathNone {
		t.Fatalf("the backup ended %+v", res)
	}
}

func TestExpectClusterBackup_isOnlyForAMember(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	_, err := s.ExpectClusterBackup(context.Background(), &pb.ExpectClusterBackupRequest{
		Store: &pb.StoreTarget{ObjectKey: leftKey}, SourceFingerprint: strings.Repeat("ab", 32),
	})
	if err == nil {
		t.Fatal("a server outside any cluster took a backup's word")
	}
}

func TestCopyPath_aStreamOverARelayIsRelayed(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if got := s.copyPath(tls.Client(a, &tls.Config{}), ""); got != pathDirect {
		t.Fatalf("a direct stream is %q", got)
	}
	c1, c2 := net.Pipe()
	server, _ := yamux.Server(c1, nil)
	client, _ := yamux.Client(c2, nil)
	defer server.Close()
	defer client.Close()
	st, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if got := s.copyPath(tls.Client(st, &tls.Config{}), ""); got != pathRelay {
		t.Fatalf("a stream through a relay is %q", got)
	}
}
