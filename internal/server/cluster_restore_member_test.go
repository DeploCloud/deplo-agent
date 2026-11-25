package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// c38: lab-r's backup kept on eu-1 was restored onto eu-3 through Deplo, the store file read from eu-1 and streamed on.
func TestRestoreFromMember_readsTheBackupStraightFromTheMemberDeploToldAndSaysHow(t *testing.T) {
	p := newLeftPair(t)
	if _, err := p.b.ExpectClusterBackup(context.Background(), &pb.ExpectClusterBackupRequest{
		Store: &pb.StoreTarget{ObjectKey: leftKey}, SourceFingerprint: p.fpA,
	}); err != nil {
		t.Fatal(err)
	}
	if res := p.memberBackup(t); !res.GetOk() {
		t.Fatalf("the backup ended %+v", res)
	}
	kept, err := os.ReadFile(filepath.Join(p.root, leftKey))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, path, err := p.a.openFromMember(context.Background(), p.fpB, leftKey); err == nil || path != pathRelay {
		t.Fatalf("b sent a backup nobody told it of: %v %q", err, path)
	}
	if _, err := p.b.ExpectClusterFetch(context.Background(), &pb.ExpectClusterFetchRequest{
		Store: &pb.StoreTarget{ObjectKey: leftKey}, ReaderFingerprint: p.fpA,
	}); err != nil {
		t.Fatal(err)
	}
	r, done, path, err := p.a.openFromMember(context.Background(), p.fpB, leftKey)
	if err != nil || path != pathRelay {
		t.Fatalf("open: %v %q", err, path)
	}
	got, err := io.ReadAll(r)
	done()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if sha(got) != sha(kept) {
		t.Fatalf("a read %d bytes, b keeps %d", len(got), len(kept))
	}
	if _, _, _, err := p.a.openFromMember(context.Background(), p.fpB, leftKey); err == nil {
		t.Fatal("Deplo's word was used twice")
	}
}

func TestRestoreFromMember_saysNoneWhenNoPathReachesTheMember(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	st := &fakeRestoreStream{}
	if err := s.Restore(&pb.RestoreRequest{
		Kind: pb.BackupKind_BACKUP_KIND_PROJECT, AgeIdentity: "AGE-SECRET-KEY-1X",
		Store: &pb.StoreTarget{ObjectKey: leftKey}, MemberFingerprint: strings.Repeat("ab", 32),
	}, st); err != nil {
		t.Fatal(err)
	}
	if res := st.lastResult(t); res.GetOk() || res.GetPath() != pathNone {
		t.Fatalf("the restore ended %+v", res)
	}
}

func TestExpectClusterFetch_isOnlyForAMember(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	if _, err := s.ExpectClusterFetch(context.Background(), &pb.ExpectClusterFetchRequest{
		Store: &pb.StoreTarget{ObjectKey: leftKey}, ReaderFingerprint: strings.Repeat("ab", 32),
	}); err == nil {
		t.Fatal("a server outside any cluster took a read's word")
	}
}

func sha(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
