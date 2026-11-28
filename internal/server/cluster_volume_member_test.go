package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// eu, 24 Nov 2026: a move's 600 MB went from eu-4 to eu-3 through Deplo, across the lab's internet line twice.
func TestSendClusterVolume_movesAVolumeMemberToMemberOnlyWhenDeploSaidSo(t *testing.T) {
	p := newLeftPair(t)
	src, dst := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(src, "data.txt"), "moved")
	dockerOnPath(t, `case "$*" in
  *"-cf -"*) exec tar -C `+src+` -cf - . ;;
  *"-xf -"*) exec tar -C `+dst+` -xf - ;;
esac`)
	send := func() (*pb.SendClusterVolumeResponse, error) {
		return p.a.SendClusterVolume(context.Background(), &pb.SendClusterVolumeRequest{VolumeName: leftVol, TargetFingerprint: p.fpB})
	}
	if _, err := send(); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("b took a volume nobody told it of: %v", err)
	}
	if _, err := p.b.ExpectClusterVolume(context.Background(), &pb.ExpectClusterVolumeRequest{VolumeName: leftVol, SourceFingerprint: p.fpA}); err != nil {
		t.Fatal(err)
	}
	res, err := send()
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "data.txt")); string(got) != "moved" {
		t.Fatalf("b holds %q", got)
	}
	// This pair meets only through a relay, and says so.
	if res.GetPath() != pathRelay || res.GetBytes() == 0 || len(res.GetSha256()) != 64 {
		t.Fatalf("the copy ended %+v", res)
	}
	if _, err := send(); err == nil {
		t.Fatal("Deplo's word was used twice")
	}
}

func TestSendClusterVolume_saysNoneWhenNoPathReachesTheMember(t *testing.T) {
	dockerOnPath(t, "exit 0")
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	res, err := s.SendClusterVolume(context.Background(), &pb.SendClusterVolumeRequest{VolumeName: leftVol, TargetFingerprint: strings.Repeat("ab", 32)})
	if err != nil || res.GetPath() != pathNone {
		t.Fatalf("got %+v, %v", res, err)
	}
}
