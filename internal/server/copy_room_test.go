package server

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/room"
)

// fullDisk makes every disk the agent looks at hold free of total bytes, each write taking its size from free.
func fullDisk(t *testing.T, free, total int64) *int64 {
	t.Helper()
	stat, file := room.Stat, room.StatFile
	room.Stat = func(string) (int64, int64, bool) { return free, total, true }
	room.StatFile = func(f *os.File) (int64, int64, bool) {
		info, _ := f.Stat()
		return free - info.Size(), total, true
	}
	t.Cleanup(func() { room.Stat, room.StatFile = stat, file })
	return &free
}

// eu, 24 Nov 2026: a set-aside copy saved on eu-2 took it to 8.4% free; a growing backup stops at a tenth.
func TestWriteStoreFile_aBackupStopsAtATenthFreeAndLeavesNothing(t *testing.T) {
	fullDisk(t, 1<<30+16<<20, 10<<30)
	s, root := newStoreService(t)
	target := &pb.StoreTarget{Root: root, ObjectKey: "prj_1/run.tar.gz.age"}
	ws := &fakeWriteStoreStream{msgs: framedStoreChunks(target, make([]byte, 64<<20))}
	if err := s.WriteStoreFile(ws); err != nil {
		t.Fatal(err)
	}
	if ws.result.GetOk() || !strings.Contains(ws.result.GetError(), room.Marker) {
		t.Fatalf("a backup past a tenth free: ok=%v %q", ws.result.GetOk(), ws.result.GetError())
	}
	for _, p := range []string{"", storePartialSuffix} {
		if _, err := os.Stat(filepath.Join(root, "prj_1/run.tar.gz.age"+p)); err == nil {
			t.Fatalf("%q stayed on the disk", "run.tar.gz.age"+p)
		}
	}
}

// A destination power-cut mid-send keeps its partial file; once back, it is gone before the next copy lands.
func TestWriteStoreFile_aPartialFromARunThatEndedGoesFirst(t *testing.T) {
	s, root := newStoreService(t)
	dead := filepath.Join(root, "prj_1", "cut.tar.gz.age"+storePartialSuffix)
	mine := filepath.Join(root, "prj_1", "now.tar.gz.age"+storePartialSuffix)
	for _, p := range []string{dead, mine} {
		mustWrite(t, p, "half")
	}
	was := agentStarted
	agentStarted = time.Now()
	t.Cleanup(func() { agentStarted = was })
	_ = os.Chtimes(dead, agentStarted.Add(-time.Minute), agentStarted.Add(-time.Minute))
	_ = os.Chtimes(mine, agentStarted.Add(time.Second), agentStarted.Add(time.Second))
	target := &pb.StoreTarget{Root: root, ObjectKey: "prj_1/next.tar.gz.age"}
	ws := &fakeWriteStoreStream{msgs: framedStoreChunks(target, []byte("a backup"))}
	if err := s.WriteStoreFile(ws); err != nil || !ws.result.GetOk() {
		t.Fatalf("%v %q", err, ws.result.GetError())
	}
	if _, err := os.Stat(dead); err == nil {
		t.Fatal("the partial file of the run that ended stayed")
	}
	if _, err := os.Stat(mine); err != nil {
		t.Fatal("a partial file this run still writes was removed")
	}
}

// The member whose disk holds the backups says its room; a set-aside copy that does not fit is never read.
func TestLeft_aCopyThatDoesNotFitIsKeptWhereItIs(t *testing.T) {
	p := newLeftPair(t)
	p.told(lease.Store{From: p.fpA, ObjectKey: leftKey})
	fullDisk(t, 3<<30, 10<<30)
	was := leftVolumeBytes
	leftVolumeBytes = func(context.Context, string) int64 { return 2<<30 + 1 }
	t.Cleanup(func() { leftVolumeBytes = was })
	_, err := p.save()
	if !errors.Is(err, room.ErrNoRoom) || !strings.Contains(err.Error(), "the server that holds the backups has 3.0 GB free, too little for this copy (about 2.0 GB)") {
		t.Fatalf("saved with no room: %v", err)
	}
	eventually(t, "b to keep nothing of it", func() bool { return len(p.stored(t)) == 0 })
	if all := strings.Join(p.argv(), "\n"); strings.Contains(all, "tar -C /v") {
		t.Fatal("a read the volume though b had no room for it")
	}
	leftVolumeBytes = func(context.Context, string) int64 { return 2 << 30 }
	if _, err := p.save(); err != nil {
		t.Fatalf("a copy that fits: %v", err)
	}
}

func TestStageSync_aCopyOnTheAppsDiskStopsAtATenthFree(t *testing.T) {
	fullDisk(t, 1<<30+4<<20, 10<<30)
	src, dst := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(src, "big"), strings.Repeat("x", 32<<20))
	for _, gentle := range []bool{false, true} {
		err := copyRegular(filepath.Join(src, "big"), filepath.Join(dst, "big"), gentle)
		if !errors.Is(err, room.ErrNoRoom) {
			t.Fatalf("gentle=%v: %v", gentle, err)
		}
	}
}

func TestSetAside_aCopyThatDoesNotFitIsRefusedAndItsVolumeGoes(t *testing.T) {
	from, to := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(from, "data"), strings.Repeat("x", 4096))
	fullDisk(t, 1<<30+1024, 10<<30)
	var calls []string
	run := func(_ context.Context, _ time.Duration, args ...string) (dockercli.Result, error) {
		calls = append(calls, strings.Join(args, " "))
		switch {
		case args[0] == "volume" && args[1] == "inspect" && len(args) > 2 && args[2] == "--format":
			if strings.Contains(args[len(args)-1], "aside") {
				return dockercli.Result{Stdout: to}, nil
			}
			return dockercli.Result{Stdout: from}, nil
		case args[0] == "volume" && args[1] == "inspect":
			return dockercli.Result{Code: 1}, nil
		}
		return dockercli.Result{}, nil
	}
	_, err := setAside(context.Background(), run, "web", []string{"deplo-web_data"}, "", time.Now())
	if !errors.Is(err, room.ErrNoRoom) {
		t.Fatalf("set aside with no room: %v", err)
	}
	all := strings.Join(calls, "\n")
	if strings.Contains(all, "cp -a") || !strings.Contains(all, "volume rm deplo-web_data-aside-") {
		t.Fatalf("docker was asked:\n%s", all)
	}
}

// A move or a set-aside copy from another server arrives through ImportVolume: it stops at a tenth free.
func TestImportVolume_aCopyStopsAtATenthFree(t *testing.T) {
	mount := t.TempDir()
	argv := dockerOnPath(t, `case "$*" in *"volume inspect"*) echo `+mount+` ;; *"tar -C /v -xf -"*) cat > /dev/null ;; esac`)
	fullDisk(t, 1<<30-1, 10<<30)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	_ = tw.WriteHeader(&tar.Header{Name: "f", Mode: 0o644, Size: 4})
	_, _ = tw.Write([]byte("data"))
	tw.Close()
	gz.Close()
	st := &fakeImportStream{in: []*pb.VolumeChunk{
		{Frame: &pb.VolumeChunk_Header_{Header: &pb.VolumeChunk_Header{VolumeName: "deplo-web_data", WipeFirst: true}}},
		{Frame: &pb.VolumeChunk_Data{Data: buf.Bytes()}},
	}}
	if err := (&Service{}).ImportVolume(st); err != nil {
		t.Fatal(err)
	}
	if st.result.GetOk() || !strings.Contains(st.result.GetError(), room.Marker) {
		t.Fatalf("a copy into a full disk: ok=%v %q\n%s", st.result.GetOk(), st.result.GetError(), strings.Join(argv(), "\n"))
	}
}
