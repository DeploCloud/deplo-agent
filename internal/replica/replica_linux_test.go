//go:build linux

package replica

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/DeploCloud/deplo-agent/internal/room"
	"golang.org/x/sys/unix"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/pace"
)

func init() {
	settleFor, settleMax = 10*time.Millisecond, 50*time.Millisecond
	BeatEvery = 50 * time.Millisecond
}

// pair runs a sender over src and a receiver into a fresh copy dir, over a real socket, until stop.
func pair(t *testing.T, src string) (copyDir string, st *Status, stop func()) {
	t.Helper()
	copyDir = t.TempDir()
	st, _, stop = stream(t, src, copyDir, NewIndex())
	return copyDir, st, stop
}

// stream runs one sender over src and one receiver into dir; got says how the receiver ended.
func stream(t *testing.T, src, dir string, ix *Index) (st *Status, got <-chan error, stop func()) {
	t.Helper()
	st = &Status{}
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			sent <- err
			return
		}
		defer c.Close()
		sent <- Send(ctx, c, []string{VolumeRoot("data")}, []string{src})
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	recv := make(chan error, 1)
	ended := make(chan error, 1)
	go func() {
		err := Receive(ctx, c, dir, []string{VolumeRoot("data")}, st, ix)
		ended <- err
		recv <- err
	}()
	var once sync.Once
	return st, ended, func() {
		once.Do(func() {
			cancel()
			c.Close()
			ln.Close()
			<-recv
			<-sent
		})
	}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("never: %s", what)
}

func read(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}

func TestReplica_snapshotThenEveryChangeFollows(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "a/b"), 0o755)
	os.WriteFile(filepath.Join(src, "a/b/one.txt"), []byte("one"), 0o640)
	os.WriteFile(filepath.Join(src, "gone.txt"), []byte("x"), 0o600)
	dir, st, stop := pair(t, src)
	defer stop()
	data := filepath.Join(dir, "volume-data")
	eventually(t, "the snapshot lands", func() bool { _, synced, _, _, _ := st.Snapshot(); return synced })
	if read(filepath.Join(data, "a/b/one.txt")) != "one" || !Ready(dir) {
		t.Fatal("the snapshot is not in the copy")
	}
	if fi, _ := os.Stat(filepath.Join(data, "a/b/one.txt")); fi.Mode().Perm() != 0o640 {
		t.Fatalf("mode %v", fi.Mode().Perm())
	}

	os.WriteFile(filepath.Join(src, "a/b/one.txt"), []byte("changed"), 0o640)
	os.MkdirAll(filepath.Join(src, "new/deep"), 0o755)
	os.WriteFile(filepath.Join(src, "new/deep/two.txt"), []byte("two"), 0o644)
	os.Remove(filepath.Join(src, "gone.txt"))
	os.Rename(filepath.Join(src, "a/b"), filepath.Join(src, "a/c"))
	eventually(t, "the changes land", func() bool {
		_, err := os.Stat(filepath.Join(data, "gone.txt"))
		_, old := os.Stat(filepath.Join(data, "a/b"))
		return read(filepath.Join(data, "a/c/one.txt")) == "changed" &&
			read(filepath.Join(data, "new/deep/two.txt")) == "two" && os.IsNotExist(err) && os.IsNotExist(old)
	})
}

func TestReplica_aReconnectSendsOnlyWhatChangedAndRemovesWhatWentMeanwhile(t *testing.T) {
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "big.bin"), []byte(strings.Repeat("z", 1<<20)), 0o644)
	os.WriteFile(filepath.Join(src, "stays.txt"), []byte("s"), 0o644)
	dir, st, stop := pair(t, src)
	eventually(t, "first snapshot", func() bool { _, s, _, _, _ := st.Snapshot(); return s })
	stop()
	os.Remove(filepath.Join(src, "stays.txt"))

	st2 := &Status{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := net.Pipe()
	go func() { _ = Send(ctx, a, []string{VolumeRoot("data")}, []string{src}) }()
	go func() { _ = Receive(ctx, b, dir, []string{VolumeRoot("data")}, st2, nil) }()
	eventually(t, "second snapshot", func() bool { _, s, _, _, _ := st2.Snapshot(); return s })
	if _, _, _, bytes, _ := st2.Snapshot(); bytes != 0 {
		t.Fatalf("an unchanged file travelled again: %d bytes", bytes)
	}
	if _, err := os.Stat(filepath.Join(dir, "volume-data", "stays.txt")); !os.IsNotExist(err) {
		t.Fatal("a file removed while apart is still in the copy")
	}
}

func TestReplica_aSymlinkTravelsAsOneAndNothingIsWrittenThroughIt(t *testing.T) {
	src := t.TempDir()
	outside := t.TempDir()
	os.Symlink(outside, filepath.Join(src, "escape"))
	dir, st, stop := pair(t, src)
	defer stop()
	eventually(t, "snapshot", func() bool { _, s, _, _, _ := st.Snapshot(); return s })
	target, err := os.Readlink(filepath.Join(dir, "volume-data", "escape"))
	if err != nil || target != outside {
		t.Fatalf("the link is %q, %v", target, err)
	}
	// The receiver refuses a path below a link, whatever a sender says.
	r, _ := openRoot(filepath.Join(dir, "volume-data"))
	defer r.close()
	if err := r.put("escape/planted", Entry{Kind: kindFile, Mode: 0o644, Size: 1}, strings.NewReader("x")); err == nil {
		t.Fatal("a file was written through a link")
	}
	if _, err := os.Stat(filepath.Join(outside, "planted")); !os.IsNotExist(err) {
		t.Fatal("a file landed outside the copy")
	}
}

// eu, 24 Nov 2026: a 2 GB App's copy was copied into place at its failover, and that copy starved every server on
// the lab's shared disk until their leases ran out. On the same disk a promoted copy is moved, at once, and a copy
// moved half into place can never be promoted again.
func TestReplica_aPromotedCopyIsMovedNotCopied(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "pg"), 0o700)
	os.WriteFile(filepath.Join(src, "pg", "base.dat"), noise(1<<20, 7), 0o600)
	dir, st, stop := pair(t, src)
	eventually(t, "snapshot", func() bool { _, s, _, _, _ := st.Snapshot(); return s })
	stop()
	held := filepath.Join(dir, "volume-data", "pg", "base.dat")
	var before, after unix.Stat_t
	if err := unix.Stat(held, &before); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "volume-data", "pg", ".deplo-replica-1-1"), []byte("half"), 0o600)
	live := t.TempDir()
	if err := Promote(dir, []string{VolumeRoot("data")}, map[string]string{VolumeRoot("data"): live}); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(filepath.Join(live, "pg", "base.dat"), &after); err != nil || after.Ino != before.Ino {
		t.Fatalf("the copy was not moved into place (inode %d, was %d): %v", after.Ino, before.Ino, err)
	}
	if _, err := os.Stat(filepath.Join(live, "pg", ".deplo-replica-1-1")); !os.IsNotExist(err) {
		t.Fatal("a half-written file of the copy became data")
	}
	if Ready(dir) {
		t.Fatal("a copy used up by a promote can be promoted again")
	}
}

func TestReplica_aPathThatClimbsOutIsRefused(t *testing.T) {
	for _, p := range []string{"../x", "/etc/passwd", "a/../../x", "", "a//b", "./a"} {
		if _, err := cleanRel(p); err == nil {
			t.Errorf("%q was taken", p)
		}
	}
}

func TestReplica_promoteReplacesLiveDataWithTheCopy(t *testing.T) {
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "data.txt"), []byte("replicated"), 0o600)
	dir, st, stop := pair(t, src)
	eventually(t, "snapshot", func() bool { _, s, _, _, _ := st.Snapshot(); return s })
	stop()
	live := t.TempDir()
	os.WriteFile(filepath.Join(live, "stale.txt"), []byte("old"), 0o600)
	if err := Promote(dir, []string{VolumeRoot("data")}, map[string]string{VolumeRoot("data"): live}); err != nil {
		t.Fatal(err)
	}
	if read(filepath.Join(live, "data.txt")) != "replicated" {
		t.Fatal("the copy is not in place")
	}
	if _, err := os.Stat(filepath.Join(live, "stale.txt")); !os.IsNotExist(err) {
		t.Fatal("what the live volume held before is still there")
	}
}

func TestReplica_aCopyThatNeverHeldASnapshotIsNeverPromoted(t *testing.T) {
	if err := Promote(t.TempDir(), []string{FilesRoot}, map[string]string{FilesRoot: t.TempDir()}); err == nil {
		t.Fatal("an empty copy was promoted")
	}
}

func noise(n int, seed uint64) []byte {
	b := make([]byte, n)
	r := rand.NewChaCha8([32]byte{byte(seed)})
	r.Read(b)
	return b
}

// putWhole puts a whole file in at once, as an App that writes beside and renames does: a sender never reads half.
func putWhole(t *testing.T, p string, b []byte) {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "next")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, p); err != nil {
		t.Fatal(err)
	}
}

func sent(st *Status) int64 { _, _, _, b, _ := st.Snapshot(); return b }

func synced(st *Status) func() bool {
	return func() bool { _, s, _, _, _ := st.Snapshot(); return s }
}

// Each change to a big file travels as the chunk or two around it, wherever in the file it is.
func TestReplica_aChangedFileSendsOnlyTheBytesAroundTheChange(t *testing.T) {
	src := t.TempDir()
	big := filepath.Join(src, "big.bin")
	data := noise(8<<20, 1)
	os.WriteFile(big, data, 0o644)
	dir, st, stop := pair(t, src)
	defer stop()
	eventually(t, "snapshot", synced(st))
	if b := sent(st); b != int64(len(data)) {
		t.Fatalf("the first copy sent %d bytes, not the file's %d", b, len(data))
	}
	cp := filepath.Join(dir, "volume-data", "big.bin")
	// A database writes in place: the middle of its file changes, nothing else does.
	before := sent(st)
	f, _ := os.OpenFile(big, os.O_WRONLY, 0)
	f.WriteAt([]byte("edited in place"), 4<<20)
	f.Close()
	copy(data[4<<20:], "edited in place")
	eventually(t, "an edit in place lands", func() bool { b, _ := os.ReadFile(cp); return bytes.Equal(b, data) })
	d := sent(st) - before
	t.Logf("an edit in place: %d bytes sent of %d", d, len(data))
	if d > 2*maxChunk {
		t.Errorf("an edit in place sent %d bytes of a %d-byte file", d, len(data))
	}
	for _, c := range []struct {
		what   string
		change func([]byte) []byte
	}{
		{"bytes inserted at the start", func(b []byte) []byte { return append([]byte("a new header "), b...) }},
		{"bytes appended", func(b []byte) []byte { return append(b, noise(3000, 2)...) }},
		{"bytes cut from the middle", func(b []byte) []byte { return append(b[:2<<20:2<<20], b[2<<20+5000:]...) }},
	} {
		before := sent(st)
		data = c.change(bytes.Clone(data))
		putWhole(t, big, data)
		eventually(t, c.what+" lands", func() bool { b, _ := os.ReadFile(cp); return bytes.Equal(b, data) })
		d := sent(st) - before
		t.Logf("%s: %d bytes sent of %d", c.what, d, len(data))
		if d > 3*maxChunk {
			t.Errorf("%s sent %d bytes of a %d-byte file", c.what, d, len(data))
		}
	}
}

// A file that changed while two members were apart travels as its changed chunks too, whether the copy's
// index was kept or has to be read again.
func TestReplica_aReconnectSendsOnlyTheChangedChunks(t *testing.T) {
	for _, keep := range []bool{true, false} {
		src := t.TempDir()
		big := filepath.Join(src, "big.bin")
		data := noise(4<<20, 3)
		os.WriteFile(big, data, 0o644)
		dir := t.TempDir()
		ix := NewIndex()
		st, _, stop := stream(t, src, dir, ix)
		eventually(t, "first snapshot", synced(st))
		stop()
		copy(data[1<<20:], "changed while apart")
		os.WriteFile(big, data, 0o644)
		if !keep {
			ix = nil
		}
		st2, _, stop2 := stream(t, src, dir, ix)
		eventually(t, "second snapshot", synced(st2))
		stop2()
		if got := read(filepath.Join(dir, "volume-data", "big.bin")); got != string(data) {
			t.Fatalf("index kept %v: the copy is not the file", keep)
		}
		t.Logf("index kept %v: %d bytes sent of %d", keep, sent(st2), len(data))
		if b := sent(st2); b == 0 || b > 2*maxChunk {
			t.Fatalf("index kept %v: the reconnect sent %d bytes", keep, b)
		}
	}
}

// A chunk that moved from one file to another is copied there, not sent again.
func TestReplica_aChunkHeldByAnotherFileIsCopiedFromIt(t *testing.T) {
	src := t.TempDir()
	data := noise(2<<20, 4)
	os.WriteFile(filepath.Join(src, "one.bin"), data, 0o644)
	dir, st, stop := pair(t, src)
	defer stop()
	eventually(t, "snapshot", synced(st))
	before := sent(st)
	putWhole(t, filepath.Join(src, "two.bin"), data)
	eventually(t, "the second file lands", func() bool { return read(filepath.Join(dir, "volume-data", "two.bin")) == string(data) })
	if d := sent(st) - before; d != 0 {
		t.Fatalf("a file the copy held whole under another name sent %d bytes", d)
	}
}

// A copy that holds other bytes than its index says is caught by the file's SHA-256, read again, and repaired.
func TestReplica_aCopyThatIsNotWhatItsIndexSaysIsReadAgainAndRepaired(t *testing.T) {
	src := t.TempDir()
	big := filepath.Join(src, "big.bin")
	data := noise(2<<20, 5)
	os.WriteFile(big, data, 0o644)
	dir := t.TempDir()
	ix := NewIndex()
	st, _, stop := stream(t, src, dir, ix)
	eventually(t, "snapshot", synced(st))
	stop()
	cp := filepath.Join(dir, "volume-data", "big.bin")
	fi, _ := os.Stat(cp)
	f, _ := os.OpenFile(cp, os.O_WRONLY, 0)
	f.WriteAt([]byte("damaged on disk"), 1<<20)
	f.Close()
	os.Chtimes(cp, fi.ModTime(), fi.ModTime())
	copy(data[100:], "the App wrote here")
	os.WriteFile(big, data, 0o644)

	_, ended, stop2 := stream(t, src, dir, ix)
	select {
	case err := <-ended:
		if err == nil || !strings.Contains(err.Error(), "not the sender's") {
			t.Fatalf("the damaged copy ended the stream with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a file rebuilt from damaged bytes was taken")
	}
	stop2()
	if read(cp) == string(data) {
		t.Fatal("the damaged rebuild went in place")
	}
	st3, _, stop3 := stream(t, src, dir, ix)
	defer stop3()
	eventually(t, "the copy is repaired", func() bool { _, s, _, _, _ := st3.Snapshot(); return s && read(cp) == string(data) })
}

// eu, 24 Nov 2026: a 2 GB file rebuilt here at page-cache speed stalled its whole server for 3 s, a renewal with it.
func TestReplica_aRebuiltFileIsWrittenInStepsItWaitsOn(t *testing.T) {
	var mu sync.Mutex
	var steps []int64
	was := flushRange
	flushRange = func(f *os.File, off, n int64) {
		mu.Lock()
		steps = append(steps, n)
		mu.Unlock()
		was(f, off, n)
	}
	t.Cleanup(func() { flushRange = was })
	src := t.TempDir()
	data := noise(3*gentleStep+5, 4)
	os.WriteFile(filepath.Join(src, "big.bin"), data, 0o644)
	dir, st, stop := pair(t, src)
	defer stop()
	eventually(t, "snapshot", synced(st))
	if b, _ := os.ReadFile(filepath.Join(dir, "volume-data", "big.bin")); !bytes.Equal(b, data) {
		t.Fatal("the copy is not the file")
	}
	mu.Lock()
	defer mu.Unlock()
	var total int64
	for _, n := range steps {
		if n > gentleStep+maxChunk+256<<10 {
			t.Fatalf("a step of %d bytes went to the disk unwaited", n)
		}
		total += n
	}
	if total != int64(len(data)) || len(steps) < 3 {
		t.Fatalf("%d bytes in %d steps, for a file of %d", total, len(steps), len(data))
	}
}

// eu, 24 Nov 2026: three copies read a 2.2 GB file through the page cache of a 2 GB server, every task stalled on
// reclaim and the sender's lease ran out. What a copy reads goes from memory as it is read, on both sides.
func TestReplica_whatACopyReadsLeavesMemoryAsItGoes(t *testing.T) {
	var mu sync.Mutex
	dropped := map[string]int64{}
	was := dropPages
	dropPages = func(f *os.File, off, n int64) {
		mu.Lock()
		dropped[filepath.Base(f.Name())] += n
		mu.Unlock()
		was(f, off, n)
	}
	t.Cleanup(func() { dropPages = was })
	src := t.TempDir()
	data := noise(3*gentleStep+5, 7)
	os.WriteFile(filepath.Join(src, "big.bin"), data, 0o644)
	dir, st, stop := pair(t, src)
	defer stop()
	eventually(t, "snapshot", synced(st))
	if b, _ := os.ReadFile(filepath.Join(dir, "volume-data", "big.bin")); !bytes.Equal(b, data) {
		t.Fatal("the copy is not the file")
	}
	mu.Lock()
	defer mu.Unlock()
	if dropped["big.bin"] < int64(len(data)) {
		t.Fatalf("%d of %d bytes the sender read left memory", dropped["big.bin"], len(data))
	}
}

// eu, 24 Nov 2026: a copy that began while its App was small grew with it and took eu-1 to 92%. A copy stops at a
// tenth of its disk free, the file it was writing goes, and it says why.
func TestReplica_aGrowingCopyStopsAtATenthFree(t *testing.T) {
	was := room.StatFile
	room.StatFile = func(f *os.File) (int64, int64, bool) {
		info, _ := f.Stat()
		return 1<<30 + 3*gentleStep/2 - info.Size(), 10 << 30, true
	}
	t.Cleanup(func() { room.StatFile = was })
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "big.bin"), noise(4*gentleStep, 9), 0o644)
	dir := t.TempDir()
	_, got, stop := stream(t, src, dir, NewIndex())
	defer stop()
	select {
	case err := <-got:
		if err == nil || !strings.Contains(err.Error(), room.Marker) {
			t.Fatalf("the copy ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the copy went on past a tenth free")
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "volume-data"))
	if len(entries) != 0 {
		t.Fatalf("the copy holds %v", entries)
	}
}

// Deplo reads a copy's status at its own pace: a stop for room is still said while the copy dials again.
func TestReplica_aStopForRoomIsSaidUntilAWholeSnapshotLands(t *testing.T) {
	mid, goOn := make(chan struct{}), make(chan struct{})
	var once sync.Once
	was := flushRange
	flushRange = func(f *os.File, off, n int64) {
		once.Do(func() { close(mid); <-goOn })
		was(f, off, n)
	}
	t.Cleanup(func() { flushRange = was })
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "big.bin"), noise(2*gentleStep, 3), 0o644)
	dir := t.TempDir()
	st := &Status{}
	st.Fail("replica: big: this server has 1.4 GB free, too little to go on with this copy and " + room.Marker)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if c, err := ln.Accept(); err == nil {
			defer c.Close()
			_ = Send(ctx, c, []string{VolumeRoot("data")}, []string{src})
		}
	}()
	c, _ := net.Dial("tcp", ln.Addr().String())
	defer c.Close()
	go func() { _ = Receive(ctx, c, dir, []string{VolumeRoot("data")}, st, NewIndex()) }()
	<-mid // the next stream is open and half way through the file
	connected, _, _, _, err := st.Snapshot()
	close(goOn)
	if !connected || !strings.Contains(err, room.Marker) {
		t.Fatalf("connected=%v: the stop for room was cleared before a snapshot landed: %q", connected, err)
	}
	eventually(t, "a whole snapshot to clear it", func() bool {
		_, synced, _, _, err := st.Snapshot()
		return synced && err == ""
	})
}

// eu, 24 Nov 2026: a 600 MB copy took a 1-core server's CPU and every App through it timed out for 24 s.
func TestReplica_bothEndsOfACopyArePaced(t *testing.T) {
	src := t.TempDir()
	data := noise(3*gentleStep+5, 6)
	os.WriteFile(filepath.Join(src, "big.bin"), data, 0o644)
	before := pace.Looks.Load()
	_, st, stop := pair(t, src)
	defer stop()
	eventually(t, "snapshot", synced(st))
	if n := pace.Looks.Load() - before; n < 6 {
		t.Fatalf("a copy of 3 steps looked %d times", n)
	}
}

// A file rebuilt beside the one it replaces holds both until it goes in place: it starts only if both fit.
func TestReplica_aRebuildStartsOnlyIfTheOldAndTheNewFit(t *testing.T) {
	src := t.TempDir()
	big := filepath.Join(src, "big.bin")
	putWhole(t, big, noise(4*gentleStep, 11))
	dir := t.TempDir()
	st, got, stop := stream(t, src, dir, NewIndex())
	defer stop()
	eventually(t, "snapshot", synced(st))
	was := room.StatFile
	// What the disk holds now leaves a tenth free, but not a second copy of big.bin.
	room.StatFile = func(*os.File) (int64, int64, bool) { return 1<<30 + 2*gentleStep, 10 << 30, true }
	t.Cleanup(func() { room.StatFile = was })
	putWhole(t, big, noise(4*gentleStep, 12))
	select {
	case err := <-got:
		if err == nil || !strings.Contains(err.Error(), room.Marker) {
			t.Fatalf("the copy ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the rebuild started without room for both")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "volume-data", "big.bin")); !bytes.Equal(b, noise(4*gentleStep, 11)) {
		t.Fatal("the old copy of big.bin went")
	}
}
