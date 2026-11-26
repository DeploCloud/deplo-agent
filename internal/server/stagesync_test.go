package server

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func writeAt(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
}

// sameCopy fails unless dst holds what src does: every entry, its type, mode, time, bytes and link target.
func sameCopy(t *testing.T, src, dst string) {
	t.Helper()
	seen := map[string]bool{}
	_ = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(src, p)
		seen[rel] = true
		si, _ := os.Lstat(p)
		di, err := os.Lstat(filepath.Join(dst, rel))
		if err != nil {
			t.Fatalf("%s is missing from the copy", rel)
		}
		if si.Mode() != di.Mode() {
			t.Fatalf("%s: mode %v, want %v", rel, di.Mode(), si.Mode())
		}
		switch {
		case si.Mode()&fs.ModeSymlink != 0:
			a, _ := os.Readlink(p)
			b, _ := os.Readlink(filepath.Join(dst, rel))
			if a != b {
				t.Fatalf("%s points at %q, want %q", rel, b, a)
			}
		case si.Mode().IsRegular():
			a, _ := os.ReadFile(p)
			b, _ := os.ReadFile(filepath.Join(dst, rel))
			if !bytes.Equal(a, b) || !si.ModTime().Equal(di.ModTime()) {
				t.Fatalf("%s differs in the copy", rel)
			}
		}
		return nil
	})
	_ = filepath.WalkDir(dst, func(p string, d fs.DirEntry, err error) error {
		rel, _ := filepath.Rel(dst, p)
		if !seen[rel] {
			t.Fatalf("%s is in the copy but no longer in the volume", rel)
		}
		return nil
	})
}

// eu, 24 Nov 2026: a 2 GB App was paused 20 to 44 s, its whole copy. Copied once while it runs, the pass made while
// it is paused takes only what changed in between, including a write that kept the file's size and time.
func TestStageSync_aSecondPassCopiesOnlyWhatChanged(t *testing.T) {
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "0")
	writeAt(t, src, "db/base.dat", string(bytes.Repeat([]byte("a"), 1<<20)))
	writeAt(t, src, "db/wal.log", "0123456789")
	writeAt(t, src, "gone.txt", "bye")
	writeAt(t, src, "turns-into-a-dir", "file")
	if err := os.Symlink("db/base.dat", filepath.Join(src, "latest")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(src, "db"), 0o750); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	// A file's change time comes from a coarser clock: the agent asks from a little before the first pass, too.
	start := time.Now().Add(-10 * time.Millisecond)

	files, n, err := syncTree(src, dst, time.Time{})
	if err != nil || files != 4 || n != 1<<20+10+3+4 {
		t.Fatalf("the first pass copied %d files, %d bytes (%v)", files, n, err)
	}
	sameCopy(t, src, dst)

	wal := filepath.Join(src, "db/wal.log")
	was, _ := os.Stat(wal)
	if err := os.WriteFile(wal, []byte("9876543210"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(wal, was.ModTime(), was.ModTime()); err != nil {
		t.Fatal(err)
	}
	writeAt(t, src, "db/new.log", "new")
	_ = os.Remove(filepath.Join(src, "gone.txt"))
	_ = os.Remove(filepath.Join(src, "turns-into-a-dir"))
	writeAt(t, src, "turns-into-a-dir/inside", "x")

	files, n, err = syncTree(src, dst, start)
	if err != nil || files != 3 || n != 10+3+1 {
		t.Fatalf("the second pass copied %d files, %d bytes (%v), want only the 3 that changed", files, n, err)
	}
	sameCopy(t, src, dst)

	if files, n, err = syncTree(src, dst, time.Now().Add(time.Second)); err != nil || files != 0 || n != 0 {
		t.Fatalf("with nothing changed a pass copied %d files, %d bytes (%v)", files, n, err)
	}
}

func TestStageSync_printsWhatItCopied(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeAt(t, src, "a", "abc")
	var out, errs bytes.Buffer
	if code := StageSync([]string{"--since", "1", src, dst}, &out, &errs); code != 0 || out.String() != "1 3\n" {
		t.Fatalf("got %d %q %q", code, out.String(), errs.String())
	}
	if code := StageSync([]string{src}, &out, &errs); code != 2 {
		t.Fatalf("a call without both folders must be refused, got %d", code)
	}
}

// eu, 24 Nov 2026: the first copy of 2 GB, made at page-cache speed beside a running App, left its 1-core server
// unable to renew its lease for 5 s, and its writeback stalled every server on the lab's disk for 20 s. The first
// pass now writes in steps it waits on; the pass made while the App is paused does not wait.
func TestStageSync_aFirstPassWritesInStepsItWaitsOn(t *testing.T) {
	src := t.TempDir()
	writeAt(t, src, "big", string(bytes.Repeat([]byte("z"), 20<<20)))
	was := flushRange
	var flushed int64
	steps := 0
	flushRange = func(f *os.File, off, n int64) {
		steps++
		flushed += n
		was(f, off, n)
	}
	t.Cleanup(func() { flushRange = was })

	dst := filepath.Join(t.TempDir(), "0")
	if _, _, err := syncTree(src, dst, time.Time{}); err != nil {
		t.Fatal(err)
	}
	sameCopy(t, src, dst)
	if steps < 3 || flushed != 20<<20 {
		t.Fatalf("the first pass wrote 20 MB in %d waited steps (%d bytes), want steps of at most 8 MB", steps, flushed)
	}
	steps = 0
	if _, _, err := syncTree(src, filepath.Join(t.TempDir(), "1"), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if steps != 0 {
		t.Fatalf("the pass made while the App is paused must not wait on the disk, it waited %d times", steps)
	}
}

// c49: the pass an App is paused for started as a process of its own inside the pause. Started before it, it waits
// to be told since when, copies what changed and says what it copied.
func TestStageSync_aPassStartedBeforeThePauseCopiesWhenTold(t *testing.T) {
	src, dst, ctl := t.TempDir(), t.TempDir(), t.TempDir()
	writeAt(t, src, "old.txt", "old")
	if files, _, err := syncTree(src, dst, time.Time{}); err != nil || files != 1 {
		t.Fatalf("first copy: %d files, %v", files, err)
	}
	ended := make(chan int, 1)
	var errb bytes.Buffer
	go func() { ended <- StageSync([]string{"--wait", ctl, src, dst}, io.Discard, &errb) }()
	for end := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if _, err := os.Stat(filepath.Join(ctl, ".ready")); err == nil {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the pass never said it waits: %s", errb.String())
		}
	}
	since := time.Now()
	writeAt(t, src, "new.txt", "new!")
	if err := renameInto(filepath.Join(ctl, ".go"), strconv.FormatInt(since.UnixNano(), 10)); err != nil {
		t.Fatal(err)
	}
	if code := <-ended; code != 0 {
		t.Fatalf("exited %d: %s", code, errb.String())
	}
	raw, _ := os.ReadFile(filepath.Join(ctl, ".done"))
	var files, n, ns int64
	if _, err := fmt.Sscan(string(raw), &files, &n, &ns); err != nil || files != 1 || n != 4 || ns <= 0 {
		t.Fatalf("the pass said %q", raw)
	}
	sameCopy(t, src, dst)
}
