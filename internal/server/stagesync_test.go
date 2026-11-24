package server

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
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
