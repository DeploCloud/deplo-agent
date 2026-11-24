package room

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fake(t *testing.T, free, total int64) *int64 {
	t.Helper()
	left := free
	stat, file := Stat, StatFile
	Stat = func(string) (int64, int64, bool) { return left, total, true }
	StatFile = func(*os.File) (int64, int64, bool) { return left, total, true }
	t.Cleanup(func() { Stat, StatFile = stat, file })
	return &left
}

func TestCheck_aCopyStartsOnlyWhenATenthStaysFree(t *testing.T) {
	fake(t, 3<<30, 10<<30)
	if err := Check("/x", 2<<30); err != nil {
		t.Fatalf("2 GB onto 3 GB free of 10 GB leaves a tenth: %v", err)
	}
	err := Check("/x", 2<<30+1)
	if !errors.Is(err, ErrNoRoom) || !strings.Contains(err.Error(), "3.0 GB free, too little for this copy (about 2.0 GB) and to "+Marker) {
		t.Fatalf("a copy past the tenth: %v", err)
	}
}

func TestWriter_aGrowingCopyStopsAtATenthFree(t *testing.T) {
	left := fake(t, 1<<30+20<<20, 10<<30)
	f, err := os.Create(filepath.Join(t.TempDir(), "copy"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := &Writer{F: f}
	buf := make([]byte, 1<<20)
	wrote := 0
	for ; wrote < 200; wrote++ {
		if _, err = w.Write(buf); err != nil {
			break
		}
		*left -= 1 << 20 // what was written takes the disk's room
		if *left < 1<<30-Step {
			t.Fatalf("the disk went %d MB under a tenth free", (1<<30-*left)>>20)
		}
	}
	if !errors.Is(err, ErrNoRoom) || !strings.Contains(err.Error(), "too little to go on with this copy") {
		t.Fatalf("wrote %d MB and got %v", wrote, err)
	}
}

func TestWriter_unknownDiskWritesOn(t *testing.T) {
	stat := StatFile
	StatFile = func(*os.File) (int64, int64, bool) { return 0, 0, false }
	defer func() { StatFile = stat }()
	f, _ := os.Create(filepath.Join(t.TempDir(), "copy"))
	defer f.Close()
	if _, err := (&Writer{F: f}).Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
}
