package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func fakeProc(t *testing.T, stats []string, meminfo string) {
	t.Helper()
	dir := t.TempDir()
	st, mi := filepath.Join(dir, "stat"), filepath.Join(dir, "meminfo")
	mustWrite(t, mi, meminfo)
	mustWrite(t, st, stats[0])
	origStat, origMem, origWin := procStat, procMeminfo, cpuSampleWindow
	procStat, procMeminfo, cpuSampleWindow = st, mi, 50*time.Millisecond
	t.Cleanup(func() { procStat, procMeminfo, cpuSampleWindow = origStat, origMem, origWin })
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = os.WriteFile(st, []byte(stats[1]), 0o644)
	}()
}

const plenty = "MemTotal: 8000000 kB\nMemAvailable: 4000000 kB\n"

func TestCaptureHeadroom(t *testing.T) {
	// 100 jiffies pass: 10 idle means 90% busy.
	fakeProc(t, []string{"cpu 100 0 100 800 0 0 0 0 0 0\n", "cpu 145 0 145 810 0 0 0 0 0 0\n"}, plenty)
	if err := captureHeadroom(context.Background()); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("a 90%% busy host must make the picture wait, got %v", err)
	}
	fakeProc(t, []string{"cpu 100 0 100 800 0 0 0 0 0 0\n", "cpu 110 0 110 880 0 0 0 0 0 0\n"}, plenty)
	if err := captureHeadroom(context.Background()); err != nil {
		t.Fatalf("a 20%% busy host has room, got %v", err)
	}
	fakeProc(t, []string{"cpu 100 0 100 800 0 0 0 0 0 0\n", "cpu 110 0 110 880 0 0 0 0 0 0\n"}, "MemAvailable: 300000 kB\n")
	if err := captureHeadroom(context.Background()); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("300 MB free is too little for a browser, got %v", err)
	}
	fakeProc(t, []string{"garbage\n", "garbage\n"}, "nothing\n")
	if err := captureHeadroom(context.Background()); err != nil {
		t.Fatalf("an unreadable /proc never blocks a picture, got %v", err)
	}
}
