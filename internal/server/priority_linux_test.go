package server

import (
	"context"
	"slices"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestTakePriority_putsTheAgentsServiceFirst(t *testing.T) {
	var calls []string
	was := systemctl
	systemctl = func(_ context.Context, args ...string) error {
		calls = append(calls, strings.Join(args, " "))
		return nil
	}
	t.Cleanup(func() { systemctl = was })
	takePriority(t.Context(), "0::/system.slice/deplo-agent.service\n")
	if len(calls) == 0 || calls[0] != "set-property --runtime deplo-agent.service CPUWeight=1000 IOWeight=1000 MemoryLow=128M" {
		t.Fatalf("systemctl was asked %q", calls)
	}
	calls = nil
	takePriority(t.Context(), "0::/user.slice/user-0.slice/session-4.scope\n")
	if len(calls) != 0 {
		t.Fatalf("a run outside a service changed %q", calls)
	}
}

func TestVolumeHelperRun_aCopyRunsLast(t *testing.T) {
	argv := volumeHelperRun(t.Context(), volumeHelperImage, "true")
	if !slices.Contains(argv, "--cpu-shares=2") {
		t.Fatalf("a helper runs at the CPU share of an App: %q", argv)
	}
	copyParent.Store(copySlice)
	t.Cleanup(func() { copyParent.Store("") })
	if argv := volumeHelperRun(t.Context(), volumeHelperImage, "true"); !slices.Contains(argv, "--cgroup-parent="+copySlice) {
		t.Fatalf("a helper runs outside the copy slice: %q", argv)
	}
}

func TestAtLowPriority_runsOnALoweredThreadOfItsOwn(t *testing.T) {
	var nice, class int
	_ = atLowPriority(func() error {
		tid := unix.Gettid()
		p, _ := unix.Getpriority(unix.PRIO_PROCESS, tid)
		nice = 20 - p
		r, _, _ := unix.Syscall(unix.SYS_IOPRIO_GET, 1, uintptr(tid), 0)
		class = int(r) >> 13
		return nil
	})
	if nice != 19 || class != 3 {
		t.Fatalf("the copy ran at nice %d, disk class %d", nice, class)
	}
	p, _ := unix.Getpriority(unix.PRIO_PROCESS, unix.Gettid())
	if 20-p == 19 {
		t.Fatal("the caller's thread was lowered too")
	}
}
