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
	t.Cleanup(func() { systemctl = was; copyParent.Store("") })
	takePriority(t.Context(), "0::/system.slice/deplo-agent.service\n")
	if len(calls) == 0 || calls[0] != "set-property --runtime deplo-agent.service CPUWeight=1000 IOWeight=1000 MemoryLow=128M" {
		t.Fatalf("systemctl was asked %q", calls)
	}
	calls = nil
	takePriority(t.Context(), "0::/user.slice/user-0.slice/session-4.scope\n")
	for _, c := range calls {
		if strings.Contains(c, ".service") || strings.Contains(c, ".scope") {
			t.Fatalf("a run outside a service changed %q", c)
		}
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

// A copy yields the disk, never the CPU: a thread at nice 19 that holds Go's only scheduler slot on a 1-core server
// holds up every goroutine behind it, a lease renewal's included.
func TestAtLowPriority_yieldsTheDiskButNeverTheCPU(t *testing.T) {
	var prio, class int
	_ = atLowPriority(func() error {
		tid := unix.Gettid()
		p, err := unix.Getpriority(unix.PRIO_PROCESS, tid)
		if err != nil {
			return err
		}
		r, _, _ := unix.Syscall(unix.SYS_IOPRIO_GET, 1, uintptr(tid), 0)
		prio, class = p, int(r>>13)
		return nil
	})
	// The kernel answers 20 - nice.
	if mine, _ := unix.Getpriority(unix.PRIO_PROCESS, 0); prio != mine {
		t.Fatalf("a copy runs at nice %d, the agent at nice %d", 20-prio, 20-mine)
	}
	if class != 3 {
		t.Fatalf("a copy's disk class is %d, want idle (3)", class)
	}
}

// eu, 24 Nov 2026: a disk with no scheduler ignores the idle class, and a copy's unwritten pages flushed at once.
func TestCopySlice_capsWhatACopyKeepsInMemory(t *testing.T) {
	if !strings.Contains(copyWeights, "MemoryHigh=") {
		t.Fatalf("the copy slice is %q", copyWeights)
	}
}

// c37: eu-2's proxy waited for the CPU behind the agent's copies, and every App through it timed out.
func TestProxyFirst_putsTheProxyLevelWithTheAgent(t *testing.T) {
	argv := dockerOnPath(t, `case "$1" in inspect) echo running;; esac`)
	proxyFirst(context.Background())
	if !slices.Contains(argv(), "update --cpu-shares 26215 deplo-traefik") {
		t.Fatalf("the proxy keeps Docker's default weight: %q", argv())
	}
}
