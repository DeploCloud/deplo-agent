package server

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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
	dir := t.TempDir()
	was := containerCgroup
	containerCgroup = func(_ context.Context, id string) (string, error) {
		if id != "deplo-traefik" {
			t.Fatalf("asked for %q", id)
		}
		return dir, nil
	}
	t.Cleanup(func() { containerCgroup = was })
	proxyFirst(context.Background(), traefikContainer)
	if raw, _ := os.ReadFile(filepath.Join(dir, "cpu.weight")); string(raw) != "1000" {
		t.Fatalf("the proxy's weight is %q, want the agent's 1000", raw)
	}
}

// weightsAt fakes the proxies' cgroups: each id given its weight, with how many docker commands had run by then.
func weightsAt(t *testing.T, argv func() []string) *[]string {
	var got []string
	containerCgroup = func(_ context.Context, id string) (string, error) {
		got = append(got, strconv.Itoa(len(argv()))+" "+id)
		return t.TempDir(), nil
	}
	return &got
}

// c43: the new proxy started at Docker's default weight, behind the rest of its file: 3.6 s to its routes on one core.
func TestBringUpTraefik_theNewProxyTakesItsWeightOnceCreated(t *testing.T) {
	argv := dockerOnPath(t, `case "$*" in *compose.service*) echo traefik;; esac`)
	weights := weightsAt(t, argv)
	path := filepath.Join(t.TempDir(), "docker-compose.yml")
	if err := os.WriteFile(path, []byte("services: {traefik: {image: traefik}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	_ = s.bringUpTraefik(context.Background(), path, false)
	for _, w := range *weights {
		n, id, _ := strings.Cut(w, " ")
		i, _ := strconv.Atoi(n)
		if id == traefikContainer && i > 0 && strings.Contains(argv()[i-1], "--force-recreate --no-deps") {
			return
		}
	}
	t.Fatalf("no weight right after the recreate: %v in %q", *weights, argv())
}

// c43: the standby carried every visitor at Docker's default weight while the new proxy started beside it.
func TestStartStandbyProxy_takesTheProxysWeight(t *testing.T) {
	argv := dockerOnPath(t, `case "$*" in
"inspect deplo-traefik") echo '[{"Image":"traefik","Config":{"Cmd":["--log.level=ERROR"]}}]';;
"run -d"*) echo sid;;
esac`)
	weights := weightsAt(t, argv)
	_, _, _ = startStandbyProxy(context.Background(), t.TempDir(), 0)
	if !slices.ContainsFunc(*weights, func(w string) bool { return strings.HasSuffix(w, " sid") }) {
		t.Fatalf("the standby kept the default weight: %v in %q", *weights, argv())
	}
}
