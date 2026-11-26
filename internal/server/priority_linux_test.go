package server

import (
	"context"
	"errors"
	"net"
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

// c44: the standby started at the proxy's weight and took its CPU on one core (2.75 s): it starts aside.
func TestStartStandbyProxy_startsAsideOfTheProxy(t *testing.T) {
	argv := dockerOnPath(t, `case "$*" in
"inspect deplo-traefik") echo '[{"Image":"traefik","Config":{"Cmd":["--log.level=ERROR"]}}]';;
"run -d"*) echo sid;;
esac`)
	weights := weightsAt(t, argv)
	_, _, _ = startStandbyProxy(context.Background(), t.TempDir(), 0)
	if slices.ContainsFunc(*weights, func(w string) bool { return strings.HasSuffix(w, " sid") }) {
		t.Fatalf("the standby took a weight while it started: %v in %q", *weights, argv())
	}
}

// c44: the proxy that carries the visitors comes first on the CPU, the one starting or draining beside it after.
func TestProxyRestart_theProxyCarryingTheVisitorsComesFirst(t *testing.T) {
	dirs := map[string]string{traefikContainer: t.TempDir(), standbyContainer: t.TempDir()}
	was, up := containerCgroup, standbyUp
	t.Cleanup(func() { containerCgroup, standbyUp = was, up })
	containerCgroup = func(_ context.Context, id string) (string, error) { return dirs[id], nil }
	standbyUp = func(context.Context, string, uint32) (standbyDialer, func(), error) {
		return func(context.Context, uint32) (net.Conn, error) { return nil, errors.New("none") }, func() {}, nil
	}
	weight := func(id string) string {
		raw, _ := os.ReadFile(filepath.Join(dirs[id], "cpu.weight"))
		return string(raw)
	}
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	s.proxyGoingDown()
	if weight(standbyContainer) != "1000" {
		t.Fatalf("the standby carrying the visitors weighs %q", weight(standbyContainer))
	}
	s.newProxyWeight(context.Background())
	if weight(traefikContainer) != "" {
		t.Fatalf("the new proxy took %q while the standby carried its visitors", weight(traefikContainer))
	}
	s.proxyBack()
	if weight(traefikContainer) != "1000" || weight(standbyContainer) != "100" {
		t.Fatalf("after the hand-back: proxy %q, standby %q", weight(traefikContainer), weight(standbyContainer))
	}
}

// c49: the pass a backup paused its App for ran at the copy slice's tenth and waited past 1 s on a busy core. While
// the App is paused, the copier and its slice weigh what the agent does; after, what they weighed before.
func TestCopierFirst_weighsTheCopierLikeTheAgentWhileItsAppIsPaused(t *testing.T) {
	slice := filepath.Join(t.TempDir(), copySlice)
	scope := filepath.Join(slice, "docker-cop999.scope")
	mustWrite(t, filepath.Join(scope, "cpu.weight"), "1\n")
	mustWrite(t, filepath.Join(slice, "cpu.weight"), "10\n")
	weight := func(d string) string {
		b, _ := os.ReadFile(filepath.Join(d, "cpu.weight"))
		return strings.TrimSpace(string(b))
	}
	back := copierFirst(scope)
	if weight(scope) != proxyWeight || weight(slice) != proxyWeight {
		t.Fatalf("while paused the copier weighs %s in a slice of %s", weight(scope), weight(slice))
	}
	back()
	if weight(scope) != "1" || weight(slice) != "10" {
		t.Fatalf("after the pause the copier weighs %s in a slice of %s", weight(scope), weight(slice))
	}
}
