package server

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A busy disk or CPU never costs a lease (deplo ADR-0043): the agent, and the leader it runs, come first, and every
// copy, replica and backup last.
const (
	agentWeights = "CPUWeight=1000 IOWeight=1000 MemoryLow=128M"
	// copySlice holds every helper container: a tenth of an App's share of the CPU, and the disk only when idle; its
	// memory cap keeps what it writes from piling up unwritten, which a disk with no scheduler flushes all at once.
	copySlice   = "deplocopy.slice"
	copyWeights = "CPUWeight=10 IOWeight=1 MemoryHigh=256M"
	// cliSlice holds every docker and git command the agent runs: level with Docker's own daemon, under the agent.
	cliSlice   = "deplocli.slice"
	cliWeights = "CPUWeight=100 IOWeight=100"
)

// proxyWeight gives the proxy every request crosses the agent's own CPU weight (Docker's default is 100): a copy
// inside the agent never outranks it (c37: eu-2's proxy waited for the CPU and every App timed out).
const proxyWeight = "1000"

// proxyFirst puts the proxy level with the agent on the CPU, in its cgroup: Docker's --cpu-shares maps to a weight
// that differs between runc versions. Best effort; again whenever the proxy is restarted or recreated.
// ponytail: a restart Docker makes on its own keeps the default until the agent's next start.
func proxyFirst(ctx context.Context) {
	dir, err := containerCgroup(ctx, traefikContainer)
	if err != nil {
		return
	}
	if err := os.WriteFile(filepath.Join(dir, "cpu.weight"), []byte(proxyWeight), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not put the proxy level with the agent on the CPU: %v\n", err)
	}
}

// copyParent is the slice helper containers run in, once TakePriority made it; empty, they run where Docker puts them.
var copyParent atomic.Value

// ownUnit is the systemd service this process runs in, from /proc/self/cgroup; empty outside one.
func ownUnit(cgroup string) string {
	for _, line := range strings.Split(cgroup, "\n") {
		path, ok := strings.CutPrefix(line, "0::")
		if !ok {
			continue
		}
		unit := path[strings.LastIndexByte(path, '/')+1:]
		if strings.HasSuffix(unit, ".service") {
			return unit
		}
	}
	return ""
}

var systemctl = func(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// TakePriority raises this agent's service, until it stops, and makes the slice the copies run in. Best effort: a
// host without systemd keeps its defaults.
func TakePriority(ctx context.Context) {
	raw, _ := os.ReadFile("/proc/self/cgroup")
	takePriority(ctx, string(raw))
}

func takePriority(ctx context.Context, cgroup string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if unit := ownUnit(cgroup); unit != "" {
		if err := systemctl(ctx, append([]string{"set-property", "--runtime", unit}, strings.Fields(agentWeights)...)...); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: could not put the agent first on the CPU and the disk: %v\n", err)
		}
	}
	proxyFirst(ctx)
	if systemctl(ctx, "start", cliSlice) == nil &&
		systemctl(ctx, append([]string{"set-property", "--runtime", cliSlice}, strings.Fields(cliWeights)...)...) == nil &&
		!dockercli.UseCgroup("/sys/fs/cgroup/"+cliSlice) {
		fmt.Fprintf(os.Stderr, "deplo-agent: docker commands run beside the agent: %s takes no process\n", cliSlice)
	}
	res, err := dockercli.Run(ctx, 10*time.Second, "info", "--format", "{{.CgroupDriver}}")
	if err != nil || res.Code != 0 || strings.TrimSpace(res.Stdout) != "systemd" {
		return
	}
	if systemctl(ctx, "start", copySlice) != nil ||
		systemctl(ctx, append([]string{"set-property", "--runtime", copySlice}, strings.Fields(copyWeights)...)...) != nil {
		return
	}
	// The disk's own priority: what a copy writes waits for every other writer, including its writeback.
	_ = os.WriteFile("/sys/fs/cgroup/"+copySlice+"/io.prio.class", []byte("idle"), 0o644)
	copyParent.Store(copySlice)
}

// helperPriority is what every helper container is started with: the copy slice, or at least a low CPU share.
func helperPriority() []string {
	if p, _ := copyParent.Load().(string); p != "" {
		return []string{"--cgroup-parent=" + p, "--cpu-shares=2"}
	}
	return []string{"--cpu-shares=2"}
}

// atLowPriority runs fn on a thread of its own at the lowest disk priority; the thread ends with fn. Never a lower
// CPU priority: a starved thread keeps the Go scheduler's slot, the only one on a 1-core server, and every goroutine,
// a renewal's too, waits for it (eu, 24 Nov 2026: renewals 3 to 4.6 s late while a copy ran at nice 19).
func atLowPriority(fn func() error) error {
	done := make(chan error, 1)
	go func() {
		// Never unlocked: the lowered thread exits with this goroutine instead of serving another.
		runtime.LockOSThread()
		tid := unix.Gettid()
		_, _, _ = unix.Syscall(unix.SYS_IOPRIO_SET, 1, uintptr(tid), 3<<13)
		done <- fn()
	}()
	return <-done
}
