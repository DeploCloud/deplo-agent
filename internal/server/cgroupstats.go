package server

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/hostmetrics"
)

const cgroup2SuperMagic = 0x63677270

const cgroupUnhealthyTicks = 3

func cgroup2Available() bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs("/sys/fs/cgroup", &st); err != nil {
		return false
	}
	return int64(st.Type) == cgroup2SuperMagic
}

type cgroupPrevCPU struct {
	usageUsec int64
	at        time.Time
}

type cgroupSampler struct {
	sampleMu sync.Mutex

	mu sync.Mutex

	prev map[string]cgroupPrevCPU

	fails     int
	unhealthy bool

	machineMem int64

	procRoot string
}

func newCgroupSampler() *cgroupSampler {
	c := &cgroupSampler{
		prev:     map[string]cgroupPrevCPU{},
		procRoot: "/proc",
	}
	if raw, ok := readFileTrimmed("/proc/meminfo"); ok {
		c.machineMem = parseMemTotal(raw)
	}
	return c
}

// Unhealthy reports that this backend has failed wholesale for cgroupUnhealthyTicks consecutive ticks and should be abandoned.
func (c *cgroupSampler) Unhealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unhealthy
}

// Sample returns one stat per entry.
func (c *cgroupSampler) Sample(entries []rosterEntry, now time.Time) []*pb.ContainerStat {
	c.sampleMu.Lock()
	defer c.sampleMu.Unlock()

	c.mu.Lock()
	prev := c.prev
	c.mu.Unlock()

	hostNetNs := netNsOf(c.procRoot, os.Getpid())

	next := make(map[string]cgroupPrevCPU, len(entries))
	out := make([]*pb.ContainerStat, 0, len(entries))

	expected, failed := 0, 0

	for _, e := range entries {
		if e.State != "running" {
			st := &pb.ContainerStat{}
			applyIdentity(st, e, false)
			out = append(out, st)
			carryCPUBaseline(next, prev, e.ID)
			continue
		}
		expected++
		if e.CgroupPath == "" {
			failed++
			carryCPUBaseline(next, prev, e.ID)
			continue
		}
		r, ok := c.readOne(e, now, prev, hostNetNs)
		if !ok {
			failed++
			carryCPUBaseline(next, prev, e.ID)
			continue
		}
		if r.haveUsage {
			next[e.ID] = cgroupPrevCPU{usageUsec: r.usageUsec, at: now}
		} else {
			carryCPUBaseline(next, prev, e.ID)
		}
		out = append(out, r.stat)
	}

	c.mu.Lock()
	c.prev = next
	if expected > 0 && failed == expected {
		c.fails++
		if c.fails >= cgroupUnhealthyTicks {
			c.unhealthy = true
		}
	} else {
		c.fails = 0
	}
	c.mu.Unlock()
	return out
}

func carryCPUBaseline(next, prev map[string]cgroupPrevCPU, id string) {
	if p, ok := prev[id]; ok {
		next[id] = p
	}
}

type cgroupRead struct {
	stat      *pb.ContainerStat
	usageUsec int64
	haveUsage bool
}

func (c *cgroupSampler) readOne(e rosterEntry, now time.Time, prev map[string]cgroupPrevCPU, hostNetNs uint64) (cgroupRead, bool) {
	cpuRaw, cpuOK := readFileTrimmed(filepath.Join(e.CgroupPath, "cpu.stat"))
	memRaw, memOK := readFileTrimmed(filepath.Join(e.CgroupPath, "memory.current"))

	if !cpuOK && !memOK {
		return cgroupRead{}, false
	}

	usage, haveUsage := parseCPUUsageUsec(cpuRaw)
	cpuPct := 0.0
	if haveUsage {
		if p, ok := prev[e.ID]; ok {
			cpuPct = cpuPercentFromUsage(p.usageUsec, usage, now.Sub(p.at))
		}
	}

	var memUsed int64
	if memOK {
		current, _ := parseUint64Value(memRaw)
		inactive := int64(0)
		if raw, ok := readFileTrimmed(filepath.Join(e.CgroupPath, "memory.stat")); ok {
			inactive = parseMemoryStatInactiveFile(raw)
		}
		memUsed = current - inactive
		if memUsed < 0 {
			memUsed = 0
		}
	}

	var memLimit int64
	if raw, ok := readFileTrimmed(filepath.Join(e.CgroupPath, "memory.max")); ok {
		memLimit = parseMemoryMax(raw, c.machineMem)
	}
	memPct := 0.0
	if memLimit > 0 {
		memPct = round2(float64(memUsed) / float64(memLimit) * 100)
	}

	var blockRead, blockWrite int64
	if raw, ok := readFileTrimmed(filepath.Join(e.CgroupPath, "io.stat")); ok {
		blockRead, blockWrite = parseIOStat(raw)
	}

	var pids int32
	if raw, ok := readFileTrimmed(filepath.Join(e.CgroupPath, "pids.current")); ok {
		pids, _ = parsePidsCurrent(raw)
	}

	var netRx, netTx int64
	var netNs uint64
	netNsHost := false
	if e.PID > 0 {
		netNs = netNsOf(c.procRoot, e.PID)
		netNsHost = netNs != 0 && netNs == hostNetNs
		if !netNsHost {
			if raw, ok := readFileTrimmed(filepath.Join(c.procRoot, strconv.Itoa(e.PID), "net", "dev")); ok {
				netRx, netTx = parseNetDev(raw)
			}
		}
	}

	return cgroupRead{stat: &pb.ContainerStat{
		Name:         e.Name,
		ProjectId:    e.ProjectID,
		ContainerId:  e.ID,
		State:        e.State,
		Health:       e.Health,
		RestartCount: e.RestartCount,
		Running:      e.State == "running",
		CpuPct:       cpuPct,
		MemUsed:      memUsed,
		MemLimit:     memLimit,
		MemPct:       memPct,
		NetRx:        netRx,
		NetTx:        netTx,
		BlockRead:    blockRead,
		BlockWrite:   blockWrite,
		Pids:         pids,
		NetNsId:      netNs,
		NetNsHost:    netNsHost,
	}, usageUsec: usage, haveUsage: haveUsage}, true
}

func netNsOf(procRoot string, pid int) uint64 {
	fi, err := os.Stat(filepath.Join(procRoot, strconv.Itoa(pid), "ns", "net"))
	if err != nil {
		return 0
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return st.Ino
}

func cpuPercentFromUsage(prevUsec, curUsec int64, elapsed time.Duration) float64 {
	elapsedUsec := float64(elapsed.Microseconds())
	if elapsedUsec <= 0 {
		return 0
	}
	delta := curUsec - prevUsec
	if delta <= 0 {
		return 0
	}
	return round2(float64(delta) / elapsedUsec * 100)
}

func parseCPUUsageUsec(content string) (int64, bool) {
	for _, line := range strings.Split(content, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "usage_usec" {
			continue
		}
		v, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

func parseMemoryStatInactiveFile(content string) int64 {
	for _, line := range strings.Split(content, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "inactive_file" {
			continue
		}
		v, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil || v < 0 {
			return 0
		}
		return v
	}
	return 0
}

func parseMemoryMax(content string, machineMem int64) int64 {
	s := strings.TrimSpace(content)
	if s == "max" {
		return machineMem
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

func parseIOStat(content string) (read, write int64) {
	for _, line := range strings.Split(content, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || !strings.Contains(f[0], ":") {
			continue
		}
		for _, kv := range f[1:] {
			key, val, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			if key != "rbytes" && key != "wbytes" {
				continue
			}
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil || v < 0 {
				continue
			}
			if key == "rbytes" {
				read += v
			} else {
				write += v
			}
		}
	}
	return read, write
}

func parsePidsCurrent(content string) (int32, bool) {
	v, err := strconv.ParseInt(strings.TrimSpace(content), 10, 32)
	if err != nil || v < 0 {
		return 0, false
	}
	return int32(v), true
}

func parseNetDev(content string) (rx, tx int64) {
	for _, line := range strings.Split(content, "\n") {
		idx := strings.IndexByte(line, ':')
		if idx < 0 {
			continue
		}
		iface := strings.TrimSpace(line[:idx])
		if skipNetIface(iface) {
			continue
		}
		f := strings.Fields(line[idx+1:])
		if len(f) < 9 {
			continue
		}
		r, errR := strconv.ParseInt(f[0], 10, 64)
		t, errT := strconv.ParseInt(f[8], 10, 64)
		if errR != nil || errT != nil {
			continue
		}
		rx += r
		tx += t
	}
	return rx, tx
}

func skipNetIface(name string) bool {
	return hostmetrics.SkipNetIface(name)
}

func parseMemTotal(content string) int64 {
	for _, line := range strings.Split(content, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "MemTotal:" {
			continue
		}
		kb, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil || kb < 0 {
			return 0
		}
		return kb * 1024
	}
	return 0
}

func parseUint64Value(content string) (int64, bool) {
	v, err := strconv.ParseInt(strings.TrimSpace(content), 10, 64)
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

func readFileTrimmed(path string) (string, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

func round2(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }
