package server

// https://deplo.build/docs/guides/monitoring

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

// cgroupstats.go is the zero-subprocess data source for container metrics: it reads
// cgroup v2 and /proc directly instead of shelling out to `docker stats`. This backend
// supplies NUMBERS ONLY.

// cgroup2SuperMagic identifies a unified (v2) hierarchy; from linux/magic.h.
const cgroup2SuperMagic = 0x63677270

// cgroupUnhealthyTicks is how many CONSECUTIVE ticks may fail wholesale before the
// backend admits it does not work on this host.
const cgroupUnhealthyTicks = 3

// cgroup2Available reports whether this host can use the cgroup v2 backend. v1 and the
// "hybrid" layout scatter every controller across its own mount (memory/, cpu,cpuacct/,
// blkio/) with different filenames AND different semantics - memory.usage_in_bytes
// includes page cache in a place the v2 file does not, so a shared parser would
// silently over-report on half the fleet.
func cgroup2Available() bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs("/sys/fs/cgroup", &st); err != nil {
		return false
	}
	return int64(st.Type) == cgroup2SuperMagic
}

// cgroupPrevCPU is the previous CPU reading for one container, with the instant it was
// taken.
type cgroupPrevCPU struct {
	usageUsec int64
	at        time.Time
}

type cgroupSampler struct {
	// sampleMu serializes Sample calls; mu guards the mutable state below.
	sampleMu sync.Mutex

	mu sync.Mutex

	// prev is keyed by CONTAINER ID, never by name.
	prev map[string]cgroupPrevCPU

	fails     int
	unhealthy bool

	// machineMem substitutes for an unlimited memory.max. Read once at construction:
	// installed RAM cannot change under a running kernel, and re-reading /proc/meminfo per
	// container per tick would give back a chunk of the win this file exists to capture.
	machineMem int64

	// procRoot is "/proc" in production. Tests point it at a t.TempDir tree so
	// the per-namespace network parsing runs with no containers and no root.
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

// Unhealthy reports that this backend has failed wholesale for cgroupUnhealthyTicks
// consecutive ticks and should be abandoned.
func (c *cgroupSampler) Unhealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unhealthy
}

// Sample returns one stat per entry. A RUNNING container whose files cannot be read is
// omitted entirely (a fabricated zero would read as idle).
func (c *cgroupSampler) Sample(entries []rosterEntry, now time.Time) []*pb.ContainerStat {
	c.sampleMu.Lock()
	defer c.sampleMu.Unlock()

	// prev is only ever REPLACED, never mutated in place, and only Sample replaces it,
	// which sampleMu already serializes.
	c.mu.Lock()
	prev := c.prev
	c.mu.Unlock()

	// The agent's own network namespace, resolved once per tick: a container whose
	// namespace matches it is on `network_mode: host` and its /proc/<pid>/net/dev is
	// the WHOLE MACHINE's counters, not its own.
	hostNetNs := netNsOf(c.procRoot, os.Getpid())

	next := make(map[string]cgroupPrevCPU, len(entries))
	out := make([]*pb.ContainerStat, 0, len(entries))

	// Health accounting counts only entries that OUGHT to be readable, i.e. running ones.
	expected, failed := 0, 0

	for _, e := range entries {
		if e.State != "running" {
			// A non-running container has no cgroup to read, but it must NOT vanish from the
			// stream.
			st := &pb.ContainerStat{}
			applyIdentity(st, e, false)
			out = append(out, st)
			carryCPUBaseline(next, prev, e.ID)
			continue
		}
		expected++
		if e.CgroupPath == "" {
			// The roster could not resolve a path for a RUNNING container; there is nothing to
			// read and nothing to guess at.
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
			// A FAILED read advances NOTHING.
			carryCPUBaseline(next, prev, e.ID)
		}
		out = append(out, r.stat)
	}

	// REPLACING the map rather than updating it is what drops a vanished container: an id
	// absent from this tick is absent from `next`, so its stale counter can never be
	// differenced against a future container, and the map cannot grow without bound across
	// months of redeploys.
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

// carryCPUBaseline preserves an id's existing CPU baseline (value AND timestamp)
// into the next tick's map when this tick could not measure usage. It is a no-op
// for an id with no baseline yet, which correctly leaves it unprimed.
func carryCPUBaseline(next, prev map[string]cgroupPrevCPU, id string) {
	if p, ok := prev[id]; ok {
		next[id] = p
	}
}

// cgroupRead is one container's tick. usageUsec/haveUsage are separate from the
// ok flag on purpose: a sample can be emittable (memory read fine) while CPU was
// NOT measured, and only haveUsage may advance the rate baseline.
type cgroupRead struct {
	stat      *pb.ContainerStat
	usageUsec int64
	haveUsage bool
}

// readOne reads one container's files. ok=false means the container could not be read
// at all and must be omitted from the tick entirely. Dropping the whole container would
// blank a rootless host's charts entirely over one optional file.
func (c *cgroupSampler) readOne(e rosterEntry, now time.Time, prev map[string]cgroupPrevCPU, hostNetNs uint64) (cgroupRead, bool) {
	cpuRaw, cpuOK := readFileTrimmed(filepath.Join(e.CgroupPath, "cpu.stat"))
	memRaw, memOK := readFileTrimmed(filepath.Join(e.CgroupPath, "memory.current"))

	// The cgroup directory is the container's proof of life. cpu.stat and memory.current
	// exist in every configuration including rootless, so if NEITHER can be read the
	// cgroup is gone (exited, or never ours) and the honest answer is to report nothing.
	if !cpuOK && !memOK {
		return cgroupRead{}, false
	}

	usage, haveUsage := parseCPUUsageUsec(cpuRaw)
	cpuPct := 0.0
	if haveUsage {
		// An id seen for the first time is UNPRIMED: there is no earlier reading to
		// difference against, so it reports 0 for exactly one tick.
		if p, ok := prev[e.ID]; ok {
			cpuPct = cpuPercentFromUsage(p.usageUsec, usage, now.Sub(p.at))
		}
	}

	var memUsed int64
	if memOK {
		current, _ := parseUint64Value(memRaw)
		// memory.current MINUS inactive_file.
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

	// /proc/<pid>/net/dev read FROM THE HOST returns the CONTAINER's namespace counters -
	// verified against a live host, where the container's eth0 showed 33,481,934 rx bytes
	// while the host's eth0 showed 1,188,594,390. procfs resolves the network files
	// through the target task's netns, so no setns, no nsenter and no privileged helper is
	// involved.
	var netRx, netTx int64
	var netNs uint64
	netNsHost := false
	if e.PID > 0 {
		netNs = netNsOf(c.procRoot, e.PID)
		// Reading the host namespace would hand this container the machine's whole
		// traffic - measured at 51 GB on an idle `alpine sleep`. Report nothing and
		// let the flag point the panel at the host chart.
		netNsHost = netNs != 0 && netNs == hostNetNs
		if !netNsHost {
			if raw, ok := readFileTrimmed(filepath.Join(c.procRoot, strconv.Itoa(e.PID), "net", "dev")); ok {
				netRx, netTx = parseNetDev(raw)
			}
		}
	}

	// Identity is copied from the roster, never derived from anything read here:
	// the cgroup path knows an id, not which App a container belongs to.
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

// netNsOf is the inode of a process's network namespace. Two containers reporting
// the same one share a namespace (a compose sidecar on `network_mode: service:x`)
// and their identical counters must be summed ONCE. 0 means unreadable.
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

// cpuPercentFromUsage converts two cumulative usage_usec readings into a percent.
func cpuPercentFromUsage(prevUsec, curUsec int64, elapsed time.Duration) float64 {
	elapsedUsec := float64(elapsed.Microseconds())
	if elapsedUsec <= 0 {
		return 0
	}
	delta := curUsec - prevUsec
	// A counter that went BACKWARDS means the accounting was reset under us - a restart
	// reuses the container id but not its cgroup.
	if delta <= 0 {
		return 0
	}
	return round2(float64(delta) / elapsedUsec * 100)
}

// parseCPUUsageUsec pulls usage_usec (total CPU time, microseconds) out of a cgroup v2
// cpu.stat.
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

// parseMemoryStatInactiveFile pulls inactive_file (reclaimable page cache) out of
// memory.stat.
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

// parseMemoryMax turns memory.max into a byte limit.
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

// parseIOStat sums rbytes= and wbytes= across every device line of io.stat.
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

// parsePidsCurrent reads pids.current, the live task count in the cgroup.
func parsePidsCurrent(content string) (int32, bool) {
	v, err := strconv.ParseInt(strings.TrimSpace(content), 10, 32)
	if err != nil || v < 0 {
		return 0, false
	}
	return int32(v), true
}

// parseNetDev sums the receive/transmit byte counters of a /proc/<pid>/net/dev.
func parseNetDev(content string) (rx, tx int64) {
	for _, line := range strings.Split(content, "\n") {
		// The two header rows carry no colon, so the interface split filters
		// them without needing to count lines.
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
		r, errR := strconv.ParseInt(f[0], 10, 64) // receive bytes
		t, errT := strconv.ParseInt(f[8], 10, 64) // transmit bytes
		if errR != nil || errT != nil {
			continue
		}
		rx += r
		tx += t
	}
	return rx, tx
}

// skipNetIface drops the interfaces that would misreport a container's traffic: `lo` is
// intra-container chatter that `docker stats` never counts, and a veth/bridge device
// can only appear if we are reading the HOST namespace by mistake. Shared with the
// host gauge (hostmetrics.SkipNetIface) so the rule cannot be fixed on one side only.
func skipNetIface(name string) bool {
	return hostmetrics.SkipNetIface(name)
}

// parseMemTotal pulls MemTotal out of /proc/meminfo, converting its kB to bytes.
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

// parseUint64Value reads a cgroup file holding a single non-negative integer.
func parseUint64Value(content string) (int64, bool) {
	v, err := strconv.ParseInt(strings.TrimSpace(content), 10, 64)
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

// readFileTrimmed reads a small procfs/cgroupfs file. ok=false covers both "not
// there" and "not permitted", which callers treat identically: the metric
// degrades, it is never faked.
func readFileTrimmed(path string) (string, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

func round2(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }
