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
)

// cgroupstats.go is the zero-subprocess data source for container metrics: it
// reads cgroup v2 and /proc directly instead of shelling out to `docker stats`.
//
// WHY it exists, measured on a real host carrying 44 containers: one
// `docker stats --no-stream` over that set costs ~3.2% of a core per 5s poll and
// BLOCKS for 2.16 seconds. The identical numbers read straight off the cgroup
// files cost ~0.18% and ~9ms. That 18x is the whole reason for this file — and
// the 2.16s stall matters as much as the CPU, because a poll that occupies half
// its own 5s interval cannot be tightened and drifts under load.
//
// The daemon is also the wrong clock. `docker stats` computes CPU against ITS
// collection window; sampling here means we timestamp our own readings, so the
// rate is computed over the interval that actually elapsed rather than one the
// daemon estimated (see cpuPercentFromUsage for the algebra).
//
// This backend supplies NUMBERS ONLY. Identity — name, project, state, health,
// restart count — comes from the roster, the one place that still talks to
// docker. And nothing here invents a sample: a container whose files cannot be
// read is ABSENT from the result, never a zeroed row. A zeroed row is worse than
// a gap, because it draws a confident flat line through a hole in the data and
// the operator reads it as "idle" instead of "unknown".

// cgroup2SuperMagic identifies a unified (v2) hierarchy; from linux/magic.h.
const cgroup2SuperMagic = 0x63677270

// cgroupUnhealthyTicks is how many CONSECUTIVE ticks may fail wholesale before
// the backend admits it does not work on this host. Three, not one: a single
// empty tick is normal during a stack recreation, when every container's cgroup
// is legitimately gone for a moment. A structural problem (an unexpected
// delegation layout, a hierarchy we cannot traverse) fails every tick forever,
// so three is enough to tell the two apart without flapping.
const cgroupUnhealthyTicks = 3

// cgroup2Available reports whether this host can use the cgroup v2 backend.
//
// v1 and the "hybrid" layout scatter every controller across its own mount
// (memory/, cpu,cpuacct/, blkio/) with different filenames AND different
// semantics — memory.usage_in_bytes includes page cache in a place the v2 file
// does not, so a shared parser would silently over-report on half the fleet.
// Rather than carry two of everything, detect the unified hierarchy and let
// every other layout fall back to the `docker stats` path, which works anywhere.
func cgroup2Available() bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs("/sys/fs/cgroup", &st); err != nil {
		return false
	}
	return int64(st.Type) == cgroup2SuperMagic
}

// cgroupPrevCPU is the previous CPU reading for one container, with the instant
// it was taken. Both halves are required: a cumulative counter alone cannot
// produce a rate, and borrowing the poll interval instead of the real elapsed
// time reintroduces exactly the daemon-clock error this backend removes.
type cgroupPrevCPU struct {
	usageUsec int64
	at        time.Time
}

type cgroupSampler struct {
	// sampleMu serializes Sample calls; mu guards the mutable state below. Two
	// locks rather than one because a tick issues ~7 file reads per container
	// (~300 syscalls on the 44-container host this file was measured against),
	// and Unhealthy() — the health check the caller polls to decide whether to
	// demote to `docker stats` — must not queue behind that IO. mu is therefore
	// taken only around the map swap and the counters, never across a read.
	sampleMu sync.Mutex

	mu sync.Mutex

	// prev is keyed by CONTAINER ID, never by name. A recreated container keeps
	// its name but gets a fresh id and a fresh (zeroed) cgroup; keying by name
	// would subtract the old container's usage from the new one's and emit a
	// large negative delta on every redeploy. This is precisely why container_id
	// was added to the proto.
	prev map[string]cgroupPrevCPU

	fails     int
	unhealthy bool

	// machineMem substitutes for an unlimited memory.max. Read once at
	// construction: installed RAM cannot change under a running kernel, and
	// re-reading /proc/meminfo per container per tick would give back a chunk of
	// the win this file exists to capture.
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

// Unhealthy reports that this backend has failed wholesale for
// cgroupUnhealthyTicks consecutive ticks and should be abandoned. It LATCHES:
// the caller is expected to demote to `docker stats` for the rest of the process
// lifetime, and a backend that flickered back to life would otherwise invite it
// to flap between two sources with different rounding on every tick.
func (c *cgroupSampler) Unhealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unhealthy
}

// Sample returns one stat per entry. A RUNNING container whose files cannot be
// read is omitted entirely (a fabricated zero would read as idle). A NON-RUNNING
// container is emitted with identity, its real state and zeroed usage — matching
// the docker-stats backend, so a stopped container never vanishes from the stream
// depending on which backend this host uses.
func (c *cgroupSampler) Sample(entries []rosterEntry, now time.Time) []*pb.ContainerStat {
	c.sampleMu.Lock()
	defer c.sampleMu.Unlock()

	// prev is only ever REPLACED, never mutated in place, and only Sample
	// replaces it — which sampleMu already serializes. Taking the reference
	// under the state lock and then reading the map lock-free is therefore safe
	// and keeps mu off the read loop.
	c.mu.Lock()
	prev := c.prev
	c.mu.Unlock()

	next := make(map[string]cgroupPrevCPU, len(entries))
	out := make([]*pb.ContainerStat, 0, len(entries))

	// Health accounting counts only entries that OUGHT to be readable, i.e.
	// running ones. Without that qualifier a host whose containers are all
	// stopped — every read correctly failing, because a stopped container has no
	// cgroup — would declare the backend broken and permanently demote itself to
	// the slow path it was built to replace.
	expected, failed := 0, 0

	for _, e := range entries {
		if e.State != "running" {
			// A non-running container has no cgroup to read, but it must NOT vanish
			// from the stream. The docker-stats backend reports stopped and crashed
			// containers too — with identity, their real state and zeroed usage —
			// and the two backends have to agree on MEMBERSHIP, or a container would
			// disappear from the control plane's view depending only on which path
			// this host happens to use. A zeroed row is honest here, unlike the
			// false-idle a zeroed RUNNING row would draw: State carries the truth
			// and a stopped container genuinely uses nothing. Deliberately NOT
			// counted in the health accounting below — a missing cgroup is EXPECTED
			// for a stopped container, never the structural breakage
			// cgroupUnhealthyTicks exists to detect.
			st := &pb.ContainerStat{}
			applyIdentity(st, e, false)
			out = append(out, st)
			carryCPUBaseline(next, prev, e.ID)
			continue
		}
		expected++
		if e.CgroupPath == "" {
			// The roster could not resolve a path for a RUNNING container; there is
			// nothing to read and nothing to guess at. This counts as
			// EXPECTED-AND-FAILED: unresolvable paths are the exact structural
			// breakage cgroupUnhealthyTicks exists to detect, and skipping the
			// accounting here would leave `expected` at 0 forever, so the demotion
			// to `docker stats` could never fire on the one failure mode that most
			// needs it — the operator would just get permanently empty charts.
			failed++
			carryCPUBaseline(next, prev, e.ID)
			continue
		}
		r, ok := c.readOne(e, now, prev)
		if !ok {
			failed++
			carryCPUBaseline(next, prev, e.ID)
			continue
		}
		if r.haveUsage {
			next[e.ID] = cgroupPrevCPU{usageUsec: r.usageUsec, at: now}
		} else {
			// A FAILED read advances NOTHING. Priming with the 0 that
			// parseCPUUsageUsec returns on failure would make the next tick
			// difference a full since-start cumulative counter against zero and
			// emit a fabricated spike — the very thing the unprimed rule below
			// exists to prevent, arrived at from the other direction. Keeping the
			// older baseline instead means the next successful read measures
			// honestly ACROSS the gap.
			carryCPUBaseline(next, prev, e.ID)
		}
		out = append(out, r.stat)
	}

	// REPLACING the map rather than updating it is what drops a vanished
	// container: an id absent from this tick is absent from `next`, so its stale
	// counter can never be differenced against a future container, and the map
	// cannot grow without bound across months of redeploys. Carrying a baseline
	// forward above is scoped to ids still in THIS tick's roster, so it cannot
	// resurrect a vanished one.
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

// readOne reads one container's files. ok=false means the container could not
// be read at all and must be omitted from the tick entirely.
//
// Degradation is PER METRIC, not per sample. Rootless docker delegates only the
// memory and pids controllers by default, so io.stat is simply absent there;
// that must cost block IO and nothing else. Dropping the whole container would
// blank a rootless host's charts entirely over one optional file.
//
// Memory is degraded the same way, and that asymmetry with the file header's
// "a zeroed row is worse than a gap" deserves saying out loud: MemUsed 0 next to
// a real MemLimit does read as a false idle. It is accepted because the two
// cases are not symmetric — the header's rule is about a container we cannot see
// AT ALL, which is why an unreadable cgroup is dropped wholesale above, whereas
// reaching here means the cgroup demonstrably exists and only one file inside it
// blipped, for one tick, on a metric the control plane charts as a rate it can
// interpolate. Requiring memOK to emit anything would trade a one-tick low
// reading for dropping every other metric of a container we can prove is there.
func (c *cgroupSampler) readOne(e rosterEntry, now time.Time, prev map[string]cgroupPrevCPU) (cgroupRead, bool) {
	cpuRaw, cpuOK := readFileTrimmed(filepath.Join(e.CgroupPath, "cpu.stat"))
	memRaw, memOK := readFileTrimmed(filepath.Join(e.CgroupPath, "memory.current"))

	// The cgroup directory is the container's proof of life. cpu.stat and
	// memory.current exist in every configuration including rootless, so if
	// NEITHER can be read the cgroup is gone (exited, or never ours) and the
	// honest answer is to report nothing.
	if !cpuOK && !memOK {
		return cgroupRead{}, false
	}

	usage, haveUsage := parseCPUUsageUsec(cpuRaw)
	cpuPct := 0.0
	if haveUsage {
		// An id seen for the first time is UNPRIMED: there is no earlier reading
		// to difference against, so it reports 0 for exactly one tick. The
		// alternative — treating the cumulative counter as if it were a delta —
		// prints a container's entire lifetime of CPU as one instantaneous spike
		// the moment it appears.
		if p, ok := prev[e.ID]; ok {
			cpuPct = cpuPercentFromUsage(p.usageUsec, usage, now.Sub(p.at))
		}
	}

	var memUsed int64
	if memOK {
		current, _ := parseUint64Value(memRaw)
		// memory.current MINUS inactive_file. The subtraction is mandatory:
		// memory.current charges reclaimable page cache to the container, so
		// reporting it raw shows a database that has merely read its own files
		// as sitting at 100% of its limit. That is Coolify's open bug #7230, and
		// this subtraction is what makes the number match `docker stats` to the
		// byte.
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

	// /proc/<pid>/net/dev read FROM THE HOST returns the CONTAINER's namespace
	// counters — verified against a live host, where the container's eth0 showed
	// 33,481,934 rx bytes while the host's eth0 showed 1,188,594,390. procfs
	// resolves the network files through the target task's netns, so no setns,
	// no nsenter and no privileged helper is involved.
	var netRx, netTx int64
	if e.PID > 0 {
		if raw, ok := readFileTrimmed(filepath.Join(c.procRoot, strconv.Itoa(e.PID), "net", "dev")); ok {
			netRx, netTx = parseNetDev(raw)
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
	}, usageUsec: usage, haveUsage: haveUsage}, true
}

// cpuPercentFromUsage converts two cumulative usage_usec readings into a percent.
//
// THE ALGEBRA, documented because the formula looks like it is missing a term:
// docker computes (cpu_delta / system_delta) * ncpu * 100, where system_delta is
// the host's total busy time across ALL cores over the same window. Over a
// window of `elapsed`, system_delta IS ncpu * elapsed — so the two ncpu factors
// cancel and the whole expression reduces exactly to
// usage_delta / elapsed * 100. Nothing is approximated away; /proc/stat is
// simply not needed, and timing the window ourselves is strictly more accurate
// than adopting the daemon's jittery collection interval.
//
// Semantics match `docker stats`: this is per-core percent summed, so 100 means
// one core saturated and a busy 4-core container legitimately reads ~400.
func cpuPercentFromUsage(prevUsec, curUsec int64, elapsed time.Duration) float64 {
	elapsedUsec := float64(elapsed.Microseconds())
	if elapsedUsec <= 0 {
		return 0
	}
	delta := curUsec - prevUsec
	// A counter that went BACKWARDS means the accounting was reset under us — a
	// restart reuses the container id but not its cgroup. Report 0 rather than a
	// negative, which would render as a downward spike and, worse, poison any
	// average the control plane takes over the window.
	if delta <= 0 {
		return 0
	}
	return round2(float64(delta) / elapsedUsec * 100)
}

// parseCPUUsageUsec pulls usage_usec (total CPU time, microseconds) out of a
// cgroup v2 cpu.stat. The file also carries user_usec, system_usec, nr_periods,
// nr_throttled, throttled_usec and — on newer kernels — burst counters; every
// one of them is ignored, so a kernel that adds another field cannot break this.
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

// parseMemoryStatInactiveFile pulls inactive_file (reclaimable page cache) out
// of memory.stat. Absent line means 0 — an honest floor, since the effect of
// missing it is only that we report page cache as used, never that we invent a
// container's memory out of nothing.
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
//
// VERIFIED: the file holds the literal string "max" when the container is
// unconstrained, which is the common case — nobody sets a limit by default.
// Substitute total machine memory, which is what moby itself reports, so the
// percentage means "share of the machine" instead of dividing by zero and
// rendering an empty gauge for almost every container on the host.
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
//
// Per-device totals are summed rather than reported separately because that is
// what the proto's block_read / block_write promise and what `docker stats`
// shows: a container spanning an overlay device and a mounted volume does IO on
// both, and splitting it would just move the summing to the control plane.
// Lines are keyed by "major:minor"; anything else is a kernel format we do not
// recognise and is skipped rather than guessed at.
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
// Cumulative totals, matching the proto's contract for net_rx / net_tx — the
// control plane differences consecutive samples, which is also what lets it
// survive a counter reset on restart.
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

// skipNetIface drops the interfaces that would misreport a container's traffic:
// `lo` is intra-container chatter that `docker stats` never counts, and a
// veth*/docker* device can only appear if we are reading the HOST namespace by
// mistake — counting those would attribute the entire host bridge's throughput
// to one container, which looks plausible enough that nobody would question it.
func skipNetIface(name string) bool {
	return name == "lo" || strings.HasPrefix(name, "veth") || strings.HasPrefix(name, "docker")
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
