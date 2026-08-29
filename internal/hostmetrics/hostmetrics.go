// Package hostmetrics is the agent's host telemetry: a Go port of lib/infra/host.ts.
// This is the per-server replacement for the control plane measuring only its own host.
package hostmetrics

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Metrics mirrors the proto HostMetrics / the TS HostMetrics shape.
type Metrics struct {
	CPU       float64
	CPUCores  int
	MemUsed   int64
	MemTotal  int64
	MemPct    float64
	MemFree   int64 // MemFree, so the panel can show htop's split
	MemCache  int64 // Buffers + Cached + SReclaimable
	DiskUsed  int64
	DiskTotal int64
	DiskPct   float64
	NetRx     int64 // bytes/sec over the sample window
	NetTx     int64
	Load1     float64
	Load5     float64
	Load15    float64
	UptimeSec int64
}

// Collect takes a point-in-time snapshot. Like the TS version it samples over a
// ~1s window for CPU and net rates, so it blocks ~1s.
func Collect(dataDir string) Metrics {
	if dataDir == "" {
		dataDir = "/"
	}
	cpu0 := readCPUTimes()
	rx0, tx0 := readNetCounters()

	time.Sleep(time.Second)

	cpu1 := readCPUTimes()
	rx1, tx1 := readNetCounters()

	mem := readMem()
	memUsed := mem.total - mem.available
	if memUsed < 0 {
		memUsed = 0
	}
	memTotal := mem.total
	diskUsed, diskTotal, diskAvail := diskBytes(dataDir)
	l1, l5, l15 := loadavg()

	m := Metrics{
		CPU:       cpuPercent(cpu0, cpu1),
		CPUCores:  numCPU(),
		MemUsed:   memUsed,
		MemTotal:  memTotal,
		MemFree:   mem.free,
		MemCache:  mem.cache,
		DiskUsed:  diskUsed,
		DiskTotal: diskTotal,
		NetRx:     max64(0, rx1-rx0),
		NetTx:     max64(0, tx1-tx0),
		Load1:     l1,
		Load5:     l5,
		Load15:    l15,
		UptimeSec: uptimeSec(),
	}
	if memTotal > 0 {
		m.MemPct = round1(float64(memUsed) / float64(memTotal) * 100)
	}
	m.DiskPct = diskPercent(diskUsed, diskAvail)
	return m
}

// cpuTimes is one /proc/stat reading. `idle` is idle PLUS iowait, and `total`
// stops before guest/guest_nice, which /proc already counts inside user/nice -
// both so the percentage matches what htop's meter shows by default.
type cpuTimes struct{ idle, total uint64 }

// readCPUTimes keeps the lossy shape Collect was written against: a failed read is
// reported as a zero cpuTimes. Anything that KEEPS a baseline must use readCPUTimesOK
// instead.
func readCPUTimes() cpuTimes {
	c, _ := readCPUTimesOK()
	return c
}

// readCPUTimesOK is readCPUTimes plus the one bit of information the lossy form throws
// away: whether the numbers are real.
func readCPUTimesOK() (cpuTimes, bool) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuTimes{}, false
	}
	defer f.Close()
	return parseCPUTimes(f)
}

// parseCPUTimes reads the aggregate "cpu " line of a /proc/stat.
func parseCPUTimes(r io.Reader) (cpuTimes, bool) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)[1:]
		var total, idle uint64
		for i, fld := range fields {
			// user nice system idle iowait irq softirq steal | guest guest_nice
			if i > 7 {
				break
			}
			v, _ := strconv.ParseUint(fld, 10, 64)
			total += v
			if i == 3 || i == 4 { // idle, iowait
				idle += v
			}
		}
		return cpuTimes{idle: idle, total: total}, true
	}
	// Opened but no aggregate "cpu " line, or the scan died part way: either way
	// we have no usable reading, so say so rather than returning a plausible 0.
	return cpuTimes{}, false
}

func cpuPercent(a, b cpuTimes) float64 {
	idle := float64(b.idle - a.idle)
	total := float64(b.total - a.total)
	if total <= 0 {
		return 0
	}
	pct := (1 - idle/total) * 100
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return round1(pct)
}

// memInfo is the slice of /proc/meminfo the panel reports. `available` drives
// MemUsed (total-available, the same figure `free` calls used); free + cache are
// carried so the UI can also show the reclaimable half instead of leaving the
// difference from htop unexplained.
type memInfo struct{ total, available, free, cache int64 }

func readMem() memInfo {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return memInfo{}
	}
	defer f.Close()
	var m memInfo
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		kb, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			m.total = kb * 1024
		case "MemAvailable:":
			m.available = kb * 1024
		case "MemFree:":
			m.free = kb * 1024
		case "Buffers:", "Cached:", "SReclaimable:":
			m.cache += kb * 1024
		}
	}
	return m
}

// diskBytes reports the same three numbers `df` does: Size, Used and Avail.
// Used counts the root-reserved blocks (Bfree, not Bavail) and the percentage
// divides by used+avail, both so a full filesystem reads identically in the
// panel and in a shell.
func diskBytes(path string) (used, total, avail int64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, 0
	}
	bsize := int64(st.Bsize)
	total = int64(st.Blocks) * bsize
	avail = int64(st.Bavail) * bsize
	used = total - int64(st.Bfree)*bsize
	if used < 0 {
		used = 0
	}
	return used, total, avail
}

// diskPercent is df's Use%: the reserved blocks are neither used nor available.
func diskPercent(used, avail int64) float64 {
	denom := used + avail
	if denom <= 0 {
		return 0
	}
	return round1(float64(used) / float64(denom) * 100)
}

// readNetCounters is the lossy form, kept for Collect - see readCPUTimes for
// why a baseline-keeping caller must not use it.
func readNetCounters() (rx, tx int64) {
	r, t, _ := readNetCountersOK()
	return r, t
}

// readNetCountersOK also reports whether the file was read to the end.
func readNetCountersOK() (rx, tx int64, ok bool) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	return parseNetCounters(f)
}

// parseNetCounters sums the byte counters of a /proc/net/dev, skipping every
// interface whose traffic is already counted elsewhere - see SkipNetIface.
func parseNetCounters(r io.Reader) (rx, tx int64, ok bool) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		idx := strings.IndexByte(line, ':')
		if idx < 0 {
			continue
		}
		iface := strings.TrimSpace(line[:idx])
		if SkipNetIface(iface) {
			continue
		}
		fields := strings.Fields(line[idx+1:])
		if len(fields) < 9 {
			continue
		}
		r, _ := strconv.ParseInt(fields[0], 10, 64) // rx bytes
		t, _ := strconv.ParseInt(fields[8], 10, 64) // tx bytes
		rx += r
		tx += t
	}
	if sc.Err() != nil {
		return 0, 0, false
	}
	return rx, tx, true
}

// SkipNetIface drops the interfaces whose bytes are a second copy of somebody
// else's: `lo` never leaves the box, and a Docker bridge (docker0, br-<id>) plus
// the host end of each container veth all mirror what the physical NIC counted.
// Summing them made a 10 MB download read as a 10 MB upload.
func SkipNetIface(name string) bool {
	return name == "lo" ||
		strings.HasPrefix(name, "veth") ||
		strings.HasPrefix(name, "docker") ||
		strings.HasPrefix(name, "br-")
}

func loadavg() (l1, l5, l15 float64) {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0
	}
	fields := strings.Fields(string(b))
	if len(fields) < 3 {
		return 0, 0, 0
	}
	l1, _ = strconv.ParseFloat(fields[0], 64)
	l5, _ = strconv.ParseFloat(fields[1], 64)
	l15, _ = strconv.ParseFloat(fields[2], 64)
	return round2(l1), round2(l5), round2(l15)
}

func uptimeSec() int64 {
	b, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(fields[0], 64)
	return int64(v)
}

func numCPU() int {
	n := 0
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 1
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "cpu") && len(line) > 3 && line[3] >= '0' && line[3] <= '9' {
			n++
		}
	}
	if n == 0 {
		return 1
	}
	return n
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }
func round2(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }
