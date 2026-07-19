// Package hostmetrics is the agent's host telemetry: a Go port of
// lib/infra/host.ts. It measures the server the agent runs on — CPU from
// /proc/stat deltas, memory from /proc/meminfo, disk via statfs, net from
// /proc/net/dev. No value is fabricated; an unmeasurable field is 0. This is the
// per-server replacement for the control plane measuring only its own host.
package hostmetrics

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Metrics mirrors the proto HostMetrics / the TS HostMetrics shape.
type Metrics struct {
	CPU          float64
	CPUCores     int
	MemUsed      int64
	MemTotal     int64
	MemPct       float64
	DiskUsed     int64
	DiskTotal    int64
	DiskPct      float64
	NetRx        int64 // bytes/sec over the sample window
	NetTx        int64
	Load1        float64
	Load5        float64
	Load15       float64
	UptimeSec    int64
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

	memTotal, memAvail := readMem()
	memUsed := memTotal - memAvail
	if memUsed < 0 {
		memUsed = 0
	}
	diskUsed, diskTotal := diskBytes(dataDir)
	l1, l5, l15 := loadavg()

	m := Metrics{
		CPU:       cpuPercent(cpu0, cpu1),
		CPUCores:  numCPU(),
		MemUsed:   memUsed,
		MemTotal:  memTotal,
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
	if diskTotal > 0 {
		m.DiskPct = round1(float64(diskUsed) / float64(diskTotal) * 100)
	}
	return m
}

type cpuTimes struct{ idle, total uint64 }

// readCPUTimes keeps the lossy shape Collect was written against: a failed read
// is reported as a zero cpuTimes. That is safe for Collect, which diffs two
// reads taken within one call and whose cpuPercent returns 0 when total is 0 —
// there is no baseline living across calls to corrupt. Anything that KEEPS a
// baseline must use readCPUTimesOK instead.
func readCPUTimes() cpuTimes {
	c, _ := readCPUTimesOK()
	return c
}

// readCPUTimesOK is readCPUTimes plus the one bit of information the lossy form
// throws away: whether the numbers are real. Without it a caller cannot tell an
// unreadable /proc/stat from an idle CPU, and a streaming sampler that adopted
// the zero as its next baseline would diff the whole since-boot counter against
// it on the following tick.
func readCPUTimesOK() (cpuTimes, bool) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuTimes{}, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)[1:]
		var total, idle uint64
		for i, fld := range fields {
			v, _ := strconv.ParseUint(fld, 10, 64)
			total += v
			if i == 3 { // idle
				idle = v
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

func readMem() (total, avail int64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		kb, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			total = kb * 1024
		case "MemAvailable:":
			avail = kb * 1024
		}
	}
	return total, avail
}

func diskBytes(path string) (used, total int64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0
	}
	bsize := int64(st.Bsize)
	total = int64(st.Blocks) * bsize
	free := int64(st.Bavail) * bsize
	used = total - free
	if used < 0 {
		used = 0
	}
	return used, total
}

// readNetCounters is the lossy form, kept for Collect — see readCPUTimes for
// why a baseline-keeping caller must not use it.
func readNetCounters() (rx, tx int64) {
	r, t, _ := readNetCountersOK()
	return r, t
}

// readNetCountersOK also reports whether the file was read to the end. A
// half-read /proc/net/dev sums fewer interfaces than the previous read did, so
// it looks exactly like a counter reset; clamping hides the dip, but adopting
// the short total as the next baseline turns the following tick into a fake
// multi-gigabyte spike. An empty-but-clean read (a netns with only lo) is a
// genuine 0 and reports ok.
func readNetCountersOK() (rx, tx int64, ok bool) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		idx := strings.IndexByte(line, ':')
		if idx < 0 {
			continue
		}
		iface := strings.TrimSpace(line[:idx])
		if iface == "lo" {
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
