package hostmetrics

import "time"

// sampler.go exists because Collect is the wrong shape for a streaming loop:
// it blocks on time.Sleep(1s) purely to manufacture a delta window, and reports
// NetRx/NetTx as the RAW delta over that window — only accidentally "bytes/sec"
// because the window happens to be one second. Drive it from a 5s ticker and you
// pay a wasted second per tick AND under-report net rates by 5x.
//
// Sampler keeps the previous counter reading instead: successive Sample() calls
// diff against it and divide by the REAL elapsed time, so the cadence is the
// caller's business. Collect stays untouched — it is the contract of the unary
// Metrics RPC, whose one-shot caller has no previous reading and genuinely has
// to buy its window with a sleep.

// minWindow is the shortest elapsed time we are willing to divide by. Below it
// the deltas are dominated by read jitter and /proc's own update granularity,
// and dividing a one-tick delta by a few hundred microseconds manufactures a
// spectacular fake spike. An unmeasurable field is 0; we do not invent one.
const minWindow = 50 * time.Millisecond

// Sampler holds the previous CPU/net counters so successive Sample() calls
// derive rates from the REAL elapsed time between them instead of sleeping.
// Not safe for concurrent use — one Sampler per stream.
type Sampler struct {
	dataDir string

	// Baseline: the counters as of prevAt. Advanced only by a Sample() that both
	// measured a usable window and actually READ the counter (see Sample).
	prevCPU   cpuTimes
	prevCPUOK bool
	prevRx    int64
	prevTx    int64
	prevNetOK bool
	prevAt    time.Time

	// Injection seam for the failure-path tests. A dropped /proc read is the one
	// case that corrupts a rate baseline, and it cannot be waited on: on a healthy
	// test host /proc always reads. Held per-Sampler rather than as package vars so
	// overriding one test's reader cannot leak into another's.
	readCPU func() (cpuTimes, bool)
	readNet func() (rx, tx int64, ok bool)
}

// NewSampler primes the baseline by reading the counters NOW, so the first
// Sample() one tick later already spans a real window. This is why priming
// lives here and not in Sample: with a zero baseline the first sample would
// divide the machine's entire since-boot byte count by one tick and report it
// as the current rate — a garbage spike every consumer would have to
// special-case. There is no throwaway first sample.
//
// Sample() called before any window exists is the one honest exception: it
// reports the point-in-time fields (mem/disk/load/uptime/cores) truthfully and
// leaves the rate fields at 0 rather than inventing them.
//
// dataDir is the filesystem whose usage is reported; "" means "/", as in Collect.
func NewSampler(dataDir string) *Sampler {
	if dataDir == "" {
		dataDir = "/"
	}
	s := &Sampler{
		dataDir: dataDir,
		readCPU: readCPUTimesOK,
		readNet: readNetCountersOK,
		prevAt:  time.Now(),
	}
	s.prevCPU, s.prevCPUOK = s.readCPU()
	s.prevRx, s.prevTx, s.prevNetOK = s.readNet()
	return s
}

// Sample takes a snapshot WITHOUT blocking. CPU and net are rates over the
// window since the previous Sample (or since NewSampler); memory, disk, load,
// uptime and cores are point-in-time, read exactly as Collect reads them.
func (s *Sampler) Sample() Metrics {
	now := time.Now()
	cpu, cpuOK := s.readCPU()
	rx, tx, netOK := s.readNet()

	memTotal, memAvail := readMem()
	memUsed := memTotal - memAvail
	if memUsed < 0 {
		memUsed = 0
	}
	diskUsed, diskTotal := diskBytes(s.dataDir)
	l1, l5, l15 := loadavg()

	m := Metrics{
		CPUCores:  numCPU(),
		MemUsed:   memUsed,
		MemTotal:  memTotal,
		DiskUsed:  diskUsed,
		DiskTotal: diskTotal,
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

	// Degenerate window — two calls effectively back-to-back. Report the rates as
	// 0 (unmeasured, not fabricated) and do NOT advance the baseline: keeping the
	// last GOOD reading means the next call measures across the whole span. Advancing
	// here would let a hot caller starve itself forever, resetting the baseline
	// faster than a window can form and reporting a permanent flat zero.
	//
	// time.Now carries a monotonic reading, so this cannot go negative from a
	// wall-clock jump; the guard is about cadence, not clock skew.
	window := now.Sub(s.prevAt)
	if window < minWindow {
		return m
	}
	elapsed := window.Seconds()

	// A FAILED read must advance NOTHING — the same discipline as the degenerate
	// window above, and for a sharper reason. readCPUTimes/readNetCounters report
	// failure as a zero value indistinguishable from a real 0, so adopting it as
	// the baseline makes the NEXT tick diff a full since-boot counter against 0 and
	// divide by one tick: a measured 11.4 GB/s on a host that has never seen it.
	// Leaving the baseline alone instead makes the following window measure honestly
	// across the gap, and costs only this tick's rate, which we report as 0.
	if cpuOK && s.prevCPUOK {
		// cpuPercent is already a ratio of deltas — self-normalising over any
		// window, and clamped to [0,100] — so it needs no elapsed-time division,
		// and it stays correct when the window spans a skipped tick.
		m.CPU = cpuPercent(s.prevCPU, cpu)
	}
	if netOK && s.prevNetOK {
		// max64 clamps a counter RESET (interface bounced, netns recreated) to 0
		// instead of emitting a negative rate; the next window measures normally
		// because the reset value below becomes the new baseline.
		m.NetRx = perSecond(max64(0, rx-s.prevRx), elapsed)
		m.NetTx = perSecond(max64(0, tx-s.prevTx), elapsed)
	}

	if cpuOK {
		s.prevCPU, s.prevCPUOK = cpu, true
	}
	// prevAt is the timestamp the NET baseline was taken at, so it advances with
	// that baseline and not without it: advancing the clock while keeping stale
	// counters would divide a multi-tick delta by a single tick. CPU needs no
	// timestamp, which is why the two can advance independently.
	if netOK {
		s.prevRx, s.prevTx, s.prevNetOK = rx, tx, true
		s.prevAt = now
	}
	return m
}

// perSecond converts a non-negative byte delta over `elapsed` seconds into
// bytes/sec, rounded to nearest so a slow trickle does not truncate to a flat 0
// and read as "no traffic". Callers must have rejected a degenerate elapsed
// first — this function does not guard the division.
func perSecond(delta int64, elapsed float64) int64 {
	return int64(float64(delta)/elapsed + 0.5)
}
