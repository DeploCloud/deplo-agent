package hostmetrics

import (
	"testing"
	"time"
)

// These tests read real /proc on the test host, so they assert SHAPE and
// INVARIANTS rather than pinning values — CI runners are shared and noisy, and a
// test that expects a particular CPU or byte count would fail for reasons that
// have nothing to do with this package. The exception is the failure-path tests
// below, which feed the sampler synthetic counters through its reader seam:
// those CAN pin numbers, because nothing about them depends on the host.

// The regression guard this whole file exists for: Sampler's reason to live is
// that it does NOT buy its delta window with a sleep the way Collect does. If
// someone "fixes" an edge case by reintroducing a sleep, the streaming loop
// silently goes back to paying a second per tick — this test fails instead.
//
// It deliberately times a Sample() that CROSSES a usable window, so the rate
// branch is the thing on the clock. Timing back-to-back calls instead would
// return early at the minWindow guard and measure a branch nobody is worried
// about — a sleep added after that guard would sail straight through. The
// baseline assertion is what keeps it honest: it fails if the call under the
// stopwatch ever silently degenerates back into the early return.
func TestSampler_Sample_doesNotSleep(t *testing.T) {
	s := NewSampler("/")
	primed := s.prevAt
	time.Sleep(2 * minWindow) // the TEST may sleep; Sample may not

	start := time.Now()
	s.Sample()
	elapsed := time.Since(start)

	if !s.prevAt.After(primed) {
		t.Fatal("timed a Sample() that returned early at the minWindow guard: the rate path was never on the clock")
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("Sample() over a usable window took %v, want well under 300ms — a sleep has been reintroduced", elapsed)
	}

	// The degenerate branch carries the same guard, so a sleep cannot hide there
	// either.
	start = time.Now()
	s.Sample()
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Errorf("Sample() on a degenerate window took %v, want well under 300ms — a sleep has been reintroduced", d)
	}
}

// Same register as TestCollect_returnsSaneShape: on a real Linux host the basic
// facts hold. Sampled across a genuine window so the rate path is exercised too.
func TestSampler_Sample_returnsSaneShape(t *testing.T) {
	s := NewSampler("/")
	time.Sleep(2 * minWindow) // let a real window form; the TEST may sleep, Sample may not
	m := s.Sample()

	if m.CPUCores < 1 {
		t.Errorf("CPUCores = %d, want >= 1", m.CPUCores)
	}
	if m.MemTotal <= 0 {
		t.Errorf("MemTotal = %d, want > 0", m.MemTotal)
	}
	if m.MemUsed < 0 || m.MemUsed > m.MemTotal {
		t.Errorf("MemUsed = %d out of range [0,%d]", m.MemUsed, m.MemTotal)
	}
	if m.CPU < 0 || m.CPU > 100 {
		t.Errorf("CPU = %f out of range [0,100]", m.CPU)
	}
	if m.NetRx < 0 || m.NetTx < 0 {
		t.Errorf("negative rate: NetRx = %d, NetTx = %d", m.NetRx, m.NetTx)
	}
	if m.MemPct < 0 || m.MemPct > 100 {
		t.Errorf("MemPct = %f out of range [0,100]", m.MemPct)
	}
	if m.DiskTotal < 0 {
		t.Errorf("DiskTotal = %d, want >= 0", m.DiskTotal)
	}
}

// A counter reset between samples must clamp to 0, not wrap negative, AND the
// sampler must recover on the next window: adopting the post-reset counter as
// the new baseline is what makes the tick after a bounce report a real rate
// instead of a since-boot total. Forced deterministically by rewinding the
// stored baseline above any plausible live counter — waiting for a real
// interface to bounce is not a test.
func TestSampler_counterResetClampsToZero(t *testing.T) {
	s := NewSampler("/")
	s.prevAt = time.Now().Add(-time.Second)
	s.prevRx = 1 << 60 // baseline far above the live counter, as after a reset
	s.prevTx = 1 << 60

	m := s.Sample()
	if m.NetRx != 0 || m.NetTx != 0 {
		t.Errorf("after counter reset: NetRx = %d, NetTx = %d, want 0/0", m.NetRx, m.NetTx)
	}

	// Recovery: the following window must report a plausible rate. An idle test
	// host moves a few KB in 100ms; a baseline that was not re-adopted would
	// instead report the whole since-boot byte count divided by that window.
	time.Sleep(2 * minWindow)
	m = s.Sample()
	const sane = 100 << 20 // 100 MB/s, generous for an idle runner
	if m.NetRx < 0 || m.NetRx > sane || m.NetTx < 0 || m.NetTx > sane {
		t.Errorf("after recovery: NetRx = %d, NetTx = %d, want a plausible rate in [0,%d]", m.NetRx, m.NetTx, sane)
	}
}

// A window too short to measure must report rates of 0 and leave the baseline
// ALONE, so the next call measures across the full span instead of being reset
// back to zero-width forever. Forced with a future baseline rather than by
// calling Sample twice quickly, which would depend on how loaded the runner is.
func TestSampler_degenerateWindowKeepsBaseline(t *testing.T) {
	s := NewSampler("/")
	baseline := time.Now().Add(time.Hour)
	s.prevAt = baseline

	m := s.Sample()
	if m.NetRx != 0 || m.NetTx != 0 || m.CPU != 0 {
		t.Errorf("degenerate window: got CPU=%f NetRx=%d NetTx=%d, want all 0", m.CPU, m.NetRx, m.NetTx)
	}
	// Point-in-time fields are still real — a too-short window makes RATES
	// unmeasurable, it does not make the whole sample unmeasurable.
	if m.MemTotal <= 0 {
		t.Errorf("MemTotal = %d, want > 0 even on a degenerate window", m.MemTotal)
	}
	if m.CPUCores < 1 {
		t.Errorf("CPUCores = %d, want >= 1 even on a degenerate window", m.CPUCores)
	}
	if !s.prevAt.Equal(baseline) {
		t.Error("degenerate window advanced the baseline; a hot caller would then never form a usable window")
	}
}

// The complement of the test above: a usable window MUST advance the baseline,
// or every sample would keep diffing against the original priming read and the
// reported rates would drift into a since-startup average.
func TestSampler_goodWindowAdvancesBaseline(t *testing.T) {
	s := NewSampler("/")
	primed := s.prevAt

	time.Sleep(2 * minWindow)
	s.Sample()

	if !s.prevAt.After(primed) {
		t.Error("baseline not advanced after a usable window; rates would decay into a since-start average")
	}
}

// windowed re-arms the sampler so the next Sample() is guaranteed to clear
// minWindow with a known elapsed of ~1s, without the test sleeping for it.
func windowed(s *Sampler) { s.prevAt = time.Now().Add(-time.Second) }

// THE failure path: /proc/net/dev fails to read (fd exhaustion under load, a
// restricted /proc in a container, a hostile ulimit). readNetCounters reports
// that as 0/0, indistinguishable from a real reading, so a sampler that adopted
// it as its baseline would diff the entire since-boot counter against 0 on the
// very next tick — measured at 11.4 GB/s before this was fixed. Driven through
// the reader seam because a healthy host never drops a /proc read on demand,
// and the whole point is that this shipped with only the happy path covered.
func TestSampler_failedNetReadDoesNotPoisonBaseline(t *testing.T) {
	const sinceBoot = int64(1) << 40 // ~1.1 TB, an ordinary long-lived host
	counter, ok := sinceBoot, true
	s := NewSampler("/")
	// Faithful to readNetCounters: a failed read yields ZERO values, which is
	// exactly what makes it indistinguishable from a real reading downstream.
	s.readNet = func() (int64, int64, bool) {
		if !ok {
			return 0, 0, false
		}
		return counter, counter, true
	}

	// A good tick establishes the baseline at the since-boot value.
	windowed(s)
	s.Sample()
	if s.prevRx != sinceBoot {
		t.Fatalf("setup: baseline = %d, want %d", s.prevRx, sinceBoot)
	}

	// The read fails: rates are unmeasured (0) and the baseline must not move.
	ok = false
	windowed(s)
	m := s.Sample()
	if m.NetRx != 0 || m.NetTx != 0 {
		t.Errorf("failed read: NetRx = %d, NetTx = %d, want 0/0 — a failed read is unmeasured, not zero traffic", m.NetRx, m.NetTx)
	}
	if s.prevRx != sinceBoot || s.prevTx != sinceBoot {
		t.Fatalf("failed read advanced the baseline to %d/%d; the next tick would divide a since-boot counter by one tick", s.prevRx, s.prevTx)
	}

	// The read recovers after ~1s having moved 1000 bytes: the rate must be
	// measured across the gap. A poisoned baseline reports ~1.1e12 bytes/sec here.
	ok = true
	counter = sinceBoot + 1000
	windowed(s)
	m = s.Sample()
	if m.NetRx < 900 || m.NetRx > 1010 {
		t.Errorf("after recovery: NetRx = %d, want ~1000 bytes/sec measured across the gap", m.NetRx)
	}
	if m.NetTx < 900 || m.NetTx > 1010 {
		t.Errorf("after recovery: NetTx = %d, want ~1000 bytes/sec measured across the gap", m.NetTx)
	}
}

// The CPU half of the same bug, and the more insidious one: a poisoned CPU
// baseline does not produce an obviously absurd number, it produces the
// since-boot AVERAGE, which looks entirely plausible and so is never noticed.
func TestSampler_failedCPUReadDoesNotPoisonBaseline(t *testing.T) {
	// A long-lived host that has been ~90% idle since boot.
	cur, ok := cpuTimes{idle: 900, total: 1000}, true
	s := NewSampler("/")
	// Faithful to readCPUTimes: a failed read yields a zero cpuTimes.
	s.readCPU = func() (cpuTimes, bool) {
		if !ok {
			return cpuTimes{}, false
		}
		return cur, true
	}

	windowed(s)
	s.Sample()
	if s.prevCPU != cur {
		t.Fatalf("setup: CPU baseline = %+v, want %+v", s.prevCPU, cur)
	}

	ok = false
	windowed(s)
	if m := s.Sample(); m.CPU != 0 {
		t.Errorf("failed read: CPU = %f, want 0 — unmeasured, not idle", m.CPU)
	}
	if s.prevCPU != (cpuTimes{idle: 900, total: 1000}) {
		t.Fatalf("failed read advanced the CPU baseline to %+v; the next tick would report the since-boot average", s.prevCPU)
	}

	// Recovery across a fully busy gap: 100 ticks of total, none of them idle.
	ok = true
	cur = cpuTimes{idle: 900, total: 1100}
	windowed(s)
	if m := s.Sample(); m.CPU != 100 {
		t.Errorf("after recovery: CPU = %f, want 100 — a poisoned baseline would report the since-boot average (~18)", m.CPU)
	}
}

// A failed net read must not drag the CPU baseline down with it, and vice
// versa: the two counters come from different files and fail independently, so
// one unreadable file must not cost the other its window.
func TestSampler_readFailuresAreIndependent(t *testing.T) {
	s := NewSampler("/")
	cpu := cpuTimes{idle: 900, total: 1000}
	s.readCPU = func() (cpuTimes, bool) { return cpu, true }
	s.readNet = func() (int64, int64, bool) { return 0, 0, false }

	windowed(s)
	s.Sample()
	if s.prevCPU != cpu {
		t.Errorf("CPU baseline = %+v, want it advanced to %+v despite the net read failing", s.prevCPU, cpu)
	}

	// prevAt timestamps the NET baseline, so a net failure must leave it behind
	// too — advancing the clock while keeping stale counters would divide a
	// multi-tick delta by a single tick, the same fake spike by another route.
	if time.Since(s.prevAt) < time.Second {
		t.Error("net read failed but prevAt advanced; the next window would be measured too short")
	}
}
