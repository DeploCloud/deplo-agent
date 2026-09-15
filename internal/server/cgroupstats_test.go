package server

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const goldenCPUStat = `usage_usec 1000000
user_usec 700000
system_usec 300000
nr_periods 0
nr_throttled 0
throttled_usec 0
`

const goldenMemoryStat = `anon 12582912
file 8388608
kernel_stack 65536
slab 1048576
sock 0
inactive_anon 0
active_anon 12582912
inactive_file 4194304
active_file 4194304
unevictable 0
`

const goldenIOStat = `259:0 rbytes=1024000 wbytes=2048000 rios=100 wios=200 dbytes=0 dios=0
253:1 rbytes=512000 wbytes=256000 rios=50 wios=25 dbytes=0 dios=0
`

const goldenNetDev = `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:    5000      50    0    0    0     0          0         0     5000      50    0    0    0     0       0          0
  eth0: 33481934   24567    0    0    0     0          0         0  4211234   19876    0    0    0     0       0          0
  eth1:  1000000     100    0    0    0     0          0         0   500000      50    0    0    0     0       0          0
`

func TestParseCPUUsageUsec(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int64
		wantOK  bool
	}{
		{"typical with extra fields", goldenCPUStat, 1000000, true},
		{"usage_usec last", "user_usec 5\nusage_usec 987654321\n", 987654321, true},
		{"no usage_usec line", "user_usec 700000\nsystem_usec 300000\n", 0, false},
		{"non-numeric value", "usage_usec notanumber\n", 0, false},
		{"empty file", "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseCPUUsageUsec(tt.content)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("parseCPUUsageUsec = (%d, %v), want (%d, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestParseMemoryStatInactiveFile(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int64
	}{
		{"with inactive_file", goldenMemoryStat, 4194304},
		{"without inactive_file", "anon 12582912\nfile 8388608\n", 0},
		{"malformed value", "inactive_file whoops\n", 0},
		{"empty", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseMemoryStatInactiveFile(tt.content); got != tt.want {
				t.Errorf("parseMemoryStatInactiveFile = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseMemoryMax(t *testing.T) {
	const machineMem = int64(8589934592)
	tests := []struct {
		name    string
		content string
		want    int64
	}{
		{"unlimited substitutes machine memory", "max\n", machineMem},
		{"explicit limit", "536870912\n", 536870912},
		{"garbage is zero, not machine memory", "banana", 0},
		{"empty is zero", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseMemoryMax(tt.content, machineMem); got != tt.want {
				t.Errorf("parseMemoryMax(%q) = %d, want %d", tt.content, got, tt.want)
			}
		})
	}
}

func TestParseIOStat(t *testing.T) {
	tests := []struct {
		name                string
		content             string
		wantRead, wantWrite int64
	}{
		{"multiple devices sum", goldenIOStat, 1536000, 2304000},
		{"single device", "259:0 rbytes=100 wbytes=200 rios=1 wios=2\n", 100, 200},
		{
			"malformed line skipped, good line still counted",
			"this is not a device line\n259:0 rbytes=100 wbytes=200\n",
			100, 200,
		},
		{
			"unparseable value skipped without losing the rest",
			"259:0 rbytes=oops wbytes=200\n",
			0, 200,
		},
		{"empty", "", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, w := parseIOStat(tt.content)
			if r != tt.wantRead || w != tt.wantWrite {
				t.Errorf("parseIOStat = (%d, %d), want (%d, %d)", r, w, tt.wantRead, tt.wantWrite)
			}
		})
	}
}

func TestParsePidsCurrent(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int32
		wantOK  bool
	}{
		{"typical", "42\n", 42, true},
		{"zero", "0", 0, true},
		{"garbage", "max", 0, false},
		{"empty", "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parsePidsCurrent(tt.content)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("parsePidsCurrent(%q) = (%d, %v), want (%d, %v)", tt.content, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestParseNetDev(t *testing.T) {
	tests := []struct {
		name           string
		content        string
		wantRx, wantTx int64
	}{
		{"multiple interfaces summed, lo excluded", goldenNetDev, 34481934, 4711234},
		{
			"veth and docker0 excluded",
			"  eth0: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0\n" +
				"veth1a2b3c: 999999 9 0 0 0 0 0 0 888888 8 0 0 0 0 0 0\n" +
				"docker0: 777777 7 0 0 0 0 0 0 666666 6 0 0 0 0 0 0\n",
			100, 200,
		},
		{
			"malformed line skipped, good line still counted",
			"  eth0: 100 1 0 0\n  eth1: 500 5 0 0 0 0 0 0 700 7 0 0 0 0 0 0\n",
			500, 700,
		},
		{"header only", "Inter-|   Receive                |  Transmit\n face |bytes\n", 0, 0},
		{"empty", "", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rx, tx := parseNetDev(tt.content)
			if rx != tt.wantRx || tx != tt.wantTx {
				t.Errorf("parseNetDev = (%d, %d), want (%d, %d)", rx, tx, tt.wantRx, tt.wantTx)
			}
		})
	}
}

func TestCPUPercentFromUsage(t *testing.T) {
	tests := []struct {
		name    string
		prev    int64
		cur     int64
		elapsed time.Duration
		want    float64
	}{
		{"half a second over five seconds", 1000000, 1500000, 5 * time.Second, 10},
		{"multicore exceeds 100", 0, 10000000, 5 * time.Second, 200},
		{"idle container", 1000000, 1000000, 5 * time.Second, 0},
		{"counter went backwards", 5000000, 100, 5 * time.Second, 0},
		{"zero elapsed", 1000000, 2000000, 0, 0},
		{"negative elapsed (clock stepped)", 1000000, 2000000, -time.Second, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cpuPercentFromUsage(tt.prev, tt.cur, tt.elapsed)
			if got != tt.want {
				t.Errorf("cpuPercentFromUsage(%d, %d, %v) = %v, want %v", tt.prev, tt.cur, tt.elapsed, got, tt.want)
			}
			if got < 0 {
				t.Errorf("cpuPercentFromUsage returned a negative percentage: %v", got)
			}
		})
	}
}

func TestParseMemTotal(t *testing.T) {
	const meminfo = `MemTotal:        8388608 kB
MemFree:          123456 kB
MemAvailable:    4194304 kB
`
	if got := parseMemTotal(meminfo); got != 8388608*1024 {
		t.Errorf("parseMemTotal = %d, want %d", got, int64(8388608*1024))
	}
	if got := parseMemTotal("MemFree: 123 kB\n"); got != 0 {
		t.Errorf("parseMemTotal(no MemTotal) = %d, want 0", got)
	}
}

// cgroup2Available must agree with the presence of the unified hierarchy's root interface file: /sys/fs/cgroup/cgroup.controllers exists only when cgroup v2 is mounted AT that path (on the hybrid layout v2 lives under unified/, so the root has no such file).
func TestCgroup2Available(t *testing.T) {
	_, err := os.Stat("/sys/fs/cgroup/cgroup.controllers")
	wantV2 := err == nil
	if got := cgroup2Available(); got != wantV2 {
		t.Errorf("cgroup2Available() = %v, but cgroup.controllers present = %v", got, wantV2)
	}
}

func newTestSampler(t *testing.T, procRoot string) *cgroupSampler {
	t.Helper()
	c := newCgroupSampler()
	c.procRoot = procRoot
	c.machineMem = 8589934592
	return c
}

func writeCgroupFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

func writeProcNetDev(t *testing.T, procRoot string, pid int, body string) {
	t.Helper()
	dir := filepath.Join(procRoot, strconv.Itoa(pid), "net")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dev"), []byte(body), 0o644); err != nil {
		t.Fatalf("write net/dev: %v", err)
	}
}

func fullCgroup() map[string]string {
	return map[string]string{
		"cpu.stat":       goldenCPUStat,
		"memory.current": "104857600\n",
		"memory.stat":    goldenMemoryStat,
		"memory.max":     "max\n",
		"io.stat":        goldenIOStat,
		"pids.current":   "42\n",
	}
}

func TestCgroupSampler_ReadsEveryMetricAndCarriesRosterIdentity(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg", "container-1")
	proc := filepath.Join(tmp, "proc")
	writeCgroupFiles(t, cg, fullCgroup())
	writeProcNetDev(t, proc, 4242, goldenNetDev)

	c := newTestSampler(t, proc)
	e := rosterEntry{
		ID:           "abc123def456",
		Name:         "deplo-web-1",
		ProjectID:    "prj_web",
		State:        "running",
		Health:       "healthy",
		RestartCount: 3,
		PID:          4242,
		CgroupPath:   cg,
	}

	out := c.Sample([]rosterEntry{e}, time.Unix(1000, 0))
	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1", len(out))
	}
	st := out[0]

	if st.Name != "deplo-web-1" || st.ProjectId != "prj_web" || st.ContainerId != "abc123def456" {
		t.Errorf("identity = %q/%q/%q, want deplo-web-1/prj_web/abc123def456", st.Name, st.ProjectId, st.ContainerId)
	}
	if st.State != "running" || st.Health != "healthy" || st.RestartCount != 3 || !st.Running {
		t.Errorf("state fields = %q/%q/%d/%v, want running/healthy/3/true", st.State, st.Health, st.RestartCount, st.Running)
	}

	if st.CpuPct != 0 {
		t.Errorf("first-tick CpuPct = %v, want 0 (unprimed)", st.CpuPct)
	}

	if st.MemUsed != 100663296 {
		t.Errorf("MemUsed = %d, want 100663296 (current - inactive_file)", st.MemUsed)
	}
	if st.MemLimit != 8589934592 {
		t.Errorf("MemLimit = %d, want 8589934592", st.MemLimit)
	}
	if st.MemPct != 1.17 {
		t.Errorf("MemPct = %v, want 1.17", st.MemPct)
	}
	if st.BlockRead != 1536000 || st.BlockWrite != 2304000 {
		t.Errorf("block = %d/%d, want 1536000/2304000", st.BlockRead, st.BlockWrite)
	}
	if st.Pids != 42 {
		t.Errorf("Pids = %d, want 42", st.Pids)
	}
	if st.NetRx != 34481934 || st.NetTx != 4711234 {
		t.Errorf("net = %d/%d, want 34481934/4711234", st.NetRx, st.NetTx)
	}
}

func TestCgroupSampler_SecondTickProducesTheCPURate(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg")
	proc := filepath.Join(tmp, "proc")
	writeCgroupFiles(t, cg, fullCgroup())
	writeProcNetDev(t, proc, 7, goldenNetDev)

	c := newTestSampler(t, proc)
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", PID: 7, CgroupPath: cg}

	t0 := time.Unix(2000, 0)
	c.Sample([]rosterEntry{e}, t0)

	writeCgroupFiles(t, cg, map[string]string{"cpu.stat": "usage_usec 1500000\nuser_usec 1\n"})
	out := c.Sample([]rosterEntry{e}, t0.Add(5*time.Second))
	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1", len(out))
	}
	if out[0].CpuPct != 10 {
		t.Errorf("CpuPct = %v, want 10", out[0].CpuPct)
	}
}

func TestCgroupSampler_BackwardsCounterNeverGoesNegative(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg")
	writeCgroupFiles(t, cg, fullCgroup())

	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", CgroupPath: cg}

	t0 := time.Unix(3000, 0)
	c.Sample([]rosterEntry{e}, t0)

	writeCgroupFiles(t, cg, map[string]string{"cpu.stat": "usage_usec 250\n"})
	out := c.Sample([]rosterEntry{e}, t0.Add(5*time.Second))
	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1", len(out))
	}
	if out[0].CpuPct != 0 {
		t.Errorf("CpuPct after a reset = %v, want 0", out[0].CpuPct)
	}
}

func TestCgroupSampler_VanishedIDIsDroppedAndComesBackUnprimed(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg")
	writeCgroupFiles(t, cg, fullCgroup())

	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", CgroupPath: cg}

	t0 := time.Unix(4000, 0)
	c.Sample([]rosterEntry{e}, t0)

	c.Sample(nil, t0.Add(5*time.Second))
	if _, ok := c.prev["c1"]; ok {
		t.Fatal("previous sample for a vanished container id was retained")
	}

	writeCgroupFiles(t, cg, map[string]string{"cpu.stat": "usage_usec 999000000\n"})
	out := c.Sample([]rosterEntry{e}, t0.Add(10*time.Second))
	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1", len(out))
	}
	if out[0].CpuPct != 0 {
		t.Errorf("CpuPct on re-appearance = %v, want 0 (unprimed)", out[0].CpuPct)
	}
}

func TestCgroupSampler_DegradesPerMetricNotPerSample(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg")
	writeCgroupFiles(t, cg, map[string]string{
		"cpu.stat":       goldenCPUStat,
		"memory.current": "104857600\n",
		"memory.stat":    goldenMemoryStat,
		"pids.current":   "9\n",
	})

	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", CgroupPath: cg}

	out := c.Sample([]rosterEntry{e}, time.Unix(5000, 0))
	if len(out) != 1 {
		t.Fatalf("a container missing io.stat/memory.max must still be reported; got %d stats", len(out))
	}
	st := out[0]
	if st.MemUsed != 100663296 || st.Pids != 9 {
		t.Errorf("delegated metrics lost: MemUsed=%d Pids=%d", st.MemUsed, st.Pids)
	}
	if st.BlockRead != 0 || st.BlockWrite != 0 {
		t.Errorf("undelegated block IO = %d/%d, want 0/0", st.BlockRead, st.BlockWrite)
	}
	if st.MemLimit != 0 || st.MemPct != 0 {
		t.Errorf("unreadable memory.max gave limit=%d pct=%v, want 0/0", st.MemLimit, st.MemPct)
	}
	if st.NetRx != 0 || st.NetTx != 0 {
		t.Errorf("net without a PID = %d/%d, want 0/0", st.NetRx, st.NetTx)
	}
}

// A RUNNING container the backend cannot read is ABSENT, never a zeroed row: a flat line at zero reads as "idle" instead of the "unknown" that actually happened.
func TestCgroupSampler_UnreadableEntriesAreAbsentNotZeroed(t *testing.T) {
	tmp := t.TempDir()
	c := newTestSampler(t, filepath.Join(tmp, "proc"))

	entries := []rosterEntry{
		{ID: "gone", Name: "old", ProjectID: "prj_1", State: "exited", CgroupPath: filepath.Join(tmp, "nope")},
		{ID: "unresolved", Name: "mystery", ProjectID: "prj_1", State: "running", CgroupPath: ""},
	}
	out := c.Sample(entries, time.Unix(6000, 0))
	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1 (the exited container zeroed; the running-unresolved one dropped)", len(out))
	}
	st := out[0]
	if st.ContainerId != "gone" || st.State != "exited" || st.Running {
		t.Errorf("emitted row = id %q/state %q/running %v, want gone/exited/false", st.ContainerId, st.State, st.Running)
	}
	if st.CpuPct != 0 || st.MemUsed != 0 {
		t.Errorf("stopped container carried non-zero usage: cpu=%v mem=%d", st.CpuPct, st.MemUsed)
	}
}

func TestCgroupSampler_UnhealthyAfterConsecutiveTotalFailures(t *testing.T) {
	tmp := t.TempDir()
	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", CgroupPath: filepath.Join(tmp, "missing")}

	now := time.Unix(7000, 0)
	for i := 1; i < cgroupUnhealthyTicks; i++ {
		c.Sample([]rosterEntry{e}, now.Add(time.Duration(i)*time.Second))
		if c.Unhealthy() {
			t.Fatalf("declared unhealthy after %d ticks, want only after %d", i, cgroupUnhealthyTicks)
		}
	}
	c.Sample([]rosterEntry{e}, now.Add(time.Duration(cgroupUnhealthyTicks)*time.Second))
	if !c.Unhealthy() {
		t.Fatalf("still healthy after %d total failures", cgroupUnhealthyTicks)
	}
}

func TestCgroupSampler_OneGoodReadResetsTheFailureRun(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg")
	writeCgroupFiles(t, cg, fullCgroup())

	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	bad := rosterEntry{ID: "bad", Name: "b", ProjectID: "prj_1", State: "running", CgroupPath: filepath.Join(tmp, "missing")}
	good := rosterEntry{ID: "good", Name: "g", ProjectID: "prj_1", State: "running", CgroupPath: cg}

	now := time.Unix(8000, 0)
	c.Sample([]rosterEntry{bad}, now)
	c.Sample([]rosterEntry{bad}, now.Add(time.Second))
	c.Sample([]rosterEntry{bad, good}, now.Add(2*time.Second))
	for i := 3; i < 3+cgroupUnhealthyTicks-1; i++ {
		c.Sample([]rosterEntry{bad}, now.Add(time.Duration(i)*time.Second))
	}
	if c.Unhealthy() {
		t.Error("a successful read must reset the consecutive-failure run")
	}
}

// A tick that could not read usage_usec must not PRIME the baseline.
func TestCgroupSampler_UnreadableCPUDoesNotPrimeTheBaseline(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg")
	writeCgroupFiles(t, cg, map[string]string{
		"memory.current": "104857600\n",
		"memory.stat":    goldenMemoryStat,
	})

	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", CgroupPath: cg}

	t0 := time.Unix(11000, 0)
	out := c.Sample([]rosterEntry{e}, t0)
	if len(out) != 1 {
		t.Fatalf("a container missing only cpu.stat must still be reported; got %d stats", len(out))
	}
	if _, primed := c.prev["c1"]; primed {
		t.Fatal("an unmeasured usage_usec was stored as a CPU baseline; the next tick will fabricate a spike")
	}

	writeCgroupFiles(t, cg, map[string]string{"cpu.stat": "usage_usec 3600000000\n"})
	out = c.Sample([]rosterEntry{e}, t0.Add(5*time.Second))
	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1", len(out))
	}
	if out[0].CpuPct != 0 {
		t.Errorf("CpuPct = %v, want 0 (still unprimed - a cumulative counter is not a delta)", out[0].CpuPct)
	}
}

// A failed read advances NOTHING: an existing baseline keeps BOTH its value and its timestamp, so the next successful read measures honestly across the gap rather than either restarting from scratch or dividing a two-window delta by one window.
func TestCgroupSampler_FailedCPUReadKeepsTheOlderBaseline(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg")
	writeCgroupFiles(t, cg, fullCgroup())

	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", CgroupPath: cg}

	t0 := time.Unix(12000, 0)
	c.Sample([]rosterEntry{e}, t0)

	if err := os.Remove(filepath.Join(cg, "cpu.stat")); err != nil {
		t.Fatalf("remove cpu.stat: %v", err)
	}
	out := c.Sample([]rosterEntry{e}, t0.Add(5*time.Second))
	if len(out) != 1 {
		t.Fatalf("during the gap: got %d stats, want the row kept alive by memory.current", len(out))
	}
	if out[0].CpuPct != 0 {
		t.Fatalf("during the gap: CpuPct = %v, want 0 (unmeasured)", out[0].CpuPct)
	}

	writeCgroupFiles(t, cg, map[string]string{"cpu.stat": "usage_usec 2000000\n"})
	out = c.Sample([]rosterEntry{e}, t0.Add(10*time.Second))
	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1", len(out))
	}
	if out[0].CpuPct != 10 {
		t.Errorf("CpuPct across the gap = %v, want 10 (1s of CPU over the full 10s the baseline spans)", out[0].CpuPct)
	}
}

// Same rule for a tick where the whole cgroup was unreadable and the row was dropped: the container is still in the roster, so its baseline survives.
func TestCgroupSampler_WhollyUnreadableTickKeepsTheBaseline(t *testing.T) {
	tmp := t.TempDir()
	cg := filepath.Join(tmp, "cg")
	writeCgroupFiles(t, cg, fullCgroup())

	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", CgroupPath: cg}

	t0 := time.Unix(13000, 0)
	c.Sample([]rosterEntry{e}, t0)

	blind := e
	blind.CgroupPath = filepath.Join(tmp, "missing")
	if out := c.Sample([]rosterEntry{blind}, t0.Add(5*time.Second)); len(out) != 0 {
		t.Fatalf("got %d stats for an unreadable cgroup, want 0", len(out))
	}
	if _, ok := c.prev["c1"]; !ok {
		t.Fatal("baseline dropped by a failed read; the container is still rostered, only the read failed")
	}

	writeCgroupFiles(t, cg, map[string]string{"cpu.stat": "usage_usec 2000000\n"})
	out := c.Sample([]rosterEntry{e}, t0.Add(10*time.Second))
	if len(out) != 1 {
		t.Fatalf("got %d stats, want 1", len(out))
	}
	if out[0].CpuPct != 10 {
		t.Errorf("CpuPct across the gap = %v, want 10", out[0].CpuPct)
	}
}

// Path resolution breaking wholesale is THE failure mode the demotion exists for; if an unresolved path skipped the health accounting, `expected` would sit at 0 forever and the fallback could never fire - empty charts, no recovery.
func TestCgroupSampler_UnresolvedPathsTripTheUnhealthyLatch(t *testing.T) {
	tmp := t.TempDir()
	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "running", CgroupPath: ""}

	now := time.Unix(14000, 0)
	for i := 1; i < cgroupUnhealthyTicks; i++ {
		c.Sample([]rosterEntry{e}, now.Add(time.Duration(i)*time.Second))
		if c.Unhealthy() {
			t.Fatalf("declared unhealthy after %d ticks, want only after %d", i, cgroupUnhealthyTicks)
		}
	}
	c.Sample([]rosterEntry{e}, now.Add(time.Duration(cgroupUnhealthyTicks)*time.Second))
	if !c.Unhealthy() {
		t.Fatal("unresolved cgroup paths never demote the backend, so a host that cannot resolve any path is stuck with empty charts")
	}
}

// The reason this backend exists is that `docker stats --no-stream` BLOCKS for 2.16s over 44 containers.
func TestCgroupSampler_SampleDoesNotBlock(t *testing.T) {
	tmp := t.TempDir()
	proc := filepath.Join(tmp, "proc")
	entries := make([]rosterEntry, 0, 20)
	for i := 0; i < 20; i++ {
		id := "c" + strconv.Itoa(i)
		cg := filepath.Join(tmp, "cg", id)
		writeCgroupFiles(t, cg, fullCgroup())
		writeProcNetDev(t, proc, 1000+i, goldenNetDev)
		entries = append(entries, rosterEntry{
			ID: id, Name: id, ProjectID: "prj_1", State: "running", PID: 1000 + i, CgroupPath: cg,
		})
	}

	c := newTestSampler(t, proc)
	start := time.Now()
	out := c.Sample(entries, time.Unix(15000, 0))
	elapsed := time.Since(start)
	if len(out) != len(entries) {
		t.Fatalf("got %d stats, want %d", len(out), len(entries))
	}
	if elapsed > 250*time.Millisecond {
		t.Errorf("Sample took %v for %d containers; this backend replaces a 2.16s blocking call and must not acquire one of its own", elapsed, len(entries))
	}
}

func TestCgroupSampler_StoppedContainersNeverMarkTheBackendUnhealthy(t *testing.T) {
	tmp := t.TempDir()
	c := newTestSampler(t, filepath.Join(tmp, "proc"))
	e := rosterEntry{ID: "c1", Name: "web", ProjectID: "prj_1", State: "exited", CgroupPath: filepath.Join(tmp, "missing")}

	now := time.Unix(9000, 0)
	for i := 0; i < cgroupUnhealthyTicks*3; i++ {
		c.Sample([]rosterEntry{e}, now.Add(time.Duration(i)*time.Second))
	}
	if c.Unhealthy() {
		t.Error("stopped containers must not count as backend failures")
	}
}
