package hostmetrics

import (
	"strings"
	"testing"
)

const procNetDev = `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 13606586770 28748142    0    0    0     0          0         0 13606586770 28748142    0    0    0     0       0          0
  ens3: 10705001  12000    0    0    0     0          0         0   182241   4000    0    0    0     0       0          0
docker0:    87445    900    0    0    0     0          0         0 10640877  11000    0    0    0     0       0          0
br-b9bf039ae034:     5053    40    0    0    0     0          0         0     4552     38    0    0    0     0       0          0
vethe31fa30:   223689   1200    0    0    0     0          0         0     8727    300    0    0    0     0       0          0
vethca9a922:      261      4    0    0    0     0          0         0     1022     12    0    0    0     0       0          0
`

func TestParseNetCounters_countsOnlyThePhysicalInterface(t *testing.T) {
	rx, tx, ok := parseNetCounters(strings.NewReader(procNetDev))
	if !ok {
		t.Fatal("parseNetCounters reported failure on a well-formed file")
	}
	if rx != 10705001 || tx != 182241 {
		t.Errorf("got rx=%d tx=%d, want ens3's own 10705001/182241 - a bridge or veth leaked in", rx, tx)
	}
}

func TestSkipNetIface(t *testing.T) {
	for _, name := range []string{"lo", "docker0", "br-b9bf039ae034", "vethe31fa30"} {
		if !SkipNetIface(name) {
			t.Errorf("%s should be skipped: its bytes are a second copy", name)
		}
	}
	for _, name := range []string{"ens3", "eth0", "enp3s0", "vmbr0", "bond0"} {
		if SkipNetIface(name) {
			t.Errorf("%s must be counted", name)
		}
	}
}

// iowait is time the CPU spent doing nothing while a disk answered.
func TestParseCPUTimes_iowaitIsIdleAndGuestIsNotDoubleCounted(t *testing.T) {
	const stat = "cpu  100  0    100    600  200    0   0       0     50    0\ncpu0 1 2 3 4 5 6 7 8 9 10\n"
	c, ok := parseCPUTimes(strings.NewReader(stat))
	if !ok {
		t.Fatal("parseCPUTimes reported failure")
	}
	if c.total != 1000 {
		t.Errorf("total = %d, want 1000 (fields 0-7 only, guest excluded)", c.total)
	}
	if c.idle != 800 {
		t.Errorf("idle = %d, want 800 (idle 600 + iowait 200)", c.idle)
	}
	prev := cpuTimes{}
	if pct := cpuPercent(prev, c); pct != 20 {
		t.Errorf("cpuPercent = %v, want 20 (200 busy of 1000; iowait is not busy)", pct)
	}
}

// df calls the root-reserved blocks neither used nor available, so its Use% is used/(used+avail) and not used/size.
func TestDiskPercent_matchesDf(t *testing.T) {
	const gib = int64(1) << 30
	if got := diskPercent(132*gib, 110*gib); got != 54.5 {
		t.Errorf("diskPercent = %v, want 54.5 (df reports 55%%)", got)
	}
	if got := diskPercent(0, 0); got != 0 {
		t.Errorf("an unreadable filesystem = %v, want 0", got)
	}
}
