package server

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func boottime(t *testing.T) time.Duration {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		t.Fatal(err)
	}
	return time.Duration(ts.Nano())
}

// The lease's clock is CLOCK_BOOTTIME: what passes on it is what the boot clock counted, a machine's sleep included.
func TestBootClock_countsWhatTheBootClockCounts(t *testing.T) {
	b0, c0 := boottime(t), bootClock()
	time.Sleep(50 * time.Millisecond)
	b1, c1 := boottime(t), bootClock()
	if d := c1.Sub(c0) - (b1 - b0); d < -time.Millisecond || d > time.Millisecond {
		t.Fatalf("the lease's clock moved %v, the boot clock %v", c1.Sub(c0), b1-b0)
	}
}

func TestLeaseGuard_runsOnTheBootClock(t *testing.T) {
	g := (&Service{agentDir: t.TempDir()}).leaseGuard()
	if reflect.ValueOf(g.now).Pointer() != reflect.ValueOf(bootClock).Pointer() {
		t.Fatal("the lease guard does not count its leases on the boot clock")
	}
}
