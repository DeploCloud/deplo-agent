package server

import (
	"time"

	"golang.org/x/sys/unix"
)

// bootClock is now on CLOCK_BOOTTIME, which counts a suspended machine's sleep where Go's monotonic clock does
// not: a lease that ran out while the server slept is over when it wakes (c41).
func bootClock() time.Time {
	return bootBase.Add(time.Duration(bootNanos() - bootBaseNanos))
}

var bootBase, bootBaseNanos = time.Now(), bootNanos()

func bootNanos() int64 {
	var ts unix.Timespec
	if unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts) != nil {
		return time.Since(processStart).Nanoseconds()
	}
	return ts.Nano()
}

var processStart = time.Now()
