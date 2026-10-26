package server

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A picture is never worth slowing the apps down: below this headroom it waits for the next try.
const (
	captureMaxCPUBusy  = 0.85
	captureMinMemAvail = 600 << 20
)

var (
	procStat        = "/proc/stat"
	procMeminfo     = "/proc/meminfo"
	cpuSampleWindow = time.Second
)

func captureHeadroom(ctx context.Context) error {
	if avail, ok := memAvailable(); ok && avail < captureMinMemAvail {
		return status.Error(codes.ResourceExhausted, "This server is short of free memory, so the picture waits for the next deploy.")
	}
	if busy, ok := cpuBusy(ctx, cpuSampleWindow); ok && busy > captureMaxCPUBusy {
		return status.Error(codes.ResourceExhausted, "This server is busy right now, so the picture waits for the next deploy.")
	}
	return nil
}

func memAvailable() (uint64, bool) {
	b, err := os.ReadFile(procMeminfo)
	if err != nil {
		return 0, false
	}
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == "MemAvailable:" {
			kb, err := strconv.ParseUint(f[1], 10, 64)
			return kb << 10, err == nil
		}
	}
	return 0, false
}

// cpuBusy is the share of the window the whole host spent not idle, steal included.
func cpuBusy(ctx context.Context, window time.Duration) (float64, bool) {
	idle0, total0, ok := cpuTimes()
	if !ok {
		return 0, false
	}
	select {
	case <-ctx.Done():
		return 0, false
	case <-time.After(window):
	}
	idle1, total1, ok := cpuTimes()
	if !ok || total1 <= total0 {
		return 0, false
	}
	return 1 - float64(idle1-idle0)/float64(total1-total0), true
}

func cpuTimes() (idle, total uint64, ok bool) {
	b, err := os.ReadFile(procStat)
	if err != nil {
		return 0, 0, false
	}
	line, _, _ := strings.Cut(string(b), "\n")
	f := strings.Fields(line)
	if len(f) < 9 || f[0] != "cpu" {
		return 0, 0, false
	}
	for i, v := range f[1:9] {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		total += n
		if i == 3 || i == 4 {
			idle += n
		}
	}
	return idle, total, true
}
