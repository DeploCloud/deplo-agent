package dockercli

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSpawnCancelKillsAGrandchildHoldingThePipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Spawn(ctx, time.Minute, func(string) {}, "", "sh", "-c", "sleep 100 & wait")
	if err == nil || !strings.Contains(err.Error(), "canceled") && !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("took %s: the grandchild kept the pipe open", took)
	}
}

func TestSpawnKeepsReadingPastAnOverlongLine(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	start := time.Now()
	code, err := Spawn(context.Background(), time.Minute, func(l string) {
		mu.Lock()
		lines = append(lines, l)
		mu.Unlock()
	}, "", "sh", "-c", "head -c 9000000 /dev/zero | tr '\\0' x; echo; echo after")
	if err != nil || code != 0 {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if time.Since(start) > 20*time.Second {
		t.Fatalf("took %s: the child stalled on a full pipe", time.Since(start))
	}
	if len(lines) != 2 || !strings.HasSuffix(lines[0], truncatedMark) || lines[1] != "after" {
		t.Fatalf("got %d lines, last %q", len(lines), lines[len(lines)-1])
	}
	if len(lines[0]) != maxLineBytes+len(truncatedMark) {
		t.Fatalf("truncated line is %d bytes", len(lines[0]))
	}
}

func TestScanLines(t *testing.T) {
	var got []string
	err := ScanLines(strings.NewReader("a\r\n\nbcdefg\nlast"), 4, func(l string) { got = append(got, l) })
	want := []string{"a", "", "bcde" + truncatedMark, "last"}
	if err != nil || strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q err=%v", got, err)
	}
}
