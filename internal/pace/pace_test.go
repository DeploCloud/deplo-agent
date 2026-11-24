package pace

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fake drives the clock: each step of a copy takes 100 ms, and the Apps wait what the test says.
func fake(t *testing.T, waitedPerStep func(i int) int64) (slept *[]time.Duration) {
	t.Helper()
	clock := time.Unix(1000, 0)
	var stall int64
	i := 0
	var out []time.Duration
	oldNow, oldSleep, oldWaited := now, sleep, appsWaited
	now = func() time.Time { return clock }
	sleep = func(d time.Duration) { out = append(out, d); clock = clock.Add(d) }
	appsWaited = func() int64 { return stall }
	t.Cleanup(func() { now, sleep, appsWaited = oldNow, oldSleep, oldWaited; lastApp.Store(0) })
	step := func() {
		clock = clock.Add(100 * time.Millisecond)
		stall += waitedPerStep(i)
		i++
	}
	stepFn = step
	t.Cleanup(func() { stepFn = nil })
	return &out
}

var stepFn func()

func copySteps(p *Pacer, n int) {
	p.Moved(0)
	for range n {
		stepFn()
		p.Moved(Step)
	}
}

func TestACopyOnAnIdleServerNeverWaits(t *testing.T) {
	slept := fake(t, func(int) int64 { return 1000 }) // 1 ms of 100: under a fifth
	copySteps(&Pacer{}, 10)
	if len(*slept) != 0 {
		t.Fatalf("waited %v on an idle server", *slept)
	}
}

func TestACopyStepsAsideWhileAppsWait(t *testing.T) {
	slept := fake(t, func(i int) int64 {
		if i == 3 {
			return 30_000 // 30 ms of a 100 ms step
		}
		return 0
	})
	copySteps(&Pacer{}, 6)
	if len(*slept) != 1 || (*slept)[0] != 400*time.Millisecond {
		t.Fatalf("want one wait of 4 steps, got %v", *slept)
	}
}

func TestAForwardedRequestMakesACopyWait(t *testing.T) {
	slept := fake(t, func(int) int64 { return 0 })
	p := &Pacer{}
	p.Moved(0)
	stepFn()
	Touch()
	p.Moved(Step)
	stepFn()
	p.Moved(Step)
	if len(*slept) != 1 {
		t.Fatalf("want one wait after the request, got %v", *slept)
	}
}

func TestAWaitIsCapped(t *testing.T) {
	slept := fake(t, func(int) int64 { return 0 })
	p := &Pacer{}
	p.Moved(0)
	stepFn()
	now = func() time.Time { return time.Unix(1010, 0) } // a 10 s step
	Touch()
	p.Moved(Step)
	if len(*slept) != 1 || (*slept)[0] != maxWait {
		t.Fatalf("want a wait of %v, got %v", maxWait, *slept)
	}
}

func TestWriterAndReaderPaceWhatTheyMove(t *testing.T) {
	looks := 0
	old := appsWaited
	appsWaited = func() int64 { looks++; return 0 }
	t.Cleanup(func() { appsWaited = old })
	data := bytes.Repeat([]byte{1}, 3*Step)
	if _, err := io.Copy(Writer(io.Discard), Reader(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	if looks < 6 {
		t.Fatalf("looked %d times for 3 steps each way", looks)
	}
}

func TestWaitedReadsSomeTotal(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cpu.pressure")
	_ = os.WriteFile(p, []byte("some avg10=1.00 avg60=0.50 avg300=0.10 total=123456\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=99\n"), 0o644)
	if got := waited(p); got != 123456 {
		t.Fatalf("got %d", got)
	}
	if waited(filepath.Join(t.TempDir(), "missing")) != 0 {
		t.Fatal("a missing file waited")
	}
}

// eu, 24 Nov 2026: one 8 MB step of a copy took 5 s, so a wait capped at a second let it run 83% of the time.
func TestASlowCopyLooksEveryFifthOfASecond(t *testing.T) {
	clock := time.Unix(1000, 0)
	var slept []time.Duration
	oldNow, oldSleep, oldWaited := now, sleep, appsWaited
	now = func() time.Time { return clock }
	sleep = func(d time.Duration) { slept = append(slept, d); clock = clock.Add(d) }
	appsWaited = func() int64 { return 0 }
	t.Cleanup(func() { now, sleep, appsWaited = oldNow, oldSleep, oldWaited; lastApp.Store(0) })
	p := &Pacer{}
	p.Moved(0)
	// 64 KB every 50 ms while a request crosses the agent: far from a Step, it still looks and waits.
	for range 10 {
		clock = clock.Add(50 * time.Millisecond)
		Touch()
		p.Moved(64 << 10)
	}
	if len(slept) < 2 {
		t.Fatalf("a slow copy waited %v", slept)
	}
}
