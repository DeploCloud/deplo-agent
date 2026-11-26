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
	appsWaited = func() map[string]int64 { return map[string]int64{"app": stall} }
	t.Cleanup(func() { now, sleep, appsWaited = oldNow, oldSleep, oldWaited })
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

// c39: every visitor crossing the agent paused a copy, and a 300 MB backup ran at 13 KB/s.
func TestAppsThatBarelyWaitNeverHoldACopy(t *testing.T) {
	slept := fake(t, func(int) int64 { return 6000 }) // 6 ms of 100
	copySteps(&Pacer{}, 10)
	if len(*slept) != 0 {
		t.Fatalf("waited %v while the Apps waited 6%% of each step", *slept)
	}
}

// c39: ten containers each waiting a little added up, and the forwarders alone held every copy.
func TestSmallWaitsOfManyContainersDoNotAddUp(t *testing.T) {
	slept := fake(t, func(int) int64 { return 0 })
	var stall int64
	appsWaited = func() map[string]int64 {
		out := map[string]int64{}
		for _, g := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
			out[g] = stall
		}
		return out
	}
	p := &Pacer{}
	p.Moved(0)
	for range 10 {
		stepFn()
		stall += 3000 // 3 ms of 100 each, 30 ms in all
		p.Moved(Step)
	}
	if len(*slept) != 0 {
		t.Fatalf("waited %v for ten containers that each waited 3%%", *slept)
	}
}

// c40: a copy gives way to Apps that keep waiting, but never runs slower than Floor.
func TestACopyNeverDropsUnderTheFloor(t *testing.T) {
	clock := time.Unix(1000, 0)
	var slept []time.Duration
	var stall int64
	oldNow, oldSleep, oldWaited := now, sleep, appsWaited
	now = func() time.Time { return clock }
	sleep = func(d time.Duration) { slept = append(slept, d); clock = clock.Add(d) }
	appsWaited = func() map[string]int64 { return map[string]int64{"app": stall} }
	t.Cleanup(func() { now, sleep, appsWaited = oldNow, oldSleep, oldWaited })
	p := &Pacer{}
	p.Moved(0)
	start := clock
	const chunk = 1 << 20
	for range 40 {
		clock = clock.Add(Every)
		stall += int64(Every / time.Microsecond / 2) // the Apps wait half of every step
		p.Moved(chunk)
	}
	if len(slept) == 0 {
		t.Fatal("a copy never gave way to Apps that kept waiting")
	}
	if rate := float64(40*chunk) / clock.Sub(start).Seconds(); rate < Floor*0.99 {
		t.Fatalf("a copy ran at %.0f bytes a second, under the floor of %d", rate, Floor)
	}
}

func TestWriterAndReaderPaceWhatTheyMove(t *testing.T) {
	looks := 0
	old := appsWaited
	appsWaited = func() map[string]int64 { looks++; return nil }
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
func TestASlowCopyLooksEveryFiftiethOfASecond(t *testing.T) {
	clock := time.Unix(1000, 0)
	oldNow, oldWaited := now, appsWaited
	now = func() time.Time { return clock }
	appsWaited = func() map[string]int64 { return nil }
	t.Cleanup(func() { now, appsWaited = oldNow, oldWaited })
	p := &Pacer{}
	p.Moved(0)
	before := Looks.Load()
	// 64 KB every 50 ms: far from a Step, it still looks every time.
	for range 10 {
		clock = clock.Add(Every)
		p.Moved(64 << 10)
	}
	if got := Looks.Load() - before; got != 10 {
		t.Fatalf("a slow copy looked %d times in 10 steps", got)
	}
}

// c38: one copy into eu-2, whose Apps already waited for the CPU, timed every App through it out.
func TestACopyBacksOffWhileAppsKeepWaitingAndComesBack(t *testing.T) {
	slept := fake(t, func(i int) int64 {
		if i < 6 {
			return 30_000
		}
		return 0
	})
	copySteps(&Pacer{}, 10)
	want := []time.Duration{400, 800, 1600, 2000, 2000, 2000, 1600, 800, 400}
	for i := range want {
		want[i] *= time.Millisecond
	}
	if len(*slept) != len(want) {
		t.Fatalf("waited %v, want %v", *slept, want)
	}
	for i := range want {
		if (*slept)[i] != want[i] {
			t.Fatalf("waited %v, want %v", *slept, want)
		}
	}
}

// c46: the second move onto one member waited stopped 10 to 17 s while other copies took its disk.
func TestFirst_everyPacedCopyWaitsUntilItEnds(t *testing.T) {
	release := First()
	moved := make(chan struct{})
	go func() {
		var p Pacer
		p.Moved(Step)
		close(moved)
	}()
	select {
	case <-moved:
		t.Fatal("a paced copy went on while a first one ran")
	case <-time.After(200 * time.Millisecond):
	}
	release()
	select {
	case <-moved:
	case <-time.After(time.Second):
		t.Fatal("the paced copy still waits after the first one ended")
	}
}
