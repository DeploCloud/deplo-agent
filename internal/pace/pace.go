// Package pace runs the agent's copies last: one gives way while one of this server's Apps waits for the CPU or the
// disk more than busyShare of a step, and never drops under Floor (deplo ADR-0043).
package pace

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Step is how much a copy moves between two looks, and Every the longest it goes without one: a slow copy
// (eu, 24 Nov 2026: 8 MB took 5 s) would otherwise run most of the time between capped waits, and the proxy
// waits at most that long behind one.
const (
	Step  = 8 << 20
	Every = 50 * time.Millisecond
)

// Floor is the rate a copy keeps whatever the Apps wait: 300 MB in two and a half minutes (c40, eu at 13 KB/s).
const Floor = 2 << 20

const (
	// busyShare is how much of a step one App may wait for the CPU or the disk before the copy gives way: eu's
	// Apps waited under 1% with only visitors (c40).
	busyShare = 0.10
	// yieldTimes is how many times its last step a copy waits once an App waited: a fifth of the server. Each
	// step one waits again doubles it, up to maxTimes (a thirtieth); each step none does halves it (c38: one
	// copy into eu-2, already busy, timed every App out).
	yieldTimes = 4
	maxTimes   = 32
	maxWait    = 2 * time.Second
)

// Looks counts every look a copy took, so a test can tell a copy is paced.
var Looks atomic.Int64

// Vars so tests drive the clock, the waits and what the Apps waited.
var (
	now   = time.Now
	sleep = time.Sleep
	// appsWaited is how long, in microseconds since some start, each container waited for the CPU or the disk.
	appsWaited = func() map[string]int64 {
		out := map[string]int64{}
		for _, g := range appGroups() {
			out[g] = waited(filepath.Join(g, "cpu.pressure")) + waited(filepath.Join(g, "io.pressure"))
		}
		return out
	}
)

var groups struct {
	sync.Mutex
	at   time.Time
	list []string
}

// appGroups is every container's cgroup, systemd's and cgroupfs's way; the copies' own run in deplocopy.slice.
func appGroups() []string {
	groups.Lock()
	defer groups.Unlock()
	if time.Since(groups.at) > 5*time.Second {
		a, _ := filepath.Glob("/sys/fs/cgroup/system.slice/docker-*.scope")
		b, _ := filepath.Glob("/sys/fs/cgroup/docker/*")
		groups.list, groups.at = append(a, b...), time.Now()
	}
	return groups.list
}

// waited reads the "some ... total=" of a pressure file: microseconds any of its tasks waited.
func waited(path string) int64 {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	line, _, _ := strings.Cut(string(raw), "\n")
	_, total, ok := strings.Cut(line, "total=")
	if !ok {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(total), 10, 64)
	return n
}

// worst is the largest share of took any one container waited between two looks.
func worst(before, after map[string]int64, took time.Duration) float64 {
	var most int64
	for g, t := range after {
		if b, ok := before[g]; ok && t-b > most {
			most = t - b
		}
	}
	return float64(time.Duration(most)*time.Microsecond) / float64(took)
}

// Pacer is one copy's pace. The zero value is ready.
type Pacer struct {
	n     int64
	last  time.Time
	stall map[string]int64
	// times is how many times its last step the copy waits now.
	times time.Duration
}

// Moved notes n bytes the copy moved and, every Step, waits while an App was held up during it.
func (p *Pacer) Moved(n int) {
	t := now()
	if p.n += int64(n); p.n < Step && !p.last.IsZero() && t.Sub(p.last) < Every {
		return
	}
	moved := p.n
	p.n = 0
	Looks.Add(1)
	st := appsWaited()
	if took := t.Sub(p.last); !p.last.IsZero() && took > 0 {
		if worst(p.stall, st, took) > busyShare {
			p.times = min(max(2*p.times, yieldTimes), maxTimes)
		} else {
			p.times /= 2
		}
		wait := time.Duration(0)
		if p.times >= yieldTimes {
			// Never under Floor: the step and its wait together take no longer than its bytes at that rate.
			wait = min(took*p.times, maxWait, time.Duration(moved)*time.Second/Floor-took)
		}
		if wait > 0 {
			sleep(wait)
			t, st = now(), appsWaited()
		}
	}
	p.last, p.stall = t, st
}

type writer struct {
	w io.Writer
	p Pacer
}

func (w *writer) Write(b []byte) (int, error) {
	n, err := w.w.Write(b)
	w.p.Moved(n)
	return n, err
}

// Writer paces what is written to w; nil stays nil.
func Writer(w io.Writer) io.Writer {
	if w == nil {
		return nil
	}
	return &writer{w: w}
}

type reader struct {
	r io.Reader
	p Pacer
}

func (r *reader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	r.p.Moved(n)
	return n, err
}

// Reader paces what is read from r; nil stays nil.
func Reader(r io.Reader) io.Reader {
	if r == nil {
		return nil
	}
	return &reader{r: r}
}
