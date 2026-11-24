// Package pace runs the agent's copies last: one waits whenever this server's Apps waited for the CPU or the disk,
// or an App's request crossed the agent, while it moved its last step (deplo ADR-0043).
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

// Step is how much a copy moves between two looks.
const Step = 8 << 20

const (
	// busyShare is how long, of a step, the Apps may wait before the copy steps aside.
	busyShare = 20
	// yieldTimes is how many times its last step a copy then waits: it takes at most a fifth of the server.
	yieldTimes = 4
	maxWait    = time.Second
)

var lastApp atomic.Int64

// Looks counts every look a copy took, so a test can tell a copy is paced.
var Looks atomic.Int64

// Touch notes that an App's traffic just crossed this agent.
func Touch() { lastApp.Store(now().UnixNano()) }

// TouchedSince says whether an App's traffic crossed this agent after t.
func TouchedSince(t time.Time) bool { return lastApp.Load() > t.UnixNano() }

// Vars so tests drive the clock, the waits and what the Apps waited.
var (
	now   = time.Now
	sleep = time.Sleep
	// appsWaited is how long, in microseconds since some start, this server's Apps waited for the CPU or the disk.
	appsWaited = func() int64 {
		var total int64
		for _, g := range appGroups() {
			total += waited(filepath.Join(g, "cpu.pressure")) + waited(filepath.Join(g, "io.pressure"))
		}
		return total
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

// Pacer is one copy's pace. The zero value is ready.
type Pacer struct {
	n     int64
	last  time.Time
	stall int64
}

// Moved notes n bytes the copy moved and, every Step, waits while the Apps were held up during it.
func (p *Pacer) Moved(n int) {
	if p.n += int64(n); p.n < Step && !p.last.IsZero() {
		return
	}
	p.n = 0
	Looks.Add(1)
	t, st := now(), appsWaited()
	if !p.last.IsZero() {
		took := t.Sub(p.last)
		if took > 0 && (time.Duration(st-p.stall)*time.Microsecond > took/busyShare || TouchedSince(p.last)) {
			sleep(min(took*yieldTimes, maxWait))
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
