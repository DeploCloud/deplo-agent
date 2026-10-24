package server

import (
	"fmt"
	"slices"
	"time"
)

const (
	readySettle    = 10 * time.Second // up this long with no restart before a container counts
	readyPortGrace = 10 * time.Second // how long the expected port may lag behind another one
	workerGrace    = 20 * time.Second // after settling, how long a process may take to open a port
)

// readySample is one inspect of a container the ready check watches.
type readySample struct {
	id, name, service string
	status            string
	restarts          int
	startedAt         time.Time
	pid               int
	exitCode          int
	oomKilled         bool
	health            string
	restartPolicy     string
	listeners         []listener
}

// readyCheck decides, from successive samples, whether a deploy is up (see DeployRequest.ready).
type readyCheck struct {
	port     int32
	required bool
	service  string
	stack    bool

	first    map[string]readySample
	opened   map[int32]time.Time
	loopback time.Time
	pending  string
}

type readyVerdict struct {
	done, ok  bool
	err       string // the failure
	note      string // a line for the log on success
	observed  int32
	listening []int32
	culprit   string // the container whose log explains the failure
	exitCode  int
}

func newReadyCheck(port int32, required bool, service string, stack bool) *readyCheck {
	return &readyCheck{port: port, required: required, service: service, stack: stack,
		first: map[string]readySample{}, opened: map[int32]time.Time{}}
}

func (r *readyCheck) who(c readySample) string {
	if !r.stack {
		return "The app"
	}
	return "Service " + orDefault(c.service, c.name)
}

func (r *readyCheck) fail(c readySample, msg string) readyVerdict {
	return readyVerdict{done: true, err: msg, culprit: c.id, exitCode: c.exitCode}
}

func (r *readyCheck) observe(now time.Time, samples []readySample) readyVerdict {
	settled := len(samples) > 0
	switch {
	case settled:
		r.pending = "The app did not stay up long enough to count as started"
	case r.stack:
		r.pending = "No container of the stack was found"
	default:
		r.pending = "The app's container was not found"
	}
	var ports []readySample
	var latest time.Time
	for _, c := range samples {
		base, seen := r.first[c.id]
		if !seen || base.startedAt.IsZero() {
			r.first[c.id], base = c, c
		}
		who := r.who(c)
		switch {
		case c.status == "restarting" || c.restarts > base.restarts || !c.startedAt.Equal(base.startedAt):
			if c.oomKilled {
				return r.fail(c, who+" was stopped: out of memory")
			}
			return r.fail(c, fmt.Sprintf("%s keeps restarting (exit code %d)", who, c.exitCode))
		case c.status == "exited" || c.status == "dead":
			if r.stack && c.exitCode == 0 && slices.Contains([]string{"", "no", "on-failure"}, c.restartPolicy) {
				continue // a one-shot that finished
			}
			if c.oomKilled {
				return r.fail(c, who+" was stopped: out of memory")
			}
			return r.fail(c, fmt.Sprintf("%s stopped right after it started (exit code %d)", who, c.exitCode))
		case c.health == "unhealthy":
			return r.fail(c, who+"'s health check fails")
		}
		switch {
		case c.status != "running":
			settled, r.pending = false, who+" did not start (it is "+c.status+")"
		case c.health == "starting":
			settled, r.pending = false, who+" never passed its health check"
		case now.Sub(c.startedAt) < readySettle:
			settled = false
		}
		if r.service == "" || c.service == r.service {
			ports = append(ports, c)
			if c.startedAt.After(latest) {
				latest = c.startedAt
			}
		}
	}
	var all []listener
	for _, c := range ports {
		all = append(all, c.listeners...)
	}
	public, loop := splitListeners(all)
	for _, p := range public {
		if _, ok := r.opened[p]; !ok {
			r.opened[p] = now
		}
	}
	if len(loop) > 0 && r.loopback.IsZero() {
		r.loopback = now
	}
	v := readyVerdict{listening: public}
	if !settled || r.port <= 0 {
		v.done, v.ok = settled, settled
		return v
	}
	if len(ports) == 0 {
		r.pending = "Service " + r.service + " has no container"
		return v
	}
	who, culprit := r.who(ports[0]), ports[0]
	others := slices.DeleteFunc(slices.Clone(public), func(p int32) bool { return p == r.port })
	switch {
	case len(others) < len(public):
		v.done, v.ok = true, true
	case len(others) > 0:
		first := now
		for _, p := range others {
			if r.opened[p].Before(first) {
				first = r.opened[p]
			}
		}
		if now.Sub(first) >= readyPortGrace {
			v.done, v.ok, v.observed = true, true, pickPort(others)
		}
	case len(loop) > 0:
		lp := loop[0]
		if slices.Contains(loop, r.port) {
			lp = r.port
		}
		msg := fmt.Sprintf("%s listens only on 127.0.0.1:%d inside its container - make it listen on 0.0.0.0", who, lp)
		if now.Sub(r.loopback) >= readyPortGrace {
			if r.required {
				return r.fail(culprit, msg)
			}
			v.done, v.ok, v.note = true, true, msg
		}
	default:
		r.pending = fmt.Sprintf("%s is running but never listened on port %d (PORT=%d)", who, r.port, r.port)
		if !r.required && now.Sub(latest) >= readySettle+workerGrace {
			v.done, v.ok, v.note = true, true, "Not listening on any port - fine for a background worker"
		}
	}
	if v.done {
		r.pending = ""
	}
	return v
}

// expire is the verdict when the timeout ends the wait: what was still missing.
func (r *readyCheck) expire(last readyVerdict) readyVerdict {
	msg := r.pending
	if msg == "" {
		msg = "The app did not become ready in time"
	}
	return readyVerdict{done: true, err: msg, listening: last.listening}
}
