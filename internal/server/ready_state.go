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
	// A stack's service may crash a few times while a sibling (its database) boots.
	stackRestartLimit = 5
	// Linux hands out ports from here up when a process asks for any port (ip_local_port_range).
	ephemeralPortFloor = 32768
)

// readySample is one inspect of a container the ready check watches.
type readySample struct {
	id, name, service string
	status            string
	restarts          int
	created           time.Time
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
	since    time.Time // the deploy's start: a stack container made after it has every restart counted

	first     map[string]readySample
	exits     map[string]int
	opened    map[int32]time.Time
	loopback  time.Time
	pending   string
	pendingID string
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
		first: map[string]readySample{}, exits: map[string]int{}, opened: map[int32]time.Time{}}
}

func (r *readyCheck) who(c readySample) string {
	if !r.stack {
		return "The app"
	}
	return "Service " + orDefault(c.service, c.name)
}

func (r *readyCheck) fail(c readySample, msg string) readyVerdict {
	return readyVerdict{done: true, err: msg, culprit: c.id, exitCode: r.exitOf(c)}
}

// exitOf is the container's exit code, or its last failing one while it runs again.
func (r *readyCheck) exitOf(c readySample) int {
	if c.exitCode == 0 {
		return r.exits[c.id]
	}
	return c.exitCode
}

func (r *readyCheck) wait(c readySample, msg string) {
	r.pending, r.pendingID = msg, c.id
}

func (r *readyCheck) observe(now time.Time, samples []readySample) readyVerdict {
	settled := len(samples) > 0
	r.pendingID = ""
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
		switch {
		case !seen:
			base = c
			if r.stack && !r.since.IsZero() && !c.created.IsZero() && !c.created.Before(r.since) {
				base.restarts = 0
			}
		case base.startedAt.IsZero():
			base.startedAt = c.startedAt
		}
		r.first[c.id] = base
		if c.status != "running" && c.exitCode != 0 {
			r.exits[c.id] = c.exitCode
		}
		who := r.who(c)
		restarted := c.status == "restarting" || c.restarts > base.restarts || !c.startedAt.Equal(base.startedAt)
		switch {
		case restarted && c.oomKilled:
			return r.fail(c, who+" was stopped: out of memory")
		case restarted && (!r.stack || c.restarts-base.restarts >= stackRestartLimit):
			return r.fail(c, fmt.Sprintf("%s keeps restarting (exit code %d)", who, r.exitOf(c)))
		case restarted && (c.status == "restarting" || c.status == "running" && now.Sub(c.startedAt) < readySettle):
			// It counts once it has stayed up through the settle window since its last start.
			r.wait(c, fmt.Sprintf("%s keeps restarting (exit code %d)", who, r.exitOf(c)))
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
			settled = false
			if !restarted {
				r.wait(c, who+" did not start (it is "+c.status+")")
			}
		case c.health == "starting":
			settled = false
			r.wait(c, who+" never passed its health check")
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
	// An ephemeral loopback port is a client socket or a setup step (a database's first-run init),
	// not the app picking the wrong address: only the expected port or a fixed one says that.
	loop = slices.DeleteFunc(loop, func(p int32) bool { return p != r.port && p >= ephemeralPortFloor })
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
		r.wait(culprit, fmt.Sprintf("%s is running but never listened on port %d (PORT=%d)", who, r.port, r.port))
		if !r.required && now.Sub(latest) >= readySettle+workerGrace {
			v.done, v.ok, v.note = true, true, "Not listening on any port - fine for a background worker"
		}
	}
	if v.done {
		r.pending, r.pendingID = "", ""
	}
	return v
}

// expire is the verdict when the timeout ends the wait: what was still missing, and whose log says why.
func (r *readyCheck) expire(last readyVerdict) readyVerdict {
	msg := r.pending
	if msg == "" {
		msg = "The app did not become ready in time"
	}
	v := readyVerdict{done: true, err: msg, listening: last.listening, culprit: r.pendingID}
	if c, ok := r.first[r.pendingID]; ok {
		v.exitCode = r.exitOf(c)
	}
	return v
}
