package server

import (
	"context"
	"time"
)

// startQuiet bounds, from the agent's start, how long the rest of its docker calls wait for what comes first.
const startQuiet = 2 * time.Second

// lookAtStart reads, as this run's first docker call, whether each App this server answers for runs here: a visitor
// carried here meanwhile waits for that one look, not for docker's answer to the rest of the start (c47: 4 s).
func (f *forwarders) lookAtStart(ctx context.Context, st forwardState) {
	done := make(chan struct{})
	defer close(done)
	var apps []string
	f.mu.Lock()
	now := time.Now()
	for _, app := range st.ServeAnswers {
		rc := f.readiness[app]
		if rc == nil {
			rc = &answerReadyCheck{}
			f.readiness[app] = rc
		}
		if rc.pending == nil && !now.Before(rc.until) {
			rc.pending = done
			apps = append(apps, app)
		}
	}
	f.mu.Unlock()
	if len(apps) == 0 {
		return
	}
	ops := f.ops()
	got := map[string]appReadiness{}
	if ops.readyAll != nil {
		got = ops.readyAll(ctx, apps)
	} else if ops.ready != nil {
		for _, app := range apps {
			got[app] = ops.ready(ctx, app)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now = time.Now()
	for _, app := range apps {
		rc := f.readiness[app]
		f.noteReadiness(app, rc, got[app], now)
		rc.pending = nil
	}
}

// doneLooking lets the rest of the start go on (see afterStart).
func (f *forwarders) doneLooking() { f.lookedOnce.Do(func() { close(f.looked) }) }

// afterStart waits, at most startQuiet past the agent's start, until this run looked at what it answers for and took
// its first renewal: the start's other docker calls come after both, so neither waits on them (c47).
func (s *Service) afterStart(ctx context.Context) {
	t := time.NewTimer(time.Until(agentStarted.Add(startQuiet)))
	defer t.Stop()
	for _, first := range []<-chan struct{}{s.forwarders().looked, s.leaseGuard().firstRenewal} {
		select {
		case <-first:
		case <-t.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

// nextRenewal waits at most d for this run's next renewal while a lease is current here; it says when that came,
// zero when none did.
// ponytail: polls every 10 ms, a notification once something waits on it often.
func (g *leaseGuard) nextRenewal(d time.Duration) time.Time {
	g.mu.Lock()
	from, current := g.renewals, false
	for _, c := range g.clusters {
		current = current || c.current(g.now())
	}
	g.mu.Unlock()
	for end := time.Now().Add(d); current && time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		g.mu.Lock()
		n := g.renewals
		g.mu.Unlock()
		if n != from {
			return time.Now()
		}
	}
	return time.Time{}
}
