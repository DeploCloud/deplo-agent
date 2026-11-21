package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// appGateWait is how long a deploy waits for a backup's pause of the same App, and a backup for a deploy (#229).
var appGateWait = 10 * time.Minute

// appGate keeps an App's deploys and its backup's volume hold apart: deploys share it, a hold takes it alone.
type appGate struct {
	mu      sync.Mutex
	deploys map[string]int
	holds   map[string]bool
	changed chan struct{}
}

var errGateWait = errors.New("waited too long")

// enter waits until slug admits a deploy (hold false) or a hold (hold true), at most wait; waiting runs once if it has to.
func (g *appGate) enter(ctx context.Context, slug string, hold bool, wait time.Duration, waiting func()) (leave func(), waited time.Duration, err error) {
	start := time.Now()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	said := false
	for {
		g.mu.Lock()
		if g.deploys == nil {
			g.deploys, g.holds, g.changed = map[string]int{}, map[string]bool{}, make(chan struct{})
		}
		if !g.holds[slug] && (!hold || g.deploys[slug] == 0) {
			if hold {
				g.holds[slug] = true
			} else {
				g.deploys[slug]++
			}
			g.mu.Unlock()
			if said {
				waited = time.Since(start)
			}
			return g.leaver(slug, hold), waited, nil
		}
		changed := g.changed
		g.mu.Unlock()
		if !said && waiting != nil {
			waiting()
		}
		said = true
		select {
		case <-changed:
		case <-timer.C:
			return nil, time.Since(start), errGateWait
		case <-ctx.Done():
			return nil, time.Since(start), ctx.Err()
		}
	}
}

func (g *appGate) leaver(slug string, hold bool) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			if hold {
				delete(g.holds, slug)
			} else if g.deploys[slug]--; g.deploys[slug] <= 0 {
				delete(g.deploys, slug)
			}
			close(g.changed)
			g.changed = make(chan struct{})
		})
	}
}

// waitForBackup holds a deploy back while a backup has the App paused; false when it stopped waiting, its result sent.
func (s *Service) waitForBackup(ctx context.Context, slug string, e *emitter) (func(), bool) {
	leave, waited, err := s.gate.enter(ctx, slug, false, appGateWait, func() {
		e.log("info", fmt.Sprintf("A backup of this app has paused it to copy its data, so the deploy waits until it runs again (%s at most).", spoken(appGateWait)))
	})
	if errors.Is(err, errGateWait) {
		e.result(false, fmt.Sprintf("This app was still paused by a backup after %s, so the deploy stopped waiting. Deploy again once the backup has finished.", spoken(appGateWait)), "")
		return nil, false
	}
	if err != nil {
		e.result(false, "the deploy was stopped while it waited for a backup of this app: "+err.Error(), "")
		return nil, false
	}
	if waited > 0 {
		e.log("info", fmt.Sprintf("Waited %s for the backup to finish.", spoken(waited)))
	}
	return leave, true
}

// spoken says a duration the way a person would: "4.2s", "3m 10s", "10 minutes".
func spoken(d time.Duration) string {
	switch {
	case d < 100*time.Millisecond:
		return "a moment"
	case d >= time.Minute && d%time.Minute == 0:
		if n := int(d / time.Minute); n != 1 {
			return fmt.Sprintf("%d minutes", n)
		}
		return "1 minute"
	case d >= time.Minute:
		return d.Round(time.Second).String()
	default:
		return d.Round(100 * time.Millisecond).String()
	}
}
