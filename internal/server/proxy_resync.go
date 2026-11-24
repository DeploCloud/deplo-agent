package server

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// The proxy reads containers when one starts, stops or changes health, never at an unpause, and a paused one has
// no routes when it does. An App unpaused after such a read has none until the next, so this makes one.
const proxyResyncAfter = 2 * time.Second

type routeWatch struct {
	mu     sync.Mutex
	paused map[string]bool // with routes, paused now
	stale  map[string]bool // with routes, paused at the proxy's last read
	due    time.Time
}

func newRouteWatch() *routeWatch {
	return &routeWatch{paused: map[string]bool{}, stale: map[string]bool{}}
}

// event takes one container event; routed says the container has routes on the proxy.
func (w *routeWatch) event(action, id string, routed bool, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case action == "pause" && routed:
		w.paused[id] = true
	case action == "unpause":
		delete(w.paused, id)
		if w.stale[id] && w.due.IsZero() {
			w.due = now.Add(proxyResyncAfter)
		}
	case action == "start" || action == "die" || strings.HasPrefix(action, "health_status"):
		w.stale = map[string]bool{}
		for id := range w.paused {
			w.stale[id] = true
		}
		w.due = time.Time{}
	case action == "destroy":
		delete(w.paused, id)
		delete(w.stale, id)
	}
}

func (w *routeWatch) dueAt(now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.due.IsZero() && !now.Before(w.due)
}

func (w *routeWatch) resynced() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.due = time.Time{}
}

// unknown is a read the agent may have missed: after a restart that unpaused something, or events lost.
func (w *routeWatch) unknown(now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.due.IsZero() {
		w.due = now.Add(proxyResyncAfter)
	}
}

// proxyResync starts and stops a container, which makes the proxy read every container again.
func proxyResync(ctx context.Context) {
	if _, err := dockercli.Run(ctx, holdStepTimeout, volumeHelperRun(ctx, "--network", "none", "--label", copierLabel, volumeHelperImage, "true")...); err != nil {
		log.Printf("deplo-agent: could not make the proxy read its routes again: %v", err)
	}
}

// StartProxyResync watches container events for an App unpaused after the proxy read it paused.
func (s *Service) StartProxyResync(ctx context.Context) {
	w := newRouteWatch()
	if s.unpausedAtStart {
		w.unknown(time.Now())
	}
	go func() {
		s.removeCopiers()
		first := true
		for ctx.Err() == nil {
			if !first {
				w.unknown(time.Now())
			}
			first = false
			_ = streamRouteEvents(ctx, w)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				if w.dueAt(now) {
					w.resynced()
					proxyResync(ctx)
				}
			}
		}
	}()
}

func streamRouteEvents(ctx context.Context, w *routeWatch) error {
	cmd := dockercli.Command(ctx, "docker", "events", "--filter", "type=container", "--format", "{{json .}}")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var ev struct {
			Action string `json:"Action"`
			Actor  struct {
				ID         string            `json:"ID"`
				Attributes map[string]string `json:"Attributes"`
			} `json:"Actor"`
		}
		if json.Unmarshal(sc.Bytes(), &ev) != nil || ev.Actor.ID == "" {
			continue
		}
		w.event(ev.Action, ev.Actor.ID, ev.Actor.Attributes["traefik.enable"] == "true", time.Now())
	}
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	return cmd.Wait()
}
