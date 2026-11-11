package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const (
	restartTick      = 5 * time.Second
	restartEventsCap = 200
	restartRuleExt   = ".restart.json"
	// Not a rule: the name must not end in restartRuleExt.
	restartEventsFile = "restart-events.jsonl"
)

// restartWatcher probes every app that carries a restart rule, with or without the control plane.
type restartWatcher struct {
	dir string
	now func() time.Time

	list    func(ctx context.Context, slug string) []readySample
	probe   func(ctx context.Context, rule *pb.RestartRule, c readySample) bool
	restart func(ctx context.Context, id string) error
	// held reports a container paused for a volume copy; it is neither probed nor counted.
	held func(id string) bool

	mu     sync.Mutex
	states map[string]*restartState
	hold   map[string]int
	due    map[string]time.Time
	events []*pb.RestartEvent
}

func newRestartWatcher(dir string) *restartWatcher {
	return &restartWatcher{
		dir:     dir,
		now:     time.Now,
		list:    func(ctx context.Context, slug string) []readySample { return sampleReady(ctx, slug, "", true) },
		probe:   probeContainer,
		restart: restartContainer,
		states:  map[string]*restartState{},
		hold:    map[string]int{},
		due:     map[string]time.Time{},
	}
}

func (s *Service) restartWatch() *restartWatcher {
	s.restartOnce.Do(func() {
		s.rw = newRestartWatcher(s.stackDir)
		s.rw.held = s.HeldForBackup
	})
	return s.rw
}

// StartRestartWatch loads the stored rules and probes until ctx ends.
func (s *Service) StartRestartWatch(ctx context.Context) {
	w := s.restartWatch()
	w.load()
	go func() {
		t := time.NewTicker(restartTick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				w.tick(ctx)
			}
		}
	}()
}

func (w *restartWatcher) path(slug string) string { return filepath.Join(w.dir, slug+restartRuleExt) }

func (w *restartWatcher) load() {
	matches, _ := filepath.Glob(filepath.Join(w.dir, "*"+restartRuleExt))
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.keepEvents()
	if raw, err := os.ReadFile(w.eventsPath()); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			ev := &pb.RestartEvent{}
			if line != "" && protojson.Unmarshal([]byte(line), ev) == nil {
				w.events = append(w.events, ev)
			}
		}
	}
	for _, m := range matches {
		slug := strings.TrimSuffix(filepath.Base(m), restartRuleExt)
		raw, err := os.ReadFile(m)
		if err != nil || validateSlug(slug) != nil {
			continue
		}
		rule := &pb.RestartRule{}
		if protojson.Unmarshal(raw, rule) != nil || !usableRule(rule) {
			continue
		}
		w.states[slug] = newRestartState(rule)
		// The pause was in the memory of the agent that ended: restarting is back on, and Deplo is told.
		// ponytail: the buffer keeps the last 200 events, so a server with more Apps than that tells of the newest only.
		w.emit(&pb.RestartEvent{Kind: pb.RestartEvent_KIND_RESUMED, Slug: slug, Reason: "agent"})
	}
}

func usableRule(r *pb.RestartRule) bool {
	switch r.GetProbe() {
	case pb.RestartRule_PROBE_HTTP:
		return r.GetPort() >= 1 && r.GetPort() <= 65535 && strings.HasPrefix(r.GetPath(), "/")
	case pb.RestartRule_PROBE_DOCKER_HEALTH:
		return true
	}
	return false
}

// holdFor keeps the app's counter silent while a deploy runs; release re-arms it.
func (w *restartWatcher) holdFor(slug string) {
	w.mu.Lock()
	w.hold[slug]++
	w.mu.Unlock()
}

// release ends a hold. A successful deploy replaces the rule (nil removes it) and resumes restarting.
func (w *restartWatcher) release(slug string, rule *pb.RestartRule, deployed bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.hold[slug]--; w.hold[slug] <= 0 {
		delete(w.hold, slug)
	}
	if !deployed {
		return
	}
	prev := w.states[slug]
	if rule == nil || !usableRule(rule) {
		delete(w.states, slug)
		_ = os.Remove(w.path(slug))
		return
	}
	st := newRestartState(rule)
	if prev != nil && prev.paused {
		w.emit(&pb.RestartEvent{Kind: pb.RestartEvent_KIND_RESUMED, Slug: slug, Reason: "deploy"})
	}
	w.states[slug] = st
	if raw, err := protojson.Marshal(rule); err == nil {
		_ = os.WriteFile(w.path(slug), raw, 0o600)
	}
}

// resume is a manual restart: the app gets restarting back.
func (w *restartWatcher) resume(slug, reason string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if st := w.states[slug]; st != nil && st.resume() {
		w.emit(&pb.RestartEvent{Kind: pb.RestartEvent_KIND_RESUMED, Slug: slug, Reason: reason})
	}
}

func (w *restartWatcher) forget(slug string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.states, slug)
	_ = os.Remove(w.path(slug))
}

// emit needs w.mu held.
func (w *restartWatcher) emit(ev *pb.RestartEvent) {
	ev.AtUnixMs = w.now().UnixMilli()
	w.events = append(w.events, ev)
	if len(w.events) > restartEventsCap {
		w.events = w.events[len(w.events)-restartEventsCap:]
	}
	w.keepEvents()
}

func (w *restartWatcher) eventsPath() string { return filepath.Join(w.dir, restartEventsFile) }

// keepEvents writes what no frame has taken yet, so it outlives this process. It needs w.mu held.
func (w *restartWatcher) keepEvents() {
	if len(w.events) == 0 {
		_ = os.Remove(w.eventsPath())
		return
	}
	var buf []byte
	for _, ev := range w.events {
		if raw, err := protojson.Marshal(ev); err == nil {
			buf = append(append(buf, raw...), '\n')
		}
	}
	if err := writeFileAtomic(w.eventsPath(), buf, 0o600); err != nil {
		log.Printf("deplo-agent: could not keep %d restart events for Deplo: %v", len(w.events), err)
	}
}

// drain hands the buffered events to a metrics frame; give puts them back when the frame did not go out.
func (w *restartWatcher) drain() []*pb.RestartEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.events
	w.events = nil
	if len(out) > 0 {
		w.keepEvents()
	}
	return out
}

func (w *restartWatcher) give(evs []*pb.RestartEvent) {
	if len(evs) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(evs, w.events...)
	if len(w.events) > restartEventsCap {
		w.events = w.events[len(w.events)-restartEventsCap:]
	}
	w.keepEvents()
}

// answerFor is what the last probe said about one container: "starting", "answering" or
// "not_answering". Empty when no rule watches it, none has run yet, or a deploy holds the app.
func (w *restartWatcher) answerFor(slug, container string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.states[slug]
	if st == nil || w.hold[slug] > 0 {
		return ""
	}
	return st.answer[container]
}

// stampAnswers puts answerFor on a metrics frame, whose containers carry a name and no slug.
func (w *restartWatcher) stampAnswers(stats []*pb.ContainerStat) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for slug, st := range w.states {
		if w.hold[slug] > 0 {
			continue
		}
		for _, c := range stats {
			if a := st.answer[c.GetName()]; a != "" {
				c.Answer = a
			}
		}
	}
}

func (w *restartWatcher) slugs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.states))
	for slug := range w.states {
		if w.hold[slug] == 0 {
			out = append(out, slug)
		}
	}
	return out
}

func (w *restartWatcher) tick(ctx context.Context) {
	for _, slug := range w.slugs() {
		w.tickSlug(ctx, slug)
	}
}

func (w *restartWatcher) tickSlug(ctx context.Context, slug string) {
	w.mu.Lock()
	st := w.states[slug]
	w.mu.Unlock()
	if st == nil {
		return
	}
	now := w.now()
	w.mu.Lock()
	if st.quiet(now) {
		st.resume()
		w.emit(&pb.RestartEvent{Kind: pb.RestartEvent_KIND_RESUMED, Slug: slug, Reason: "quiet"})
	}
	w.mu.Unlock()

	all := w.list(ctx, slug)
	var targets []readySample
	for _, c := range all {
		if c.standIn() {
			return
		}
		if c.status != "running" || (st.rule.GetService() != "" && c.service != st.rule.GetService()) {
			continue
		}
		if w.held != nil && w.held(c.id) {
			continue
		}
		targets = append(targets, c)
	}
	for _, c := range targets {
		w.mu.Lock()
		held := w.hold[slug] > 0 || w.states[slug] != st
		skip := now.Before(w.due[c.id])
		w.mu.Unlock()
		if held {
			return
		}
		if skip {
			continue
		}
		w.check(ctx, slug, st, c, now)
	}
}

func (w *restartWatcher) check(ctx context.Context, slug string, st *restartState, c readySample, now time.Time) {
	interval := time.Duration(max(st.rule.GetIntervalS(), 1)) * time.Second
	if st.rule.GetProbe() == pb.RestartRule_PROBE_DOCKER_HEALTH && c.health != "healthy" && c.health != "unhealthy" {
		return
	}
	ok := w.probe(ctx, st.rule, c)
	w.mu.Lock()
	// A container that is gone is never asked about again: its entry goes once its time has passed.
	if len(w.due) > 1024 {
		for id, at := range w.due {
			if !now.Before(at) {
				delete(w.due, id)
			}
		}
	}
	w.due[c.id] = now.Add(interval)
	v := st.record(now, c.id, c.startedAt, ok)
	st.note(now, c, ok, v.restart)
	if v.pause {
		w.emit(&pb.RestartEvent{Kind: pb.RestartEvent_KIND_PAUSED, Slug: slug, Container: c.name, Restarts: int32(v.inWindowNow)})
	}
	if v.recovered {
		w.emit(&pb.RestartEvent{Kind: pb.RestartEvent_KIND_RECOVERED, Slug: slug, Container: c.name})
	}
	w.mu.Unlock()
	if !v.restart {
		return
	}
	if err := w.restart(ctx, c.id); err != nil {
		log.Printf("deplo-agent: restart of %s failed: %v", c.name, err)
		return
	}
	w.mu.Lock()
	w.emit(&pb.RestartEvent{Kind: pb.RestartEvent_KIND_RESTARTED, Slug: slug, Container: c.name,
		UnansweredS: int32(v.unanswered / time.Second), Restarts: int32(v.inWindowNow)})
	w.mu.Unlock()
}

func probeContainer(ctx context.Context, rule *pb.RestartRule, c readySample) bool {
	if rule.GetProbe() == pb.RestartRule_PROBE_DOCKER_HEALTH {
		return c.health != "unhealthy"
	}
	if c.ip == "" {
		return true
	}
	return httpAnswers(ctx, net.JoinHostPort(c.ip, strconv.Itoa(int(rule.GetPort()))), rule.GetPath(),
		time.Duration(max(rule.GetTimeoutS(), 1))*time.Second, rule.GetFailOnErrorStatus())
}

var aliveClient = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// httpAnswers: a refused connection or a timeout is no answer. Any HTTP status is one, unless strict: the App's
// own check fails from 400 up.
func httpAnswers(ctx context.Context, addr, path string, timeout time.Duration, strict bool) bool {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return true
	}
	req.Header.Set("User-Agent", "deplo-agent/"+AgentVersion)
	resp, err := aliveClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return !strict || resp.StatusCode < 400
}

func restartContainer(ctx context.Context, id string) error {
	res, err := dockercli.Run(ctx, 90*time.Second, "restart", id)
	if err != nil {
		return err
	}
	if res.Code != 0 {
		return fmt.Errorf("%s", firstLine(res.Stderr))
	}
	return nil
}
