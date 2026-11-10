package server

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

type fakeHost struct {
	now        time.Time
	containers []readySample
	alive      bool
	restarted  []string
}

func newWatcherOn(t *testing.T, h *fakeHost) *restartWatcher {
	w := newRestartWatcher(t.TempDir())
	w.now = func() time.Time { return h.now }
	w.list = func(context.Context, string) []readySample { return h.containers }
	w.probe = func(context.Context, *pb.RestartRule, readySample) bool { return h.alive }
	w.restart = func(_ context.Context, id string) error { h.restarted = append(h.restarted, id); return nil }
	return w
}

func runTicks(w *restartWatcher, h *fakeHost, n int) {
	for i := 0; i < n; i++ {
		h.now = h.now.Add(30 * time.Second)
		w.tick(context.Background())
	}
}

func web(started time.Time) readySample {
	return readySample{id: "c1", name: "deplo-web", status: "running", startedAt: started, ip: "10.0.0.2"}
}

func kinds(evs []*pb.RestartEvent) []pb.RestartEvent_Kind {
	var out []pb.RestartEvent_Kind
	for _, e := range evs {
		out = append(out, e.Kind)
	}
	return out
}

func TestWatcherRestartsAHungApp(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	runTicks(w, h, 3)
	if len(h.restarted) != 1 || h.restarted[0] != "c1" {
		t.Fatalf("restarted = %v", h.restarted)
	}
	evs := w.drain()
	if len(evs) != 1 || evs[0].Kind != pb.RestartEvent_KIND_RESTARTED || evs[0].Slug != "web" || evs[0].UnansweredS != 60 {
		t.Fatalf("events = %v", evs)
	}
	h.alive = true
	runTicks(w, h, 1)
	if k := kinds(w.drain()); len(k) != 1 || k[0] != pb.RestartEvent_KIND_RECOVERED {
		t.Fatalf("kinds = %v", k)
	}
}

func TestWatcherLeavesAHealthyAppAlone(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}, alive: true}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	runTicks(w, h, 20)
	if len(h.restarted) != 0 || len(w.drain()) != 0 {
		t.Fatal("a healthy app was touched")
	}
}

func TestWatcherProbesOnTheRulesInterval(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}}
	w := newWatcherOn(t, h)
	var probes int
	w.probe = func(context.Context, *pb.RestartRule, readySample) bool { probes++; return true }
	w.release("web", httpRule(), true)
	for i := 0; i < 6; i++ {
		h.now = h.now.Add(10 * time.Second)
		w.tick(context.Background())
	}
	if probes != 2 {
		t.Fatalf("probes in 60 s at a 30 s interval = %d, want 2", probes)
	}
}

func TestWatcherIsSilentWhileADeployRuns(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	w.holdFor("web")
	runTicks(w, h, 10)
	if len(h.restarted) != 0 {
		t.Fatal("restarted during a deploy")
	}
	w.release("web", httpRule(), false)
	runTicks(w, h, 3)
	if len(h.restarted) != 1 {
		t.Fatal("a failed deploy keeps the old rule armed")
	}
}

func TestWatcherIsSilentWhileAStandInRuns(t *testing.T) {
	standIn := readySample{id: "c2", name: "deplo-web" + standInSuffix, status: "running", startedAt: rt0}
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0), standIn}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	runTicks(w, h, 10)
	if len(h.restarted) != 0 {
		t.Fatal("restarted while the stand-in ran")
	}
}

func TestWatcherSkipsContainersInTheirStartPeriod(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Second), containers: []readySample{web(rt0)}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	runTicks(w, h, 1)
	if len(h.restarted) != 0 || len(w.states["web"].fails) != 0 {
		t.Fatal("counted inside the start period")
	}
}

func TestWatcherWatchesOnlyTheRulesService(t *testing.T) {
	api := web(rt0)
	api.service = "api"
	db := readySample{id: "c9", name: "deplo-web-db-1", service: "db", status: "running", startedAt: rt0}
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{api, db}}
	w := newWatcherOn(t, h)
	r := httpRule()
	r.Service = "api"
	w.release("web", r, true)
	runTicks(w, h, 3)
	if len(h.restarted) != 1 || h.restarted[0] != "c1" {
		t.Fatalf("restarted = %v", h.restarted)
	}
}

func TestWatcherDockerHealthIgnoresStarting(t *testing.T) {
	c := web(rt0)
	c.health = "starting"
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{c}}
	w := newWatcherOn(t, h)
	w.probe = probeContainer
	w.release("web", &pb.RestartRule{Probe: pb.RestartRule_PROBE_DOCKER_HEALTH, IntervalS: 30}, true)
	runTicks(w, h, 3)
	if len(h.restarted) != 0 {
		t.Fatal("a starting container is not unhealthy")
	}
	h.containers[0].health = "unhealthy"
	runTicks(w, h, 1)
	if len(h.restarted) != 1 {
		t.Fatal("unhealthy restarts at once")
	}
}

func pauseWatcher(t *testing.T) (*restartWatcher, *fakeHost) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	// Each restart resets the container's start time, as Docker does.
	w.restart = func(_ context.Context, id string) error {
		h.restarted = append(h.restarted, id)
		h.containers[0].startedAt = h.now.Add(-time.Hour)
		return nil
	}
	runTicks(w, h, 3*6)
	if len(h.restarted) != 5 || !w.states["web"].paused {
		t.Fatalf("setup: %d restarts, paused %v", len(h.restarted), w.states["web"].paused)
	}
	if k := kinds(w.drain()); k[len(k)-1] != pb.RestartEvent_KIND_PAUSED {
		t.Fatalf("kinds = %v", k)
	}
	return w, h
}

func TestWatcherPausesThenResumesAfterQuietMinutes(t *testing.T) {
	w, h := pauseWatcher(t)
	h.alive = true
	runTicks(w, h, 61)
	if w.states["web"].paused {
		t.Fatal("30 quiet minutes should resume")
	}
	evs := w.drain()
	if last := evs[len(evs)-1]; last.Kind != pb.RestartEvent_KIND_RESUMED || last.Reason != "quiet" {
		t.Fatalf("events = %v", evs)
	}
}

func TestWatcherResumesOnManualRestartAndDeploy(t *testing.T) {
	w, _ := pauseWatcher(t)
	w.resume("web", "restart")
	if w.states["web"].paused {
		t.Fatal("manual restart should resume")
	}
	if evs := w.drain(); len(evs) != 1 || evs[0].Reason != "restart" {
		t.Fatalf("events = %v", evs)
	}
	w, _ = pauseWatcher(t)
	w.release("web", httpRule(), true)
	if w.states["web"].paused {
		t.Fatal("a deploy should resume")
	}
	if evs := w.drain(); len(evs) != 1 || evs[0].Reason != "deploy" {
		t.Fatalf("events = %v", evs)
	}
}

func TestWatcherNoRuleMeansNoWatching(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	w.release("web", nil, true)
	runTicks(w, h, 5)
	if len(h.restarted) != 0 {
		t.Fatal("a deploy without a rule must stop the watching")
	}
	if _, err := os.Stat(w.path("web")); err == nil {
		t.Fatal("the stored rule should be gone")
	}
}

func TestWatcherKeepsItsRulesAcrossARestartOfTheAgent(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0)}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	again := newWatcherOn(t, h)
	again.dir = w.dir
	again.load()
	runTicks(again, h, 3)
	if len(h.restarted) != 1 {
		t.Fatal("the rule must survive an agent restart, with no control plane involved")
	}
}

func TestWatcherForgetsADestroyedApp(t *testing.T) {
	h := &fakeHost{now: rt0, containers: nil}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	w.forget("web")
	if len(w.slugs()) != 0 {
		t.Fatal("destroyed app still watched")
	}
}

func TestEventBufferIsBoundedAndKeptWhenAFrameFails(t *testing.T) {
	w := newRestartWatcher(t.TempDir())
	for i := 0; i < restartEventsCap+50; i++ {
		w.mu.Lock()
		w.emit(&pb.RestartEvent{Slug: "web"})
		w.mu.Unlock()
	}
	evs := w.drain()
	if len(evs) != restartEventsCap || len(w.drain()) != 0 {
		t.Fatalf("buffer = %d", len(evs))
	}
	w.give(evs)
	if len(w.drain()) != restartEventsCap {
		t.Fatal("events given back must be delivered next time")
	}
}

func TestHttpAnswersCountsAnyStatus(t *testing.T) {
	for _, code := range []int{200, 404, 500} {
		srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(code) }))
		if !httpAnswers(context.Background(), strings.TrimPrefix(srv.URL, "http://"), "/", time.Second) {
			t.Errorf("status %d should count as alive", code)
		}
		srv.Close()
	}
}

func TestHttpAnswersFailsOnRefusedAndOnTimeout(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	l.Close()
	if httpAnswers(context.Background(), addr, "/", time.Second) {
		t.Fatal("a refused connection is not an answer")
	}
	var hit atomic.Bool
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		hit.Store(true)
		<-r.Context().Done()
	}))
	defer slow.Close()
	start := time.Now()
	if httpAnswers(context.Background(), strings.TrimPrefix(slow.URL, "http://"), "/", 200*time.Millisecond) {
		t.Fatal("a hang is not an answer")
	}
	if !hit.Load() || time.Since(start) > 2*time.Second {
		t.Fatal("the probe must give up at its timeout")
	}
}

func TestRestartCapabilityIsAdvertised(t *testing.T) {
	for _, c := range Capabilities {
		if c == "health.restart" {
			return
		}
	}
	t.Fatal("health.restart missing from Hello")
}

func TestUsableRule(t *testing.T) {
	if usableRule(&pb.RestartRule{}) || usableRule(&pb.RestartRule{Probe: pb.RestartRule_PROBE_HTTP, Port: 0, Path: "/"}) ||
		usableRule(&pb.RestartRule{Probe: pb.RestartRule_PROBE_HTTP, Port: 80, Path: "x"}) {
		t.Fatal("a rule without a probe, port or absolute path must not arm")
	}
	if !usableRule(httpRule()) {
		t.Fatal("a full http rule is usable")
	}
}
