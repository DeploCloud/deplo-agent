package server

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const procTCP = `  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000:0BB8 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31337 1 0000000000000000 100 0 0 10 0
   1: 0100007F:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31338 1 0000000000000000 100 0 0 10 0
   2: 0100007F:1F90 0100007F:D3C2 01 00000000:00000000 00:00000000 00000000     0        0 31339 1 0000000000000000 20 4 30 10 -1
   3: 0200A8C0:2328 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 31340 1 0000000000000000 100 0 0 10 0
`

const procTCP6 = `  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000000000000000000000000000:1F91 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 41 1 0000000000000000 100 0 0 10 0
   1: 00000000000000000000000001000000:0CEA 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 42 1 0000000000000000 100 0 0 10 0
   2: 0000000000000000FFFF00000100007F:0050 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 43 1 0000000000000000 100 0 0 10 0
`

// The kernel's tables: only LISTEN rows, the address decoded word by word, loopback told apart.
func TestParseListeners(t *testing.T) {
	got := append(parseListeners(procTCP), parseListeners(procTCP6)...)
	want := []listener{{3000, false}, {8080, true}, {9000, false}, {8081, false}, {3306, true}, {80, true}}
	if !slices.Equal(got, want) {
		t.Fatalf("listeners = %v, want %v", got, want)
	}
	public, loop := splitListeners(append(got, listener{8080, false}))
	if !slices.Equal(public, []int32{3000, 8080, 8081, 9000}) || !slices.Equal(loop, []int32{80, 3306}) {
		t.Fatalf("public %v loopback %v", public, loop)
	}
}

func TestContainerListenersReadsTheProcessNetns(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "42", "net", "tcp"), procTCP)
	prev := procRoot
	procRoot = root
	t.Cleanup(func() { procRoot = prev })
	if got := containerListeners(42); len(got) != 3 {
		t.Fatalf("listeners = %v", got)
	}
	if got := containerListeners(0); got != nil {
		t.Fatalf("no pid must read nothing: %v", got)
	}
}

func TestPickPort(t *testing.T) {
	for _, c := range []struct {
		in   []int32
		want int32
	}{
		{[]int32{8080}, 8080},
		{[]int32{8080, 9090, 9229}, 8080},
		{[]int32{4000, 5005, 8080}, 4000},
		{[]int32{9090, 9229}, 9090},
		{nil, 0},
	} {
		if got := pickPort(c.in); got != c.want {
			t.Errorf("pickPort(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

var t0 = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

func up(id string, startedAt time.Time, ls ...listener) readySample {
	return readySample{id: id, name: "deplo-" + id, status: "running", startedAt: startedAt, pid: 7, listeners: ls,
		restartPolicy: "unless-stopped"}
}

func at(s time.Duration) time.Time { return t0.Add(s * time.Second) }

func public(p int32) listener   { return listener{port: p} }
func loopback(p int32) listener { return listener{port: p, loopback: true} }

// Steps feed the check one inspect round at a time; the last step's verdict is returned.
func runReady(rc *readyCheck, steps ...func() (time.Time, []readySample)) readyVerdict {
	var v readyVerdict
	for _, step := range steps {
		now, samples := step()
		if v = rc.observe(now, samples); v.done {
			return v
		}
	}
	return v
}

func step(now time.Time, samples ...readySample) func() (time.Time, []readySample) {
	return func() (time.Time, []readySample) { return now, samples }
}

func TestReadyCheckSettlesBeforeItCounts(t *testing.T) {
	rc := newReadyCheck(3000, true, "", false)
	v := runReady(rc, step(at(1), up("app", t0, public(3000))), step(at(9), up("app", t0, public(3000))))
	if v.done {
		t.Fatalf("ready before the settle window: %+v", v)
	}
	v = rc.observe(at(10), []readySample{up("app", t0, public(3000))})
	if !v.done || !v.ok || v.observed != 0 || !slices.Equal(v.listening, []int32{3000}) {
		t.Fatalf("verdict = %+v", v)
	}
}

// Docker keeps a restarting container "running", so a crash loop is caught by its restart count,
// its start time or its status - whichever moves first.
func TestReadyCheckFailsFastOnARestart(t *testing.T) {
	restarted := up("app", at(3))
	restarted.restarts, restarted.exitCode = 1, 1
	loop := up("app", t0)
	loop.status, loop.exitCode = "restarting", 1
	moved := up("app", at(4))
	for name, next := range map[string]readySample{"restart count": restarted, "status": loop, "start time": moved} {
		v := runReady(newReadyCheck(3000, true, "", false), step(at(1), up("app", t0)), step(at(4), next))
		if !v.done || v.ok || v.culprit != "app" || !strings.HasPrefix(v.err, "The app keeps restarting") {
			t.Errorf("%s: %+v", name, v)
		}
	}
}

func TestReadyCheckNamesHowItStopped(t *testing.T) {
	exited := up("app", t0)
	exited.status, exited.exitCode = "exited", 1
	oom := exited
	oom.oomKilled, oom.exitCode = true, 137
	sick := up("app", t0)
	sick.health = "unhealthy"
	for want, c := range map[string]readySample{
		"The app stopped right after it started (exit code 1)": exited,
		"The app was stopped: out of memory":                   oom,
		"The app's health check fails":                         sick,
	} {
		if v := newReadyCheck(3000, true, "", false).observe(at(2), []readySample{c}); !v.done || v.ok || v.err != want {
			t.Errorf("%s: %+v", want, v)
		}
	}
}

// A compose one-shot that exits 0 with no restart policy is done, not failed; the rest still settle.
func TestReadyCheckLetsAComposeOneShotFinish(t *testing.T) {
	migrate := up("migrate", t0)
	migrate.service, migrate.status, migrate.restartPolicy = "migrate", "exited", "no"
	web := up("web", t0, public(3000))
	web.service = "web"
	if v := newReadyCheck(3000, true, "web", true).observe(at(11), []readySample{migrate, web}); !v.done || !v.ok {
		t.Fatalf("verdict = %+v", v)
	}
	migrate.restartPolicy = "always"
	if v := newReadyCheck(3000, true, "web", true).observe(at(11), []readySample{migrate, web}); v.ok ||
		v.err != "Service migrate stopped right after it started (exit code 0)" {
		t.Fatalf("a service that should keep running exited: %+v", v)
	}
}

// The routed port is read from its own service only: a database listening elsewhere is no answer.
func TestReadyCheckReadsTheRoutedService(t *testing.T) {
	db := up("db", t0, public(5432))
	db.service = "db"
	web := up("web", t0)
	web.service = "web"
	rc := newReadyCheck(3000, true, "web", true)
	v := runReady(rc, step(at(11), db, web), step(at(30), db, web))
	if v.done || rc.pending != "Service web is running but never listened on port 3000 (PORT=3000)" {
		t.Fatalf("verdict = %+v pending %q", v, rc.pending)
	}
	web.listeners = []listener{public(3000)}
	if v := rc.observe(at(31), []readySample{db, web}); !v.ok || !slices.Equal(v.listening, []int32{3000}) {
		t.Fatalf("verdict = %+v", v)
	}
}

// The app opened another port and never the expected one: once the grace passes it is adopted.
func TestReadyCheckAdoptsTheObservedPort(t *testing.T) {
	rc := newReadyCheck(3000, true, "", false)
	v := runReady(rc, step(at(5), up("app", t0, public(8080), public(9229))), step(at(12), up("app", t0, public(8080), public(9229))))
	if v.done {
		t.Fatalf("adopted before the grace: %+v", v)
	}
	v = rc.observe(at(15), []readySample{up("app", t0, public(8080), public(9229))})
	if !v.ok || v.observed != 8080 || !slices.Equal(v.listening, []int32{8080, 9229}) {
		t.Fatalf("verdict = %+v", v)
	}
	if v := newReadyCheck(3000, true, "", false).observe(at(30), []readySample{up("app", t0, public(8080), public(3000))}); !v.ok || v.observed != 0 {
		t.Fatalf("the expected port among others is no mismatch: %+v", v)
	}
}

func TestReadyCheckLoopbackOnly(t *testing.T) {
	steps := []func() (time.Time, []readySample){step(at(5), up("app", t0, loopback(3000))), step(at(16), up("app", t0, loopback(3000)))}
	v := runReady(newReadyCheck(3000, true, "", false), steps...)
	if v.ok || v.err != "The app listens only on 127.0.0.1:3000 inside its container - make it listen on 0.0.0.0" {
		t.Fatalf("required: %+v", v)
	}
	if v := runReady(newReadyCheck(3000, false, "", false), steps...); !v.ok || !strings.Contains(v.note, "127.0.0.1:3000") {
		t.Fatalf("not required: %+v", v)
	}
}

// Nothing listening: a routed app waits for the timeout and says so; a worker is fine after a grace.
func TestReadyCheckNothingListening(t *testing.T) {
	rc := newReadyCheck(3000, true, "", false)
	if v := runReady(rc, step(at(11), up("app", t0)), step(at(170), up("app", t0))); v.done {
		t.Fatalf("a routed app with no port was ready: %+v", v)
	}
	if v := rc.expire(readyVerdict{}); v.err != "The app is running but never listened on port 3000 (PORT=3000)" {
		t.Fatalf("expired: %+v", v)
	}
	worker := newReadyCheck(3000, false, "", false)
	if v := runReady(worker, step(at(11), up("app", t0)), step(at(29), up("app", t0))); v.done {
		t.Fatalf("a worker was ready before its port grace: %+v", v)
	}
	if v := worker.observe(at(30), []readySample{up("app", t0)}); !v.ok || v.note != "Not listening on any port - fine for a background worker" {
		t.Fatalf("worker: %+v", v)
	}
	if v := newReadyCheck(0, false, "", false).observe(at(10), []readySample{up("app", t0)}); !v.ok {
		t.Fatalf("no port to check: %+v", v)
	}
}

func TestReadyCheckExpiresWithWhatWasMissing(t *testing.T) {
	rc := newReadyCheck(3000, true, "", false)
	rc.observe(at(1), nil)
	if v := rc.expire(readyVerdict{}); v.ok || v.err != "The app's container was not found" {
		t.Fatalf("missing container: %+v", v)
	}
	starting := up("app", t0, public(3000))
	starting.health = "starting"
	rc.observe(at(100), []readySample{starting})
	if v := rc.expire(readyVerdict{}); v.err != "The app never passed its health check" {
		t.Fatalf("health: %+v", v)
	}
}

func TestParseReadySamples(t *testing.T) {
	out := `{"id":"abc","name":"/deplo-web-web-1","service":"web","status":"running","restarts":2,"startedAt":"2026-10-24T12:00:00.123456789Z","pid":0,"exitCode":0,"oomKilled":false,"health":"healthy","restartPolicy":"unless-stopped"}
{"id":"def","name":"/deplo-web-job-1","service":"job","status":"created","restarts":0,"startedAt":"0001-01-01T00:00:00Z","pid":0,"exitCode":127,"oomKilled":true,"health":"","restartPolicy":"no"}
not json`
	got := parseReadySamples(out)
	if len(got) != 2 || got[0].name != "deplo-web-web-1" || got[0].restarts != 2 || got[0].health != "healthy" ||
		got[0].startedAt.Nanosecond() != 123456789 || !got[1].startedAt.IsZero() || got[1].exitCode != 127 || !got[1].oomKilled {
		t.Fatalf("samples = %+v", got)
	}
}

func TestStartHint(t *testing.T) {
	if startHint(127, nil) == "" || startHint(1, []string{"sh: 1: gunicorn: command not found"}) == "" ||
		startHint(0, []string{`exec: "bun": executable file not found in $PATH: unknown`}) == "" {
		t.Error("a missing program must get the start command hint")
	}
	if h := startHint(1, []string{"Error: listen EADDRINUSE"}); h != "" {
		t.Errorf("unrelated failure got %q", h)
	}
}

// Unset `ready` keeps the old rule exactly: one running check, no listeners read.
func TestDeployWithoutReadyKeepsTheRunningCheck(t *testing.T) {
	calls := fakeImageDocker(t, nil, map[string]string{"deplo-agent-test-img:1": "null"}, nil)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	var res *pb.DeployResult
	s.runDeploy(context.Background(), &pb.DeployRequest{
		DeployId: "deplo-agent-test-legacy", Slug: "deplo-agent-test-legacy", ImageRef: "deplo-agent-test-img:1",
		SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml: "services: {}\n", Network: testNetwork, ReadyTimeoutMs: 1,
	}, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return nil
	}})
	if res == nil || !res.GetReady() || !strings.Contains(readCalls(t, calls), "inspect -f {{.State.Running}} deplo-deplo-agent-test-legacy") {
		t.Fatalf("result %v, calls:\n%s", res, readCalls(t, calls))
	}
	if strings.Contains(readCalls(t, calls), "RestartCount") {
		t.Fatal("the ready check ran for a deploy that did not ask for it")
	}
}

func TestCapabilities_advertisesReadyCheck(t *testing.T) {
	if !containsString(Capabilities, "deploy.ready-check") {
		t.Error(`Capabilities must advertise "deploy.ready-check"`)
	}
}
