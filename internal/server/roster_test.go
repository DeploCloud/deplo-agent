package server

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseCgroupV2Path(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "cgroupfs driver",
			content: "0::/docker/3f8a1c9e5b2d\n",
			want:    "/docker/3f8a1c9e5b2d",
		},
		{
			name:    "systemd driver",
			content: "0::/system.slice/docker-3f8a1c9e5b2d.scope\n",
			want:    "/system.slice/docker-3f8a1c9e5b2d.scope",
		},
		{
			name: "rootless nests under a user slice",
			content: "0::/user.slice/user-1000.slice/user@1000.service/" +
				"user.slice/docker-3f8a1c9e5b2d.scope\n",
			want: "/user.slice/user-1000.slice/user@1000.service/user.slice/docker-3f8a1c9e5b2d.scope",
		},
		{
			name: "cgroup v1 hierarchies only",
			content: "12:pids:/docker/3f8a1c9e5b2d\n" +
				"11:memory:/docker/3f8a1c9e5b2d\n" +
				"10:cpu,cpuacct:/docker/3f8a1c9e5b2d\n",
			want: "",
		},
		{
			name: "hybrid v1/v2 with an empty unified hierarchy",
			content: "12:pids:/docker/3f8a1c9e5b2d\n" +
				"0::/\n",
			want: "",
		},
		{
			name:    "root cgroup is refused",
			content: "0::/\n",
			want:    "",
		},
		{name: "empty file", content: "", want: ""},
		{name: "no trailing newline", content: "0::/docker/abc", want: "/docker/abc"},
		{name: "relative path is refused", content: "0::docker/abc\n", want: ""},
		{name: "empty relpath is refused", content: "0::\n", want: ""},
		{
			name:    "0:: line is not first",
			content: "1:name=systemd:/docker/abc\n0::/docker/abc\n",
			want:    "/docker/abc",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseCgroupV2Path(tc.content); got != tc.want {
				t.Fatalf("parseCgroupV2Path() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseRosterPsLine(t *testing.T) {
	const fullID = "3f8a1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	cases := []struct {
		name string
		line string
		want rosterPsRow
		ok   bool
	}{
		{
			name: "running container",
			line: `{"ID":"` + fullID + `","Names":"deplo-web-app-1","State":"running","Image":"deplo/web:d1"}`,
			want: rosterPsRow{ID: fullID, Name: "deplo-web-app-1", State: "running"},
			ok:   true,
		},
		{
			name: "exited container is still a row",
			line: `{"ID":"` + fullID + `","Names":"deplo-web-worker-1","State":"exited"}`,
			want: rosterPsRow{ID: fullID, Name: "deplo-web-worker-1", State: "exited"},
			ok:   true,
		},
		{
			name: "multiple names keeps the first",
			line: `{"ID":"` + fullID + `","Names":"deplo-web-app-1,web-alias","State":"running"}`,
			want: rosterPsRow{ID: fullID, Name: "deplo-web-app-1", State: "running"},
			ok:   true,
		},
		{name: "blank line", line: "", ok: false},
		{name: "whitespace only", line: "   \t ", ok: false},
		{name: "not json", line: "Cannot connect to the Docker daemon", ok: false},
		{
			name: "row with no id is unusable",
			line: `{"ID":"","Names":"deplo-web-app-1","State":"running"}`,
			ok:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseRosterPsLine(tc.line)
			if ok != tc.ok {
				t.Fatalf("parseRosterPsLine() ok = %v, want %v", ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Fatalf("parseRosterPsLine() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseRosterInspectLines(t *testing.T) {
	const idA = "aaaa1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"
	const idB = "bbbb1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	stdout := strings.Join([]string{
		`{"id":"` + idA + `","name":"/deplo-web-app-1","project":"prj_abc","state":"running","health":"healthy","restartCount":0,"pid":4242}`,
		`{"id":"` + idB + `","name":"/deplo-web-db-1","project":"db_xyz","state":"restarting","health":"","restartCount":17,"pid":0}`,
	}, "\n")

	got := parseRosterInspectLines(stdout)
	if len(got) != 2 {
		t.Fatalf("parsed %d details, want 2", len(got))
	}

	a := got[idA]
	if a.Name != "deplo-web-app-1" {
		t.Errorf("name = %q, want the leading slash stripped", a.Name)
	}
	if a.ProjectID != "prj_abc" || a.State != "running" || a.Health != "healthy" || a.PID != 4242 {
		t.Errorf("detail A = %+v", a)
	}

	b := got[idB]
	if b.State != "restarting" || b.RestartCount != 17 {
		t.Errorf("detail B = %+v, want restarting with 17 restarts", b)
	}
	if b.Health != "" {
		t.Errorf("health = %q, want empty for an image with no healthcheck", b.Health)
	}
}

func TestParseRosterInspectLinesSkipsJunk(t *testing.T) {
	const id = "cccc1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	stdout := "\n" +
		"Error: No such object: gone\n" +
		`{"id":"` + id + `","name":"/deplo-web-app-1","project":"prj_abc","state":"running","health":"","restartCount":0,"pid":7}` + "\n" +
		`{"id":"","name":"/nameless","state":"running"}` + "\n"

	got := parseRosterInspectLines(stdout)
	if len(got) != 1 {
		t.Fatalf("parsed %d details, want only the well-formed one", len(got))
	}
	if _, ok := got[id]; !ok {
		t.Fatalf("the well-formed row was dropped: %+v", got)
	}
}

func TestParseEventLine(t *testing.T) {
	const id = "3f8a1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	cases := []struct {
		name        string
		line        string
		ok          bool
		wantAction  string
		wantID      string
		wantManaged bool
	}{
		{
			name: "managed container start",
			line: `{"Type":"container","Action":"start","Actor":{"ID":"` + id + `",` +
				`"Attributes":{"name":"deplo-web-app-1","deplo.managed":"true","deplo.project":"prj_abc"}},` +
				`"time":1752900000}`,
			ok: true, wantAction: "start", wantID: id, wantManaged: true,
		},
		{
			name: "managed container die",
			line: `{"Type":"container","Action":"die","Actor":{"ID":"` + id + `",` +
				`"Attributes":{"exitCode":"1","deplo.managed":"true"}}}`,
			ok: true, wantAction: "die", wantID: id, wantManaged: true,
		},
		{
			name: "managed container destroy",
			line: `{"Type":"container","Action":"destroy","Actor":{"ID":"` + id + `",` +
				`"Attributes":{"deplo.managed":"true"}}}`,
			ok: true, wantAction: "destroy", wantID: id, wantManaged: true,
		},
		{
			name: "foreign container start is parsed but not managed",
			line: `{"Type":"container","Action":"start","Actor":{"ID":"` + id + `",` +
				`"Attributes":{"name":"ci-job-9000"}}}`,
			ok: true, wantAction: "start", wantID: id, wantManaged: false,
		},
		{
			name: "deplo.managed must be exactly true",
			line: `{"Type":"container","Action":"start","Actor":{"ID":"` + id + `",` +
				`"Attributes":{"deplo.managed":"false"}}}`,
			ok: true, wantAction: "start", wantID: id, wantManaged: false,
		},
		{
			name: `legacy id/status shape`,
			line: `{"status":"start","id":"` + id + `","from":"nginx","time":1752900000}`,
			ok:   true, wantAction: "start", wantID: id, wantManaged: false,
		},
		{
			name: "non-container event is ignored",
			line: `{"Type":"network","Action":"connect","Actor":{"ID":"net123","Attributes":{}}}`,
			ok:   false,
		},
		{
			name: "non-churn action is ignored",
			line: `{"Type":"container","Action":"exec_start: bash -c ls","Actor":{"ID":"` + id + `","Attributes":{}}}`,
			ok:   false,
		},
		{
			name: "health_status is not churn",
			line: `{"Type":"container","Action":"health_status: healthy","Actor":{"ID":"` + id + `","Attributes":{}}}`,
			ok:   false,
		},
		{
			name: "event with no id is unusable",
			line: `{"Type":"container","Action":"start","Actor":{"ID":"","Attributes":{}}}`,
			ok:   false,
		},
		{name: "blank line", line: "", ok: false},
		{name: "not json", line: "docker: command not found", ok: false},
		{
			name: "nil attributes does not panic",
			line: `{"Type":"container","Action":"start","Actor":{"ID":"` + id + `"}}`,
			ok:   true, wantAction: "start", wantID: id, wantManaged: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseEventLine(tc.line)
			if ok != tc.ok {
				t.Fatalf("parseEventLine() ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if got.Action != tc.wantAction {
				t.Errorf("action = %q, want %q", got.Action, tc.wantAction)
			}
			if got.ID != tc.wantID {
				t.Errorf("id = %q, want %q", got.ID, tc.wantID)
			}
			if got.Managed != tc.wantManaged {
				t.Errorf("managed = %v, want %v", got.Managed, tc.wantManaged)
			}
		})
	}
}

func TestIsChurnAction(t *testing.T) {
	churn := []string{"start", "die", "destroy"}
	for _, a := range churn {
		if !isChurnAction(a) {
			t.Errorf("isChurnAction(%q) = false, want true", a)
		}
	}
	quiet := []string{"", "create", "exec_start", "exec_die", "health_status", "attach", "top", "resize", "stop"}
	for _, a := range quiet {
		if isChurnAction(a) {
			t.Errorf("isChurnAction(%q) = true, want false", a)
		}
	}
}

func TestBuildRosterEntries(t *testing.T) {
	const idApp = "aaaa1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"
	const idDB = "bbbb1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	rows := []rosterPsRow{
		{ID: idDB, Name: "deplo-web-db-1", State: "exited"},
		{ID: idApp, Name: "deplo-web-app-1", State: "running"},
	}
	details := map[string]rosterDetail{
		idApp: {ID: idApp, Name: "deplo-web-app-1", ProjectID: "prj_abc", State: "running", Health: "healthy", RestartCount: 2, PID: 4242},
		idDB:  {ID: idDB, Name: "deplo-web-db-1", ProjectID: "db_xyz", State: "exited", RestartCount: 0, PID: 3131},
	}
	cgroups := map[string]string{idApp: "/sys/fs/cgroup/system.slice/docker-aaaa.scope"}

	got := buildRosterEntries(rows, details, cgroups)
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got[0].Name != "deplo-web-app-1" || got[1].Name != "deplo-web-db-1" {
		t.Fatalf("entries are not name-ordered: %q, %q", got[0].Name, got[1].Name)
	}

	app := got[0]
	if app.ProjectID != "prj_abc" || app.State != "running" || app.Health != "healthy" ||
		app.RestartCount != 2 || app.PID != 4242 ||
		app.CgroupPath != "/sys/fs/cgroup/system.slice/docker-aaaa.scope" {
		t.Errorf("app entry = %+v", app)
	}

	db := got[1]
	if db.PID != 0 {
		t.Errorf("pid = %d, want 0 for a container that is not running", db.PID)
	}
	if db.CgroupPath != "" {
		t.Errorf("cgroupPath = %q, want empty", db.CgroupPath)
	}
	if db.State != "exited" || db.ProjectID != "db_xyz" {
		t.Errorf("db entry = %+v", db)
	}
}

func TestBuildRosterEntriesSurvivesMissingInspect(t *testing.T) {
	const id = "cccc1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	got := buildRosterEntries(
		[]rosterPsRow{{ID: id, Name: "deplo-web-app-1", State: "running"}},
		map[string]rosterDetail{},
		map[string]string{},
	)
	if len(got) != 1 {
		t.Fatalf("got %d entries, want the ps row to survive an inspect failure", len(got))
	}
	e := got[0]
	if e.ID != id || e.Name != "deplo-web-app-1" || e.State != "running" {
		t.Errorf("entry = %+v, want the ps row's own fields", e)
	}
	if e.ProjectID != "" || e.Health != "" || e.RestartCount != 0 || e.PID != 0 || e.CgroupPath != "" {
		t.Errorf("entry = %+v, want unmeasured fields left at zero", e)
	}
}

func TestBuildRosterEntriesNeverInventsContainers(t *testing.T) {
	const ghost = "dddd1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	got := buildRosterEntries(
		nil,
		map[string]rosterDetail{ghost: {ID: ghost, Name: "deplo-web-ghost-1", State: "running"}},
		map[string]string{ghost: "/sys/fs/cgroup/system.slice/docker-dddd.scope"},
	)
	if len(got) != 0 {
		t.Fatalf("got %d entries, want none - nothing was listed", len(got))
	}
}

func TestBuildRosterEntriesPrefersInspectState(t *testing.T) {
	const id = "eeee1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	got := buildRosterEntries(
		[]rosterPsRow{{ID: id, Name: "deplo-web-app-1", State: "running"}},
		map[string]rosterDetail{id: {ID: id, Name: "deplo-web-app-1", State: "restarting", RestartCount: 9, PID: 555}},
		map[string]string{},
	)
	if got[0].State != "restarting" {
		t.Fatalf("state = %q, want the inspect's answer to win", got[0].State)
	}
	if got[0].RestartCount != 9 {
		t.Errorf("restartCount = %d, want 9", got[0].RestartCount)
	}
	if got[0].PID != 0 {
		t.Errorf("pid = %d, want 0 - a restarting container's pid is not stable", got[0].PID)
	}
}

func TestBuildRosterEntriesClearsTheCgroupOfAStoppedContainer(t *testing.T) {
	const id = "ffff1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

	got := buildRosterEntries(
		[]rosterPsRow{{ID: id, Name: "deplo-web-app-1", State: "exited"}},
		map[string]rosterDetail{id: {ID: id, Name: "deplo-web-app-1", State: "exited", PID: 4242}},
		map[string]string{id: "/sys/fs/cgroup/system.slice/docker-ffff.scope"},
	)
	if got[0].CgroupPath != "" || got[0].PID != 0 {
		t.Fatalf("entry = %+v, want pid AND cgroup cleared for a non-running container", got[0])
	}
}

func TestCgroupPathForPIDRefusesImpossiblePIDs(t *testing.T) {
	for _, pid := range []int{0, -1} {
		if got := cgroupPathForPID("/proc", rosterCgroupRoot, pid); got != "" {
			t.Errorf("cgroupPathForPID(%d) = %q, want empty", pid, got)
		}
	}
}

func TestCgroupPathForPIDResolvesAndValidates(t *testing.T) {
	procRoot := t.TempDir()
	cgroupRoot := t.TempDir()
	const rel = "/system.slice/docker-abc.scope"

	writeFakeProcCgroup(t, procRoot, 4242, "0::"+rel+"\n")
	writeFakeProcCgroup(t, procRoot, 4343, "0::/system.slice/docker-gone.scope\n")
	writeFakeProcCgroup(t, procRoot, 4444, "11:memory:/docker/abc\n")

	if err := os.MkdirAll(filepath.Join(cgroupRoot, "system.slice", "docker-abc.scope"), 0o755); err != nil {
		t.Fatal(err)
	}

	want := filepath.Join(cgroupRoot, "system.slice", "docker-abc.scope")
	if got := cgroupPathForPID(procRoot, cgroupRoot, 4242); got != want {
		t.Errorf("resolved = %q, want %q", got, want)
	}
	if got := cgroupPathForPID(procRoot, cgroupRoot, 4343); got != "" {
		t.Errorf("nonexistent cgroup dir = %q, want empty", got)
	}
	if got := cgroupPathForPID(procRoot, cgroupRoot, 4444); got != "" {
		t.Errorf("cgroup v1 host = %q, want empty", got)
	}
	if got := cgroupPathForPID(procRoot, cgroupRoot, 9999); got != "" {
		t.Errorf("missing pid = %q, want empty", got)
	}
}

func writeFakeProcCgroup(t *testing.T, procRoot string, pid int, body string) {
	t.Helper()
	dir := filepath.Join(procRoot, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

type fakeDocker struct {
	mu            sync.Mutex
	rows          []rosterPsRow
	details       map[string]rosterDetail
	listErr       error
	inspErr       error
	listHits      int
	inspHits      int
	hostRunning   int
	hostCountFail bool
}

func (f *fakeDocker) hostCount(context.Context) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hostCountFail {
		return 0, false
	}
	return f.hostRunning, true
}

func (f *fakeDocker) list(context.Context) ([]rosterPsRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listHits++
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]rosterPsRow(nil), f.rows...), nil
}

func (f *fakeDocker) inspect(context.Context, []string) (map[string]rosterDetail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspHits++
	if f.inspErr != nil {
		return nil, f.inspErr
	}
	out := map[string]rosterDetail{}
	for k, v := range f.details {
		out[k] = v
	}
	return out, nil
}

func (f *fakeDocker) set(fn func(*fakeDocker)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func newFakeRoster(t *testing.T) (*roster, *fakeDocker) {
	t.Helper()
	f := &fakeDocker{details: map[string]rosterDetail{}}
	r := newRosterDefaults()
	r.debounce = 20 * time.Millisecond
	r.backstop = time.Hour
	r.listFn = f.list
	r.inspectFn = f.inspect
	r.hostCountFn = f.hostCount
	r.watchFn = func(ctx context.Context) { <-ctx.Done() }
	return r, f
}

const rosterTestIDA = "aaaa1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"
const rosterTestIDB = "bbbb1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"

func seedOneApp(f *fakeDocker) {
	f.rows = []rosterPsRow{{ID: rosterTestIDA, Name: "deplo-web-app-1", State: "running"}}
	f.details = map[string]rosterDetail{
		rosterTestIDA: {ID: rosterTestIDA, Name: "deplo-web-app-1", ProjectID: "prj_abc", State: "running", PID: 0},
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRosterStartPopulatesSynchronously(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)

	r.start(context.Background())
	defer r.Close()

	entries, running := r.Snapshot()
	if len(entries) != 1 || running != 1 {
		t.Fatalf("Snapshot() = %+v, %d running; want the roster already populated", entries, running)
	}
	if entries[0].ProjectID != "prj_abc" {
		t.Errorf("projectID = %q, want the demux key populated on the first read", entries[0].ProjectID)
	}
}

func TestRosterInitialRebuildIsBounded(t *testing.T) {
	r, _ := newFakeRoster(t)

	var deadline time.Time
	var ok bool
	r.rebuildFn = func(ctx context.Context) { deadline, ok = ctx.Deadline() }

	r.start(context.Background())
	defer r.Close()

	if !ok {
		t.Fatal("the initial rebuild ran with no deadline; a wedged daemon would block the caller")
	}
	if d := time.Until(deadline); d > rosterInitialRebuild+time.Second {
		t.Errorf("initial rebuild deadline is %s out, want <= %s", d, rosterInitialRebuild)
	}
}

func TestRosterDebounceCoalescesABurst(t *testing.T) {
	r, _ := newFakeRoster(t)

	var rebuilds atomic.Int32
	r.rebuildFn = func(context.Context) { rebuilds.Add(1) }

	r.start(context.Background())
	defer r.Close()
	if got := rebuilds.Load(); got != 1 {
		t.Fatalf("start() ran %d rebuilds, want exactly the 1 synchronous one", got)
	}

	for i := 0; i < 8; i++ {
		r.markDirty()
	}
	waitFor(t, "the debounced rebuild", func() bool { return rebuilds.Load() == 2 })

	time.Sleep(5 * r.debounce)
	if got := rebuilds.Load(); got != 2 {
		t.Fatalf("8 events produced %d rebuilds (incl. the initial one), want 2", got)
	}
}

func TestRosterEventDuringARebuildTriggersAnother(t *testing.T) {
	r, _ := newFakeRoster(t)

	var rebuilds atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	r.rebuildFn = func(context.Context) {
		n := rebuilds.Add(1)
		if n == 2 {
			started <- struct{}{}
			<-release
		}
	}

	r.start(context.Background())
	defer r.Close()

	r.markDirty()
	<-started
	r.markDirty()
	close(release)

	waitFor(t, "the follow-up rebuild", func() bool { return rebuilds.Load() >= 3 })
}

func TestRosterBackstopRebuildsWithoutAnyEvent(t *testing.T) {
	r, _ := newFakeRoster(t)
	r.backstop = 20 * time.Millisecond

	var rebuilds atomic.Int32
	r.rebuildFn = func(context.Context) { rebuilds.Add(1) }

	r.start(context.Background())
	defer r.Close()

	waitFor(t, "backstop rebuilds with no events at all", func() bool { return rebuilds.Load() >= 3 })
}

func TestRosterKeepsTheLastGoodRosterWhenTheListingFails(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)

	r.start(context.Background())
	defer r.Close()

	f.set(func(f *fakeDocker) { f.listErr = context.DeadlineExceeded })
	r.rebuild(context.Background())

	entries, running := r.Snapshot()
	if len(entries) != 1 || running != 1 || entries[0].ProjectID != "prj_abc" {
		t.Fatalf("Snapshot() = %+v (%d running), want the last good roster untouched", entries, running)
	}
}

func TestRosterKeepsTheLastGoodRosterWhenTheInspectFails(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)

	r.start(context.Background())
	defer r.Close()

	f.set(func(f *fakeDocker) { f.inspErr = context.DeadlineExceeded })
	r.rebuild(context.Background())

	entries, _ := r.Snapshot()
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want the last good roster kept", len(entries))
	}
	if entries[0].ProjectID != "prj_abc" {
		t.Fatalf("projectID = %q - a failed inspect blanked the demux key", entries[0].ProjectID)
	}
	if entries[0].State != "running" {
		t.Errorf("state = %q, want the last good value", entries[0].State)
	}
}

func TestRosterSwapsInAFreshSnapshotAndPrunesTheCgroupCache(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)

	procRoot, cgroupRoot := t.TempDir(), t.TempDir()
	writeFakeProcCgroup(t, procRoot, 4242, "0::/system.slice/docker-aaaa.scope\n")
	if err := os.MkdirAll(filepath.Join(cgroupRoot, "system.slice", "docker-aaaa.scope"), 0o755); err != nil {
		t.Fatal(err)
	}
	r.procRoot, r.cgroupRoot = procRoot, cgroupRoot
	f.details[rosterTestIDA] = rosterDetail{
		ID: rosterTestIDA, Name: "deplo-web-app-1", ProjectID: "prj_abc", State: "running", PID: 4242,
	}

	r.start(context.Background())
	defer r.Close()

	entries, _ := r.Snapshot()
	want := filepath.Join(cgroupRoot, "system.slice", "docker-aaaa.scope")
	if entries[0].CgroupPath != want {
		t.Fatalf("cgroupPath = %q, want %q", entries[0].CgroupPath, want)
	}
	if _, ok := r.cachedCgroup(rosterTestIDA); !ok {
		t.Fatal("the resolved path was not cached")
	}

	f.set(func(f *fakeDocker) {
		f.rows = []rosterPsRow{{ID: rosterTestIDB, Name: "deplo-web-app-2", State: "running"}}
		f.details = map[string]rosterDetail{
			rosterTestIDB: {ID: rosterTestIDB, Name: "deplo-web-app-2", ProjectID: "prj_abc", State: "running"},
		}
	})
	r.rebuild(context.Background())

	entries, _ = r.Snapshot()
	if len(entries) != 1 || entries[0].ID != rosterTestIDB {
		t.Fatalf("entries = %+v, want only the new container", entries)
	}
	if _, ok := r.cachedCgroup(rosterTestIDA); ok {
		t.Error("the destroyed container's cgroup entry survived; the cache grows without bound")
	}
	if !r.relevant(dockerEvent{ID: rosterTestIDB}) {
		t.Error("an unlabeled event for a TRACKED id was dropped; a destroy would strand a dead container")
	}
	if r.relevant(dockerEvent{ID: rosterTestIDA}) {
		t.Error("an unlabeled event for a destroyed id still counts; foreign churn would cost rebuilds")
	}
}

func TestRosterDoesNotResolveACgroupForANonRunningContainer(t *testing.T) {
	r, f := newFakeRoster(t)

	procRoot, cgroupRoot := t.TempDir(), t.TempDir()
	writeFakeProcCgroup(t, procRoot, 4242, "0::/system.slice/docker-aaaa.scope\n")
	if err := os.MkdirAll(filepath.Join(cgroupRoot, "system.slice", "docker-aaaa.scope"), 0o755); err != nil {
		t.Fatal(err)
	}
	r.procRoot, r.cgroupRoot = procRoot, cgroupRoot
	f.rows = []rosterPsRow{{ID: rosterTestIDA, Name: "deplo-web-app-1", State: "exited"}}
	f.details = map[string]rosterDetail{
		rosterTestIDA: {ID: rosterTestIDA, Name: "deplo-web-app-1", ProjectID: "prj_abc", State: "exited", PID: 4242},
	}

	r.start(context.Background())
	defer r.Close()

	if p, ok := r.cachedCgroup(rosterTestIDA); ok {
		t.Fatalf("cached %q for a non-running container", p)
	}
	entries, running := r.Snapshot()
	if entries[0].CgroupPath != "" || entries[0].PID != 0 {
		t.Fatalf("entry = %+v, want no cgroup and no pid for an exited container", entries[0])
	}
	if running != 0 {
		t.Errorf("running = %d, want 0", running)
	}
}

func TestRosterEntriesReturnsACopy(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)

	r.start(context.Background())
	defer r.Close()

	got := r.Entries()
	got[0].ProjectID = "clobbered"
	got[0].State = "exited"

	again, running := r.Snapshot()
	if again[0].ProjectID != "prj_abc" || again[0].State != "running" || running != 1 {
		t.Fatalf("the roster was mutated through the returned slice: %+v", again[0])
	}
}

func TestRosterCloseIsIdempotentAndDrainsBothGoroutines(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)

	var watcherExited atomic.Bool
	r.watchFn = func(ctx context.Context) {
		<-ctx.Done()
		watcherExited.Store(true)
	}

	r.start(context.Background())

	done := make(chan struct{})
	go func() {
		r.Close()
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return; a goroutine was not drained")
	}
	if !watcherExited.Load() {
		t.Fatal("the watcher goroutine was still running after Close() returned")
	}

	before := f.listHits
	r.markDirty()
	time.Sleep(5 * r.debounce)
	if f.listHits != before {
		t.Errorf("a rebuild ran after Close(): %d -> %d listings", before, f.listHits)
	}
}

func TestRosterStopsWhenTheParentContextIsCancelled(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)

	ctx, cancel := context.WithCancel(context.Background())
	r.start(ctx)
	cancel()

	waitFor(t, "the rebuild loop to stop", func() bool {
		before := f.listHits
		r.markDirty()
		time.Sleep(5 * r.debounce)
		return f.listHits == before
	})
	r.Close()
}

func TestMarkDirtyCoalescesWithoutBlocking(t *testing.T) {
	r, _ := newFakeRoster(t)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			r.markDirty()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("markDirty blocked; the events reader would wedge behind it")
	}
	if got := len(r.dirty); got != 1 {
		t.Fatalf("1000 events left %d tokens, want exactly 1", got)
	}
}

// The host gauge must count EVERY running container on the host, while the roster's own RunningCount stays scoped to deplo.managed ones.
func TestRosterHostCountIsUnfilteredWhileRunningCountIsScoped(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)
	f.set(func(f *fakeDocker) { f.hostRunning = 7 })

	r.start(context.Background())
	defer r.Close()

	if got := r.RunningCount(); got != 1 {
		t.Errorf("RunningCount() = %d, want 1 (deplo.managed only)", got)
	}
	if got := r.HostRunningCount(); got != 7 {
		t.Errorf("HostRunningCount() = %d, want 7 (every container on the host)", got)
	}
}

// A FAILED `docker ps -q` yields no count.
func TestRosterHostCountKeepsLastKnownOnFailure(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)
	f.set(func(f *fakeDocker) { f.hostRunning = 4 })

	r.start(context.Background())
	defer r.Close()

	if got := r.HostRunningCount(); got != 4 {
		t.Fatalf("precondition: HostRunningCount() = %d, want 4", got)
	}

	f.set(func(f *fakeDocker) { f.hostCountFail = true })
	r.markDirty()
	waitFor(t, "a rebuild after the failing count", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.listHits >= 2
	})

	if got := r.HostRunningCount(); got != 4 {
		t.Errorf("HostRunningCount() = %d after a failed count, want the last known 4", got)
	}
}

// The counterpart to the failure test: a host that legitimately has 0 running containers must report 0, not a stale figure.
func TestRosterHostCountPublishesGenuineZero(t *testing.T) {
	r, f := newFakeRoster(t)
	seedOneApp(f)
	f.set(func(f *fakeDocker) { f.hostRunning = 4 })

	r.start(context.Background())
	defer r.Close()

	if got := r.HostRunningCount(); got != 4 {
		t.Fatalf("precondition: HostRunningCount() = %d, want 4", got)
	}

	f.set(func(f *fakeDocker) { f.hostRunning = 0 })
	r.markDirty()
	waitFor(t, "the host count to fall to a genuine 0", func() bool {
		return r.HostRunningCount() == 0
	})

	if got := r.HostRunningCount(); got != 0 {
		t.Errorf("HostRunningCount() = %d after a genuine empty read, want 0", got)
	}
}

func TestParseEventLineOom(t *testing.T) {
	const id = "cccc1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"
	ev, ok := parseEventLine(`{"Type":"container","Action":"oom","Actor":{"ID":"` + id + `","Attributes":{"deplo.managed":"true"}}}`)
	if !ok || ev.Action != "oom" || ev.ID != id || !ev.Managed {
		t.Fatalf("parseEventLine(oom) = %+v, %v", ev, ok)
	}
	if isChurnAction("oom") {
		t.Error("an oom must not trigger a rebuild on its own: the die that follows does")
	}
}

func TestRosterCountsOomKillsPerContainer(t *testing.T) {
	const idA = "aaaa1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"
	const idB = "bbbb1c9e5b2d4a7c8e1f0b6d9a2c5e8f1b4d7a0c3e6f9b2d5a8c1e4f7b0d3a6c"
	r := newRosterDefaults()
	r.entries = []rosterEntry{{ID: idA}, {ID: idB}}
	r.recordOom(idA)
	r.recordOom(idA)

	got := map[string]int32{}
	for _, e := range r.Entries() {
		got[e.ID] = e.OomKills
	}
	if got[idA] != 2 || got[idB] != 0 {
		t.Fatalf("oom kills = %v, want A=2 B=0", got)
	}
	if r.entries[0].OomKills != 0 {
		t.Error("Entries must hand out a copy, not write into the live roster")
	}

	r.listFn = func(context.Context) ([]rosterPsRow, error) {
		return []rosterPsRow{{ID: idB, Name: "b", State: "running"}}, nil
	}
	r.inspectFn = func(context.Context, []string) (map[string]rosterDetail, error) {
		return map[string]rosterDetail{}, nil
	}
	r.hostCountFn = func(context.Context) (int, bool) { return 1, true }
	r.rebuild(context.Background())
	if _, kept := r.ooms[idA]; kept {
		t.Error("a container that left the roster must not keep its oom count")
	}
}
