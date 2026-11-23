package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

const writer = "db-writer"

// fakeWriters is Docker for the supervisor: the containers of each stack, their policies and what was started.
type fakeWriters struct {
	mu       sync.Mutex
	cs       map[string][]writerContainer
	started  []string
	policies [][2]string
}

func (f *fakeWriters) list(_ context.Context, slug string) ([]writerContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.cs[slug]), nil
}

func (f *fakeWriters) setPolicy(_ context.Context, ids []string, policy string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for slug, cs := range f.cs {
		for i := range cs {
			if slices.Contains(ids, cs[i].ID) {
				f.cs[slug][i].Policy = policy
				f.policies = append(f.policies, [2]string{cs[i].ID, policy})
			}
		}
	}
	return nil
}

func (f *fakeWriters) start(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, id)
	for slug, cs := range f.cs {
		for i := range cs {
			if cs[i].ID == id {
				f.cs[slug][i].State = "running"
			}
		}
	}
	return nil
}

// stops makes a container stop by itself, as a crash does.
func (f *fakeWriters) stops(id string, code int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for slug, cs := range f.cs {
		for i := range cs {
			if cs[i].ID == id {
				f.cs[slug][i].State, f.cs[slug][i].ExitCode = "exited", code
			}
		}
	}
}

func (f *fakeWriters) startedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.started)
}

// supervisedMember is a member whose single-writer stack has one container, stopped by a power cut.
func supervisedMember(t *testing.T, saved map[string]*clusterLease) (*Service, *fakeClock, *fakeWriters) {
	t.Helper()
	s := newTestService(t)
	s.SetAgentDir(t.TempDir())
	clk := &fakeClock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	if saved != nil {
		raw, _ := json.Marshal(saved)
		if err := os.WriteFile(s.leaseFile(), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s.leaseOnce.Do(func() {
		s.lease = newLeaseGuard(s.leaseFile(), clk.Now, func(context.Context, []lease.Entry) error { return nil })
	})
	fw := &fakeWriters{cs: map[string][]writerContainer{writer: {{ID: "c1", Name: writer, Service: writer, State: "exited",
		Policy: "unless-stopped", ExitCode: 255}}}}
	s.supOnce.Do(func() {
		s.sup = newSupervisor(s.supervisorFile(), clk.Now)
		s.sup.list, s.sup.setPolicy, s.sup.start = fw.list, fw.setPolicy, fw.start
	})
	return s, clk, fw
}

func placedOn(member string) []lease.Entry {
	return []lease.Entry{{Kind: "database", ID: "db_1", Member: member, Slug: writer, SingleWriter: true}}
}

func renewHere(t *testing.T, s *Service) {
	t.Helper()
	if r := s.leaseGuard().Renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1, Placement: placedOn("srv_a")}); !r.OK {
		t.Fatalf("renewal refused: %+v", r)
	}
}

// supervise runs one pass and waits for the starts it began.
func supervise(t *testing.T, s *Service) {
	t.Helper()
	s.superviseOnce(context.Background())
	eventually(t, "the supervisor's starts", func() bool {
		s.sup.mu.Lock()
		defer s.sup.mu.Unlock()
		return len(s.sup.busy) == 0
	})
}

func TestSupervise_aLeaseReadBackFromDiskTurnsThePolicyOffAndStartsNothing(t *testing.T) {
	clk := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	saved := map[string]*clusterLease{"clu_1": {Epoch: 1, Member: "srv_a", RenewedAt: clk, Placement: placedOn("srv_a"), Boot: bootID()}}
	s, _, fw := supervisedMember(t, saved)
	supervise(t, s)
	if got := fw.startedIDs(); len(got) != 0 {
		t.Fatalf("started %v on a lease no leader renewed since the agent started", got)
	}
	if fw.cs[writer][0].Policy != "no" {
		t.Fatalf("Docker's restart policy is still %q", fw.cs[writer][0].Policy)
	}
	raw, _ := os.ReadFile(s.supervisorFile())
	var st supervisorState
	if json.Unmarshal(raw, &st) != nil || st.Kept[writer][writer] != "unless-stopped" {
		t.Fatalf("the policy turned off was not kept on disk: %s", raw)
	}

	renewHere(t, s)
	supervise(t, s)
	if got := fw.startedIDs(); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("started %v, want c1 once a leader renewed the lease", got)
	}
}

// The listing as Docker prints it for a container Deplo rendered before the label existed (deplo#237 lab, canary.10).
const unlabelledListing = "c1\t/db-writer\tdb-writer\texited\tunless-stopped\t<no value>\t255\t" +
	"2026-03-01T11:00:00.1Z\t2026-03-01T11:59:00.2Z\tFalse\n"

func TestParseWriterContainers_aLabelTheContainerLacksIsEmpty(t *testing.T) {
	cs := parseWriterContainers(unlabelledListing + "c2\t/x\t<no value>\texited\tno\talways\t0\t\t\t<no value>\n")
	if len(cs) != 2 || cs[0].Label != "" || cs[0].Service != writer || cs[0].Policy != "unless-stopped" {
		t.Fatalf("parsed %+v, want db-writer with no label and its policy", cs)
	}
	if cs[1].Service != "" || cs[1].Label != "always" {
		t.Fatalf("parsed %+v, want no service and the label always", cs[1])
	}
}

func TestSupervise_aContainerWithoutTheLabelIsStartedAfterTheAgentRestarts(t *testing.T) {
	clk := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	saved := map[string]*clusterLease{"clu_1": {Epoch: 1, Member: "srv_a", RenewedAt: clk, Placement: placedOn("srv_a"), Boot: bootID()}}
	s, _, fw := supervisedMember(t, saved)
	fw.cs[writer] = parseWriterContainers(unlabelledListing)
	supervise(t, s)
	if fw.cs[writer][0].Policy != "no" {
		t.Fatalf("Docker's restart policy is still %q", fw.cs[writer][0].Policy)
	}
	// The agent starts again: only what is on disk says the container was meant to come back.
	sv := newSupervisor(s.supervisorFile(), s.sup.now)
	sv.list, sv.setPolicy, sv.start = fw.list, fw.setPolicy, fw.start
	s.sup = sv
	renewHere(t, s)
	supervise(t, s)
	if got := fw.startedIDs(); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("started %v, want c1: its policy was unless-stopped", got)
	}
}

// logged keeps what the supervisor logs, to look for a line in it.
type logged struct {
	mu    sync.Mutex
	lines []string
}

func captureLog(s *Service) *logged {
	l := &logged{}
	s.sup.logf = func(format string, a ...any) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.lines = append(l.lines, fmt.Sprintf(format, a...))
	}
	return l
}

// count is how many lines say what.
func (l *logged) count(what string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, what) {
			n++
		}
	}
	return n
}

func TestRestartPolicy_readsWhatDockerAndComposeWrite(t *testing.T) {
	for in, want := range map[string]string{"unless-stopped": "unless-stopped", " Always ": "always", "on-failure:3": "on-failure", "no": "no"} {
		if got, ok := restartPolicy(in); !ok || got != want {
			t.Errorf("restartPolicy(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "<no value>", "sometimes"} {
		if got, ok := restartPolicy(in); ok {
			t.Errorf("restartPolicy(%q) = %q, want no policy", in, got)
		}
	}
}

func TestSupervise_aLabelPresentButEmptyIsSupervised(t *testing.T) {
	s, _, fw := supervisedMember(t, nil)
	// The policy is off and nothing was kept: an empty label says nothing either.
	fw.cs[writer] = parseWriterContainers("c1\t/db-writer\tdb-writer\texited\tno\t\t255\t\t\tFalse\n")
	if fw.cs[writer][0].Label != "" {
		t.Fatalf("parsed label %q", fw.cs[writer][0].Label)
	}
	renewHere(t, s)
	supervise(t, s)
	if got := fw.startedIDs(); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("started %v, want c1: a writer with no record of its policy is supervised", got)
	}
}

func TestSupervise_aRecordThatCannotBeReadMeansSupervision(t *testing.T) {
	s, _, fw := supervisedMember(t, nil)
	if err := os.WriteFile(s.supervisorFile(), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	sv := newSupervisor(s.supervisorFile(), s.sup.now)
	sv.list, sv.setPolicy, sv.start = fw.list, fw.setPolicy, fw.start
	s.sup = sv
	fw.cs[writer][0].Policy = "no"
	renewHere(t, s)
	supervise(t, s)
	if got := fw.startedIDs(); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("started %v, want c1 although its record could not be read", got)
	}
}

func TestSupervise_theStacksFileSaysWhatTheRecordLost(t *testing.T) {
	s, _, fw := supervisedMember(t, nil)
	stack := []byte(`services:
  db-writer:
    image: postgres:17-alpine
    restart: "no"
    labels:
      - deplo.restart=unless-stopped
  migrate:
    image: busybox:1.36.1
    restart: "no"
  web:
    image: busybox:1.36.1
    labels:
      deplo.restart: always
`)
	if got := renderedPolicies(stack); got["db-writer"] != "unless-stopped" || got["migrate"] != "no" || got["web"] != "always" {
		t.Fatalf("renderedPolicies = %v", got)
	}
	s.sup.rendered = func(string) map[string]string { return renderedPolicies(stack) }
	fw.cs[writer] = []writerContainer{
		{ID: "c1", Name: writer, Service: "db-writer", State: "exited", Policy: "no", ExitCode: 255},
		{ID: "c2", Name: "migrate", Service: "migrate", State: "exited", Policy: "no", ExitCode: 0},
	}
	renewHere(t, s)
	supervise(t, s)
	if got := fw.startedIDs(); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("started %v, want c1 only: the file keeps a one-off service down", got)
	}
}

func TestSupervise_logsEveryReasonOnceEachTimeItChanges(t *testing.T) {
	clk := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	saved := map[string]*clusterLease{"clu_1": {Epoch: 1, Member: "srv_a", RenewedAt: clk, Placement: placedOn("srv_a"), Boot: bootID()}}
	s, _, fw := supervisedMember(t, saved)
	log := captureLog(s)
	supervise(t, s)
	supervise(t, s)
	if n := log.count(writer + " is not started here: no leader has renewed this server's lease since the agent started"); n != 1 {
		t.Fatalf("the missing lease was logged %d times, want once: %q", n, log.lines)
	}
	renewHere(t, s)
	supervise(t, s)
	if n := log.count("started " + writer + " again under its lease"); n != 1 {
		t.Fatalf("the start was logged %d times: %q", n, log.lines)
	}
	s.writerStopped(stoppedBecause(context.Background(), "its restore failed, so it never serves empty"), writer, nil)
	fw.stops("c1", 1)
	supervise(t, s)
	supervise(t, s)
	if log.count("stays down until it is started or deployed again: its restore failed") != 1 || log.count(writer+" stays down: it was stopped on purpose") != 1 {
		t.Fatalf("the stop on purpose was not logged once with its reason: %q", log.lines)
	}
}

// bootRace is three writers and a stateless App on srv_a, with the fence, the return and Docker sharing one picture
// of what runs, as on lab-2 after the lab-wide boot of deplo#237's case 1b-5.
type bootRace struct {
	s   *Service
	clk *fakeClock
	fw  *fakeWriters
	ids map[string]string
}

func placedFour(web string) []lease.Entry {
	return []lease.Entry{{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-pg", SingleWriter: true},
		{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "notes", SingleWriter: true},
		{Kind: "app", ID: "prj_2", Member: "srv_a", Slug: "volw", SingleWriter: true},
		{Kind: "app", ID: "prj_3", Member: web, Slug: "web"}}
}

func newBootRace(t *testing.T) *bootRace {
	clk := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	saved := map[string]*clusterLease{"clu_1": {Epoch: 1, Member: "srv_a", RenewedAt: clk.Add(-time.Hour), Placement: placedFour("srv_a"), Boot: bootID()}}
	s, c, fw := supervisedMember(t, saved)
	b := &bootRace{s: s, clk: c, fw: fw, ids: map[string]string{"db-pg": "pg1", "notes": "n1", "volw": "v1"}}
	fw.cs = map[string][]writerContainer{}
	for slug, id := range b.ids {
		fw.cs[slug] = []writerContainer{{ID: id, Name: slug, Service: slug, State: "exited", Policy: "unless-stopped", ExitCode: 255}}
	}
	g := s.leaseGuard()
	g.running = func(_ context.Context, slug string) bool { return b.runs(slug) }
	g.has = func(string) bool { return true }
	g.remove = func(context.Context, lease.Entry) error { return nil }
	g.resume = func(ctx context.Context, slug string) error {
		if id := b.ids[slug]; id != "" {
			return fw.start(ctx, id)
		}
		return nil
	}
	g.fence = func(context.Context, []lease.Entry) error { b.stopAll(); return nil }
	return b
}

func (b *bootRace) runs(slug string) bool {
	b.fw.mu.Lock()
	defer b.fw.mu.Unlock()
	cs := b.fw.cs[slug]
	return len(cs) > 0 && cs[0].State == "running"
}

func (b *bootRace) stopAll() {
	for _, id := range b.ids {
		b.fw.stops(id, 137)
	}
}

func (b *bootRace) renew(web string) {
	b.s.leaseGuard().Renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1, Placement: placedFour(web)})
	b.s.leaseGuard().work.Wait()
}

func (b *bootRace) down(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, slug := range []string{"db-pg", "notes", "volw"} {
		if !b.runs(slug) {
			out = append(out, slug)
		}
	}
	return out
}

func TestSupervise_aFenceThatRacesTheSupervisorAtBootLeavesNoWriterDown(t *testing.T) {
	b := newBootRace(t)
	b.renew("srv_a")
	supervise(t, b.s)
	if d := b.down(t); len(d) != 0 {
		t.Fatalf("down after the first renewal: %v", d)
	}
	// No renewal for the whole lease: the fence stops all three, and the supervisor starts db-pg again meanwhile,
	// so the fence fails and holds nothing. The leader moved web away while it heard nothing.
	b.clk.Add(lease.FenceAfter + time.Second)
	b.s.leaseGuard().fence = func(context.Context, []lease.Entry) error {
		b.stopAll()
		_ = b.fw.start(context.Background(), "pg1")
		return errors.New("db-pg: still running")
	}
	b.s.leaseGuard().Check(context.Background())
	b.renew("srv_b")
	for i := 0; i < 3; i++ {
		b.clk.Add(3 * time.Second)
		b.renew("srv_b")
		supervise(t, b.s)
	}
	if d := b.down(t); len(d) != 0 {
		t.Fatalf("still down after the return: %v", d)
	}
}

func TestSupervise_aSecondRunOutWhileAWriterIsDownLeavesNoWriterDown(t *testing.T) {
	b := newBootRace(t)
	b.renew("srv_a")
	supervise(t, b.s)
	// The first run-out stops everything; the lease is back before the supervisor has started volw again.
	b.clk.Add(lease.FenceAfter + time.Second)
	b.s.leaseGuard().fence = func(context.Context, []lease.Entry) error {
		b.stopAll()
		_ = b.fw.start(context.Background(), "pg1")
		return errors.New("db-pg: still running")
	}
	b.s.leaseGuard().Check(context.Background())
	b.renew("srv_b")
	_ = b.fw.start(context.Background(), "n1")
	// A second run-out finds volw down: the fence holds only db-pg and notes, and the return starts only those.
	b.clk.Add(lease.FenceAfter + time.Second)
	b.s.leaseGuard().fence = func(context.Context, []lease.Entry) error { b.stopAll(); return nil }
	b.s.leaseGuard().Check(context.Background())
	b.renew("srv_b")
	for i := 0; i < 3; i++ {
		b.clk.Add(3 * time.Second)
		b.renew("srv_b")
		supervise(t, b.s)
	}
	if d := b.down(t); len(d) != 0 {
		t.Fatalf("still down after the second return: %v", d)
	}
}

func TestSupervise_startsNothingPlacedElsewhereOrOnALeaseThatRanOut(t *testing.T) {
	s, clk, fw := supervisedMember(t, nil)
	s.leaseGuard().Renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1, Placement: placedOn("srv_b")})
	supervise(t, s)
	if got := fw.startedIDs(); len(got) != 0 {
		t.Fatalf("started %v, which the leader placed on another member", got)
	}
	if fw.cs[writer][0].Policy != "no" {
		t.Fatal("a copy that moved away keeps Docker's restart policy, so the next boot would start it")
	}

	renewHere(t, s)
	clk.Add(lease.FenceAfter + time.Second)
	s.sup.sweepSoon()
	supervise(t, s)
	if got := fw.startedIDs(); len(got) != 0 {
		t.Fatalf("started %v on a lease that ran out", got)
	}
}

func TestSupervise_leavesAStackStoppedOnPurposeDown(t *testing.T) {
	s, _, fw := supervisedMember(t, nil)
	renewHere(t, s)
	s.writerStopped(context.Background(), writer, nil)
	supervise(t, s)
	if got := fw.startedIDs(); len(got) != 0 {
		t.Fatalf("started %v, which someone stopped", got)
	}
	// The stop outlives the agent.
	if sv := newSupervisor(s.supervisorFile(), time.Now); !slices.Contains(sv.stopped[writer], "") {
		t.Fatalf("the stop was not kept on disk: %v", sv.stopped)
	}

	s.supervision().cleared(writer)
	supervise(t, s)
	if got := fw.startedIDs(); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("started %v after the stack was started again, want c1", got)
	}
}

func TestSupervise_waitsLongerAfterEachStopInARow(t *testing.T) {
	s, clk, fw := supervisedMember(t, nil)
	renewHere(t, s)
	starts := func() int { return len(fw.startedIDs()) }
	ms := time.Millisecond
	// Started at once, then 1 s, 2 s and 4 s after the start before: each stop right after the start.
	steps := []struct {
		stop  bool
		after time.Duration
		n     int
	}{{false, 0, 1}, {true, 900 * ms, 1}, {false, 100 * ms, 2}, {true, 1900 * ms, 2}, {false, 100 * ms, 3},
		{true, 3900 * ms, 3}, {false, 100 * ms, 4}}
	for i, st := range steps {
		if st.stop {
			fw.stops("c1", 1)
		}
		clk.Add(st.after)
		supervise(t, s)
		if got := starts(); got != st.n {
			t.Fatalf("step %d: %d starts, want %d", i, got, st.n)
		}
	}
	// One that ran for a while before it stopped is started at once, and the pauses start over.
	fw.mu.Lock()
	fw.cs[writer][0].StartedAt, fw.cs[writer][0].FinishedAt = clk.Now(), clk.Now().Add(superviseSettled)
	fw.mu.Unlock()
	fw.stops("c1", 1)
	clk.Add(8 * time.Second)
	supervise(t, s)
	fw.stops("c1", 1)
	clk.Add(time.Second)
	supervise(t, s)
	if got := starts(); got != 6 {
		t.Fatalf("%d starts after a long run and a stop 1 s later, want 6", got)
	}
}

func TestSupervise_followsThePolicyItTurnedOff(t *testing.T) {
	s, _, fw := supervisedMember(t, nil)
	fw.cs[writer] = []writerContainer{
		{ID: "ok", Service: "migrate", State: "exited", Policy: "on-failure", ExitCode: 0},
		{ID: "failed", Service: "worker", State: "exited", Policy: "on-failure", ExitCode: 1},
		{ID: "once", Service: "seed", State: "exited", Policy: "no", ExitCode: 0},
		{ID: "labelled", Service: "db", State: "exited", Policy: "no", Label: "unless-stopped", ExitCode: 0},
		{ID: "paused", Service: "web", State: "paused", Policy: "unless-stopped"},
	}
	// The stack's file is what says seed runs once: without it, seed would be supervised like any writer.
	s.sup.rendered = func(string) map[string]string { return map[string]string{"seed": "no"} }
	renewHere(t, s)
	supervise(t, s)
	got := fw.startedIDs()
	slices.Sort(got)
	if !slices.Equal(got, []string{"failed", "labelled"}) {
		t.Fatalf("started %v, want only what Docker's own policy would start: failed, labelled", got)
	}
}

func TestSupervise_givesThePolicyBackWhenTheServerLeavesItsCluster(t *testing.T) {
	s, _, fw := supervisedMember(t, nil)
	fw.cs["web-data"] = []writerContainer{{ID: "w1", Service: "web", State: "running", Policy: "no", Label: "always"}}
	s.leaseGuard().Renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1, Placement: append(placedOn("srv_a"),
		lease.Entry{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web-data", SingleWriter: true})})
	supervise(t, s)
	if fw.cs[writer][0].Policy != "no" {
		t.Fatal("the policy was not turned off")
	}
	all, _ := s.leaseGuard().writers()
	s.leaseGuard().Forget()
	s.supervision().release(context.Background(), all)
	if fw.cs[writer][0].Policy != "unless-stopped" || fw.cs["web-data"][0].Policy != "always" {
		t.Fatalf("policies after leaving: %q and %q, want unless-stopped and always", fw.cs[writer][0].Policy, fw.cs["web-data"][0].Policy)
	}
	if raw, _ := os.ReadFile(s.supervisorFile()); string(raw) != "{}" {
		t.Fatalf("still kept after leaving: %s", raw)
	}
}

func TestSupervise_aRestoreKeepsTheStackDownUntilItRunsAgain(t *testing.T) {
	dockerOnPath(t, "")
	s, _, fw := supervisedMember(t, nil)
	renewHere(t, s)
	after := s.restoringWriter(writer)
	supervise(t, s)
	if got := fw.startedIDs(); len(got) != 0 {
		t.Fatalf("started %v while a restore had it stopped", got)
	}
	// The restore ended without starting it, as a failed one does: it stays down, as Docker leaves a stopped container.
	after()
	supervise(t, s)
	if got := fw.startedIDs(); len(got) != 0 {
		t.Fatalf("started %v after a restore that left it stopped", got)
	}
}

func TestHolders_aLeaseReadBackFromDiskHoldsNothingHere(t *testing.T) {
	dir := t.TempDir()
	clk := &fakeClock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	placement := []lease.Entry{{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "notes", SingleWriter: true},
		{Kind: "app", ID: "prj_2", Member: "srv_b", Slug: "web"}}
	raw, _ := json.Marshal(map[string]*clusterLease{"clu_1": {Epoch: 1, Member: "srv_a", RenewedAt: clk.Now(), Placement: placement, Boot: bootID()}})
	file := dir + "/cluster-lease.json"
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	g := newLeaseGuard(file, clk.Now, func(context.Context, []lease.Entry) error { return nil })
	if here, _ := g.holders("clu_1", "app", "prj_1"); here {
		t.Fatal("a lease read back from disk answers for the copy here")
	}
	if _, here, ok := g.locate("clu_1", "app", "prj_1"); here || ok {
		t.Fatalf("a lease read back from disk locates the copy here (here %v, ok %v)", here, ok)
	}
	if m, here, ok := g.locate("clu_1", "app", "prj_2"); m != "srv_b" || here || !ok {
		t.Fatalf("another member's workload: %q %v %v, want srv_b, not here", m, here, ok)
	}

	g.Renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1, Placement: placement})
	if here, _ := g.holders("clu_1", "app", "prj_1"); !here {
		t.Fatal("a renewed lease does not answer for the copy here")
	}
	clk.Add(lease.FenceAfter + time.Second)
	if here, _ := g.holders("clu_1", "app", "prj_1"); here {
		t.Fatal("a lease that ran out answers for the copy here")
	}
}

// canary.10 also says cluster.supervise and leaves a writer with no label down: a Deplo must tell the two apart.
func TestSupervise_advertisesTheFailSafeSupervisor(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.supervise.failsafe") {
		t.Fatal("Hello does not advertise cluster.supervise.failsafe")
	}
}

// A container the fence stopped comes back once the lease is back, whatever its restart policy says.
func TestSupervise_whatTheFenceStoppedComesBackWithTheLease(t *testing.T) {
	s, clk, fw := supervisedMember(t, nil)
	fw.cs[writer][0].Policy, fw.cs[writer][0].Label, fw.cs[writer][0].ExitCode = "no", "no", 0
	fw.cs[writer][0].FinishedAt = clk.Now()
	s.sup.fencedSince(context.Background(), writer, clk.Now().Add(-time.Second))
	renewHere(t, s)
	supervise(t, s)
	if got := fw.startedIDs(); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("started %v, want c1: the fence had stopped it", got)
	}
	if len(s.sup.fenced) != 0 {
		t.Fatalf("still kept as fenced after its start: %v", s.sup.fenced)
	}
	// It stops by itself later: its policy "no" holds again.
	fw.stops("c1", 0)
	clk.Add(time.Minute)
	renewHere(t, s)
	supervise(t, s)
	if got := fw.startedIDs(); len(got) != 1 {
		t.Fatalf("started %v: a policy \"no\" container that stopped by itself came back", got)
	}
}

// One that exited before the fence began was not the fence's, and one whose lease moved elsewhere stays down.
func TestSupervise_theFenceKeepsOnlyWhatItStopped(t *testing.T) {
	s, clk, fw := supervisedMember(t, nil)
	fw.cs[writer][0].Policy, fw.cs[writer][0].Label, fw.cs[writer][0].ExitCode = "no", "no", 0
	fw.cs[writer][0].FinishedAt = clk.Now().Add(-time.Hour)
	s.sup.fencedSince(context.Background(), writer, clk.Now())
	if len(s.sup.fenced) != 0 {
		t.Fatalf("kept %v, which exited an hour before the fence", s.sup.fenced)
	}
	fw.cs[writer][0].FinishedAt = clk.Now()
	s.sup.fencedSince(context.Background(), writer, clk.Now())
	s.sup.unfenced(writer)
	renewHere(t, s)
	supervise(t, s)
	if got := fw.startedIDs(); len(got) != 0 {
		t.Fatalf("started %v after its lease went elsewhere", got)
	}
	raw, _ := os.ReadFile(s.supervisorFile())
	if strings.Contains(string(raw), "fenced") {
		t.Fatalf("the fence is still on disk: %s", raw)
	}
}
