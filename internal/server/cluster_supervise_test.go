package server

import (
	"context"
	"encoding/json"
	"os"
	"slices"
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
	s.writerStopped(writer, nil)
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
