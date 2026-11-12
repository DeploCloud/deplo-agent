package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type guardFixture struct {
	g       *leaseGuard
	clk     *fakeClock
	file    string
	fenced  []string
	failing bool
}

func newGuard(t *testing.T) *guardFixture {
	t.Helper()
	f := &guardFixture{clk: &fakeClock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}, file: filepath.Join(t.TempDir(), "lease.json")}
	f.g = f.reopen()
	return f
}

// reopen is the agent starting again: the leases come back from the file.
func (f *guardFixture) reopen() *leaseGuard {
	f.g = newLeaseGuard(f.file, f.clk.Now, func(_ context.Context, es []lease.Entry) error {
		if f.failing {
			return errors.New("docker did not answer")
		}
		for _, e := range es {
			f.fenced = append(f.fenced, e.Slug)
		}
		return nil
	})
	return f.g
}

func orderFor(epoch uint64) lease.Order {
	return lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: epoch, Placement: []lease.Entry{
		{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-main", SingleWriter: true},
		{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web", SingleWriter: false},
		{Kind: "app", ID: "prj_2", Member: "srv_a", Slug: "files", SingleWriter: true},
		{Kind: "app", ID: "prj_3", Member: "srv_b", Slug: "elsewhere", SingleWriter: true},
	}}
}

func TestLease_fencesOnlyItsOwnSingleWriterWorkloadsAtSixtySeconds(t *testing.T) {
	f := newGuard(t)
	if r := f.g.Renew(orderFor(3)); !r.OK {
		t.Fatalf("renew: %+v", r)
	}
	f.clk.Add(59*time.Second + 900*time.Millisecond)
	f.g.Check(context.Background())
	if len(f.fenced) != 0 {
		t.Fatalf("fenced before 60 s: %v", f.fenced)
	}
	f.clk.Add(100 * time.Millisecond)
	f.g.Check(context.Background())
	slices.Sort(f.fenced)
	if strings.Join(f.fenced, ",") != "db-main,files" {
		t.Fatalf("fenced %v, want only this member's single-writer stacks", f.fenced)
	}
	f.clk.Add(time.Minute)
	f.g.Check(context.Background())
	if len(f.fenced) != 2 {
		t.Fatalf("fenced again: %v", f.fenced)
	}
}

// A leader that renews every second says how long its lease lives, and hears when this member's fence is done.
func TestLease_fencesWhenTheLeaderSaysAndSaysWhenItHasStopped(t *testing.T) {
	f := newGuard(t)
	o := orderFor(3)
	o.FenceSeconds = 5
	r := f.g.Renew(o)
	if want := int((5*time.Second + fenceTakes()).Seconds()); !r.OK || r.StoppedWithin != want {
		t.Fatalf("renew: %+v, want stoppedWithin %d", r, want)
	}
	f.clk.Add(4*time.Second + 900*time.Millisecond)
	f.g.Check(context.Background())
	if len(f.fenced) != 0 || f.g.RanOut() {
		t.Fatalf("fenced before 5 s: %v", f.fenced)
	}
	f.clk.Add(100 * time.Millisecond)
	f.g.Check(context.Background())
	if len(f.fenced) != 2 || !f.g.RanOut() {
		t.Fatalf("fenced %v at 5 s", f.fenced)
	}
	// Never longer than an older leader's: a lease that would outlive the 60 s keeps those 60 s.
	o.FenceSeconds = 3600
	if r := f.g.Renew(o); r.StoppedWithin != int((lease.FenceAfter + fenceTakes()).Seconds()) {
		t.Fatalf("stoppedWithin %d", r.StoppedWithin)
	}
}

func TestLease_aRenewalStartsTheClockOverAndRearmsTheFence(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(1))
	f.clk.Add(61 * time.Second)
	f.g.Check(context.Background())
	if len(f.fenced) != 2 {
		t.Fatalf("fenced %v", f.fenced)
	}
	f.g.Renew(orderFor(1))
	f.clk.Add(30 * time.Second)
	f.g.Check(context.Background())
	if len(f.fenced) != 2 {
		t.Fatalf("fenced inside a fresh lease: %v", f.fenced)
	}
	f.clk.Add(31 * time.Second)
	f.g.Check(context.Background())
	if len(f.fenced) != 4 {
		t.Fatalf("a second expiry did not fence again: %v", f.fenced)
	}
}

func TestLease_aFailedStopIsTriedAgain(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(1))
	f.clk.Add(61 * time.Second)
	f.failing = true
	f.g.Check(context.Background())
	if len(f.fenced) != 0 {
		t.Fatal("counted a stop that failed")
	}
	f.failing = false
	f.g.Check(context.Background())
	if len(f.fenced) != 2 {
		t.Fatalf("fenced %v", f.fenced)
	}
}

func TestLease_refusesAnOlderEpochAndTheEpochSurvivesARestart(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(7))
	if r := f.g.Renew(orderFor(6)); r.OK || r.Error != lease.StaleEpoch || r.Epoch != 7 {
		t.Fatalf("an older epoch got %+v", r)
	}
	if held, ok := f.g.Admit("clu_1", 6); ok || held != 7 {
		t.Fatalf("Admit(6) = %d, %v", held, ok)
	}
	again := f.reopen()
	if r := again.Renew(orderFor(5)); r.OK {
		t.Fatalf("a restarted agent took epoch 5 after 7: %+v", r)
	}
	if r := again.Renew(orderFor(8)); !r.OK {
		t.Fatalf("a newer epoch was refused: %+v", r)
	}
}

func TestLease_aLeaseThatRanOutWhileTheAgentWasDownIsHonoured(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(1))
	f.reopen()
	f.clk.Add(2 * time.Minute)
	f.g.Check(context.Background())
	if len(f.fenced) != 2 {
		t.Fatalf("fenced %v", f.fenced)
	}
}

func TestLease_leavingTheClusterForgetsEverything(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(9))
	f.g.Forget()
	if r := f.reopen().Renew(orderFor(1)); !r.OK {
		t.Fatalf("a member that left still holds an epoch: %+v", r)
	}
}

func serveOrder(t *testing.T, g *leaseGuard, from wire.Role, line string) string {
	t.Helper()
	a, b := net.Pipe()
	defer a.Close()
	go g.Serve(wire.Party{Fingerprint: strings.Repeat("a", 64), Role: from}, b)
	a.SetDeadline(time.Now().Add(2 * time.Second))
	a.Write([]byte(line + "\n"))
	out, _ := bufio.NewReader(a).ReadString('\n')
	return strings.TrimSpace(out)
}

func TestLease_onlyTheLeaderSOrderIsAnswered(t *testing.T) {
	f := newGuard(t)
	raw, _ := json.Marshal(orderFor(2))
	if got := serveOrder(t, f.g, wire.RoleLeader, string(raw)); !strings.HasPrefix(got, `{"ok":true,"epoch":2,`) {
		t.Fatalf("leader got %q", got)
	}
	if got := serveOrder(t, f.g, wire.RoleMember, string(raw)); got != "" {
		t.Fatalf("a member's stream was answered: %q", got)
	}
	old, _ := json.Marshal(orderFor(1))
	if got := serveOrder(t, f.g, wire.RoleLeader, string(old)); !strings.Contains(got, `"stale_epoch"`) || !strings.Contains(got, `"epoch":2`) {
		t.Fatalf("an older epoch got %q", got)
	}
	if got := serveOrder(t, f.g, wire.RoleLeader, "not json"); !strings.Contains(got, "unreadable") {
		t.Fatalf("garbage got %q", got)
	}
}

type watchSink struct {
	grpc.ServerStream
	ctx   context.Context
	lines chan string
}

func (w *watchSink) Context() context.Context { return w.ctx }
func (w *watchSink) Send(l *pb.ClusterWatchLine) error {
	w.lines <- string(l.GetLine())
	return nil
}

func TestClusterWatch_forwardsTheLeadersLinesFromTheAskedSequence(t *testing.T) {
	s, dir := clusterHostFor(t)
	var asked string
	fakeLeader(t, dir, func(w http.ResponseWriter, r *http.Request) {
		asked = r.URL.RawQuery
		w.Write([]byte("{\"hello\":true,\"head\":2}\n{\"seq\":2}\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	ctx, cancel := context.WithCancel(context.Background())
	sink := &watchSink{ctx: ctx, lines: make(chan string, 8)}
	done := make(chan error, 1)
	go func() { done <- s.ClusterWatch(&pb.ClusterWatchRequest{Since: 1}, sink) }()
	for _, want := range []string{`{"hello":true,"head":2}`, `{"seq":2}`} {
		select {
		case got := <-sink.lines:
			if got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("no line %q", want)
		}
	}
	if asked != "since=1" {
		t.Fatalf("the leader was asked %q", asked)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("a caller that left is not an error: %v", err)
	}
}

func TestClusterWatch_saysWhenTheLeaderIsNotThere(t *testing.T) {
	s, _ := clusterHostFor(t)
	err := s.ClusterWatch(&pb.ClusterWatchRequest{}, &watchSink{ctx: context.Background(), lines: make(chan string, 1)})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v, want Unavailable", err)
	}
}

func TestClusterWatch_aLeaderThatGoesQuietIsGivenUp(t *testing.T) {
	old := watchPingTimeout
	watchPingTimeout = 100 * time.Millisecond
	t.Cleanup(func() { watchPingTimeout = old })
	s, dir := clusterHostFor(t)
	fakeLeader(t, dir, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{\"hello\":true}\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	err := s.ClusterWatch(&pb.ClusterWatchRequest{}, &watchSink{ctx: context.Background(), lines: make(chan string, 4)})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v, want Unavailable", err)
	}
}

// A leader that is not sure it still leads asks before it renews (spec #176). The question renews nothing, and
// the member that answered it refuses every older epoch from then on, across a restart too.
func TestLease_aProbeRenewsNothingAndRefusesOlderEpochsFromThenOn(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(3))
	f.clk.Add(50 * time.Second)
	if got := serveOrder(t, f.g, wire.RoleLeader, `{"cluster":"clu_1","member":"srv_a","epoch":4,"probe":true}`); got != `{"ok":true,"epoch":4}` {
		t.Fatalf("a probe got %q", got)
	}
	if got := serveOrder(t, f.g, wire.RoleLeader, `{"cluster":"clu_1","member":"srv_a","epoch":3,"probe":true}`); !strings.Contains(got, `"stale_epoch"`) || !strings.Contains(got, `"epoch":4`) {
		t.Fatalf("an older probe got %q", got)
	}
	if r := f.reopen().Renew(orderFor(3)); r.OK || r.Error != lease.StaleEpoch || r.Epoch != 4 {
		t.Fatalf("the leader that was replaced still renews: %+v", r)
	}
	f.clk.Add(10 * time.Second)
	f.g.Check(context.Background())
	slices.Sort(f.fenced)
	if strings.Join(f.fenced, ",") != "db-main,files" {
		t.Fatalf("fenced %v: the probe renewed the lease or dropped its placement", f.fenced)
	}
}

func TestLease_aProbeToAMemberWithNoLeaseStartsNone(t *testing.T) {
	f := newGuard(t)
	if got := serveOrder(t, f.g, wire.RoleLeader, `{"cluster":"clu_1","member":"srv_a","epoch":2,"probe":true}`); got != `{"ok":true,"epoch":2}` {
		t.Fatalf("a probe got %q", got)
	}
	f.clk.Add(time.Hour)
	f.g.Check(context.Background())
	if len(f.fenced) != 0 || f.g.RanOut() || f.g.Holds("db-main") {
		t.Fatalf("a probe made a lease that ran out: fenced %v", f.fenced)
	}
	if r := f.g.Renew(orderFor(1)); r.OK {
		t.Fatalf("an older epoch was taken after the probe: %+v", r)
	}
}

// A slug from the leader ends up in a file path. An order naming one that is not a slug is refused whole,
// before any of it is looked at.
func TestLease_anOrderNamingSomethingThatIsNoSlugIsRefused(t *testing.T) {
	f := newGuard(t)
	var looked []string
	f.g.has = func(slug string) bool { looked = append(looked, slug); return true }
	f.g.start = func(context.Context, *lease.Start) error { t.Error("a stack was started"); return nil }
	for name, o := range map[string]lease.Order{
		"a placement entry": {Cluster: "clu_1", Member: "srv_a", Epoch: 2, Placement: []lease.Entry{{Kind: "app", ID: "p", Member: "srv_a", Slug: "../../etc", SingleWriter: true}}},
		"a start": {Cluster: "clu_1", Member: "srv_a", Epoch: 2, Placement: []lease.Entry{{Kind: "app", ID: "p", Member: "srv_a", Slug: "web"}},
			Start: &lease.Start{Slug: "web/../../x", ComposeYAML: "services: {}"}},
	} {
		raw, _ := json.Marshal(o)
		if got := serveOrder(t, f.g, wire.RoleLeader, string(raw)); !strings.Contains(got, "not a stack") || strings.Contains(got, `"ok":true`) {
			t.Errorf("%s: answered %q", name, got)
		}
	}
	if _, ok := f.g.Admit("clu_1", 1); !ok || len(looked) != 0 {
		t.Fatalf("a refused order was applied: its epoch was taken (%v), looked at %v", !ok, looked)
	}
	f.clk.Add(2 * time.Minute)
	f.g.Check(context.Background())
	if len(f.fenced) != 0 || len(looked) != 0 {
		t.Fatalf("a refused order reached the disk: fenced %v, looked at %v", f.fenced, looked)
	}
}
