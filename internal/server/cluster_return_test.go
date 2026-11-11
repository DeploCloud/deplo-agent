package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

func TestReturn_anAgentThatStartsWithADeadLeaseStopsEverythingItRunsForTheCluster(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(3))
	// The server was away five minutes; Docker's restart policy brought every container back before the agent.
	f.clk.Add(5 * time.Minute)
	f.reopen().Check(context.Background())
	slices.Sort(f.fenced)
	if got := strings.Join(f.fenced, ","); got != "db-main,files,web" {
		t.Fatalf("stopped %q, want every stack this member ran for the cluster: a stateless App moved while it was away", got)
	}
}

type returnFixture struct {
	*guardFixture
	removed []lease.Entry
	started []string
	// gone are stacks with no file on this host; stopped ones did not run when the lease ran out.
	gone, stopped map[string]bool
	removeErr     error
}

func newReturn(t *testing.T) *returnFixture {
	t.Helper()
	// "elsewhere" always ran on srv_b: this host never had a copy of it.
	f := &returnFixture{guardFixture: newGuard(t), gone: map[string]bool{"elsewhere": true}, stopped: map[string]bool{}}
	f.wire()
	return f
}

func (f *returnFixture) wire() {
	f.g.remove = func(_ context.Context, e lease.Entry) error {
		if f.removeErr != nil {
			return f.removeErr
		}
		f.removed = append(f.removed, e)
		f.gone[e.Slug] = true
		return nil
	}
	f.g.resume = func(_ context.Context, slug string) error { f.started = append(f.started, slug); return nil }
	f.g.has = func(slug string) bool { return !f.gone[slug] }
	f.g.running = func(_ context.Context, slug string) bool { return !f.stopped[slug] }
}

// restart is the server coming back: the agent starts again with the lease it had on disk.
func (f *returnFixture) restart() {
	f.reopen()
	f.wire()
	f.g.Check(context.Background())
}

// renew is one order of the leader, and everything it made the member do.
func (f *returnFixture) renew(o lease.Order) lease.Reply {
	r := f.g.Renew(o)
	f.g.work.Wait()
	return r
}

func (f *returnFixture) removedSlugs() string {
	var out []string
	for _, e := range f.removed {
		out = append(out, e.Slug)
	}
	slices.Sort(out)
	return strings.Join(out, ",")
}

func (f *returnFixture) startedSlugs() string {
	out := slices.Clone(f.started)
	slices.Sort(out)
	return strings.Join(out, ",")
}

// movedAway is the placement after the leader moved web and files to srv_b and left the database where it was.
func movedAway(epoch uint64) lease.Order {
	o := orderFor(epoch)
	o.Placement[1].Member, o.Placement[2].Member = "srv_b", "srv_b"
	return o
}

func TestReturn_nothingStartsBeforeTheLeadersOrderThenWhatMovedIsRemovedAndTheRestStarts(t *testing.T) {
	f := newReturn(t)
	f.renew(orderFor(3))
	f.clk.Add(5 * time.Minute)
	f.restart()
	f.clk.Add(time.Hour)
	f.g.Check(context.Background())
	if len(f.started)+len(f.removed) != 0 {
		t.Fatalf("before any order: started %v, removed %v", f.started, f.removed)
	}

	if r := f.renew(movedAway(3)); !r.OK {
		t.Fatalf("renew: %+v", r)
	}
	if got := f.removedSlugs(); got != "files,web" {
		t.Fatalf("removed %q, want the two stacks placed on srv_b", got)
	}
	if got := f.startedSlugs(); got != "db-main" {
		t.Fatalf("started %q, want only what is still this member's", got)
	}
	for _, e := range f.removed {
		if e.Slug == "files" && !e.SingleWriter {
			t.Fatal("the removal does not say this stack holds data")
		}
	}
	// Later renewals do it once.
	f.renew(movedAway(3))
	if len(f.removed) != 2 || len(f.started) != 1 {
		t.Fatalf("done twice: removed %v, started %v", f.removed, f.started)
	}
}

func TestReturn_anOlderEpochIsRefusedAndStartsNothing(t *testing.T) {
	f := newReturn(t)
	f.renew(orderFor(3))
	f.clk.Add(5 * time.Minute)
	f.restart()
	ran := 0
	f.g.start = func(context.Context, *lease.Start) error { ran++; return nil }
	old := movedAway(2)
	old.Start = &lease.Start{Slug: "db-main", ComposeYAML: "services: {}"}
	if r := serve(t, f.g, old); r.OK || r.Error != lease.StaleEpoch || r.Epoch != 3 {
		t.Fatalf("an older epoch got %+v", r)
	}
	f.g.work.Wait()
	if ran+len(f.started)+len(f.removed) != 0 {
		t.Fatalf("an older epoch started %d, resumed %v, removed %v", ran, f.started, f.removed)
	}
	if !f.g.Holds("web") {
		t.Fatal("an older epoch let the cluster's stacks start")
	}
}

func TestReturn_aCutThatHealedRemovesTheCopyThatMovedAndStartsWhatItFenced(t *testing.T) {
	f := newReturn(t)
	f.renew(orderFor(1))
	f.clk.Add(90 * time.Second)
	f.g.Check(context.Background())
	if len(f.fenced) != 2 {
		t.Fatalf("fenced %v", f.fenced)
	}
	o := orderFor(1)
	o.Placement[1].Member = "srv_b"
	f.renew(o)
	if got := f.removedSlugs(); got != "web" {
		t.Fatalf("removed %q, want the App that failed over", got)
	}
	if got := f.startedSlugs(); got != "db-main,files" {
		t.Fatalf("started %q, want the two stacks it stopped at 60 s", got)
	}
}

func TestReturn_aStackThatWasNotRunningIsNotStarted(t *testing.T) {
	f := newReturn(t)
	f.renew(orderFor(1))
	f.stopped["files"] = true
	f.clk.Add(90 * time.Second)
	f.g.Check(context.Background())
	f.renew(orderFor(1))
	if got := f.startedSlugs(); got != "db-main" {
		t.Fatalf("started %q: a stack somebody stopped came back", got)
	}
}

func TestReturn_aLiveLeaseRemovesNothingWhenWorkIsMovedByHand(t *testing.T) {
	f := newReturn(t)
	f.renew(orderFor(1))
	f.clk.Add(5 * time.Second)
	f.renew(movedAway(1))
	if len(f.removed)+len(f.started) != 0 {
		t.Fatalf("a move Deplo makes itself: removed %v, started %v", f.removed, f.started)
	}
}

func TestReturn_theLeaderIsToldOnceWhatWasRemoved(t *testing.T) {
	f := newReturn(t)
	f.renew(orderFor(1))
	f.clk.Add(5 * time.Minute)
	f.restart()
	f.removeErr = errors.New("docker did not answer")
	if r := f.renew(movedAway(1)); r.Returned != nil {
		t.Fatalf("reported before anything was removed: %+v", r.Returned)
	}
	asks := movedAway(1)
	asks.Return = true
	if r := f.renew(asks); r.Returned != nil {
		t.Fatalf("reported a return that is not done: %+v", r.Returned)
	}
	f.removeErr = nil
	f.renew(asks)
	r := f.renew(asks)
	if r.Returned == nil || strings.Join(r.Returned.Removed, ",") != "files,web" {
		t.Fatalf("report %+v, want files and web", r.Returned)
	}
	// The leader stops asking once it has the list, and the member forgets it.
	f.renew(movedAway(1))
	if c := f.g.clusters["clu_1"]; c.Returned != nil || c.Returning {
		t.Fatalf("the report was kept: %+v", c)
	}
}

func TestReturn_theLeaderAsksAMemberWhoseLeaseNeverRanOut(t *testing.T) {
	f := newReturn(t)
	o := orderFor(1)
	o.ExtendSeconds = 300
	f.renew(o)
	f.clk.Add(2 * time.Minute)
	asks := movedAway(1)
	asks.Return = true
	f.renew(asks)
	if got := f.removedSlugs(); got != "files,web" {
		t.Fatalf("removed %q, want what the leader moved while this lease was extended", got)
	}
	if r := f.renew(asks); r.Returned == nil || len(r.Returned.Removed) != 2 {
		t.Fatalf("report %+v", r.Returned)
	}
}

func TestReturn_aStackWaitingForRoomStaysStoppedUntilItIsHandedBack(t *testing.T) {
	f := newReturn(t)
	f.renew(orderFor(1))
	f.clk.Add(5 * time.Minute)
	f.restart()
	o := orderFor(1)
	o.Placement[1].Member = ""
	f.renew(o)
	if got := f.startedSlugs(); got != "db-main,files" || len(f.removed) != 0 {
		t.Fatalf("started %q, removed %v", got, f.removed)
	}
	f.renew(orderFor(1))
	if got := f.startedSlugs(); got != "db-main,files,web" {
		t.Fatalf("started %q, want web once the leader gave it back", got)
	}
}

func TestReturn_aMemberWithADeadLeaseStartsNothingOfTheClusterOnAnyoneElsesWordEither(t *testing.T) {
	const slug = "deplo-agent-test-return"
	argv := dockerOnPath(t, "exit 0")
	dir := t.TempDir()
	dead := map[string]*clusterLease{"clu_1": {Epoch: 3, Member: "srv_a", RenewedAt: time.Now().Add(-10 * time.Minute),
		Placement: []lease.Entry{{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: slug}}}}
	raw, _ := json.Marshal(dead)
	mustWrite(t, filepath.Join(dir, "cluster-lease.json"), string(raw))
	s := &Service{agentDir: dir, stackDir: t.TempDir()}
	ctx := context.Background()

	if _, err := s.StartStack(ctx, &pb.StackRef{Slug: slug}); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), awaitingLeader) {
		t.Fatalf("StartStack: %v", err)
	}
	if _, err := s.Reroute(ctx, &pb.RerouteRequest{Slug: slug, Network: "deplo-team-t1", ComposeYaml: "services: {web: {image: x}}"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Reroute: %v", err)
	}
	f := newInflight(func() {})
	s.driveDeploy(ctx, "dep_1", &pb.DeployRequest{DeployId: "dep_1", Slug: slug, SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, ImageRef: "nginx", ComposeYaml: "services: {web: {image: nginx}}"}, "", f)
	var result *pb.DeployResult
	for _, ev := range f.events {
		if r := ev.GetResult(); r != nil {
			result = r
		}
	}
	if result == nil || result.GetReady() || result.GetError() != awaitingLeader {
		t.Fatalf("Deploy: %+v", result)
	}
	if got := strings.Join(argv(), ""); got != "" {
		t.Fatalf("docker ran: %q", got)
	}
	// A stack that is not the cluster's is none of the lease's business.
	if s.leaseGuard().Holds("deplo-agent-test-other") {
		t.Fatal("a stack outside the cluster is held")
	}
}

func TestReturn_removingAMovedStackKeepsItsVolume(t *testing.T) {
	const slug = "deplo-agent-test-return"
	argv := dockerOnPath(t, "exit 0")
	s := &Service{agentDir: t.TempDir(), stackDir: t.TempDir()}
	mustWrite(t, s.stackPath(slug), "services: {db: {image: postgres}}")
	if err := s.removeMoved(context.Background(), lease.Entry{Kind: "database", ID: "db_1", Member: "srv_b", Slug: slug, SingleWriter: true}); err != nil {
		t.Fatal(err)
	}
	all := strings.Join(argv(), "\n")
	if !strings.Contains(all, "compose -p deplo-"+slug) || !strings.Contains(all, "down --remove-orphans") {
		t.Fatalf("the stack was not taken down, connector and all: %q", all)
	}
	if strings.Contains(all, " -v") || strings.Contains(all, "volume") {
		t.Fatalf("a volume was touched: %q", all)
	}
	if isFile(s.stackPath(slug)) {
		t.Fatal("the stack file is still here, so the next start would bring the copy back")
	}
}

// send is serve without the test's goroutine: the answer to one order of the leader.
func send(g *leaseGuard, o lease.Order) lease.Reply {
	a, b := net.Pipe()
	go g.Serve(wire.Party{Role: wire.RoleLeader}, b)
	defer a.Close()
	_ = a.SetDeadline(time.Now().Add(5 * time.Second))
	raw, _ := json.Marshal(o)
	_, _ = a.Write(append(raw, 10))
	var r lease.Reply
	_ = json.NewDecoder(a).Decode(&r)
	return r
}

// stuckReturn is a member whose copy of files will not go, so its return stays open.
func stuckReturn(t *testing.T) *returnFixture {
	f := newReturn(t)
	f.g.remove = func(_ context.Context, e lease.Entry) error {
		if e.Slug == "files" {
			return errors.New("docker did not answer")
		}
		f.removed = append(f.removed, e)
		f.gone[e.Slug] = true
		return nil
	}
	return f
}

func TestReturn_aStackBeingStartedHereIsNotRemovedByAnOrderThatStillPlacesItElsewhere(t *testing.T) {
	f := stuckReturn(t)
	// web and files run on srv_b; this member is back and cannot remove its copy of files yet, so every
	// renewal tries again.
	old := orderFor(1)
	old.Placement[1].Member, old.Placement[2].Member, old.Return = "srv_b", "srv_b", true
	f.gone["web"] = true
	f.renew(old)

	began, release := make(chan struct{}), make(chan struct{})
	f.g.start = func(context.Context, *lease.Start) error { close(began); <-release; return nil }
	f.gone["web"] = false
	moved := orderFor(1)
	moved.Placement[2].Member = "srv_b"
	moved.Return, moved.Start = true, &lease.Start{Slug: "web", ComposeYAML: "services: {}"}
	done := make(chan lease.Reply)
	go func() { done <- send(f.g, moved) }()
	<-began
	f.g.work.Wait()
	// The leader's renewals carry the placement from before the move until the start has answered.
	f.renew(old)
	if len(f.removed) != 0 {
		t.Fatalf("removed %v while it was being started here", f.removedSlugs())
	}
	close(release)
	if r := <-done; !r.Started {
		t.Fatalf("start: %+v", r)
	}
	f.g.work.Wait()
	f.renew(old)
	if len(f.removed) != 0 {
		t.Fatalf("removed %v before the leader had placed it here", f.removedSlugs())
	}
	// A leader that never places it here does not leave a second copy behind for good.
	f.clk.Add(61 * time.Second)
	f.renew(old)
	if got := f.removedSlugs(); got != "web" {
		t.Fatalf("removed %q, want web once the start was never confirmed", got)
	}
}

func TestReturn_aStartThatFailedLeavesNoGuardBehind(t *testing.T) {
	f := stuckReturn(t)
	old := orderFor(1)
	old.Placement[1].Member, old.Placement[2].Member, old.Return = "srv_b", "srv_b", true
	f.gone["web"] = true
	f.renew(old)
	f.gone["web"] = false
	f.g.start = func(context.Context, *lease.Start) error { return errors.New("no room on this disk") }
	moved := orderFor(1)
	moved.Placement[2].Member = "srv_b"
	moved.Return, moved.Start = true, &lease.Start{Slug: "web", ComposeYAML: "services: {}"}
	if r := send(f.g, moved); r.Started {
		t.Fatalf("start: %+v", r)
	}
	f.g.work.Wait()
	f.renew(old)
	if got := f.removedSlugs(); got != "web" {
		t.Fatalf("removed %q, want what the failed start left behind", got)
	}
}

// spreadOrder places three replicas of web: n of them on this member, the rest on srv_b.
func spreadOrder(here int) lease.Order {
	o := lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1}
	for i := 0; i < 3; i++ {
		m := "srv_b"
		if i < here {
			m = "srv_a"
		}
		o.Placement = append(o.Placement, lease.Entry{Kind: "app", ID: "prj_1", Replica: i, Member: m, Slug: "web"})
	}
	return o
}

func TestReturn_aMemberLeftWithFewerReplicasStopsTheSurplus(t *testing.T) {
	f := newReturn(t)
	trimmed := map[string]int{}
	f.renew(spreadOrder(2))
	f.clk.Add(5 * time.Minute)
	f.restart()
	f.g.trim = func(_ context.Context, slug string, n int) (bool, error) { trimmed[slug] = n; return true, nil }
	// One of its two replicas was started on srv_b meanwhile.
	asks := spreadOrder(1)
	asks.Return = true
	f.renew(asks)
	if len(f.removed) != 0 || trimmed["web"] != 1 {
		t.Fatalf("removed %v, trimmed %v: want web kept with one replica", f.removed, trimmed)
	}
	if got := f.startedSlugs(); got != "web" {
		t.Fatalf("started %q, want the replica that is still this member's", got)
	}
	if r := f.renew(asks); r.Returned == nil || strings.Join(r.Returned.Removed, ",") != "web" {
		t.Fatalf("report %+v, want the leader told web lost a copy here", r.Returned)
	}
}

func TestReturn_aMemberThatKeptAllItsReplicasStopsNone(t *testing.T) {
	f := newReturn(t)
	called := false
	f.renew(spreadOrder(3))
	f.clk.Add(5 * time.Minute)
	f.restart()
	f.g.trim = func(context.Context, string, int) (bool, error) { called = true; return false, nil }
	f.renew(spreadOrder(3))
	if called || f.startedSlugs() != "web" {
		t.Fatalf("trimmed %v, started %q", called, f.startedSlugs())
	}
}

func TestReturn_trimRemovesTheNewestCopiesOfTheReplicatedService(t *testing.T) {
	const slug = "deplo-agent-test-return"
	argv := dockerOnPath(t, `case "$*" in "ps -a "*) printf 'c1 web 1\nc3 web 3\nc2 web 2\nc9 deplo-connector_x 1\n' ;; esac`)
	s := &Service{agentDir: t.TempDir(), stackDir: t.TempDir()}
	mustWrite(t, s.stackPath(slug), "services: {web: {image: x, deploy: {replicas: 3}}, deplo-connector_x: {image: y}}")
	cut, err := s.trimReplicas(context.Background(), slug, 1)
	if err != nil || !cut {
		t.Fatalf("trim: %v %v", cut, err)
	}
	all := argv()
	if last := all[len(all)-1]; last != "rm -f c3 c2" {
		t.Fatalf("docker ran %q, want the two newest copies removed and the connector left", all)
	}
	if cut, _ := s.trimReplicas(context.Background(), slug, 3); cut {
		t.Fatal("a stack with no surplus was trimmed")
	}
}
