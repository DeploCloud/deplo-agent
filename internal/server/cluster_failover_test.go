package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

func startOrder(epoch uint64) lease.Order {
	return lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: epoch,
		Placement: []lease.Entry{{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web"}},
		Start: &lease.Start{DeploymentID: "dep_1", Slug: "web", ProjectID: "prj_1", ImageRef: "deplo/web:abc", ComposeYAML: "services: {web: {image: x}}",
			Network: "deplo-team-t1", Env: map[string]string{"TOKEN": "s3cret"}, ComposeUpArgs: []string{"--pull", "never"},
			RegistryAuth: []lease.RegistryAuth{{Host: "ghcr.io", Username: "u", Password: "p"}}}}
}

// serve plays the leader: one order over a pipe into the member's own handler.
func serve(t *testing.T, g *leaseGuard, o lease.Order) lease.Reply {
	t.Helper()
	a, b := net.Pipe()
	go g.Serve(wire.Party{Role: wire.RoleLeader}, b)
	defer a.Close()
	_ = a.SetDeadline(time.Now().Add(5 * time.Second))
	raw, _ := json.Marshal(o)
	_, _ = a.Write(append(raw, 10))
	var r lease.Reply
	err := json.NewDecoder(a).Decode(&r)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestFailover_aStartOrderRunsTheStackDeployOfTheBundle(t *testing.T) {
	var got *pb.DeployRequest
	old := runFailoverDeploy
	defer func() { runFailoverDeploy = old }()
	runFailoverDeploy = func(_ *Service, _ context.Context, req *pb.DeployRequest, e *emitter) {
		got = req
		e.result(true, "", "")
	}
	s := &Service{}
	f := newGuard(t)
	f.g.start = s.startFromBundle
	r := serve(t, f.g, startOrder(2))
	if !r.OK || !r.Started {
		t.Fatalf("reply %+v", r)
	}
	if got == nil || got.GetSlug() != "web" || got.GetEnv()["TOKEN"] != "s3cret" || got.GetNetwork() != "deplo-team-t1" ||
		got.GetSourceKind() != pb.SourceKind_SOURCE_KIND_IMAGE || got.GetPullImage() || got.GetBuildOnly() ||
		len(got.GetRegistryAuth()) != 1 || got.GetComposeUpArgs()[0] != "--pull" || got.GetComposeYaml() == "" {
		t.Fatalf("deploy request %+v", got)
	}
}

func TestFailover_aStackNotPlacedOnThisMemberOrFromAnOlderEpochIsNotStarted(t *testing.T) {
	ran := 0
	f := newGuard(t)
	f.g.start = func(context.Context, *lease.Start) error { ran++; return nil }
	if r := serve(t, f.g, startOrder(5)); !r.Started {
		t.Fatalf("reply %+v", r)
	}
	elsewhere := startOrder(5)
	elsewhere.Placement[0].Member = "srv_b"
	if r := serve(t, f.g, elsewhere); r.Started || r.Error == "" {
		t.Fatalf("started a stack placed on another member: %+v", r)
	}
	if r := serve(t, f.g, startOrder(4)); r.Started || r.Error != lease.StaleEpoch {
		t.Fatalf("an older epoch started a stack: %+v", r)
	}
	other := startOrder(5)
	other.Start.Slug = "unrelated"
	if r := serve(t, f.g, other); r.Started {
		t.Fatalf("a slug the placement does not name was started: %+v", r)
	}
	if ran != 1 {
		t.Fatalf("the deploy ran %d times", ran)
	}
}

func TestFailover_aStackThatNeverBecomesReadyIsNotStarted(t *testing.T) {
	f := newGuard(t)
	f.g.start = func(context.Context, *lease.Start) error { return errors.New("no room on this disk") }
	if r := serve(t, f.g, startOrder(1)); r.Started || r.OK || r.Error != "no room on this disk" {
		t.Fatalf("reply %+v", r)
	}
}

func TestFailover_anExtendedLeaseSurvivesPastSixtySeconds(t *testing.T) {
	f := newGuard(t)
	o := orderFor(3)
	o.ExtendSeconds = 300
	f.g.Renew(o)
	f.clk.Add(5 * time.Minute)
	f.g.Check(context.Background())
	if len(f.fenced) != 0 {
		t.Fatalf("fenced during a planned leader restart: %v", f.fenced)
	}
	f.clk.Add(2 * time.Minute)
	f.g.Check(context.Background())
	if len(f.fenced) == 0 {
		t.Fatal("an extended lease never ran out")
	}
	// The next normal renewal drops the extension.
	f2 := newGuard(t)
	f2.g.Renew(o)
	f2.g.Renew(orderFor(3))
	f2.clk.Add(61 * time.Second)
	f2.g.Check(context.Background())
	if len(f2.fenced) == 0 {
		t.Fatal("the extension outlived the renewal that replaced it")
	}
}

// A leader that takes over waits 5 minutes for a lease it cannot read: no order buys a member more than that.
func TestFailover_noLeaderExtendsALeasePastFiveMinutes(t *testing.T) {
	f := newGuard(t)
	o := orderFor(3)
	o.ExtendSeconds = 3600
	f.g.Renew(o)
	f.clk.Add(lease.FenceAfter + lease.ExtendFor - time.Second)
	f.g.Check(context.Background())
	if len(f.fenced) != 0 {
		t.Fatalf("fenced before the extension was over: %v", f.fenced)
	}
	f.clk.Add(time.Second)
	f.g.Check(context.Background())
	if len(f.fenced) == 0 {
		t.Fatal("a lease lived past the longest extension a leader may grant")
	}
}

func TestFailover_helloNamesTheCapability(t *testing.T) {
	found := false
	for _, c := range Capabilities {
		if c == "cluster.failover" {
			found = true
		}
	}
	if !found {
		t.Fatal("Hello does not name cluster.failover")
	}
}

func TestFailover_aMovedAppKeepsItsReadyCheckRestartRuleFilesAndLateServices(t *testing.T) {
	var got *pb.DeployRequest
	old := runFailoverDeploy
	defer func() { runFailoverDeploy = old }()
	runFailoverDeploy = func(_ *Service, _ context.Context, req *pb.DeployRequest, e *emitter) {
		got = req
		e.result(true, "", "")
	}
	s := &Service{}
	f := newGuard(t)
	f.g.start = s.startFromBundle
	o := startOrder(2)
	// The control plane writes both as the deploy's own messages in JSON; a field this agent does not know is skipped.
	o.Start.Ready = json.RawMessage(`{"port":3000,"portRequired":true,"service":"web","later":1}`)
	o.Start.RestartRule = json.RawMessage(`{"probe":"PROBE_HTTP","port":3000,"path":"/health","intervalS":10,"timeoutS":5,"retries":3,"startPeriodS":30,"service":"web"}`)
	o.Start.StartAfterReady = []string{"connector"}
	o.Start.Mounts = []lease.MountFile{{Path: "config/app.toml", Content: "a = 1"}}
	o.Start.FilesBindSources = []string{"data"}
	if r := serve(t, f.g, o); !r.Started {
		t.Fatalf("reply %+v", r)
	}
	if rc := got.GetReady(); rc.GetPort() != 3000 || !rc.GetPortRequired() || rc.GetService() != "web" {
		t.Fatalf("ready check %+v", rc)
	}
	rr := got.GetRestartRule()
	if rr.GetProbe() != pb.RestartRule_PROBE_HTTP || rr.GetPort() != 3000 || rr.GetPath() != "/health" || rr.GetIntervalS() != 10 ||
		rr.GetTimeoutS() != 5 || rr.GetRetries() != 3 || rr.GetStartPeriodS() != 30 || rr.GetService() != "web" {
		t.Fatalf("restart rule %+v", rr)
	}
	if l := got.GetStartAfterReady(); len(l) != 1 || l[0] != "connector" {
		t.Fatalf("late services %v", l)
	}
	if m := got.GetMounts(); len(m) != 1 || m[0].GetPath() != "config/app.toml" || m[0].GetContent() != "a = 1" {
		t.Fatalf("mounts %+v", m)
	}
	if b := got.GetFilesBindSources(); len(b) != 1 || b[0] != "data" {
		t.Fatalf("files binds %v", b)
	}
}

func TestFailover_aBundleWithNoReadyCheckOrRestartRuleSendsNone(t *testing.T) {
	var got *pb.DeployRequest
	old := runFailoverDeploy
	defer func() { runFailoverDeploy = old }()
	runFailoverDeploy = func(_ *Service, _ context.Context, req *pb.DeployRequest, e *emitter) {
		got = req
		e.result(true, "", "")
	}
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	if r := serve(t, f.g, startOrder(2)); !r.Started {
		t.Fatalf("reply %+v", r)
	}
	if got.GetReady() != nil || got.GetRestartRule() != nil || len(got.GetStartAfterReady()) != 0 {
		t.Fatalf("deploy request %+v", got)
	}
	bad := startOrder(3)
	bad.Start.RestartRule = json.RawMessage(`{"probe":7`)
	if r := serve(t, f.g, bad); r.Started || r.Error == "" {
		t.Fatalf("an unreadable restart rule was started: %+v", r)
	}
}

func TestFailover_aStartIsGivenUpAfterTheStartTimeout(t *testing.T) {
	var left time.Duration
	f := newGuard(t)
	f.g.start = func(ctx context.Context, _ *lease.Start) error {
		d, ok := ctx.Deadline()
		if ok {
			left = time.Until(d)
		}
		return nil
	}
	serve(t, f.g, startOrder(1))
	if left <= 0 || left > lease.StartTimeout {
		t.Fatalf("the start ran with %s left, want at most %s", left, lease.StartTimeout)
	}
}
