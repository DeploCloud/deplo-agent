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
