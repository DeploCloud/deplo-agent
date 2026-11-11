package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// promoteOrder is the leader's order to the member holding db-main's standby copy, once srv_b stopped answering.
func promoteOrder(epoch uint64) lease.Order {
	return lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: epoch,
		Placement: []lease.Entry{{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-main", SingleWriter: true}},
		Start: &lease.Start{DeploymentID: "rev_1", Slug: "db-main", ProjectID: "db_1", ComposeYAML: "services: {db-main: {image: postgres:17}}",
			Network: "deplo-env-e1", ComposeUpArgs: []string{"--force-recreate"},
			Promote: &lease.Promote{Container: "db-main_standby", User: "postgres", Command: []string{"sh", "-c", "pg_ctl promote"}}}}
}

func stubPromote(t *testing.T, st *failoverStubs, fail error) *[]string {
	t.Helper()
	old := runFailoverPromote
	t.Cleanup(func() { runFailoverPromote = old })
	var ran []string
	runFailoverPromote = func(_ context.Context, projectID string, p *lease.Promote) error {
		st.steps = append(st.steps, "promote")
		ran = append(ran, projectID+" "+p.Container+" "+p.User+" "+strings.Join(p.Command, " "))
		return fail
	}
	return &ran
}

func TestStandby_theCopyIsPromotedBeforeTheDatabasesStackStartsOverIt(t *testing.T) {
	st := stubFailover(t)
	ran := stubPromote(t, st, nil)
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	r := serve(t, f.g, promoteOrder(2))
	if !r.OK || !r.Started || !r.Promoted || r.Restored {
		t.Fatalf("reply %+v", r)
	}
	if got := strings.Join(st.steps, ","); got != "promote,deploy" {
		t.Fatalf("steps %q", got)
	}
	if got := strings.Join(*ran, ";"); got != "db_1 db-main_standby postgres sh -c pg_ctl promote" {
		t.Fatalf("promoted %q", got)
	}
	if d := st.deploys[0]; d.GetSlug() != "db-main" || d.GetSourceKind() != pb.SourceKind_SOURCE_KIND_COMPOSE {
		t.Fatalf("deploy request %+v", d)
	}
}

func TestStandby_aCopyThatCannotBePromotedStartsNothing(t *testing.T) {
	st := stubFailover(t)
	stubPromote(t, st, errors.New("the standby copy had stopped following its database"))
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	r := serve(t, f.g, promoteOrder(2))
	if r.OK || r.Started || r.Promoted || !strings.Contains(r.Error, "stopped following") {
		t.Fatalf("reply %+v", r)
	}
	if got := strings.Join(st.steps, ","); got != "promote" {
		t.Fatalf("steps %q: nothing starts over a copy that was not promoted", got)
	}
}

func TestStandby_aPromotionNamesItsContainerAndCommand(t *testing.T) {
	st := stubFailover(t)
	stubPromote(t, st, nil)
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	bad := promoteOrder(2)
	bad.Start.Promote.Command = nil
	if r := serve(t, f.g, bad); r.Started || r.Error == "" || len(st.steps) != 0 {
		t.Fatalf("reply %+v, steps %v", r, st.steps)
	}
}

func standbyEntry(member string) lease.Entry {
	return lease.Entry{Kind: "database", ID: "db1", Member: member, Slug: "db-shop", Standby: true}
}

// The member holding the standby copy still holds the database's NAME: connections to it go to the database.
func TestStandby_theNameStillLeadsToTheDatabaseOnTheMemberHoldingItsCopy(t *testing.T) {
	g := newGuard(t).g
	g.Renew(lease.Order{Cluster: "cl1", Member: "srvB", Epoch: 1, Placement: []lease.Entry{dbEntry("srvA"), standbyEntry("srvB")}})
	if member, here, ok := g.locate("cl1", "database", "db1"); !ok || here || member != "srvA" {
		t.Fatalf("located on %q, here %v", member, here)
	}
	// Promoted: the leader's next placement puts the database itself here.
	g.Renew(lease.Order{Cluster: "cl1", Member: "srvB", Epoch: 1, Placement: []lease.Entry{dbEntry("srvB")}})
	if member, here, _ := g.locate("cl1", "database", "db1"); !here || member != "srvB" {
		t.Fatalf("after the promotion: located on %q, here %v", member, here)
	}
}

func TestStandby_aCutOffMemberKeepsItsCopyAndTheOldDatabaseIsRemovedWhenItsMemberReturns(t *testing.T) {
	// srv_a holds the copy of a database that runs on srv_b: its lease running out stops nothing of it.
	copy := lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 3, Placement: []lease.Entry{
		{Kind: "database", ID: "db_1", Member: "srv_b", Slug: "db-main", SingleWriter: true},
		{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-main", Standby: true}}}
	f := newReturn(t)
	f.renew(copy)
	f.clk.Add(2 * time.Minute)
	f.g.Check(context.Background())
	if len(f.fenced) != 0 {
		t.Fatalf("a standby copy was stopped at the fence: %v", f.fenced)
	}
	f.renew(copy)
	if len(f.removed) != 0 {
		t.Fatalf("a returning member removed the copy it still holds: %v", f.removed)
	}

	// srv_a ran the database; while it was away the copy on srv_b was promoted.
	g := newReturn(t)
	g.renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 3, Placement: []lease.Entry{
		{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-main", SingleWriter: true},
		{Kind: "database", ID: "db_1", Member: "srv_b", Slug: "db-main", Standby: true}}})
	g.clk.Add(5 * time.Minute)
	g.restart()
	g.renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 3, Return: true, Placement: []lease.Entry{
		{Kind: "database", ID: "db_1", Member: "srv_b", Slug: "db-main", SingleWriter: true}}})
	if got := g.removedSlugs(); got != "db-main" || len(g.started) != 0 {
		t.Fatalf("removed %q, started %v: the old database must go and never start again", got, g.started)
	}
}

func TestStandby_advertised(t *testing.T) {
	for _, c := range Capabilities {
		if c == "cluster.standby" {
			return
		}
	}
	t.Fatal("Hello does not name cluster.standby")
}
