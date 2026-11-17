package server

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// relinquishFixture is a member with a live lease whose relinquish only records what it was asked to stop.
type relinquishFixture struct {
	*returnFixture
	mu           sync.Mutex
	relinquished []string
}

func newRelinquish(t *testing.T) *relinquishFixture {
	t.Helper()
	f := &relinquishFixture{returnFixture: newReturn(t)}
	f.wireRelinquish()
	return f
}

// wireRelinquish hands the guard, a new one after a restart, the recording relinquish.
func (f *relinquishFixture) wireRelinquish() {
	f.g.relinquish = func(_ context.Context, slug string) (bool, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.relinquished = append(f.relinquished, slug)
		return true, nil
	}
}

func (f *relinquishFixture) stoppedSlugs() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := slices.Clone(f.relinquished)
	slices.Sort(out)
	return strings.Join(out, ",")
}

func TestRelinquish_aLiveLeaseThatMovesAWriterAwayStopsItsCopyHere(t *testing.T) {
	f := newRelinquish(t)
	f.renew(orderFor(1))
	if got := f.stoppedSlugs(); got != "" {
		t.Fatalf("stopped %q while everything was placed here", got)
	}
	// deplo#241: a newer leader places files on srv_b while this member, alive and renewed, still runs it.
	f.clk.Add(time.Second)
	f.renew(movedAway(2))
	if got := f.stoppedSlugs(); got != "files" {
		t.Fatalf("stopped %q, want files only: web has no data, db-main is still here", got)
	}
	if len(f.removed) != 0 {
		t.Fatalf("removed %v: a copy that lost its lease is stopped, never removed", f.removed)
	}
}

func TestRelinquish_aStandbyHereOrAStartUnderWayKeepsTheCopy(t *testing.T) {
	f := newRelinquish(t)
	f.renew(orderFor(1))
	o := movedAway(2)
	// db-main runs on srv_b now, and its standby copy is here.
	o.Placement[0].Member = "srv_b"
	o.Placement = append(o.Placement, lease.Entry{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-main", Standby: true})
	// A start order brings files up here; the leader's renewals still name srv_b until it answers.
	f.g.markStart("files", false)
	f.renew(o)
	if got := f.stoppedSlugs(); got != "" {
		t.Fatalf("stopped %q: a standby copy and a start under way stay", got)
	}
}

func TestRelinquish_aReturnAndAStackPlacedOnNoneAreLeftToTheirOwnPaths(t *testing.T) {
	f := newRelinquish(t)
	f.renew(orderFor(1))
	// Placed on no member: the leader hears this member has it and hands it back, so it keeps running.
	o := orderFor(2)
	o.Placement[2].Member = ""
	f.renew(o)
	if got := f.stoppedSlugs(); got != "" {
		t.Fatalf("stopped %q, placed on no member", got)
	}
	// A server coming back removes what moved itself, saving its data first.
	f.clk.Add(5 * time.Minute)
	f.restart()
	f.wireRelinquish()
	f.renew(movedAway(3))
	if got := f.stoppedSlugs(); got != "" {
		t.Fatalf("stopped %q during a return, which removes it itself", got)
	}
	if got := f.removedSlugs(); got != "files,web" {
		t.Fatalf("the return removed %q", got)
	}
}

func TestProbe_saysWhichWritersRunHereAndUnderWhichEpoch(t *testing.T) {
	f := newRelinquish(t)
	f.renew(orderFor(3))
	f.stopped["files"] = true
	p := lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 4, Probe: true}
	r := serve(t, f.g, p)
	if !r.OK || len(r.Running) != 1 || r.Running[0] != (lease.Writer{Slug: "db-main", Epoch: 3}) {
		t.Fatalf("a probe at epoch 4 answered %+v, want db-main placed here at epoch 3: files is stopped, web has no data", r)
	}
	// The probe took epoch 4 but placed nothing: the next one still names epoch 3 as what placed db-main here.
	p.Epoch = 5
	if r := serve(t, f.g, p); len(r.Running) != 1 || r.Running[0].Epoch != 3 {
		t.Fatalf("the second probe answered %+v", r.Running)
	}
}
