package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

func succeedOrder(epoch uint64, from string) lease.Order {
	o := orderFor(epoch)
	o.From, o.Succeed = from, true
	return o
}

func TestSuccession_aSucceedEpochIsRefusedWhileTheLeaderStillRenews(t *testing.T) {
	f := newGuard(t)
	o := orderFor(3)
	o.From = "srv_lead"
	f.g.Renew(o)
	f.clk.Add(lease.PreVoteFor - time.Second)
	if r := f.g.PreVote(succeedOrder(4, "srv_heir")); r.OK || r.Error != lease.LeaderAlive {
		t.Fatalf("a pre-vote while the leader renews got %+v", r)
	}
	if r := f.g.Probe(succeedOrder(4, "srv_heir")); r.OK || r.Error != lease.LeaderAlive {
		t.Fatalf("a probe while the leader renews got %+v", r)
	}
	// Planned: Deplo's word is no succession, so a moved leader is taken at once.
	moved := orderFor(4)
	moved.From, moved.Probe = "srv_new", true
	if r := f.g.Probe(moved); !r.OK {
		t.Fatalf("a leader Deplo moved was refused: %+v", r)
	}
}

func TestSuccession_aPreVoteTakesNothingAndTheHeirLeadsOnceTheLeaderIsSilent(t *testing.T) {
	f := newGuard(t)
	o := orderFor(3)
	o.From = "srv_lead"
	f.g.Renew(o)
	f.clk.Add(lease.PreVoteFor)
	if r := f.g.PreVote(succeedOrder(4, "srv_heir")); !r.OK {
		t.Fatalf("a pre-vote once the leader is silent got %+v", r)
	}
	// Nothing was taken: the leader that was silent renews again at its own epoch.
	if r := f.g.Renew(o); !r.OK {
		t.Fatalf("the pre-vote made the member refuse its leader: %+v", r)
	}
	f.clk.Add(lease.PreVoteFor)
	if r := f.g.Probe(succeedOrder(4, "srv_heir")); !r.OK {
		t.Fatalf("the heir's probe got %+v", r)
	}
	if r := f.g.Renew(o); r.OK || r.Error != lease.StaleEpoch {
		t.Fatalf("the old leader still renews after the heir's probe: %+v", r)
	}
	if r := f.g.Renew(succeedOrder(4, "srv_heir")); !r.OK {
		t.Fatalf("the heir's renewal got %+v", r)
	}
}

func TestSuccession_anEpochGoesToOneLeaderOnly(t *testing.T) {
	f := newGuard(t)
	a := orderFor(5)
	a.From, a.Probe = "srv_a1", true
	if r := f.g.Probe(a); !r.OK {
		t.Fatalf("the first leader's probe got %+v", r)
	}
	b := orderFor(5)
	b.From = "srv_b1"
	if r := f.g.Renew(b); r.OK || r.Error != lease.StaleEpoch || r.Epoch != 5 {
		t.Fatalf("a second leader took the same epoch: %+v", r)
	}
	// eu, 23 Nov 2026 (agent canary.40): a leader's start orders name no server, and refusing them stopped every
	// failover. An order that names nobody is taken at its leader's epoch, and the epoch stays that leader's.
	if r := f.reopen().Renew(orderFor(5)); !r.OK {
		t.Fatalf("a leader's order that names no server (a start, or an older leader) was refused: %+v", r)
	}
	if r := f.g.Renew(b); r.OK {
		t.Fatalf("after an unnamed order, a second leader took epoch 5: %+v", r)
	}
	g := newGuard(t)
	if r := g.g.Renew(orderFor(7)); !r.OK {
		t.Fatalf("an older leader's renewal got %+v", r)
	}
	named := orderFor(7)
	named.From = "srv_a1"
	if r := g.g.Renew(named); !r.OK {
		t.Fatalf("the same leader, updated to name its server, was refused: %+v", r)
	}
	named.From = "srv_b1"
	if r := g.g.Renew(named); r.OK {
		t.Fatalf("a second named leader took epoch 7: %+v", r)
	}
}

// Every order that can give an epoch, in every order from two named leaders and one that names nobody (a start
// order, or an older leader): once a named leader holds the epoch, no other named one is answered for it.
func TestSuccession_noSequenceOfOrdersGivesOneEpochToTwoLeaders(t *testing.T) {
	kinds := []string{"renew", "probe", "succeed", "prevote"}
	send := func(g *leaseGuard, kind, from string) bool {
		o := orderFor(9)
		o.From = from
		switch kind {
		case "probe":
			o.Probe = true
			return g.Probe(o).OK
		case "succeed":
			o.Succeed = true
			return g.Renew(o).OK
		case "prevote":
			o.PreVote, o.Succeed = true, true
			g.PreVote(o)
			return false // a pre-vote takes nothing
		}
		return g.Renew(o).OK
	}
	for _, k1 := range kinds {
		for _, k2 := range kinds {
			for _, k3 := range kinds {
				f := newGuard(t)
				// A leader of epoch 8 went silent long ago, so a Succeed order is not refused for it.
				old := orderFor(8)
				old.From = "srv_old"
				f.g.Renew(old)
				f.clk.Add(time.Minute)
				holder := ""
				for i, step := range []struct{ kind, from string }{{k1, "srv_a1"}, {k2, ""}, {k3, "srv_b1"}} {
					// The last order comes after a restart: what was given is read back from disk.
					if i == 2 {
						f.g = f.reopen()
					}
					if !send(f.g, step.kind, step.from) {
						continue
					}
					if holder != "" && step.from != "" && step.from != holder {
						t.Fatalf("%s, %s, %s: epoch 9 went to %q after %q", k1, k2, k3, step.from, holder)
					}
					if step.from != "" {
						holder = step.from
					}
				}
			}
		}
	}
}

type asks struct {
	mu  sync.Mutex
	got []succeedAsk
}

func (a *asks) ask(_ context.Context, s succeedAsk) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.got = append(a.got, s)
	return nil
}

func (a *asks) epochs() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []uint64
	for _, s := range a.got {
		out = append(out, s.Epoch)
	}
	return out
}

func TestSuccession_aMemberNextInLineAsksTheLeaderBesideItToLead(t *testing.T) {
	cases := []struct {
		name   string
		line   []string
		extend int
		wait   time.Duration
		want   []uint64
	}{
		{"the heir, after the removal time", []string{"srv_a", "srv_c"}, 0, lease.PreVoteFor, []uint64{4}},
		{"the heir, not before", []string{"srv_a"}, 0, lease.PreVoteFor - time.Second, nil},
		{"the next, one stagger later", []string{"srv_c", "srv_a"}, 0, lease.PreVoteFor + lease.SucceedStagger, []uint64{5}},
		{"the next, not with the heir", []string{"srv_c", "srv_a"}, 0, lease.PreVoteFor, nil},
		{"not in the line", []string{"srv_c"}, 0, time.Hour, nil},
		{"an extended lease, a planned restart", []string{"srv_a"}, 300, lease.PreVoteFor, nil},
		{"an extended lease that ran out", []string{"srv_a"}, 300, 6 * time.Minute, []uint64{4}},
	}
	// On cluster eu an heir that led and could not renew its own server asked itself again every 10 s, each time at a
	// newer epoch, and two heirs passed the cluster back and forth (epochs 4 to 34 in two minutes).
	t.Run("not while the leader beside it holds the epoch", func(t *testing.T) {
		f := newGuard(t)
		got := &asks{}
		f.g.succeed = got.ask
		o := orderFor(3)
		o.From, o.Line = "srv_a", []string{"srv_a"}
		f.g.Renew(o)
		f.clk.Add(time.Minute)
		f.g.Check(context.Background())
		f.g.work.Wait()
		if e := got.epochs(); len(e) != 0 {
			t.Fatalf("asked the leader beside it, which holds the epoch, again: %v", e)
		}
	})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newGuard(t)
			got := &asks{}
			f.g.succeed = got.ask
			o := orderFor(3)
			o.From, o.Line, o.Last, o.ExtendSeconds = "srv_lead", tc.line, "srv_deplo", tc.extend
			f.g.Renew(o)
			f.clk.Add(tc.wait)
			f.g.Check(context.Background())
			f.g.work.Wait()
			if e := got.epochs(); len(e) != len(tc.want) || len(e) > 0 && e[0] != tc.want[0] {
				t.Fatalf("asked %v, want %v", e, tc.want)
			}
		})
	}
}

func TestSuccession_aMemberBackOnLineReadsTheLineItIsGivenNow(t *testing.T) {
	f := newGuard(t)
	got := &asks{}
	f.g.succeed = got.ask
	o := orderFor(3)
	o.From, o.Line = "srv_lead", []string{"srv_a"}
	f.g.Renew(o)
	// Back after a cut, the renewal names another heir: this member asks nothing when the leader goes silent again.
	o.Line = []string{"srv_c"}
	f.clk.Add(time.Minute)
	f.g.Renew(o)
	f.clk.Add(lease.PreVoteFor + 2*lease.SucceedStagger)
	f.g.Check(context.Background())
	f.g.work.Wait()
	if e := got.epochs(); len(e) != 0 {
		t.Fatalf("a member that is no longer heir asked %v", e)
	}
}

// eu, 23 Nov 2026: with agent canary.40 the heir that led eu-3 had its start of memos refused as "stale_epoch" every
// 31 s for 6 minutes, because the leader names no server on a start order. A start at the leader's epoch is taken.
func TestSuccession_aStartOrderThatNamesNoServerIsTakenAtItsLeadersEpoch(t *testing.T) {
	f := newGuard(t)
	o := orderFor(69)
	o.From = "srv_heir"
	if r := f.g.Probe(o); !r.OK {
		t.Fatalf("the heir's probe got %+v", r)
	}
	start := orderFor(69)
	start.Start = &lease.Start{Slug: "files"}
	if r := f.g.Renew(start); !r.OK {
		t.Fatalf("the heir's start order got %+v", r)
	}
	other := orderFor(69)
	other.From = "srv_other"
	if r := f.g.Renew(other); r.OK {
		t.Fatalf("after the start, another leader took the heir's epoch: %+v", r)
	}
}

// c43: eu-3, back from a power cut, asked the leader beside it to lead on a lease 4.5 min old while eu-2 still led.
// A lease from before this boot gives its leader the whole wait from this start, and asks after every heir.
func TestSuccession_aMemberBackFromAPowerCutAsksLastAndLate(t *testing.T) {
	f := newGuard(t)
	o := orderFor(129)
	o.From, o.Line, o.Last = "srv_lead", []string{"srv_a", "srv_c"}, "srv_deplo"
	f.g.Renew(o)
	f.clk.Add(4*time.Minute + 26*time.Second)
	b := bootID
	bootID = func() string { return b() + "-next" }
	t.Cleanup(func() { bootID = b })
	f.reopen()
	got := &asks{}
	f.g.succeed = got.ask
	for _, wait := range []time.Duration{0, lease.PreVoteFor, lease.SucceedStagger} {
		f.clk.Add(wait)
		f.g.Check(context.Background())
		f.g.work.Wait()
		if e := got.epochs(); len(e) != 0 {
			t.Fatalf("asked %v %s after its start", e, f.clk.Now().Sub(f.g.started))
		}
	}
	f.clk.Add(lease.SucceedStagger)
	f.g.Check(context.Background())
	f.g.work.Wait()
	if e := got.epochs(); len(e) != 1 || e[0] != 132 {
		t.Fatalf("asked %v once its leader and both heirs had their turn, want epoch 132", e)
	}
}
