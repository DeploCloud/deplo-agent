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
	if r := f.reopen().Renew(orderFor(5)); !r.OK {
		t.Fatalf("a leader that names no server (an older one) was refused: %+v", r)
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
