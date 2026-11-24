package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"slices"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

type startRun struct {
	cluster string
	epoch   uint64
	cancel  context.CancelFunc
}

// endOlderStarts cancels every start an older epoch's leader ordered for this cluster; the caller holds g.mu.
func (g *leaseGuard) endOlderStarts(cluster string, epoch uint64) {
	for _, r := range g.runs {
		if r.cluster == cluster && r.epoch < epoch {
			r.cancel()
		}
	}
}

// Serve answers an order on one stream, from a leader Deplo named for the cluster the order is about.
func (g *leaseGuard) Serve(from wire.Party, c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if from.Role != wire.RoleLeader {
		return
	}
	cluster := ""
	if g.leader != nil {
		var leaders []string
		if cluster, leaders = g.leader(); !slices.ContainsFunc(leaders, from.Is) {
			return
		}
	}
	o, err := lease.ReadOrder(c)
	if err != nil || o.Cluster == "" || o.Member == "" {
		_ = lease.WriteReply(c, lease.Reply{Error: "unreadable order"})
		return
	}
	if g.leader != nil && o.Cluster != cluster {
		_ = lease.WriteReply(c, lease.Reply{Error: "this server is not in that cluster"})
		return
	}
	// A slug ends up in a file path: an order naming anything else is refused whole.
	if !slugsOK(o) {
		_ = lease.WriteReply(c, lease.Reply{Error: "the order names something that is not a stack"})
		return
	}
	if o.PreVote {
		_ = lease.WriteReply(c, g.PreVote(o))
		return
	}
	if o.Probe {
		placed, epoch := g.placedHere(o.Cluster)
		r := g.Probe(o)
		if r.OK {
			r.Running, r.Reports = g.runningOf(placed, epoch), true
		}
		_ = lease.WriteReply(c, r)
		return
	}
	before := ""
	if o.Start != nil {
		before = g.placedBefore(o.Cluster, o.Start.Slug)
	}
	r := g.Renew(o)
	if o.Start != nil && r.OK {
		r = g.startWorkload(c, o, before)
	}
	if r.OK {
		r.Unplaced = g.unplaced(o)
	}
	_ = lease.WriteReply(c, r)
}

func slugsOK(o lease.Order) bool {
	if o.Start != nil && validateSlug(o.Start.Slug) != nil {
		return false
	}
	for _, e := range o.Placement {
		if e.Slug != "" && validateSlug(e.Slug) != nil {
			return false
		}
	}
	for _, k := range o.Keep {
		if validateSlug(k.Slug) != nil {
			return false
		}
	}
	return true
}

// startWorkload brings one moved workload up. The order must place that stack on this member, so a leader cannot
// start a stack the placement does not give it; Started is only true once the stack is ready.
func (g *leaseGuard) startWorkload(c net.Conn, o lease.Order, before string) lease.Reply {
	mine, single := false, false
	for _, e := range o.Placement {
		if e.Member == o.Member && e.Slug == o.Start.Slug && e.Slug != "" {
			mine, single = true, e.SingleWriter
		}
	}
	if !mine || g.start == nil {
		return lease.Reply{Error: "this stack is not placed on this member"}
	}
	limit := lease.StartTimeout
	if o.Start.Restore != nil {
		limit = lease.RestoreTimeout
	}
	_ = c.SetDeadline(time.Now().Add(limit + time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	run := &startRun{o.Cluster, o.Epoch, cancel}
	g.mu.Lock()
	g.runs = append(g.runs, run)
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.runs = slices.DeleteFunc(g.runs, func(r *startRun) bool { return r == run })
		g.mu.Unlock()
	}()
	// The leader's renewals still place this stack on its old member until this has answered.
	g.markStart(o.Start.Slug, false)
	err := g.start(ctx, o.Start)
	// A restore takes minutes: a leader with a newer epoch spoke meanwhile, so this order is nobody's any more.
	if held, ok := g.Admit(o.Cluster, o.Epoch); !ok {
		g.markStart(o.Start.Slug, true)
		if single && g.stop != nil {
			sctx, done := context.WithTimeout(context.Background(), 2*time.Minute)
			defer done()
			if serr := g.stop(sctx, o.Start.Slug); serr != nil {
				fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: could not stop %s, started for a leader that was replaced: %v\n", o.Cluster, o.Start.Slug, serr)
			}
		}
		return lease.Reply{Error: lease.StaleEpoch, Epoch: held}
	}
	if err != nil {
		g.markStart(o.Start.Slug, true)
		if single {
			g.mu.Lock()
			g.noteFailedStart(o, before)
			g.mu.Unlock()
		}
		return lease.Reply{Error: err.Error()}
	}
	g.markStart(o.Start.Slug, false)
	return lease.Reply{OK: true, Started: true, Restored: o.Start.Restore != nil, Promoted: o.Start.Promote != nil, Epoch: o.Epoch}
}
