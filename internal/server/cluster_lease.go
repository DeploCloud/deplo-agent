package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// A member lives on the leader's lease: each renewal carries the leader's epoch and the placement. Without one
// for lease.FenceAfter it stops its single-writer workloads, so one never runs twice when it is only cut off.
// An order from an older epoch is refused (spec #176).

// leaseCheckEvery is how often the lease is looked at; a var so tests can hurry it.
var leaseCheckEvery = time.Second

type clusterLease struct {
	Epoch     uint64        `json:"epoch"`
	Member    string        `json:"member"`
	RenewedAt time.Time     `json:"renewedAt"`
	Placement []lease.Entry `json:"placement"`
	// Fenced is set once this lease ran out and its single-writer workloads were stopped.
	Fenced bool `json:"fenced"`
	// ExtraSeconds is the time a leader about to restart asked this lease to live on without a renewal.
	ExtraSeconds int `json:"extraSeconds,omitempty"`
}

type leaseGuard struct {
	mu    sync.Mutex
	file  string
	now   func() time.Time
	fence func(ctx context.Context, entries []lease.Entry) error
	// start brings a moved workload up from its failover bundle.
	start    func(ctx context.Context, s *lease.Start) error
	clusters map[string]*clusterLease
}

func newLeaseGuard(file string, now func() time.Time, fence func(context.Context, []lease.Entry) error) *leaseGuard {
	g := &leaseGuard{file: file, now: now, fence: fence, clusters: map[string]*clusterLease{}}
	if raw, err := os.ReadFile(file); err == nil {
		_ = json.Unmarshal(raw, &g.clusters)
	}
	return g
}

// save writes the leases; the caller holds g.mu.
func (g *leaseGuard) save() {
	if g.file == "" {
		return
	}
	raw, _ := json.Marshal(g.clusters)
	tmp := g.file + ".tmp"
	if os.WriteFile(tmp, raw, 0o600) == nil {
		_ = os.Rename(tmp, g.file)
	}
}

// Admit refuses an epoch older than the newest this member has seen: every order from a leader goes through it.
func (g *leaseGuard) Admit(cluster string, epoch uint64) (held uint64, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.clusters[cluster]; c != nil && epoch < c.Epoch {
		return c.Epoch, false
	}
	return epoch, true
}

// Renew applies one renewal: it takes the epoch and the placement, and starts the lease over.
func (g *leaseGuard) Renew(o lease.Order) lease.Reply {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.clusters[o.Cluster]; c != nil && o.Epoch < c.Epoch {
		return lease.Reply{Error: lease.StaleEpoch, Epoch: c.Epoch}
	}
	g.clusters[o.Cluster] = &clusterLease{Epoch: o.Epoch, Member: o.Member, RenewedAt: g.now(), Placement: o.Placement, ExtraSeconds: min(max(o.ExtendSeconds, 0), 3600)}
	g.save()
	return lease.Reply{OK: true, Epoch: o.Epoch}
}

// Check stops the single-writer workloads of every lease that has run out, once each. A stop that fails is tried again on the next check.
func (g *leaseGuard) Check(ctx context.Context) {
	g.mu.Lock()
	type due struct {
		cluster string
		entries []lease.Entry
	}
	var todo []due
	now := g.now()
	for id, c := range g.clusters {
		if c.Fenced || c.RenewedAt.IsZero() || now.Sub(c.RenewedAt) < lease.FenceAfter+time.Duration(c.ExtraSeconds)*time.Second {
			continue
		}
		var mine []lease.Entry
		for _, e := range c.Placement {
			if e.Member == c.Member && e.SingleWriter && e.Slug != "" {
				mine = append(mine, e)
			}
		}
		todo = append(todo, due{id, mine})
	}
	g.mu.Unlock()
	sort.Slice(todo, func(i, j int) bool { return todo[i].cluster < todo[j].cluster })
	for _, d := range todo {
		err := g.fence(ctx, d.entries)
		if err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: lease ran out, could not stop its workloads: %v\n", d.cluster, err)
			continue
		}
		g.mu.Lock()
		// A renewal that landed while stopping wins: the lease is alive again.
		if c := g.clusters[d.cluster]; c != nil && g.now().Sub(c.RenewedAt) >= lease.FenceAfter+time.Duration(c.ExtraSeconds)*time.Second {
			c.Fenced = true
			g.save()
		}
		g.mu.Unlock()
	}
}

// Forget drops everything held for a cluster this agent has left.
func (g *leaseGuard) Forget() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.clusters = map[string]*clusterLease{}
	if g.file != "" {
		_ = os.Remove(g.file)
	}
}

// Serve answers the leader's order on one stream; anyone but a leader is refused.
func (g *leaseGuard) Serve(from wire.Party, c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if from.Role != wire.RoleLeader {
		return
	}
	o, err := lease.ReadOrder(c)
	if err != nil || o.Cluster == "" || o.Member == "" {
		_ = lease.WriteReply(c, lease.Reply{Error: "unreadable order"})
		return
	}
	r := g.Renew(o)
	if o.Start != nil && r.OK {
		r = g.startWorkload(c, o)
	}
	_ = lease.WriteReply(c, r)
}

// startWorkload brings one moved workload up. The order must place that stack on this member, so a leader cannot
// start a stack the placement does not give it; Started is only true once the stack is ready.
func (g *leaseGuard) startWorkload(c net.Conn, o lease.Order) lease.Reply {
	mine := false
	for _, e := range o.Placement {
		if e.Member == o.Member && e.Slug == o.Start.Slug && e.Slug != "" {
			mine = true
		}
	}
	if !mine || g.start == nil {
		return lease.Reply{Error: "this stack is not placed on this member"}
	}
	_ = c.SetDeadline(time.Now().Add(lease.StartTimeout + time.Minute))
	if err := g.start(context.Background(), o.Start); err != nil {
		return lease.Reply{Error: err.Error()}
	}
	return lease.Reply{OK: true, Started: true, Epoch: o.Epoch}
}

// Run checks the lease until ctx ends.
func (g *leaseGuard) Run(ctx context.Context) {
	t := time.NewTicker(leaseCheckEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.Check(ctx)
		}
	}
}

func (s *Service) leaseFile() string { return filepath.Join(s.agentDir, "cluster-lease.json") }

// leaseGuard is created with the member, so a lease that ran out while the agent was down is still honoured.
func (s *Service) leaseGuard() *leaseGuard {
	s.leaseOnce.Do(func() {
		s.lease = newLeaseGuard(s.leaseFile(), time.Now, s.fenceStacks)
		s.lease.start = s.startFromBundle
	})
	return s.lease
}

// fenceStacks stops each stack; the stack lock keeps it from racing a deploy of the same one.
func (s *Service) fenceStacks(ctx context.Context, entries []lease.Entry) error {
	var first error
	for _, e := range entries {
		res, err := s.StopStack(ctx, &pb.StackRef{Slug: e.Slug})
		if err == nil && !res.GetOk() {
			err = fmt.Errorf("%s: %s", e.Slug, res.GetError())
		}
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}
