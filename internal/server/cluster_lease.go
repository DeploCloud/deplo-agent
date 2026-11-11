package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
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
	// Held are the stacks this member stopped itself, started again once the leader says they are still its own.
	Held []string `json:"held,omitempty"`
	// Returning is set by the first renewal after a dead lease, until what moved away is removed.
	Returning bool `json:"returning,omitempty"`
	// Returned is what that return removed, kept until the leader has it.
	Returned *lease.Returned `json:"returned,omitempty"`
	// Keeping is the stacks whose data stayed here past a return: tried again until saved or live here again.
	Keeping []lease.Kept `json:"keeping,omitempty"`
	// Late is what such a try wrote, kept until the leader no longer names its stack.
	Late []lease.Saved `json:"late,omitempty"`
	// cold is a lease that was dead when this agent started: nothing of it runs until the leader speaks.
	cold bool
	// since is when the return under way began, and asked whether the leader has said it moved this member's work.
	since time.Time
	asked bool
}

func (c *clusterLease) ranOut(now time.Time) bool {
	return !c.RenewedAt.IsZero() && now.Sub(c.RenewedAt) >= lease.FenceAfter+time.Duration(c.ExtraSeconds)*time.Second
}

type leaseGuard struct {
	mu    sync.Mutex
	file  string
	now   func() time.Time
	fence func(ctx context.Context, entries []lease.Entry) error
	// start brings a moved workload up from its failover bundle.
	start func(ctx context.Context, s *lease.Start) error
	// remove takes down this member's copy of a stack placed elsewhere; resume starts one it stopped itself.
	remove func(ctx context.Context, e lease.Entry) error
	resume func(ctx context.Context, slug string) error
	// trim stops the copies of a stack beyond n, and says whether there were any.
	trim func(ctx context.Context, slug string, n int) (bool, error)
	// keep archives what a moved stack still holds here, and purge then removes the stack with its volumes.
	// Each asks still under the stack's lock: a start of it here in between makes that volume the live one.
	keep  func(ctx context.Context, e lease.Entry, k lease.Keep, member string, still func() bool) (*lease.Saved, error)
	purge func(ctx context.Context, e lease.Entry, k lease.Keep, still func() bool) error
	// stop stops a stack and removes nothing.
	stop func(ctx context.Context, slug string) error
	// keeps is where each cluster's moved stacks are saved, as the leader last said. Never written to disk:
	// a keep carries the destination's credentials.
	keeps map[string]map[string]lease.Keep
	// retryAt is when what a cluster's return left here is tried again, retryWait the wait that led to it.
	retryAt   map[string]time.Time
	retryWait map[string]time.Duration
	// stores is the archives each cluster's leader last told this member to accept from other members.
	stores map[string][]lease.Store
	// has says whether a stack is on this host, running whether it runs now; nil reads as yes.
	has      func(slug string) bool
	running  func(ctx context.Context, slug string) bool
	settling map[string]bool
	// starting are the stacks a start order is bringing up here (zero), or brought up at that time: a renewal
	// that still places one elsewhere does not remove it until the leader has placed it here.
	starting map[string]time.Time
	work     sync.WaitGroup
	clusters map[string]*clusterLease
	// leader is the cluster this server is in and the certificates Deplo named as its leaders: the relay's
	// word for who leads is never enough. nil (a guard under test) takes any leader.
	leader func() (cluster string, fingerprints []string)
}

func newLeaseGuard(file string, now func() time.Time, fence func(context.Context, []lease.Entry) error) *leaseGuard {
	g := &leaseGuard{file: file, now: now, fence: fence, clusters: map[string]*clusterLease{}, settling: map[string]bool{}, starting: map[string]time.Time{}, keeps: map[string]map[string]lease.Keep{}, stores: map[string][]lease.Store{}, retryAt: map[string]time.Time{}, retryWait: map[string]time.Duration{}}
	if raw, err := os.ReadFile(file); err == nil {
		_ = json.Unmarshal(raw, &g.clusters)
	}
	// Docker's restart policy brought these stacks back before this agent; a dead lease stops them all again.
	for _, c := range g.clusters {
		if c != nil && c.ranOut(now()) {
			c.cold, c.Fenced = true, false
		}
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
	next := &clusterLease{Epoch: o.Epoch, Member: o.Member, RenewedAt: g.now(), Placement: o.Placement, ExtraSeconds: min(max(o.ExtendSeconds, 0), int(lease.ExtendFor/time.Second))}
	g.confirmStarts(o)
	r := lease.Reply{OK: true, Epoch: o.Epoch}
	g.comeBack(o, g.clusters[o.Cluster], next, &r)
	g.clusters[o.Cluster] = next
	// A start order carries neither keeps nor stores: it says nothing about them.
	if o.Start == nil {
		g.stores[o.Cluster] = o.Stores
	}
	g.save()
	return r
}

// Probe answers a leader that is not sure it still leads: the epoch is taken and nothing else changes. The
// lease is as old as it was, so a leader that hears too few members to lead never keeps this one alive.
func (g *leaseGuard) Probe(o lease.Order) lease.Reply {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[o.Cluster]
	switch {
	case c == nil:
		g.clusters[o.Cluster] = &clusterLease{Epoch: o.Epoch, Member: o.Member}
	case o.Epoch < c.Epoch:
		return lease.Reply{Error: lease.StaleEpoch, Epoch: c.Epoch}
	default:
		c.Epoch = o.Epoch
	}
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
		if c.Fenced || !c.ranOut(now) {
			continue
		}
		var mine []lease.Entry
		for _, e := range c.Placement {
			if e.Slug != "" && ((e.Member == c.Member && (e.SingleWriter || c.cold)) || (e.SingleWriter && g.hasUnplaced(c.Placement, e))) {
				mine = append(mine, e)
			}
		}
		todo = append(todo, due{id, mine})
	}
	g.mu.Unlock()
	sort.Slice(todo, func(i, j int) bool { return todo[i].cluster < todo[j].cluster })
	for _, d := range todo {
		held := g.runningNow(ctx, d.entries)
		err := g.fence(ctx, d.entries)
		if err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: lease ran out, could not stop its workloads: %v\n", d.cluster, err)
			continue
		}
		g.mu.Lock()
		// A renewal that landed while stopping wins: the lease is alive again.
		if c := g.clusters[d.cluster]; c != nil && c.ranOut(g.now()) {
			c.Fenced = true
			c.Held = union(c.Held, held)
			g.save()
		}
		g.mu.Unlock()
	}
}

// hasUnplaced says a stack the order placed on no member is on this host: the leader takes it for this
// member's once it hears so, so it stops with this member's lease.
func (g *leaseGuard) hasUnplaced(placement []lease.Entry, e lease.Entry) bool {
	placed := slices.ContainsFunc(placement, func(p lease.Entry) bool { return p.Slug == e.Slug && p.Member != "" })
	return !placed && g.has != nil && g.has(e.Slug)
}

// RanOut reports a lease that has gone unrenewed past the fence: until a renewal, this member starts no scheduled job.
func (g *leaseGuard) RanOut() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	for _, c := range g.clusters {
		if !c.RenewedAt.IsZero() && now.Sub(c.RenewedAt) >= lease.FenceAfter+time.Duration(c.ExtraSeconds)*time.Second {
			return true
		}
	}
	return false
}

// Forget drops everything held for a cluster this agent has left.
func (g *leaseGuard) Forget() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.clusters = map[string]*clusterLease{}
	g.keeps = map[string]map[string]lease.Keep{}
	g.stores = map[string][]lease.Store{}
	if g.file != "" {
		_ = os.Remove(g.file)
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
		if cluster, leaders = g.leader(); !slices.Contains(leaders, from.Fingerprint) {
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
	if o.Probe {
		_ = lease.WriteReply(c, g.Probe(o))
		return
	}
	r := g.Renew(o)
	if o.Start != nil && r.OK {
		r = g.startWorkload(c, o)
	}
	if r.OK {
		r.Unplaced = g.unplaced(o)
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
	limit := lease.StartTimeout
	if o.Start.Restore != nil {
		limit = lease.RestoreTimeout
	}
	_ = c.SetDeadline(time.Now().Add(limit + time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	// The leader's renewals still place this stack on its old member until this has answered.
	g.markStart(o.Start.Slug, false)
	if err := g.start(ctx, o.Start); err != nil {
		g.markStart(o.Start.Slug, true)
		return lease.Reply{Error: err.Error()}
	}
	g.markStart(o.Start.Slug, false)
	return lease.Reply{OK: true, Started: true, Restored: o.Start.Restore != nil, Promoted: o.Start.Promote != nil, Epoch: o.Epoch}
}

// Run checks the lease until ctx ends.
func (g *leaseGuard) Run(ctx context.Context) {
	g.Check(ctx)
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

func (s *Service) leaseFile() string {
	if s.agentDir == "" {
		return ""
	}
	return filepath.Join(s.agentDir, "cluster-lease.json")
}

// leaseGuard is created with the member, so a lease that ran out while the agent was down is still honoured.
func (s *Service) leaseGuard() *leaseGuard {
	s.leaseOnce.Do(func() {
		s.lease = newLeaseGuard(s.leaseFile(), time.Now, s.fenceStacks)
		s.lease.start, s.lease.leader = s.startFromBundle, s.leaderPins
		s.lease.remove, s.lease.resume, s.lease.trim = s.removeMoved, s.resumeHeld, s.trimReplicas
		s.lease.keep, s.lease.purge, s.lease.stop = s.keepLeftBehind, s.purgeLeftBehind, s.stopMoved
		s.lease.has = func(slug string) bool { return isFile(s.stackPath(slug)) }
		s.lease.running = stackRuns
	})
	return s.lease
}

// fenceStacks stops each stack and every running scheduled job; the stack lock keeps it from racing a deploy of the same one.
func (s *Service) fenceStacks(ctx context.Context, entries []lease.Entry) error {
	s.fenceJobs()
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
