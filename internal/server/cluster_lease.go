package server

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A member lives on the leader's lease: each renewal carries the leader's epoch and the placement. Without one
// for the lease's fence it stops its single-writer workloads, so one never runs twice when it is only cut off.
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
	// FenceSeconds is how long the leader said this lease lives without a renewal; zero is lease.FenceAfter.
	FenceSeconds int `json:"fenceSeconds,omitempty"`
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
	// Boot is the server's boot the lease was last renewed in: see cold.
	Boot string `json:"boot,omitempty"`
	// cold is a lease that was dead when this agent started after the server itself did: Docker brought every
	// stack back, so nothing of it runs until the leader speaks. An agent that only restarted fences as usual.
	cold bool
	// since is when the return under way began, and asked whether the leader has said it moved this member's work.
	since time.Time
	asked bool
}

func (c *clusterLease) ranOut(now time.Time) bool {
	return !c.RenewedAt.IsZero() && now.Sub(c.RenewedAt) >= c.fence()+time.Duration(c.ExtraSeconds)*time.Second
}

func (c *clusterLease) fence() time.Duration {
	if c.FenceSeconds > 0 {
		return time.Duration(c.FenceSeconds) * time.Second
	}
	return lease.FenceAfter
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
	// runs are the start orders under way here: a newer epoch ends the ones an older leader sent.
	runs []*startRun
	// unreadable is a lease file that would not open: see presumeDead.
	unreadable bool
	// leader is the cluster this server is in and the certificates Deplo named as its leaders: the relay's
	// word for who leads is never enough. nil (a guard under test) takes any leader.
	leader func() (cluster string, fingerprints []string)
}

// unreadableLease names the lease a file that would not open stands for: one that ran out, of nobody knows what.
const unreadableLease = "unreadable"

func newLeaseGuard(file string, now func() time.Time, fence func(context.Context, []lease.Entry) error) *leaseGuard {
	g := &leaseGuard{file: file, now: now, fence: fence, clusters: map[string]*clusterLease{}, settling: map[string]bool{}, starting: map[string]time.Time{}, keeps: map[string]map[string]lease.Keep{}, stores: map[string][]lease.Store{}, retryAt: map[string]time.Time{}, retryWait: map[string]time.Duration{}}
	if raw, err := os.ReadFile(file); err == nil {
		if err := json.Unmarshal(raw, &g.clusters); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: the cluster lease in %s cannot be read (%v): taken as a lease that ran out\n", file, err)
			g.clusters, g.unreadable = map[string]*clusterLease{}, true
		}
	} else if !os.IsNotExist(err) && file != "" {
		fmt.Fprintf(os.Stderr, "deplo-agent: the cluster lease in %s cannot be read (%v): taken as a lease that ran out\n", file, err)
		g.unreadable = true
	}
	// Docker's restart policy brought these stacks back before this agent; a dead lease stops them all again.
	for _, c := range g.clusters {
		if c != nil && c.ranOut(now()) {
			c.cold, c.Fenced = c.Boot == "" || c.Boot != bootID(), false
		}
	}
	return g
}

// bootID names this boot of the server; empty when the kernel does not say, which reads as another boot.
var bootID = func() string {
	raw, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(raw))
}

// save writes the leases; the caller holds g.mu.
func (g *leaseGuard) save() {
	if g.file == "" {
		return
	}
	// The stand-in for an unreadable file is never written back: only a leader's word replaces it.
	out := maps.Clone(g.clusters)
	delete(out, unreadableLease)
	raw, err := json.Marshal(out)
	if err == nil {
		err = writeFileAtomic(g.file, raw, 0o600)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not keep the cluster lease in %s: %v\n", g.file, err)
	}
}

// presumeDead stands in for a lease file that would not open: a cold lease that ran out long ago and places
// every stack on this host here, so all of them stop and none starts until a leader's renewal says what is whose.
func (g *leaseGuard) presumeDead(slugs []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := &clusterLease{Member: unreadableLease, RenewedAt: time.Unix(1, 0), cold: true}
	for _, slug := range slugs {
		c.Placement = append(c.Placement, lease.Entry{Member: unreadableLease, Slug: slug, SingleWriter: true})
	}
	g.clusters[unreadableLease] = c
}

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

// Admit refuses an epoch older than the newest this member has seen.
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
	g.endOlderStarts(o.Cluster, o.Epoch)
	// The leader's word replaces the stand-in for an unreadable file: what that stopped comes back as held.
	if u := g.clusters[unreadableLease]; u != nil && g.clusters[o.Cluster] == nil {
		g.clusters[o.Cluster] = u
	}
	delete(g.clusters, unreadableLease)
	next := &clusterLease{Epoch: o.Epoch, Member: o.Member, RenewedAt: g.now(), Boot: bootID(), Placement: o.Placement, ExtraSeconds: min(max(o.ExtendSeconds, 0), int(lease.ExtendFor/time.Second)),
		FenceSeconds: min(max(o.FenceSeconds, 0), int(lease.FenceAfter/time.Second))}
	g.confirmStarts(o)
	r := lease.Reply{OK: true, Epoch: o.Epoch, StoppedWithin: int((next.fence() + fenceTakes()).Seconds())}
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
	g.endOlderStarts(o.Cluster, o.Epoch)
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
			if g.fences(c, e) {
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

// fences says whether a lease that ran out stops this entry here; the caller holds g.mu.
func (g *leaseGuard) fences(c *clusterLease, e lease.Entry) bool {
	return e.Slug != "" && ((e.Member == c.Member && (e.SingleWriter || c.cold)) || (e.SingleWriter && g.hasUnplaced(c.Placement, e)))
}

// mustStayDown says a stack is one the fence stops and its lease has run out: whatever just started it stops it again.
func (g *leaseGuard) mustStayDown(slug string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	for _, c := range g.clusters {
		if !c.ranOut(now) {
			continue
		}
		for _, e := range c.Placement {
			if e.Slug == slug && g.fences(c, e) {
				return true
			}
		}
	}
	return false
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
		if c.ranOut(now) {
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
	// A slug ends up in a file path: an order naming anything else is refused whole.
	if !slugsOK(o) {
		_ = lease.WriteReply(c, lease.Reply{Error: "the order names something that is not a stack"})
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
func (g *leaseGuard) startWorkload(c net.Conn, o lease.Order) lease.Reply {
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
		s.lease.keep, s.lease.purge, s.lease.stop = s.keepLeft, s.purgeLeft, s.stopMoved
		s.lease.has = func(slug string) bool { return isFile(s.stackPath(slug)) }
		s.lease.running = stackRuns
		if s.lease.unreadable && s.loadMember().Member {
			s.lease.presumeDead(s.stackSlugs())
		}
	})
	return s.lease
}

// stackSlugs is every stack this host holds, a deploy's stand-in aside.
func (s *Service) stackSlugs() []string {
	files, _ := filepath.Glob(filepath.Join(s.stackDir, "*.yml"))
	var out []string
	for _, f := range files {
		slug := strings.TrimSuffix(filepath.Base(f), ".yml")
		if validateSlug(slug) == nil {
			out = append(out, slug)
		}
	}
	return out
}

// fenceGrace is what a fenced stack gets to stop cleanly before it is killed; a var so tests can hurry it.
var fenceGrace = 5 * time.Second

// fenceTakes is the longest from a lease running out to its stacks being down: the check, the stop and the three
// kills in fenceStack, plus a second. Under the 21 s an older leader waits after the 60 s fence.
func fenceTakes() time.Duration {
	return leaseCheckEvery + fenceGrace + 3*time.Second + 3*3*time.Second + time.Second
}

// fenceStacks stops every stack at once, and every running scheduled job. It never waits for a stack's lock: a
// deploy that holds one is cancelled, and whatever it started is stopped by the deploy itself on its way out.
func (s *Service) fenceStacks(ctx context.Context, entries []lease.Entry) error {
	s.fenceJobs()
	slugs := map[string]bool{}
	for _, e := range entries {
		slugs[e.Slug] = true
	}
	s.cancelDeploysOf(slugs)
	ctx, cancel := context.WithTimeout(ctx, fenceGrace+10*time.Second)
	defer cancel()
	errs := make(chan error, len(slugs))
	for slug := range slugs {
		go func() { errs <- s.fenceStack(ctx, slug) }()
	}
	var first error
	for range slugs {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// fenceStack stops one stack within fenceGrace, kills what is left, and fails when something of it still runs.
func (s *Service) fenceStack(ctx context.Context, slug string) error {
	if err := validateSlug(slug); err != nil {
		return err
	}
	grace := strconv.Itoa(max(int(fenceGrace/time.Second), 1))
	res, err := dockercli.Run(ctx, fenceGrace+3*time.Second, s.composeCtl(slug, "stop", "-t", grace)...)
	if err != nil || res.Code != 0 || stackRuns(ctx, slug) {
		_, _ = dockercli.Run(ctx, 3*time.Second, s.composeCtl(slug, "kill")...)
		_, _ = dockercli.Run(ctx, 3*time.Second, "kill", "deplo-"+slug)
	}
	// A deploy's stand-in mounts the same volume: it goes with the stack.
	if isFile(s.stackPath(slug + standInSuffix)) {
		_, _ = dockercli.Run(ctx, 3*time.Second, append(s.standInCompose(slug), "kill")...)
	}
	if stackRuns(ctx, slug) {
		return fmt.Errorf("%s: still running", slug)
	}
	return nil
}

// lockToStart takes a stack's lock for something that brings it up, refused while this member has no live lease.
// done releases the lock and says whether the lease ran out meanwhile: the stack is then stopped again.
func (s *Service) lockToStart(slug string) (done func() bool, ok bool) {
	unlock := s.lockStack(slug)
	if s.leaseGuard().Holds(slug) {
		unlock()
		return nil, false
	}
	return func() bool {
		defer unlock()
		if !s.leaseGuard().mustStayDown(slug) {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), fenceGrace+10*time.Second)
		defer cancel()
		if err := s.fenceStack(ctx, slug); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: the lease ran out while %s started, and it could not be stopped again: %v\n", slug, err)
		}
		return true
	}, true
}
