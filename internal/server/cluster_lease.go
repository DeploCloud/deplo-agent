package server

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// A member lives on the leader's lease: each renewal carries the leader's epoch and the placement. Without one
// for the lease's fence it stops its single-writer workloads, so one never runs twice when it is only cut off.
// An order from an older epoch is refused (spec #176).

// leaseCheckEvery is how often the lease is looked at; a var so tests can hurry it.
var leaseCheckEvery = time.Second

type clusterLease struct {
	Epoch uint64 `json:"epoch"`
	// PlacedAt is the epoch of the renewal that last set Placement: a probe takes a newer epoch and places nothing.
	PlacedAt  uint64        `json:"placedAt,omitempty"`
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
	// Witness is a lease that keeps what leaders said of extensions; one an older agent wrote did not.
	Witness bool `json:"witness,omitempty"`
	// Announced is until when a lease of the cluster may live on an extension a leader announced, and
	// Unannounced until when one may because a leader that announces nothing renewed this member.
	Announced   time.Time `json:"announced,omitempty"`
	Unannounced time.Time `json:"unannounced,omitempty"`
	// Gone is what the leader that renewed it no longer counts for a majority (ADR-0049).
	Gone []string `json:"gone,omitempty"`
	// Leader is the server whose leader holds Epoch: a member gives an epoch to one leader only.
	Leader string `json:"leader,omitempty"`
	// Line and Last are who leads after that leader, as its last renewal named them.
	Line []string `json:"line,omitempty"`
	Last string   `json:"last,omitempty"`
	// Heard is when that leader last heard each member, by this server's clock: an heir it asks to lead goes on from there.
	Heard map[string]time.Time `json:"heard,omitempty"`
	// cold is a lease that was dead when this agent started after the server itself did: Docker brought every
	// stack back, so nothing of it runs until the leader speaks. An agent that only restarted fences as usual.
	cold bool
	// heard is a lease a leader renewed since this agent started; one read back from disk says where things were.
	heard bool
	// toldStop and stopErr are what the log said of this lease running out, so a stop tried again is not said again.
	toldStop bool
	stopErr  string
	// since is when the return under way began, and asked whether the leader has said it moved this member's work.
	since time.Time
	asked bool
}

func (c *clusterLease) ranOut(now time.Time) bool {
	return !c.RenewedAt.IsZero() && now.Sub(c.RenewedAt) >= c.fence()+time.Duration(c.ExtraSeconds)*time.Second
}

// current is a lease to act on now: renewed in this run of the agent, and not run out since.
func (c *clusterLease) current(now time.Time) bool { return c.heard && !c.ranOut(now) }

// answerable is a lease this server answers visitors on: current, or read back after the agent restarted in this
// boot and not run out, which no leader can have moved a writer away within (c43: 5 s unanswered at a restart).
func (c *clusterLease) answerable(now time.Time) bool {
	return c.current(now) || (!c.ranOut(now) && c.Boot != "" && c.Boot == bootID())
}

// writers is every single-writer stack the leases name, and those a current lease places here and leaves to this
// member: no return under way, no start order bringing it up. A standby copy shares its database's stack.
func (g *leaseGuard) writers() (all, mine []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	for id, c := range g.clusters {
		quiet := c.current(now) && !c.Returning && !g.settling[id]
		for _, e := range c.Placement {
			if e.Slug == "" || !e.SingleWriter && !e.Standby {
				continue
			}
			if !slices.Contains(all, e.Slug) {
				all = append(all, e.Slug)
			}
			at, starting := g.starting[e.Slug]
			if quiet && e.Member == c.Member && (!starting || !at.IsZero()) && !slices.Contains(mine, e.Slug) {
				mine = append(mine, e.Slug)
			}
		}
	}
	slices.Sort(all)
	slices.Sort(mine)
	return all, mine
}

// whyNotMine says why writers leaves a stack out of mine, in a sentence for the log; "" when it is in mine.
func (g *leaseGuard) whyNotMine(slug string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	why := "no lease names it"
	for id, c := range g.clusters {
		for _, e := range c.Placement {
			if e.Slug != slug || !e.SingleWriter && !e.Standby {
				continue
			}
			at, starting := g.starting[e.Slug]
			switch {
			case !c.heard:
				why = "no leader has renewed this server's lease since the agent started"
			case c.ranOut(now):
				why = "its lease ran out"
			case c.Returning:
				why = "this server is coming back to its cluster, and what moved away is not removed yet"
			case g.settling[id]:
				why = "what the lease left on this server is being settled"
			case e.Member != c.Member && e.Member == "":
				why = "the leader has placed it on no server"
			case e.Member != c.Member:
				why = "the leader placed it on " + e.Member
			case starting && at.IsZero():
				why = "a start order from the leader is bringing it up"
			default:
				return ""
			}
		}
	}
	return why
}

// isWriter says a lease names the stack as one that must never run twice.
func (g *leaseGuard) isWriter(slug string) bool {
	all, _ := g.writers()
	return slices.Contains(all, slug)
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
	// aside copies a moved stack's volumes to dated copies; dropAside then takes the stack down with those volumes.
	aside     func(ctx context.Context, e lease.Entry, at time.Time) ([]lease.Aside, error)
	dropAside func(ctx context.Context, e lease.Entry, asides []lease.Aside, still func() bool) error
	// relinquish stops this member's copy of a stack a current lease places on another member, and says whether
	// one ran; relinquishing are the stacks it is stopping now.
	relinquish    func(ctx context.Context, slug string) (bool, error)
	relinquishing map[string]bool
	// failed is what failed failover starts left here, by stack; dropFailed removes it once the App runs elsewhere.
	failed     map[string]failedStart
	dropFailed func(ctx context.Context, slug string, vols []string, still func() bool) error
	// keeps is where each cluster's moved stacks are saved, as the leader last said. Never written to disk:
	// a keep carries the destination's credentials.
	keeps map[string]map[string]lease.Keep
	// retryAt is when what a cluster's return left here is tried again, retryWait the wait that led to it.
	retryAt   map[string]time.Time
	retryWait map[string]time.Duration
	// stores is the archives each cluster's leader last told this member to accept from other members.
	stores map[string][]lease.Store
	// has says whether a stack is on this host, running whether it runs now; nil reads as yes.
	has     func(slug string) bool
	running func(ctx context.Context, slug string) bool
	// runningAll answers running for many stacks in one look, so a probe waits for one docker call (c43: 1.49 s).
	runningAll func(ctx context.Context, slugs []string) map[string]bool
	// seenRunning is which unplaced stacks the last look found running; looking, whether one is under way.
	seenRunning map[string]bool
	looking     bool
	settling    map[string]bool
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
	// logf writes a line of the agent's log; tests read it.
	logf func(format string, args ...any)
	// succeed asks the leader beside this member to lead a cluster whose leader went silent; asking are those under way
	// and asked what each last answered, so the log says it once.
	succeed func(ctx context.Context, a succeedAsk) error
	asking  map[string]bool
	asked   map[string]string
	// started is when this agent read its leases back: a cold one waits for its leader from then (see askToLead).
	started time.Time
	// unsaved is the newest leases to write and version their number; written is the last on disk (see save). All three
	// under mu; wmu keeps one write at a time.
	unsaved          []byte
	version, written uint64
	writing          bool
	wmu              sync.Mutex
	// relinquished is the member each stack was last said to run on, so the stop is said once (see relinquishLocked).
	relinquished map[string]string
}

// unreadableLease names the lease a file that would not open stands for: one that ran out, of nobody knows what.
const unreadableLease = "unreadable"

func newLeaseGuard(file string, now func() time.Time, fence func(context.Context, []lease.Entry) error) *leaseGuard {
	g := &leaseGuard{file: file, now: now, fence: fence, clusters: map[string]*clusterLease{}, settling: map[string]bool{}, relinquishing: map[string]bool{}, starting: map[string]time.Time{}, keeps: map[string]map[string]lease.Keep{}, stores: map[string][]lease.Store{}, retryAt: map[string]time.Time{}, retryWait: map[string]time.Duration{}, logf: stderrf, asking: map[string]bool{}, asked: map[string]string{}, relinquished: map[string]string{}}
	if raw, err := os.ReadFile(file); err == nil {
		if err := json.Unmarshal(raw, &g.clusters); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: the cluster lease in %s cannot be read (%v): taken as a lease that ran out\n", file, err)
			g.clusters, g.unreadable = map[string]*clusterLease{}, true
		}
	} else if !os.IsNotExist(err) && file != "" {
		fmt.Fprintf(os.Stderr, "deplo-agent: the cluster lease in %s cannot be read (%v): taken as a lease that ran out\n", file, err)
		g.unreadable = true
	}
	g.started = now()
	// Docker's restart policy brought these stacks back before this agent; a dead lease stops them all again.
	for _, c := range g.clusters {
		if c != nil && c.ranOut(now()) {
			c.cold, c.Fenced = c.Boot == "" || c.Boot != bootID(), false
		}
		if c != nil {
			witness(nil, c, now(), lease.Order{Announces: true})
		}
	}
	return g
}

func stderrf(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }

// stopReason says why a lease that ran out stops an entry here, for the log; the caller holds g.mu.
func (g *leaseGuard) stopReason(c *clusterLease, e lease.Entry) string {
	switch {
	case e.Member == c.Member && e.SingleWriter:
		return "its lease ran out"
	case e.Member == c.Member && c.cold:
		return "the server booted on a lease that had run out"
	default:
		return "its lease ran out and the leader placed it on no server"
	}
}

// leaseAge says which lease a decision read, how old it was and where it came from, for the log.
func (c *clusterLease) leaseAge(now time.Time) string {
	from := "renewed in this run of the agent"
	if !c.heard {
		from = "read from disk at start"
	}
	if c.cold {
		from += ", the server booted since"
	}
	age := "never renewed"
	if !c.RenewedAt.IsZero() {
		age = "renewed " + now.Sub(c.RenewedAt).Round(100*time.Millisecond).String() + " ago"
	}
	return fmt.Sprintf("epoch %d, %s, lives %s, %s", c.Epoch, age, c.fence()+time.Duration(c.ExtraSeconds)*time.Second, from)
}

// witness carries what a member knows of extensions into its next lease, and adds what this order says. What
// leaders did before it kept count is unknown: the longest lease any of them may have given.
func witness(prev, next *clusterLease, now time.Time, o lease.Order) {
	switch {
	case prev != nil && prev.Witness:
		next.Announced, next.Unannounced = prev.Announced, prev.Unannounced
	case !next.Witness:
		next.Unannounced = now.Add(lease.FenceAfter + lease.ExtendFor)
	}
	next.Witness = true
	if !o.Probe && !o.Announces {
		next.Unannounced = latest(next.Unannounced, now.Add(lease.FenceAfter+lease.ExtendFor))
	}
	if o.Announce > 0 {
		next.Announced = latest(next.Announced, now.Add(min(time.Duration(o.Announce)*time.Second, lease.AnnouncedAtMost)))
	}
	// The leader's word that every member took a plain renewal since: the extension ended with the restart it covered.
	if o.NoExtension && !o.Probe && o.Announces {
		next.Announced, next.Unannounced = time.Time{}, time.Time{}
	}
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// unsure is how many seconds a lease of the cluster may still live on an extension, as far as this member can tell.
func (c *clusterLease) unsure(now time.Time) *int {
	s := max(int(math.Ceil(latest(c.Announced, c.Unannounced).Sub(now).Seconds())), 0)
	return &s
}

// bootID names this boot of the server; empty when the kernel does not say, which reads as another boot.
var bootID = func() string {
	raw, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(raw))
}

// writeLease writes the lease file; a var so a test can make the disk hang.
var writeLease = writeFileAtomic

// leaseWritesInline has save write under the lock, as before: tests set it, so no write lands after their directory is gone.
var leaseWritesInline bool

// save has the leases written, off the lock; the caller holds g.mu. On eu (23 Nov 2026) a sync took 6 s while a new
// App's data was copied to every member of one host's disk, and every renewal waited on it under g.mu: all fenced.
func (g *leaseGuard) save() {
	if g.file == "" {
		return
	}
	// The stand-in for an unreadable file is never written back: only a leader's word replaces it.
	out := maps.Clone(g.clusters)
	delete(out, unreadableLease)
	raw, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not keep the cluster lease in %s: %v\n", g.file, err)
		return
	}
	g.queue(raw)
}

// queue makes raw the next lease file, nil none; the caller holds g.mu.
func (g *leaseGuard) queue(raw []byte) {
	g.unsaved, g.version = raw, g.version+1
	if leaseWritesInline {
		g.write(raw)
		g.written = g.version
		return
	}
	if !g.writing {
		g.writing = true
		g.work.Add(1)
		go func() {
			defer g.work.Done()
			g.sync()
		}()
	}
}

// sync writes the newest leases save asked for, unless they are on disk already; the caller does not hold g.mu.
func (g *leaseGuard) sync() {
	g.wmu.Lock()
	defer g.wmu.Unlock()
	g.mu.Lock()
	raw, v, done := g.unsaved, g.version, g.written
	g.writing = false
	g.mu.Unlock()
	if v == done || g.file == "" {
		return
	}
	g.write(raw)
	g.mu.Lock()
	g.written = max(g.written, v)
	g.mu.Unlock()
}

// write puts raw in the lease file, or removes it for nil.
func (g *leaseGuard) write(raw []byte) {
	var err error
	if raw == nil {
		if err = os.Remove(g.file); os.IsNotExist(err) {
			err = nil
		}
	} else {
		err = writeLease(g.file, raw, 0o600)
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

// Admit refuses an epoch older than the newest this member has seen.
func (g *leaseGuard) Admit(cluster string, epoch uint64) (held uint64, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.clusters[cluster]; c != nil && epoch < c.Epoch {
		return c.Epoch, false
	}
	return epoch, true
}

// Renew applies one renewal: it takes the epoch and the placement, and starts the lease over. A new epoch, or a
// new leader, is on disk before it answers; a plain renewal is written after.
func (g *leaseGuard) Renew(o lease.Order) lease.Reply {
	r, taken := g.renew(o)
	if taken {
		g.sync()
	}
	return r
}

// taking says whether an order hands this member an epoch, or a leader, it does not hold; the caller holds g.mu.
func taking(c *clusterLease, o lease.Order) bool {
	return c == nil || c.Epoch != o.Epoch || o.From != "" && c.Leader != o.From
}

func (g *leaseGuard) renew(o lease.Order) (lease.Reply, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.clusters[o.Cluster]; g.refuse(c, o) != "" {
		return lease.Reply{Error: g.refuse(c, o), Epoch: c.Epoch}, false
	}
	prev := g.clusters[o.Cluster]
	taken := taking(prev, o)
	// A late renewal is the first sign of a server too busy for its leases: said, so a test can prove there was none.
	// A new leader's first one counts too: a hand-over must not cost a renewal either.
	if gap := g.now().Sub(prev.lastRenewed()); prev != nil && prev.heard && gap > lateRenewal {
		g.logf("deplo-agent: cluster %s: a renewal came %.1fs after the one before%s", o.Cluster, gap.Seconds(),
			map[bool]string{true: ", from a new leader"}[taken])
	}
	from := o.From
	// An order that names nobody keeps the leader its epoch was given to: one epoch stays one leader's.
	if from == "" && prev != nil && prev.Epoch == o.Epoch {
		from = prev.Leader
	}
	g.endOlderStarts(o.Cluster, o.Epoch)
	// The leader's word replaces the stand-in for an unreadable file: what that stopped comes back as held.
	if u := g.clusters[unreadableLease]; u != nil && g.clusters[o.Cluster] == nil {
		g.clusters[o.Cluster] = u
	}
	delete(g.clusters, unreadableLease)
	next := &clusterLease{Epoch: o.Epoch, PlacedAt: o.Epoch, Member: o.Member, RenewedAt: g.now(), Boot: bootID(), Placement: o.Placement, heard: true, ExtraSeconds: min(max(o.ExtendSeconds, 0), int(lease.ExtendFor/time.Second)),
		FenceSeconds: min(max(o.FenceSeconds, 0), int(lease.FenceAfter/time.Second)), Gone: o.Gone, Leader: from, Line: o.Line, Last: o.Last, Heard: heardAt(g.now(), o.Heard)}
	g.confirmStarts(o)
	witness(g.clusters[o.Cluster], next, g.now(), o)
	r := lease.Reply{OK: true, Epoch: o.Epoch, StoppedWithin: int((next.fence() + fenceTakes()).Seconds()), Unsure: next.unsure(g.now())}
	g.comeBack(o, g.clusters[o.Cluster], next, &r)
	g.clusters[o.Cluster] = next
	g.relinquishLocked(o.Cluster, next)
	g.sweepFailedLocked(o.Cluster, next)
	// A start order carries neither keeps nor stores: it says nothing about them.
	if o.Start == nil {
		g.stores[o.Cluster] = o.Stores
	}
	g.save()
	return r, taken
}

// lateRenewal is a gap between renewals, sent every second, worth saying: one more than a second late.
const lateRenewal = 2 * time.Second

func (c *clusterLease) lastRenewed() time.Time {
	if c == nil {
		return time.Time{}
	}
	return c.RenewedAt
}

// heardAt turns an order's "heard so many milliseconds ago" into times on this server's clock.
func heardAt(now time.Time, ago map[string]int64) map[string]time.Time {
	if ago == nil {
		return nil
	}
	out := make(map[string]time.Time, len(ago))
	for id, ms := range ago {
		out[id] = now.Add(-time.Duration(max(ms, 0)) * time.Millisecond)
	}
	return out
}

// relinquishLocked stops every single-writer stack a current lease places on another member and on none of its
// entries here, whatever the leader meant: a holder loses the right to run when its lease moves (deplo#241). A
// return removes its own copies, and a stack a start order is bringing up here waits for the leader. Holds g.mu.
// led says whether a leader that leads renewed this member's lease within d, and which members it no longer counts:
// only one side of a split is led, so that side's name servers may answer with exactly half.
func (g *leaseGuard) led(cluster string, now time.Time, d time.Duration) (gone []string, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if c == nil || c.Fenced || !c.heard || now.Sub(c.RenewedAt) >= d {
		return nil, false
	}
	return slices.Clone(c.Gone), true
}

func (g *leaseGuard) relinquishLocked(cluster string, c *clusterLease) {
	if g.relinquish == nil || c.Returning {
		return
	}
	here, away := map[string]bool{}, map[string]string{}
	for _, e := range c.Placement {
		switch {
		case e.Slug == "" || !e.SingleWriter && !e.Standby:
		case e.Member == c.Member:
			here[e.Slug] = true
		case e.Member != "":
			away[e.Slug] = e.Member
		}
	}
	for slug := range g.relinquished {
		if _, still := away[slug]; !still || here[slug] {
			delete(g.relinquished, slug)
		}
	}
	for slug, member := range away {
		if _, starting := g.starting[slug]; here[slug] || starting || g.relinquishing[slug] || g.has != nil && !g.has(slug) {
			continue
		}
		// Every renewal asks again while the copy is there, stopped or not: said once per member it runs on.
		if g.relinquished[slug] != member {
			g.relinquished[slug] = member
			g.logf("deplo-agent: cluster %s: stopping %s here: the lease places it on %s (lease %s)", cluster, slug, member, c.leaseAge(g.now()))
		}
		g.relinquishing[slug] = true
		g.work.Add(1)
		go func() {
			defer g.work.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*(fenceGrace+10*time.Second))
			defer cancel()
			stopped, err := g.relinquish(ctx, slug)
			g.mu.Lock()
			delete(g.relinquishing, slug)
			g.mu.Unlock()
			switch {
			case err != nil:
				fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: %s runs on %s now, and its copy here could not be stopped: %v\n", cluster, slug, member, err)
			case stopped:
				fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: stopped %s here: the leader placed it on %s\n", cluster, slug, member)
			}
		}()
	}
}

// placedHere is the single-writer stacks this member's lease of a cluster places here, and that lease's epoch: read
// before a probe takes the newer epoch.
func (g *leaseGuard) placedHere(cluster string) ([]string, uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if c == nil {
		return nil, 0
	}
	var out []string
	for _, e := range c.Placement {
		if e.Slug != "" && e.SingleWriter && e.Member == c.Member && !slices.Contains(out, e.Slug) {
			out = append(out, e.Slug)
		}
	}
	if c.PlacedAt == 0 {
		return out, c.Epoch
	}
	return out, c.PlacedAt
}

// runningOf is the stacks among placed that run here now, each with the epoch of the lease that placed them.
func (g *leaseGuard) runningOf(placed []string, epoch uint64) []lease.Writer {
	if g.running == nil || len(placed) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var runs map[string]bool
	if g.runningAll != nil {
		runs = g.runningAll(ctx, placed)
	}
	var out []lease.Writer
	for _, slug := range placed {
		if r, ok := runs[slug]; ok && r || !ok && g.running(ctx, slug) {
			out = append(out, lease.Writer{Slug: slug, Epoch: epoch})
		}
	}
	return out
}

// Probe answers a leader that is not sure it still leads: the epoch is taken and nothing else changes. The
// lease is as old as it was, so a leader that hears too few members to lead never keeps this one alive.
func (g *leaseGuard) Probe(o lease.Order) lease.Reply {
	r, taken := g.probe(o)
	if taken {
		g.sync()
	}
	return r
}

func (g *leaseGuard) probe(o lease.Order) (lease.Reply, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[o.Cluster]
	taken := taking(c, o)
	switch why := g.refuse(c, o); {
	case why != "":
		return lease.Reply{Error: why, Epoch: c.Epoch}, false
	case c == nil:
		c = &clusterLease{Epoch: o.Epoch, Member: o.Member, Leader: o.From}
		g.clusters[o.Cluster] = c
	case o.From != "" || c.Epoch != o.Epoch:
		c.Epoch, c.Leader = o.Epoch, o.From
	}
	witness(nil, c, g.now(), o)
	g.endOlderStarts(o.Cluster, o.Epoch)
	g.save()
	return lease.Reply{OK: true, Epoch: o.Epoch, Unsure: c.unsure(g.now())}, taken
}

// Check stops the single-writer workloads of every lease that has run out, once each. A stop that fails is tried again on the next check.
func (g *leaseGuard) Check(ctx context.Context) {
	g.askToLead(ctx, g.now())
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
		// deplo-agent#59: every stop decision is logged with its reason and the lease it read.
		var mine []lease.Entry
		age := c.leaseAge(now)
		for _, e := range c.Placement {
			if g.fences(c, e) {
				mine = append(mine, e)
				// A stop that failed is tried every check: the decision is said once per lease.
				if !c.toldStop {
					g.logf("deplo-agent: cluster %s: stopping %s here: %s (lease %s)", id, e.Slug, g.stopReason(c, e), age)
				}
			}
		}
		c.toldStop = true
		todo = append(todo, due{id, mine})
	}
	g.mu.Unlock()
	sort.Slice(todo, func(i, j int) bool { return todo[i].cluster < todo[j].cluster })
	for _, d := range todo {
		held := g.runningNow(ctx, d.entries)
		err := g.fence(ctx, d.entries)
		g.mu.Lock()
		if c := g.clusters[d.cluster]; err != nil && c != nil && c.stopErr != err.Error() {
			c.stopErr = err.Error()
			fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: lease ran out, could not stop its workloads: %v\n", d.cluster, err)
		}
		// What stopped is held either way, also when a stack still runs; a renewal that landed while stopping wins
		// and starts it again at once. A stop that failed is not fenced, so the next check tries it again.
		if c := g.clusters[d.cluster]; c != nil {
			c.Held = union(c.Held, held)
			switch {
			case !c.ranOut(g.now()):
				// settle says what it starts again: a stack the renewal placed elsewhere does not start (c42).
				g.settleLocked(d.cluster, c)
			case err == nil:
				c.Fenced = true
			}
			g.save()
		}
		g.mu.Unlock()
	}
	// What a fence stopped is held across a restart: started again once the leader says so.
	if len(todo) > 0 {
		g.sync()
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
	defer g.sync()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.clusters = map[string]*clusterLease{}
	g.keeps = map[string]map[string]lease.Keep{}
	g.stores = map[string][]lease.Store{}
	if g.file != "" {
		g.queue(nil)
	}
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
