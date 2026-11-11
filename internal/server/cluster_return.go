package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A member that was presumed dead runs no second copy (spec #176): it starts nothing of the cluster's until a
// leader's order arrives, then removes its copy of what was placed elsewhere and starts what is still its own.

// returnTimeout bounds one pass over what a return has to stop, remove and start.
const returnTimeout = 10 * time.Minute

func union(a, b []string) []string {
	for _, s := range b {
		if !slices.Contains(a, s) {
			a = append(a, s)
		}
	}
	return a
}

// runningNow is the stacks among entries that run before they are stopped: only those are started again.
func (g *leaseGuard) runningNow(ctx context.Context, entries []lease.Entry) []string {
	var out []string
	for _, e := range entries {
		if g.running == nil || g.running(ctx, e.Slug) {
			out = append(out, e.Slug)
		}
	}
	return out
}

// unplaced is the stacks an order places on no member that this member still has: running, or stopped by
// itself when its lease ran out. A leader that took over hears from it who has what its picture calls waiting.
func (g *leaseGuard) unplaced(o lease.Order) []string {
	g.mu.Lock()
	var held []string
	if c := g.clusters[o.Cluster]; c != nil {
		held = slices.Clone(c.Held)
	}
	g.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out []string
	for _, e := range o.Placement {
		if e.Slug == "" || slices.Contains(out, e.Slug) || !g.hasUnplaced(o.Placement, e) {
			continue
		}
		if slices.Contains(held, e.Slug) || (g.running != nil && g.running(ctx, e.Slug)) {
			out = append(out, e.Slug)
		}
	}
	return out
}

// comeBack carries a return across a renewal and starts settling it; the caller holds g.mu.
func (g *leaseGuard) comeBack(o lease.Order, prev, next *clusterLease, r *lease.Reply) {
	if prev != nil {
		next.Held, next.Returning, next.Returned = prev.Held, prev.Returning, prev.Returned
		next.since, next.asked = prev.since, prev.asked
		if prev.ranOut(g.now()) && !next.Returning {
			next.Returning, next.Returned = true, nil
		}
	}
	switch {
	case o.Return && !next.Returning && next.Returned != nil:
		r.Returned = next.Returned
	case o.Return:
		// The leader moved this member's work although its lease never ran out here.
		next.Returning = true
	case !next.Returning:
		next.Returned = nil
	}
	g.noteKeeps(o, next)
	if (next.Returning || len(next.Held) > 0) && g.remove != nil && !g.settling[o.Cluster] {
		g.settling[o.Cluster] = true
		g.work.Add(1)
		go g.settle(o.Cluster)
	}
}

// settle removes this member's copy of every stack placed elsewhere and starts what it stopped and still owns.
// A step that fails leaves the return open, and the next renewal tries again.
func (g *leaseGuard) settle(cluster string) {
	defer g.work.Done()
	ctx, cancel := context.WithTimeout(context.Background(), returnTimeout)
	defer cancel()
	g.mu.Lock()
	c := g.clusters[cluster]
	if c == nil {
		delete(g.settling, cluster)
		g.mu.Unlock()
		return
	}
	me, returning, held := c.Member, c.Returning, slices.Clone(c.Held)
	placement := slices.Clone(c.Placement)
	keeps, before, waits := maps.Clone(g.keeps[cluster]), c.savedBefore(), !c.asked && g.now().Sub(c.since) < keepGrace
	g.mu.Unlock()

	mine, moved, named := map[string]bool{}, map[string]lease.Entry{}, map[string]bool{}
	for _, e := range placement {
		named[e.Slug] = true
		if e.Member == me {
			mine[e.Slug] = true
		}
	}
	for _, e := range placement {
		if e.Slug != "" && e.Member != "" && e.Member != me && !mine[e.Slug] {
			moved[e.Slug] = e
		}
	}
	failed := false
	var removed, trimmed, done []string
	var saved []lease.Saved
	var kept []lease.Kept
	if returning && g.trim != nil {
		here, all := map[string]int{}, map[string]int{}
		for _, e := range placement {
			all[e.Slug]++
			if e.Member == me {
				here[e.Slug]++
			}
		}
		// Some of its replicas were started elsewhere: the copies beyond what is still placed here go.
		for slug, n := range here {
			if slug == "" || n == all[slug] {
				continue
			}
			cut, err := g.trim(ctx, slug, n)
			if err != nil {
				fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: could not stop the surplus copies of %s: %v\n", cluster, slug, err)
				failed = true
			} else if cut {
				trimmed = append(trimmed, slug)
			}
		}
	}
	if returning {
		for slug, e := range moved {
			if !slices.Contains(held, slug) && g.has != nil && !g.has(slug) {
				continue
			}
			if g.isStarting(slug) {
				failed = true
				continue
			}
			if e.SingleWriter && g.keep != nil {
				k, told := keeps[slug]
				if !told && waits {
					// The leader has not said yet where this data goes: the copy stops and waits for its next order.
					if err := g.stop(ctx, slug); err != nil {
						fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: could not stop %s, which runs on another server now: %v\n", cluster, slug, err)
					}
					failed = true
					continue
				}
				sv, why, gone, retry := g.keepMoved(cluster, me, e, k, before[slug])
				if sv != nil {
					saved = append(saved, *sv)
				}
				if why != nil {
					kept = append(kept, *why)
				}
				if retry {
					failed = true
					continue
				}
				if gone {
					removed = append(removed, slug)
					continue
				}
			}
			if err := g.remove(ctx, e); err != nil {
				fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: could not remove %s, which runs on another server now: %v\n", cluster, slug, err)
				failed = true
				continue
			}
			removed = append(removed, slug)
		}
	}
	for _, slug := range held {
		switch {
		// A stack the cluster no longer names is not the leader's to start; one waiting for room stays held.
		case slices.Contains(removed, slug), !named[slug]:
			done = append(done, slug)
		case !mine[slug]:
		default:
			if err := g.resume(ctx, slug); err != nil {
				fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: could not start %s again: %v\n", cluster, slug, err)
				failed = true
				continue
			}
			done = append(done, slug)
		}
	}
	removed = union(removed, trimmed)
	slices.Sort(removed)

	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.settling, cluster)
	if c = g.clusters[cluster]; c == nil || (!returning && len(done) == 0) {
		return
	}
	c.Held = slices.DeleteFunc(c.Held, func(s string) bool { return slices.Contains(done, s) })
	if returning {
		if c.Returned == nil {
			c.Returned = &lease.Returned{Removed: []string{}}
		}
		c.Returned.Removed = union(c.Returned.Removed, removed)
		slices.Sort(c.Returned.Removed)
		noteReturned(c.Returned, saved, kept)
		if c.Returning = failed; !failed {
			delete(g.keeps, cluster)
			c.since, c.asked = time.Time{}, false
		}
	}
	g.save()
}

// startGrace is how long a stack started here is kept while no renewal has placed it here yet.
const startGrace = time.Minute

// markStart notes a start order running on this member, its end, or (failed) that nothing was left to guard.
func (g *leaseGuard) markStart(slug string, failed bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, running := g.starting[slug]
	switch {
	case failed:
		delete(g.starting, slug)
	case running:
		g.starting[slug] = g.now()
	default:
		g.starting[slug] = time.Time{}
	}
}

// confirmStarts ends the guard of a started stack the leader now places here; the caller holds g.mu.
func (g *leaseGuard) confirmStarts(o lease.Order) {
	for slug, at := range g.starting {
		if at.IsZero() {
			continue
		}
		placed := slices.ContainsFunc(o.Placement, func(e lease.Entry) bool { return e.Slug == slug && e.Member == o.Member })
		if placed || g.now().Sub(at) >= startGrace {
			delete(g.starting, slug)
		}
	}
}

func (g *leaseGuard) isStarting(slug string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.starting[slug]
	return ok
}

// Holds says whether a stack is the cluster's while this member has no live lease: nothing of it starts then.
func (g *leaseGuard) Holds(slug string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	for _, c := range g.clusters {
		if !c.ranOut(now) {
			continue
		}
		for _, e := range c.Placement {
			if e.Slug == slug {
				return true
			}
		}
	}
	return false
}

// awaitingLeader is what a deploy or a start of a cluster's stack gets on a member with no live lease.
const awaitingLeader = "this server has not heard from its cluster's leader, so it starts nothing of the cluster's yet"

func stackRuns(ctx context.Context, slug string) bool {
	return len(projectContainers(ctx, "deplo-"+slug)) > 0 || dockercli.IsRunning(ctx, "deplo-"+slug)
}

// removeMoved stops and removes this member's copy of a stack that now runs elsewhere, its connector with it.
func (s *Service) removeMoved(ctx context.Context, e lease.Entry) error {
	res, err := s.DestroyStack(ctx, &pb.StackRef{Slug: e.Slug})
	if err == nil && !res.GetOk() {
		err = errors.New(res.GetError())
	}
	return err
}

func (s *Service) resumeHeld(ctx context.Context, slug string) error {
	res, err := s.StartStack(ctx, &pb.StackRef{Slug: slug})
	if err == nil && !res.GetOk() {
		err = errors.New(res.GetError())
	}
	return err
}

// trimReplicas removes the newest copies of every service that declares replicas until n are left of each.
func (s *Service) trimReplicas(ctx context.Context, slug string, n int) (bool, error) {
	raw, err := os.ReadFile(s.stackPath(slug))
	if err != nil {
		return false, nil
	}
	var doc struct {
		Services map[string]struct {
			Deploy struct {
				Replicas *int `yaml:"replicas"`
			} `yaml:"deploy"`
		} `yaml:"services"`
	}
	if yaml.Unmarshal(raw, &doc) != nil {
		return false, nil
	}
	defer s.lockStack(slug)()
	res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-a", "--filter", "label=com.docker.compose.project=deplo-"+slug,
		"--format", `{{.ID}} {{.Label "com.docker.compose.service"}} {{.Label "com.docker.compose.container-number"}}`)
	if err == nil && res.Code != 0 {
		err = errors.New(strings.TrimSpace(res.Stderr))
	}
	if err != nil {
		return false, err
	}
	type copyOf struct {
		id     string
		number int
	}
	by := map[string][]copyOf{}
	for _, line := range splitLines(res.Stdout) {
		f := strings.Fields(line)
		if len(f) != 3 || doc.Services[f[1]].Deploy.Replicas == nil {
			continue
		}
		num, _ := strconv.Atoi(f[2])
		by[f[1]] = append(by[f[1]], copyOf{f[0], num})
	}
	var cut []string
	for _, copies := range by {
		slices.SortFunc(copies, func(a, b copyOf) int { return b.number - a.number })
		for i := 0; i < len(copies)-n; i++ {
			cut = append(cut, copies[i].id)
		}
	}
	if len(cut) == 0 {
		return false, nil
	}
	res, err = dockercli.Run(ctx, 2*time.Minute, append([]string{"rm", "-f"}, cut...)...)
	if err == nil && res.Code != 0 {
		err = errors.New(strings.TrimSpace(res.Stderr))
	}
	return err == nil, err
}
