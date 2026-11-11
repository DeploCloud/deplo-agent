package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

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

// comeBack carries a return across a renewal and starts settling it; the caller holds g.mu.
func (g *leaseGuard) comeBack(o lease.Order, prev, next *clusterLease, r *lease.Reply) {
	if prev != nil {
		next.Held, next.Returning, next.Returned = prev.Held, prev.Returning, prev.Returned
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
	var removed, done []string
	if returning {
		for slug, e := range moved {
			if !slices.Contains(held, slug) && g.has != nil && !g.has(slug) {
				continue
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
		c.Returning = failed
	}
	g.save()
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
	// ponytail: the volume stays behind. Ticket #206 backs it up and removes it here, before the stack goes.
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
