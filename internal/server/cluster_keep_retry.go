package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// Data a return could not save stays on this member and is tried again, further apart each time, for as long
// as the leader hands its destination: saved, it goes with its volume and the leader is told (spec #176).
var (
	keepRetryMin = time.Minute
	keepRetryMax = time.Hour
)

func keeps(list []lease.Kept, slug string) bool {
	return slices.ContainsFunc(list, func(k lease.Kept) bool { return k.Slug == slug })
}

// keepOn remembers what a finished return left on this member; the caller holds g.mu.
func (g *leaseGuard) keepOn(cluster string, c *clusterLease) {
	if c.Returned == nil || len(c.Returned.Kept) == 0 {
		return
	}
	for _, k := range c.Returned.Kept {
		c.Keeping = append(slices.DeleteFunc(slices.Clone(c.Keeping), func(o lease.Kept) bool { return o.Slug == k.Slug }), k)
	}
	g.retryWait[cluster], g.retryAt[cluster] = keepRetryMin, g.now().Add(keepRetryMin)
}

// later answers the leader about what stayed here past a return, and starts another try when one is due; the
// caller holds g.mu.
func (g *leaseGuard) later(o lease.Order, next *clusterLease, r *lease.Reply) {
	if o.Return || next.Returning || o.Start != nil {
		return
	}
	named := map[string]bool{}
	var gone []string
	var saved []lease.Saved
	for _, k := range o.Keep {
		named[k.Slug] = true
		if keeps(next.Keeping, k.Slug) {
			continue
		}
		gone = append(gone, k.Slug)
		for _, sv := range next.Late {
			if sv.Slug == k.Slug {
				saved = append(saved, sv)
			}
		}
	}
	if len(gone) > 0 {
		r.Later = &lease.Later{Saved: saved, Gone: gone}
	}
	// What the leader no longer names it has heard of.
	next.Late = slices.DeleteFunc(slices.Clone(next.Late), func(sv lease.Saved) bool { return !named[sv.Slug] && !keeps(next.Keeping, sv.Slug) })
	if len(next.Keeping) > 0 && g.keep != nil && !g.settling[o.Cluster] && !g.now().Before(g.retryAt[o.Cluster]) {
		g.settling[o.Cluster] = true
		g.work.Add(1)
		go g.retryKept(o.Cluster)
	}
}

func (g *leaseGuard) noteLate(cluster string, sv lease.Saved) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.clusters[cluster]; c != nil {
		c.Late = append(slices.DeleteFunc(slices.Clone(c.Late), func(o lease.Saved) bool { return o.Slug == sv.Slug }), sv)
		g.save()
	}
}

// retryKept goes once over what stayed on this member. A stack placed here again, or no longer the cluster's,
// is not kept any more; one that runs on no server, or has nowhere to go, waits.
func (g *leaseGuard) retryKept(cluster string) {
	defer g.work.Done()
	g.mu.Lock()
	c := g.clusters[cluster]
	if c == nil {
		delete(g.settling, cluster)
		g.mu.Unlock()
		return
	}
	me, keeping, placement := c.Member, slices.Clone(c.Keeping), slices.Clone(c.Placement)
	told := maps.Clone(g.keeps[cluster])
	late := map[string]lease.Saved{}
	for _, sv := range c.Late {
		late[sv.Slug] = sv
	}
	g.mu.Unlock()

	var stay []lease.Kept
	tried := false
	for _, was := range keeping {
		var e lease.Entry
		named, here := false, false
		for _, p := range placement {
			if p.Slug != was.Slug {
				continue
			}
			named, here = true, here || p.Member == me
			if e.Member == "" {
				e = p
			}
		}
		k := told[was.Slug]
		switch {
		case !named || here:
			continue
		case e.Member == "" || len(k.Request) == 0 || storeOutOfReach(k, me) || g.isStarting(was.Slug):
			stay = append(stay, was)
			continue
		}
		tried = true
		if !g.saveKept(cluster, me, e, k, late, &was) {
			stay = append(stay, was)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.settling, cluster)
	if c = g.clusters[cluster]; c == nil {
		return
	}
	c.Keeping = stay
	wait := keepRetryMin
	if tried && len(stay) > 0 {
		wait = min(max(2*g.retryWait[cluster], keepRetryMin), keepRetryMax)
	}
	g.retryWait[cluster], g.retryAt[cluster] = wait, g.now().Add(wait)
	g.save()
}

// saveKept archives one kept stack, unless an earlier try already did, then removes it with its volumes. It
// says whether nothing of it is kept any more; a failure is written on `was`.
func (g *leaseGuard) saveKept(cluster, me string, e lease.Entry, k lease.Keep, late map[string]lease.Saved, was *lease.Kept) bool {
	ctx, cancel := context.WithTimeout(context.Background(), leftBehindTimeout)
	defer cancel()
	still := func() bool { return g.movedAway(cluster, e.Slug) }
	if _, written := late[e.Slug]; !written {
		sv, err := g.keep(ctx, e, k, me, still)
		switch {
		case errors.Is(err, errNothingHeld):
			return true
		case errors.Is(err, errRunsAgain):
			return false
		case err != nil:
			fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: what %s still held here could not be saved, it is tried again later: %v\n", cluster, e.Slug, err)
			was.Ref, was.Reason, was.Error = k.Ref, lease.KeptBackupFailed, err.Error()
			return false
		}
		// Written down before the volume goes, like a return's own archive.
		g.noteLate(cluster, *sv)
	}
	if err := g.purge(ctx, e, k, still); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: cluster %s: %s is saved, but its copy could not be removed: %v\n", cluster, e.Slug, err)
		return false
	}
	return true
}
