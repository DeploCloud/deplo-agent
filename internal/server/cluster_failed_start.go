package server

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// failedStart is what a failover start that failed here left: the stack and the volumes its data was put into, kept
// until the App runs whole on another member (eu, 24 Nov 2026: a promoted 2 GB volume stayed after its App moved on).
type failedStart struct {
	cluster string
	// before is the member the App was placed on when the start was ordered: the one it left, not where it runs.
	before string
	vols   []string
}

// placedMember is the member a placement gives a stack's data to, and whether this member keeps a standby of it.
func placedMember(placement []lease.Entry, slug, me string) (member string, standbyHere bool) {
	for _, e := range placement {
		switch {
		case e.Slug != slug:
		case e.Standby:
			standbyHere = standbyHere || e.Member == me
		case e.SingleWriter:
			member = e.Member
		}
	}
	return member, standbyHere
}

// placedBefore is the member this member's lease places a stack's data on now.
func (g *leaseGuard) placedBefore(cluster, slug string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.clusters[cluster]; c != nil {
		m, _ := placedMember(c.Placement, slug, c.Member)
		return m
	}
	return ""
}

// noteFailedStart remembers what a failed start of a stack with data left here; the caller holds g.mu.
func (g *leaseGuard) noteFailedStart(o lease.Order, before string) {
	restore, _ := failoverRestore(o.Start)
	if g.failed == nil {
		g.failed = map[string]failedStart{}
	}
	g.failed[o.Start.Slug] = failedStart{cluster: o.Cluster, before: before, vols: restore.GetProject().GetVolumeNames()}
}

// sweepFailedLocked removes what a failed start left once a renewal places the App on a member it moved to, which
// a leader does only once it runs there; the caller holds g.mu.
func (g *leaseGuard) sweepFailedLocked(cluster string, c *clusterLease) {
	for slug, f := range g.failed {
		if f.cluster != cluster {
			continue
		}
		m, standby := placedMember(c.Placement, slug, c.Member)
		switch {
		case m == c.Member || standby:
			delete(g.failed, slug)
		case m == "" || m == f.before || g.dropFailed == nil:
		default:
			delete(g.failed, slug)
			g.work.Add(1)
			go func() {
				defer g.work.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				if err := g.dropFailed(ctx, slug, f.vols, func() bool { return g.movedAway(cluster, slug) }); err != nil {
					g.logf("deplo-agent: cluster %s: %s runs on %s now, and what its failed start left here stays: %v", cluster, slug, m, err)
					return
				}
				g.logf("deplo-agent: cluster %s: removed what a failed start of %s left here: it runs on %s now", cluster, slug, m)
			}()
		}
	}
}

// dropFailedStart takes down what a failed failover start left of a stack, with the volumes it filled.
func (s *Service) dropFailedStart(ctx context.Context, slug string, vols []string, still func() bool) error {
	defer s.lockStack(slug)()
	if !still() {
		return errRunsAgain
	}
	res, err := s.destroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true, ReclaimVolumes: vols})
	if err = stackErr(res, err); err == nil && res.GetError() != "" {
		fmt.Fprintf(os.Stderr, "deplo-agent: %s: %s\n", slug, res.GetError())
	}
	return err
}
