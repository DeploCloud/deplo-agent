package server

import "slices"

// holders is every member the leader placed a workload on, from the last renewal: whether this one is among them,
// and the others in placement order. A standby copy holds the stack, not the name, and is never one. This member
// holds nothing on a lease that is not current: a copy back from a power cut answers for nobody.
func (g *leaseGuard) holders(cluster, kind, id string) (here bool, others []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if c == nil {
		return false, nil
	}
	current := c.current(g.now())
	for _, e := range c.Placement {
		if e.Kind != kind || e.ID != id || e.Member == "" || e.Standby {
			continue
		}
		if e.Member == c.Member {
			here = here || current
		} else if !slices.Contains(others, e.Member) {
			others = append(others, e.Member)
		}
	}
	return here, others
}

// answerHolders is holders for a passed-on domain: a server coming back answers none itself until what moved away
// is gone, or its proxy would pass the check of a renewal its stale store asked for, issuing a certificate twice.
func (g *leaseGuard) answerHolders(cluster, id string) (here bool, others []string) {
	here, others = g.holders(cluster, "app", id)
	g.mu.Lock()
	defer g.mu.Unlock()
	if c := g.clusters[cluster]; c != nil && c.Returning {
		here = false
	}
	return here, others
}

// visitorHolders is answerHolders for where a visitor goes: a lease read back after the agent restarted in this boot
// answers at once; it decides nothing else (no start, no certificate).
func (g *leaseGuard) visitorHolders(cluster, id string) (here bool, others []string) {
	here, others = g.answerHolders(cluster, id)
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if c != nil && !here && !c.Returning && c.answerable(g.now()) {
		for _, e := range c.Placement {
			here = here || (e.Kind == "app" && e.ID == id && e.Member == c.Member && !e.Standby)
		}
	}
	return here, others
}

// appOf is the App a stack belongs to in any cluster's placement; empty for a stack no cluster places.
func (g *leaseGuard) appOf(slug string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, c := range g.clusters {
		for _, e := range c.Placement {
			if e.Kind == "app" && e.Slug == slug && e.Slug != "" {
				return e.ID
			}
		}
	}
	return ""
}

// locate says which member runs a workload now, and whether that is this one, from the placement the last renewal carried.
// With replicas on several members this one is preferred, then the first in placement order.
func (g *leaseGuard) locate(cluster, kind, id string) (member string, here, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if c == nil {
		return "", false, false
	}
	current := c.current(g.now())
	for _, e := range c.Placement {
		// A standby copy holds the stack, not the name: connections still go to the workload itself.
		if e.Kind != kind || e.ID != id || e.Member == "" || e.Standby {
			continue
		}
		if e.Member == c.Member {
			// Not on a lease that is not current: the copy here may be one that moved away.
			if current {
				return e.Member, true, true
			}
			continue
		}
		if !ok {
			member, ok = e.Member, true
		}
	}
	return member, false, ok
}
