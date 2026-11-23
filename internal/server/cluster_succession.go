package server

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// A line of succession: a member gives an epoch to one leader, refuses an heir while its leader still renews it, and
// asks the leader beside it to lead when it is next in line (ADR-0043).

// succeedAsk is what a member next in line hands the leader beside it: the epoch to take and the placement it holds.
type succeedAsk struct {
	Cluster   string        `json:"cluster"`
	Epoch     uint64        `json:"epoch"`
	Placement []lease.Entry `json:"placement"`
	// Gone and Heard are what the last renewal said, Heard aged to now: the next leader goes on counting from there.
	Gone  []string         `json:"gone,omitempty"`
	Heard map[string]int64 `json:"heard,omitempty"`
}

// refuse says why this member does not take the order's epoch, "" when it does: an older epoch, this epoch from
// another leader, or a Succeed epoch while its own leader renewed it within PreVoteFor. The caller holds g.mu.
func (g *leaseGuard) refuse(c *clusterLease, o lease.Order) string {
	switch {
	case c == nil:
		return ""
	// An epoch held by a named leader goes to no other, nor to one that names nobody; an unnamed holder (an older
	// leader) is taken to be the one that names itself now.
	case o.Epoch < c.Epoch, o.Epoch == c.Epoch && c.Leader != "" && o.From != c.Leader:
		return lease.StaleEpoch
	case o.Succeed && o.Epoch > c.Epoch && !c.RenewedAt.IsZero() && g.now().Sub(c.RenewedAt) < lease.PreVoteFor:
		return lease.LeaderAlive
	}
	return ""
}

// PreVote answers whether this member would take the order's epoch, and takes nothing: a candidate whose ask a
// quorum would refuse never makes even its own member refuse the leader it has.
func (g *leaseGuard) PreVote(o lease.Order) lease.Reply {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[o.Cluster]
	if why := g.refuse(c, o); why != "" {
		return lease.Reply{Error: why, Epoch: c.Epoch}
	}
	return lease.Reply{OK: true, Epoch: o.Epoch}
}

// askToLead asks the leader beside this member to lead each cluster whose leader went silent while this member is in
// its line: the heir after PreVoteFor, each next one SucceedStagger later. An extended lease waits until it runs out.
func (g *leaseGuard) askToLead(ctx context.Context, now time.Time) {
	if g.succeed == nil {
		return
	}
	var asks []succeedAsk
	g.mu.Lock()
	for id, c := range g.clusters {
		k := slices.Index(c.Line, c.Member)
		wait := lease.PreVoteFor + time.Duration(max(k, 0))*lease.SucceedStagger
		// The leader beside this server is the one gone silent: asking it again only restarts it at a newer epoch.
		self := c.Leader != "" && c.Leader == c.Member
		if k < 0 || self || c.RenewedAt.IsZero() || g.asking[id] || now.Sub(c.RenewedAt) < wait || c.ExtraSeconds > 0 && !c.ranOut(now) {
			if g.asked[id] != "" && (k < 0 || now.Sub(c.RenewedAt) < wait) {
				delete(g.asked, id)
			}
			continue
		}
		g.asking[id] = true
		asks = append(asks, succeedAsk{Cluster: id, Epoch: c.Epoch + uint64(k) + 1, Placement: slices.Clone(c.Placement), Gone: slices.Clone(c.Gone), Heard: heardAgo(now, c.Heard)})
	}
	g.mu.Unlock()
	for _, a := range asks {
		g.work.Add(1)
		go func() {
			defer g.work.Done()
			err := g.succeed(ctx, a)
			said := "asked the leader on this server to lead it at epoch " + strconv.FormatUint(a.Epoch, 10)
			if err != nil {
				said = "could not ask the leader on this server to lead it: " + err.Error()
			}
			g.mu.Lock()
			delete(g.asking, a.Cluster)
			again := g.asked[a.Cluster] == said
			g.asked[a.Cluster] = said
			g.mu.Unlock()
			if !again {
				g.logf("deplo-agent: cluster %s: its leader is silent and this server is next in line: %s", a.Cluster, said)
			}
		}()
	}
}

// heardAgo is how many milliseconds before now each member was last heard.
func heardAgo(now time.Time, at map[string]time.Time) map[string]int64 {
	if at == nil {
		return nil
	}
	out := make(map[string]int64, len(at))
	for id, t := range at {
		out[id] = max(now.Sub(t).Milliseconds(), 0)
	}
	return out
}

// askLeaderToLead hands the leader beside this agent what it needs to lead a cluster without Deplo (leader.succeed).
func (s *Service) askLeaderToLead(ctx context.Context, a succeedAsk) error {
	payload, err := json.Marshal(a)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = s.ClusterCall(ctx, &pb.ClusterCallRequest{Method: "leader.succeed", Payload: payload})
	return err
}
