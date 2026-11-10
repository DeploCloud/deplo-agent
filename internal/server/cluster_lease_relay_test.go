package server

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testrelay"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

func TestLease_aLeaderRenewsThroughTheRelayAndTheMemberKeepsTheEpoch(t *testing.T) {
	stubBridge(t, "")
	s, id, ca := memberService(t)
	leaderID := ca.Leaf(t, "leader")
	me := wire.Party{Fingerprint: id.Fingerprint, Role: wire.RoleMember}
	lead := wire.Party{Fingerprint: leaderID.Fingerprint, Role: wire.RoleLeader}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), me, lead)
	if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{relay.URL}}); err != nil {
		t.Fatal(err)
	}
	p := clusterlink.New(clusterlink.Config{
		Role:     wire.RoleLeader,
		Identity: func() (*wire.Identity, error) { return leaderID, nil },
		Relays:   func() []string { return []string{relay.URL} },
		Accept:   func(_ wire.Party, c net.Conn) { c.Close() },
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	send := func(epoch uint64) lease.Reply {
		var reply lease.Reply
		eventually(t, "the leader to reach the member", func() bool {
			c, err := p.Dial(ctx, me)
			if err != nil {
				return false
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(3 * time.Second))
			raw, _ := json.Marshal(orderFor(epoch))
			c.Write(append(raw, '\n'))
			line, err := bufio.NewReader(c).ReadBytes('\n')
			return err == nil && json.Unmarshal(line, &reply) == nil
		})
		return reply
	}
	if r := send(4); !r.OK {
		t.Fatalf("renewal: %+v", r)
	}
	if r := send(3); r.OK || r.Error != lease.StaleEpoch || r.Epoch != 4 {
		t.Fatalf("an older leader got %+v", r)
	}
	if held, ok := s.leaseGuard().Admit("clu_1", 3); ok || held != 4 {
		t.Fatalf("the member holds %d, admits 3: %v", held, ok)
	}
}
