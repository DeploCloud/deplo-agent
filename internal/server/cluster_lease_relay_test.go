package server

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{relay.URL}, ClusterId: "clu_1", LeaderFingerprints: []string{leaderID.Fingerprint}}); err != nil {
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

// leaderParty dials the relay as a leader holding id, and sends one order to the member.
func leaderParty(t *testing.T, id *wire.Identity, relayURL string, to wire.Party) func(o lease.Order) (lease.Reply, bool) {
	t.Helper()
	p := clusterlink.New(clusterlink.Config{
		Role:     wire.RoleLeader,
		Identity: func() (*wire.Identity, error) { return id, nil },
		Relays:   func() []string { return []string{relayURL} },
		Accept:   func(_ wire.Party, c net.Conn) { c.Close() },
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	eventually(t, "the leader to reach the relay", func() bool { return p.Connected() == 1 })
	return func(o lease.Order) (lease.Reply, bool) {
		var c net.Conn
		eventually(t, "the leader to reach the member", func() bool {
			var err error
			c, err = p.Dial(ctx, to)
			return err == nil
		})
		defer c.Close()
		c.SetDeadline(time.Now().Add(3 * time.Second))
		raw, _ := json.Marshal(o)
		c.Write(append(raw, '\n'))
		var reply lease.Reply
		line, err := bufio.NewReader(c).ReadBytes('\n')
		return reply, err == nil && json.Unmarshal(line, &reply) == nil
	}
}

func TestLease_anOrderIsTakenOnlyFromTheLeaderDeploNamed(t *testing.T) {
	stubBridge(t, "")
	s, id, ca := memberService(t)
	named, other := ca.Leaf(t, "leader"), ca.Leaf(t, "another server of this instance")
	me := wire.Party{Fingerprint: id.Fingerprint, Role: wire.RoleMember}
	// The relay lets both through as leaders: its word is not what the member goes by.
	relay := testrelay.New(t, ca.Leaf(t, "relay"), me,
		wire.Party{Fingerprint: named.Fingerprint, Role: wire.RoleLeader}, wire.Party{Fingerprint: other.Fingerprint, Role: wire.RoleLeader})
	if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{
		Member: true, Relays: []string{relay.URL}, ClusterId: "clu_1", LeaderFingerprints: []string{named.Fingerprint},
	}); err != nil {
		t.Fatal(err)
	}
	if r, answered := leaderParty(t, other, relay.URL, me)(orderFor(9)); answered && r.OK {
		t.Fatalf("a certificate Deplo never named as leader was obeyed: %+v", r)
	}
	if held, _ := s.leaseGuard().Admit("clu_1", 0); held != 0 {
		t.Fatalf("the stranger's epoch %d was kept", held)
	}
	send := leaderParty(t, named, relay.URL, me)
	foreign := orderFor(4)
	foreign.Cluster = "clu_2"
	if r, _ := send(foreign); r.OK {
		t.Fatalf("an order for a cluster this server is not in was obeyed: %+v", r)
	}
	if r, _ := send(orderFor(4)); !r.OK {
		t.Fatalf("the named leader was refused: %+v", r)
	}
}

func TestSetClusterMember_refusesALeaderThatIsNotAFingerprint(t *testing.T) {
	s, _, _ := memberService(t)
	_, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, ClusterId: "clu_1", LeaderFingerprints: []string{"nope"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}

// Deplo pushes the roster on every join, certificate renewal and sweep. A push that changes no relay leaves the
// member's connections alone: a stream that was open goes on.
func TestSetClusterMember_aPushThatChangesNoRelayCutsNoStream(t *testing.T) {
	stubBridge(t, "")
	s, id, ca := memberService(t)
	leaderID, next := ca.Leaf(t, "leader"), ca.Leaf(t, "the leader after a move")
	me := wire.Party{Fingerprint: id.Fingerprint, Role: wire.RoleMember}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), me, wire.Party{Fingerprint: leaderID.Fingerprint, Role: wire.RoleLeader})
	join := func(leaders ...string) {
		t.Helper()
		if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{relay.URL}, ClusterId: "clu_1", LeaderFingerprints: leaders}); err != nil {
			t.Fatal(err)
		}
	}
	join(leaderID.Fingerprint)
	p := clusterlink.New(clusterlink.Config{
		Role:     wire.RoleLeader,
		Identity: func() (*wire.Identity, error) { return leaderID, nil },
		Relays:   func() []string { return []string{relay.URL} },
		Accept:   func(_ wire.Party, c net.Conn) { c.Close() },
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	var c net.Conn
	eventually(t, "the leader to reach the member", func() bool {
		var err error
		c, err = p.Dial(ctx, me)
		return err == nil
	})
	defer c.Close()

	join(leaderID.Fingerprint)
	join(leaderID.Fingerprint, next.Fingerprint)

	c.SetDeadline(time.Now().Add(3 * time.Second))
	raw, _ := json.Marshal(orderFor(2))
	c.Write(append(raw, '\n'))
	var reply lease.Reply
	if line, err := bufio.NewReader(c).ReadBytes('\n'); err != nil || json.Unmarshal(line, &reply) != nil || !reply.OK {
		t.Fatalf("the stream opened before the pushes was cut: %v %+v", err, reply)
	}
	// The new leader's certificate counts all the same.
	if _, leaders := s.leaderPins(); len(leaders) != 2 {
		t.Fatalf("the member holds %d leaders after the push", len(leaders))
	}
}
