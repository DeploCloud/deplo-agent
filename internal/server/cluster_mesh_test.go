package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/mesh"
)

func tunnelAddr(i int) string { return fmt.Sprintf("fdde:9100::%x", i) }

func putOnNetwork(t *testing.T, s *Service, self string, peers ...*pb.ClusterNetworkPeer) *pb.SetClusterNetworkResponse {
	t.Helper()
	resp, err := s.SetClusterNetwork(context.Background(), &pb.SetClusterNetworkRequest{ClusterId: "cl1", Address: self, Peers: peers})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func meshPeer(fp, key, addr string, port uint32, direct bool) *pb.ClusterNetworkPeer {
	p := &pb.ClusterNetworkPeer{Fingerprint: fp, PublicKey: key, Address: addr}
	if direct && port != 0 {
		p.Endpoints = []string{fmt.Sprintf("127.0.0.1:%d", port)}
	}
	return p
}

// joinNetwork puts two servers on one network the way Deplo does: each answers with its key, then gets the other's.
func joinNetwork(t *testing.T, a, b *Service, fpA, fpB string, direct bool) {
	t.Helper()
	for _, s := range []*Service{a, b} {
		t.Cleanup(func() {
			_, _ = s.SetClusterNetwork(context.Background(), &pb.SetClusterNetworkRequest{ClusterId: "cl1"})
		})
	}
	ra := putOnNetwork(t, a, tunnelAddr(1))
	rb := putOnNetwork(t, b, tunnelAddr(2), meshPeer(fpA, ra.GetPublicKey(), tunnelAddr(1), 0, direct))
	ra = putOnNetwork(t, a, tunnelAddr(1), meshPeer(fpB, rb.GetPublicKey(), tunnelAddr(2), rb.GetPort(), direct))
	putOnNetwork(t, b, tunnelAddr(2), meshPeer(fpA, ra.GetPublicKey(), tunnelAddr(1), ra.GetPort(), direct))
}

func pathFrom(s *Service, fp string) string {
	m := s.network().current()
	if m == nil {
		return ""
	}
	for _, st := range m.Status() {
		if st.Fingerprint == fp {
			return st.Path
		}
	}
	return ""
}

func forwardDB(t *testing.T, a, b *Service, fpA, fpB string) uint32 {
	t.Helper()
	port := backend(t, "on-b")
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(port)}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(port)},
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: port, SourceFingerprints: []string{fpA}}}})
	placeOn(t, a, "srvA", dbEntry("srvB"))
	placeOn(t, b, "srvB", dbEntry("srvB"))
	return port
}

func TestNetwork_membersReachEachOtherThroughTheTunnelWithNoRelay(t *testing.T) {
	a, b, fa, _, fpA, fpB := memberPair(t)
	joinNetwork(t, a, b, fpA, fpB, true)
	eventually(t, "a direct path between the members", func() bool { return pathFrom(a, fpB) == mesh.PathDirect })
	relayOf[a].Refuse(true)
	carried := relayOf[a].Streams()
	forwardDB(t, a, b, fpA, fpB)
	if got := ask(t, fa.spec(t, "db-shop")); got != "on-b:PING" {
		t.Fatalf("through the tunnel it answered %q", got)
	}
	if relayOf[a].Streams() != carried {
		t.Fatal("a stream between the members went through the relay")
	}
}

func TestNetwork_aRelayCarriesTheTunnelWhereNoDirectPathAnswers(t *testing.T) {
	a, b, fa, _, fpA, fpB := memberPair(t)
	joinNetwork(t, a, b, fpA, fpB, false)
	forwardDB(t, a, b, fpA, fpB)
	if got := ask(t, fa.spec(t, "db-shop")); got != "on-b:PING" {
		t.Fatalf("through the relayed tunnel it answered %q", got)
	}
	eventually(t, "the tunnel to go through the relay", func() bool { return pathFrom(a, fpB) == mesh.PathRelay })
}

func TestNetwork_comesBackAfterARestartWithItsKey(t *testing.T) {
	a, b, _, _, fpA, fpB := memberPair(t)
	joinNetwork(t, a, b, fpA, fpB, true)
	key := a.network().current().PublicKey()
	again := New(t.TempDir(), t.TempDir(), "/", "")
	again.SetAgentDir(a.agentDir)
	again.StartClusterNetwork()
	t.Cleanup(func() { again.network().mu.Lock(); again.network().stopLocked(); again.network().mu.Unlock() })
	m := again.network().current()
	if m == nil || m.PublicKey() != key || !m.Has(fpB) {
		t.Fatal("the network did not come back with its key and peers")
	}
}

func TestNetwork_leavingTakesTheServerOffIt(t *testing.T) {
	a, b, _, _, fpA, fpB := memberPair(t)
	joinNetwork(t, a, b, fpA, fpB, true)
	if _, err := a.SetClusterNetwork(context.Background(), &pb.SetClusterNetworkRequest{ClusterId: "cl1"}); err != nil {
		t.Fatal(err)
	}
	if a.network().current() != nil {
		t.Fatal("the network still runs with no cluster on it")
	}
	if _, err := os.Stat(filepath.Join(a.agentDir, "cluster", "mesh.sock")); err == nil {
		t.Fatal("the leader's way onto the network is still open")
	}
}

func TestNetwork_refusesWhatIsNotAPeer(t *testing.T) {
	key, _ := mesh.NewPrivateKey()
	pub, _ := key.Public()
	fp := strings.Repeat("c", 64)
	bad := []*pb.SetClusterNetworkRequest{
		{ClusterId: "cl1", Address: "10.0.0.1", Peers: []*pb.ClusterNetworkPeer{meshPeer(fp, pub.String(), tunnelAddr(2), 0, false)}},
		{ClusterId: "cl1", Address: tunnelAddr(1), Peers: []*pb.ClusterNetworkPeer{meshPeer(fp, "short", tunnelAddr(2), 0, false)}},
		{ClusterId: "cl1", Address: tunnelAddr(1), Peers: []*pb.ClusterNetworkPeer{meshPeer(fp, pub.String(), tunnelAddr(1), 0, false)}},
		{ClusterId: "cl1", Address: tunnelAddr(1), Peers: []*pb.ClusterNetworkPeer{meshPeer("nope", pub.String(), tunnelAddr(2), 0, false)}},
		{ClusterId: "cl1", Address: tunnelAddr(1), Peers: []*pb.ClusterNetworkPeer{{Fingerprint: fp, PublicKey: pub.String(), Address: tunnelAddr(2), Endpoints: []string{"0.0.0.0:51820"}}}},
		{ClusterId: "", Address: tunnelAddr(1)},
	}
	for i, req := range bad {
		if _, _, err := validateMesh(req); err == nil {
			t.Errorf("request %d was taken", i)
		}
	}
}

func TestNetwork_theLeaderBesideAServerReachesMembersThroughIt(t *testing.T) {
	a, b, _, _, fpA, fpB := memberPair(t)
	// a stands for the server that only leads: it leaves the cluster but stays on the network.
	if _, err := a.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{}); err != nil {
		t.Fatal(err)
	}
	joinNetwork(t, a, b, fpA, fpB, true)
	if a.clusterParty() == nil {
		t.Fatal("a server on a network but in no cluster has no party for its relayed packets")
	}
	door := filepath.Join(a.agentDir, "cluster", "mesh.sock")
	c, err := net.DialTimeout("unix", door, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if err := wire.WriteHeader(c, meshDoorDial, wire.Party{Fingerprint: fpB, Role: wire.RoleMember}.Key()); err != nil {
		t.Fatal(err)
	}
	if verb, why, err := wire.ReadHeader(c); err != nil || verb != wire.VerbOK {
		t.Fatalf("the door refused: %s %v", why, err)
	}
	id, err := a.clusterIdentity()
	if err != nil {
		t.Fatal(err)
	}
	_ = wire.WriteHeader(c, wire.VerbFrom, wire.Party{Fingerprint: id.Fingerprint, Role: wire.RoleLeader}.Key())
	tc := tls.Client(c, id.ClientConfig(fpB))
	if err := tc.Handshake(); err != nil {
		t.Fatalf("no end-to-end handshake with the member through the door: %v", err)
	}
}

func TestNetwork_advertised(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.network") {
		t.Fatal("Hello does not advertise cluster.mesh")
	}
}

// Off its last cluster while a leader or relay still runs beside it, a server keeps its network until they stop:
// a leader extends every lease through it as it stops.
func TestNetwork_closesOnlyAfterTheLeaderAndRelayBesideItStop(t *testing.T) {
	a, b, _, _, fpA, fpB := memberPair(t)
	joinNetwork(t, a, b, fpA, fpB, true)
	h := a.clusterHost()
	stopHost := func() {
		h.mu.Lock()
		h.stop()
		h.mu.Unlock()
	}
	h.live.Store(true)
	if _, err := a.SetClusterNetwork(context.Background(), &pb.SetClusterNetworkRequest{ClusterId: "cl1"}); err != nil {
		t.Fatal(err)
	}
	if a.network().current() == nil {
		t.Fatal("the network closed while the leader beside it still runs")
	}
	stopHost()
	if a.network().current() != nil {
		t.Fatal("the network still runs after the leader and relay stopped")
	}
	if _, err := os.Stat(filepath.Join(a.agentDir, "cluster", "mesh.sock")); err == nil {
		t.Fatal("the leader's way onto the network is still open")
	}

	// Back on a cluster before the leader stops: nothing closes then.
	joinNetwork(t, a, b, fpA, fpB, true)
	h.live.Store(true)
	if _, err := a.SetClusterNetwork(context.Background(), &pb.SetClusterNetworkRequest{ClusterId: "cl1"}); err != nil {
		t.Fatal(err)
	}
	joinNetwork(t, a, b, fpA, fpB, true)
	stopHost()
	if a.network().current() == nil {
		t.Fatal("a network back on a cluster closed when the leader stopped")
	}
}
