package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
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

// The leader on a member renews that member too: the door answers it on the spot, with no relay and no tunnel,
// so a leader cut off from Deplo still renews its own server (eu-3, 23 Nov 2026).
func TestNetwork_theLeaderReachesItsOwnServerThroughTheDoorWithNoRelay(t *testing.T) {
	a, b, _, _, fpA, fpB := memberPair(t)
	joinNetwork(t, a, b, fpA, fpB, true)
	door := filepath.Join(a.agentDir, "cluster", "mesh.sock")
	c, err := net.DialTimeout("unix", door, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if err := wire.WriteHeader(c, meshDoorDial, wire.Party{Fingerprint: fpA, Role: wire.RoleMember}.Key()); err != nil {
		t.Fatal(err)
	}
	if verb, why, err := wire.ReadHeader(c); err != nil || verb != wire.VerbOK {
		t.Fatalf("the door refused its own server: %s %v", why, err)
	}
	id, err := a.clusterIdentity()
	if err != nil {
		t.Fatal(err)
	}
	_ = wire.WriteHeader(c, wire.VerbFrom, wire.Party{Fingerprint: id.Fingerprint, Role: wire.RoleLeader}.Key())
	tc := tls.Client(c, id.ClientConfig(fpA))
	if err := tc.Handshake(); err != nil {
		t.Fatalf("no end-to-end handshake with its own server through the door: %v", err)
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

// A cluster of a Deplo this server left names the same machines under their old certificates, with the same keys:
// the cluster Deplo set last owns each key, so the network reaches the server under the certificate it has now.
func TestNetwork_theClusterSetLastOwnsAKeyBothName(t *testing.T) {
	a, b, _, _, fpA, fpB := memberPair(t)
	for _, s := range []*Service{a, b} {
		t.Cleanup(func() {
			for _, id := range []string{"cl0", "cl1"} {
				_, _ = s.SetClusterNetwork(context.Background(), &pb.SetClusterNetworkRequest{ClusterId: id})
			}
		})
	}
	stale := strings.Repeat("c", 64)
	ra := putOnNetwork(t, a, tunnelAddr(1))
	rb := putOnNetwork(t, b, tunnelAddr(2), meshPeer(fpA, ra.GetPublicKey(), tunnelAddr(1), 0, true))
	if _, err := a.SetClusterNetwork(context.Background(), &pb.SetClusterNetworkRequest{ClusterId: "cl0", Address: tunnelAddr(1),
		Peers: []*pb.ClusterNetworkPeer{meshPeer(stale, rb.GetPublicKey(), tunnelAddr(3), rb.GetPort(), true)}}); err != nil {
		t.Fatal(err)
	}
	putOnNetwork(t, a, tunnelAddr(1), meshPeer(fpB, rb.GetPublicKey(), tunnelAddr(2), rb.GetPort(), true))
	m := a.network().current()
	if !m.Has(fpB) || m.Has(stale) {
		t.Fatalf("the key is under the stale certificate: has %v, stale %v", m.Has(fpB), m.Has(stale))
	}
	eventually(t, "a direct path under the current certificate", func() bool { return pathFrom(a, fpB) == mesh.PathDirect })
}

// A network file from before setAt names no time: the cluster this server is a member of owns a key both name, so
// the first start after the update reaches every member under its current certificate.
func TestNetwork_aFileFromBeforeSetAtPutsTheMembersClusterFirst(t *testing.T) {
	dir := t.TempDir()
	m := bootstrap.Paths(dir)
	for _, p := range []string{m.CertPath, m.KeyPath, m.CAPath} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	own, _ := mesh.NewPrivateKey()
	peer, _ := mesh.NewPrivateKey()
	pub, _ := peer.Public()
	current, stale := strings.Repeat("b", 64), strings.Repeat("c", 64)
	file := meshFile{PrivateKey: own.String(), Address: tunnelAddr(1), Clusters: map[string][]meshPeerEntry{
		"cl0": {{Fingerprint: stale, PublicKey: pub.String(), Address: tunnelAddr(3)}},
		"cl1": {{Fingerprint: current, PublicKey: pub.String(), Address: tunnelAddr(2)}},
	}}
	raw, _ := json.Marshal(file)
	if err := os.WriteFile(filepath.Join(dir, "cluster-mesh.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cluster-member.json"), []byte(`{"member":true,"cluster":"cl1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(dir)
	s.StartClusterNetwork()
	t.Cleanup(func() { s.network().mu.Lock(); s.network().stopLocked(); s.network().mu.Unlock() })
	nw := s.network().current()
	if nw == nil || !nw.Has(current) || nw.Has(stale) {
		t.Fatalf("the key went to the stale cluster: current %v", nw != nil && nw.Has(current))
	}
}

// ADR-0050: a caught copy holds the original's network key, so Deplo has the original replace it. Only the key it
// names is retired, so a request sent twice makes one new key, and the others reach it under the new one.
func TestNetwork_aRetiredKeyIsReplacedOnceAndTheOthersReachTheNewOne(t *testing.T) {
	a, b, fa, _, fpA, fpB := memberPair(t)
	joinNetwork(t, a, b, fpA, fpB, true)
	old := a.network().current().PublicKey().String()
	retire := func(key string) *pb.SetClusterNetworkResponse {
		resp, err := a.SetClusterNetwork(context.Background(), &pb.SetClusterNetworkRequest{ClusterId: "cl1", Address: tunnelAddr(1),
			Peers:           []*pb.ClusterNetworkPeer{meshPeer(fpB, b.network().current().PublicKey().String(), tunnelAddr(2), uint32(b.network().current().Port()), true)},
			RetirePublicKey: key})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	ra := retire(old)
	if ra.GetPublicKey() == old || a.network().current().PublicKey().String() != ra.GetPublicKey() {
		t.Fatal("the named key was not replaced")
	}
	if again := retire(old); again.GetPublicKey() != ra.GetPublicKey() {
		t.Fatal("a key that is no longer this server's replaced the new one")
	}
	putOnNetwork(t, b, tunnelAddr(2), meshPeer(fpA, ra.GetPublicKey(), tunnelAddr(1), ra.GetPort(), true))
	forwardDB(t, a, b, fpA, fpB)
	eventually(t, "a direct path under the new key", func() bool { return pathFrom(a, fpB) == mesh.PathDirect })
	if got := ask(t, fa.spec(t, "db-shop")); got != "on-b:PING" {
		t.Fatalf("under the new key it answered %q", got)
	}
}
