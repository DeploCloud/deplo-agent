//go:build linux

package server

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/dnszone"
	"github.com/DeploCloud/deplo-agent/internal/mesh"
)

// powerOff stops a server's cluster work as a power cut does: nothing on its disk changes.
func powerOff(s *Service) {
	m := s.clusterMember()
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.cancel, m.party = nil, nil
	m.mu.Unlock()
	n := s.network()
	n.mu.Lock()
	n.stopLocked()
	n.mu.Unlock()
	f := s.forwarders()
	f.mu.Lock()
	for k, l := range f.listeners {
		l.ln.Close()
		delete(f.listeners, k)
	}
	f.mu.Unlock()
	if z := s.zone; z != nil {
		z.mu.Lock()
		if z.sup == nil {
			z.mu.Unlock()
			return
		}
		close(z.stop)
		sup, done := z.sup, z.done
		z.sup, z.sent = nil, nil
		z.mu.Unlock()
		<-done
		sup.Close()
	}
}

// rotationCluster is two members of one cluster: replication from a to b, a database of a's that b reaches by name,
// both on the private network through the relay, and both answering for the cluster's zone.
type rotationCluster struct {
	t        *testing.T
	ca       *testpki.CA
	a, b     *Service
	fpA, fpB string
	vol      string
	fb       *fakeFwdHost
	nsB      string
}

const rotationApp = "prj_1"

func newRotationCluster(t *testing.T, direct bool) *rotationCluster {
	t.Helper()
	a, b, vol, _ := replicating(t, true)
	c := &rotationCluster{t: t, ca: caOf[a], a: a, b: b, fpA: sourcePrint[a], vol: vol}
	id, err := b.clusterIdentity()
	if err != nil {
		t.Fatal(err)
	}
	c.fpB = id.Fingerprint
	joinNetwork(t, a, b, c.fpA, c.fpB, direct)
	fa := newFakeFwdHost()
	c.fb = newFakeFwdHost()
	a.fwdOps, b.fwdOps = fa.ops(), c.fb.ops()
	port := backend(t, "on-a")
	members := []*pb.ClusterForwarderMember{{ServerId: "srv_a", Fingerprint: c.fpA}, {ServerId: "srv_b", Fingerprint: c.fpB}}
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(port)}})
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(port)},
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: port, SourceFingerprints: []string{c.fpB}}}})
	app := lease.Entry{Kind: "app", ID: rotationApp, Member: "srv_a", Slug: "web", SingleWriter: true}
	placeOn(t, a, "srv_a", app, dbEntry("srv_a"))
	placeOn(t, b, "srv_b", app, dbEntry("srv_a"))
	for _, s := range []*Service{a, b} {
		s.zoneConfig = func(cfg *dnszone.Config) {
			cfg.Port = 0
			cfg.Addresses = func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("127.0.0.1")} }
		}
		t.Cleanup(func() { stopZones(s) })
	}
	setZone(t, a, c.zoneRequest(c.fpA, c.fpA))
	c.nsB = listening(t, setZone(t, b, c.zoneRequest(c.fpB, c.fpA)))
	settled(b)
	c.reaches("before anything happens", b, c.fpA, c.fb)
	return c
}

// zoneRequest is the zone as Deplo hands it to a server, naming a as fpA.
func (c *rotationCluster) zoneRequest(self, fpA string) *pb.SetClusterZoneRequest {
	return &pb.SetClusterZoneRequest{
		ClusterId: "cl1", SelfFingerprint: self,
		Zones: []*pb.ClusterZone{{Name: "go.example.com", Names: []string{"app.go.example.com"}}},
		Servers: []*pb.ClusterZoneServer{
			{Fingerprint: fpA, Addresses: []string{"203.0.113.1"}},
			{Fingerprint: c.fpB, Addresses: []string{"203.0.113.2"}},
		},
		NameServers: []string{fpA, c.fpB}, DropAfterSeconds: 5,
	}
}

var rotationWrites int

// reaches checks that b reaches a, named fpA in its tables: the private network, a's database by name,
// a change of the replicated App, and b's zone answering with both servers.
func (c *rotationCluster) reaches(when string, b *Service, fpA string, fb *fakeFwdHost) {
	t := c.t
	t.Helper()
	eventually(t, when+": the private network to reach a", func() bool { return pathFrom(b, fpA) != mesh.PathNone })
	eventually(t, when+": b to hold a's database's name", func() bool { return firstSpec(fb).Dir != "" })
	if got := ask(t, firstSpec(fb)); got != "on-a:PING" {
		t.Fatalf("%s: a's database answered %q", when, got)
	}
	rotationWrites++
	want := []byte{byte('0' + rotationWrites)}
	if err := os.WriteFile(filepath.Join(c.vol, "data.txt"), want, 0o640); err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(b.replicas().dir(rotationApp), "volume-deplo-web-data", "data.txt")
	eventually(t, when+": a's change to reach b's copy", func() bool {
		raw, _ := os.ReadFile(copied)
		return string(raw) == string(want)
	})
	b.zone.mu.Lock()
	b.zone.since = time.Now().Add(-time.Hour)
	b.zone.mu.Unlock()
	until(t, 10*time.Second, "203.0.113.1 203.0.113.2", func() string { return lookup(c.nsB, "app.go.example.com.") })
	eventually(t, when+": b's zone to hear a", func() bool {
		b.zone.mu.Lock()
		defer b.zone.mu.Unlock()
		return b.zone.verdicts["cl1"].Heard == 2
	})
}

func firstSpec(h *fakeFwdHost) forwardSpec {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range h.specs {
		return s
	}
	return forwardSpec{}
}

// rotate gives a a new key, as an installed rotation does, and Deplo tells the relay and a itself. b hears nothing.
func (c *rotationCluster) rotate() string {
	t := c.t
	t.Helper()
	next := c.ca.Provision(t, c.a.agentDir, "member").Fingerprint
	relayOf[c.a].Allow(wire.Party{Fingerprint: next, Role: wire.RoleMember})
	relayOf[c.a].Forget(wire.Party{Fingerprint: c.fpA, Role: wire.RoleMember})
	members := []*pb.ClusterForwarderMember{{ServerId: "srv_a", Fingerprint: next}, {ServerId: "srv_b", Fingerprint: c.fpB}}
	if _, err := c.a.SetClusterReplicas(context.Background(), &pb.SetClusterReplicasRequest{ClusterId: "cl1", Members: members,
		Entries: []*pb.ClusterReplicaEntry{{AppId: rotationApp, Slug: "web", VolumeNames: []string{"deplo-web-data"}}}}); err != nil {
		t.Fatal(err)
	}
	setZone(t, c.a, c.zoneRequest(next, next))
	return next
}

// bootAgain starts a server from what its disk holds, as the agent does after a power cut.
func bootAgain(t *testing.T, was *Service, fb *fakeFwdHost) *Service {
	t.Helper()
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(was.agentDir)
	s.fwdOps, s.zoneConfig = fb.ops(), was.zoneConfig
	s.replicas().mountpoint, s.replicas().running = was.replicas().mountpoint, was.replicas().running
	s.StartClusterMember()
	s.StartClusterNetwork()
	s.StartClusterZone()
	t.Cleanup(func() {
		powerOff(s)
		stopZones(s)
	})
	return s
}

// A member that was off while another rotated its key comes back while Deplo is down (ADR-0050), over a relay or a
// direct path.
func TestRotation_aMemberBackWhileDeploIsDownReachesTheMemberThatRotated(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{false: "relay", true: "direct"}[direct], func(t *testing.T) {
			c := newRotationCluster(t, direct)
			powerOff(c.b)
			c.rotate()
			// Deplo is down from here on: nothing is handed to anyone.
			c.comeBack()
		})
	}
}

// The same return with no rotation: what the case above must keep.
func TestRotation_aMemberBackWhileDeploIsDownReachesAMemberThatKeptItsKey(t *testing.T) {
	c := newRotationCluster(t, false)
	powerOff(c.b)
	c.comeBack()
}

// comeBack boots b from its disk; the leader, still running, renews its lease.
func (c *rotationCluster) comeBack() {
	t := c.t
	t.Helper()
	fb := newFakeFwdHost()
	b := bootAgain(t, c.b, fb)
	l, _ := b.zone.sup.Listening()
	c.nsB = listening(t, &pb.SetClusterZoneResponse{Listening: l})
	placeOn(t, b, "srv_b", lease.Entry{Kind: "app", ID: rotationApp, Member: "srv_a", Slug: "web", SingleWriter: true}, dbEntry("srv_a"))
	c.reaches("back while Deplo is down", b, c.fpA, fb)
}
