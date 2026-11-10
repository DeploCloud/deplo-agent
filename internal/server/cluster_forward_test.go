package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

const envNet = "deplo-env-e1"

// fakeFwdHost is one host's docker and network for name forwarders.
type fakeFwdHost struct {
	mu      sync.Mutex
	subnets map[string]string
	specs   map[string]forwardSpec
	removed []string
	extra   []string
}

func newFakeFwdHost() *fakeFwdHost {
	return &fakeFwdHost{subnets: map[string]string{}, specs: map[string]forwardSpec{}}
}

func (h *fakeFwdHost) ops() forwardOps {
	return forwardOps{
		network: func(_ context.Context, network string) (string, *net.IPNet, error) {
			sub := "127.0.0.0/8"
			h.mu.Lock()
			if s := h.subnets[network]; s != "" {
				sub = s
			}
			h.mu.Unlock()
			_, n, _ := net.ParseCIDR(sub)
			return "127.0.0.1", n, nil
		},
		ensure: func(_ context.Context, s forwardSpec) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.specs[s.Container] = s
			return nil
		},
		remove: func(_ context.Context, c string) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.removed = append(h.removed, c)
			delete(h.specs, c)
			return nil
		},
		list: func(context.Context) ([]string, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			out := append([]string{}, h.extra...)
			for c := range h.specs {
				out = append(out, c)
			}
			return out, nil
		},
		ip:     func(context.Context, string, string) (string, error) { return "127.0.0.1", nil },
		listen: func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) },
	}
}

func (h *fakeFwdHost) spec(t *testing.T, alias string) forwardSpec {
	t.Helper()
	var out forwardSpec
	eventually(t, "the forwarder for "+alias, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, s := range h.specs {
			if s.Alias == alias {
				out = s
				return true
			}
		}
		return false
	})
	return out
}

// backend answers every connection with its name, then what it is sent, upper-cased.
func backend(t *testing.T, name string) uint32 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.WriteString(c, name+":")
				buf := make([]byte, 64)
				n, _ := c.Read(buf)
				_, _ = c.Write([]byte(strings.ToUpper(string(buf[:n]))))
			}()
		}
	}()
	return uint32(ln.Addr().(*net.TCPAddr).Port)
}

// ask connects to a forwarder's own listener and returns what the workload answered.
func ask(t *testing.T, target string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", target, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(c, "ping")
	out, _ := io.ReadAll(c)
	return string(out)
}

func placeOn(t *testing.T, s *Service, self string, placement ...lease.Entry) {
	t.Helper()
	if r := s.leaseGuard().Renew(lease.Order{Cluster: "cl1", Member: self, Epoch: 1, Placement: placement}); !r.OK {
		t.Fatalf("renewal refused: %+v", r)
	}
}

func dbEntry(member string) lease.Entry {
	return lease.Entry{Kind: "database", ID: "db1", Member: member, Slug: "db-shop", SingleWriter: true}
}

func memberPair(t *testing.T) (a, b *Service, fa, fb *fakeFwdHost, fpA, fpB string) {
	t.Helper()
	a, b, fpB = twoMembers(t, newImageHost(), newImageHost())
	id, err := a.clusterIdentity()
	if err != nil {
		t.Fatal(err)
	}
	fpA = id.Fingerprint
	fa, fb = newFakeFwdHost(), newFakeFwdHost()
	a.fwdOps, b.fwdOps = fa.ops(), fb.ops()
	return
}

func hold(origin string, port uint32) *pb.ClusterHeldName {
	return &pb.ClusterHeldName{Network: envNet, Kind: "database", WorkloadId: "db1", Name: "db-shop", Port: port, Origin: origin}
}

func apply(t *testing.T, s *Service, req *pb.SetClusterForwardersRequest) {
	t.Helper()
	if _, err := s.SetClusterForwarders(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func TestForwarders_reachAWorkloadOnAnotherMemberOverTheRelay(t *testing.T) {
	a, b, fa, fb, fpA, fpB := memberPair(t)
	port := backend(t, "on-b")
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold("db-shop.origin", port)}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold("db-shop.origin", port)},
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: port, Origin: "db-shop.origin", SourceFingerprints: []string{fpA}}}})
	placeOn(t, a, "srvA", dbEntry("srvB"))
	placeOn(t, b, "srvB", dbEntry("srvB"))

	if got := ask(t, fa.spec(t, "db-shop").Target); got != "on-b:PING" {
		t.Fatalf("through the relay it answered %q", got)
	}
	if got := ask(t, fb.spec(t, "db-shop").Target); got != "on-b:PING" {
		t.Fatalf("on the same member it answered %q", got)
	}
}

func TestForwarders_followAFailoverWithoutANewTable(t *testing.T) {
	a, b, fa, _, fpA, fpB := memberPair(t)
	onA, onB := backend(t, "on-a"), backend(t, "on-b")
	_ = onA
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	// The same port on both members is the workload's; a backend per member answers on it.
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold("db-shop.origin", onB)}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: onB, Origin: "db-shop.origin", SourceFingerprints: []string{fpA}}}})
	placeOn(t, a, "srvA", dbEntry("srvB"))
	placeOn(t, b, "srvB", dbEntry("srvB"))
	target := fa.spec(t, "db-shop").Target
	if got := ask(t, target); got != "on-b:PING" {
		t.Fatalf("before the failover %q", got)
	}
	// The leader moved the database to A: only the renewal changed. A runs it under the same port.
	placeOn(t, a, "srvA", dbEntry("srvA"))
	a.forwarders().mu.Lock()
	a.forwarders().st.Hold[0].Port = onA
	a.forwarders().mu.Unlock()
	if got := ask(t, target); got != "on-a:PING" {
		t.Fatalf("after the failover %q", got)
	}
}

func TestForwarders_receiverRefusesWhatItWasNotHanded(t *testing.T) {
	a, b, _, _, fpA, fpB := memberPair(t)
	port := backend(t, "on-b")
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	other := strings.Repeat("ab", 32)
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{
			{Network: envNet, Kind: "database", WorkloadId: "db1", Port: port, Origin: "o1", SourceFingerprints: []string{fpA, other}},
			{Network: envNet, Kind: "database", WorkloadId: "db2", Port: port, Origin: "o2", SourceFingerprints: []string{fpA}},
		}})
	placeOn(t, b, "srvB", dbEntry("srvB"), lease.Entry{Kind: "database", ID: "db2", Member: "srvA"})
	f := b.forwarders()
	src := wire.Party{Fingerprint: fpA, Role: wire.RoleMember}
	cases := []struct {
		name string
		from wire.Party
		net  string
		kind string
		id   string
		ok   bool
	}{
		{"a source it was handed", src, envNet, "database", "db1", true},
		{"another Environment's network", src, "deplo-env-e2", "database", "db1", false},
		{"a workload it was not handed", src, envNet, "database", "db9", false},
		{"a workload placed on another member", src, envNet, "database", "db2", false},
		{"an app of the same id", src, envNet, "app", "db1", false},
		// In the entry's sources, but not a member of this cluster's roster: another cluster's member.
		{"another cluster's member", wire.Party{Fingerprint: other, Role: wire.RoleMember}, envNet, "database", "db1", false},
		{"a leader", wire.Party{Fingerprint: fpA, Role: wire.RoleLeader}, envNet, "database", "db1", false},
	}
	for _, c := range cases {
		if _, ok := f.authorize(c.from, c.net, c.kind, c.id); ok != c.ok {
			t.Errorf("%s: served=%v, want %v", c.name, ok, c.ok)
		}
	}

	// And on the wire: A is not in the sources of db2's entry once the table drops it.
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: port, Origin: "o1", SourceFingerprints: []string{other}}}})
	party := a.clusterMember().party
	conn, err := party.Dial(context.Background(), wire.Party{Fingerprint: fpB, Role: wire.RoleMember})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = wire.WriteHeader(conn, fwdVerb, envNet+" database:db1")
	verb, _, err := wire.ReadHeader(conn)
	if err != nil || verb != wire.VerbErr {
		t.Fatalf("the stream was answered %q, %v", verb, err)
	}
}

func TestForwarders_refuseABadTable(t *testing.T) {
	a, _, _, _, fpA, _ := memberPair(t)
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}}
	bad := map[string]*pb.SetClusterForwardersRequest{
		"a network Deplo does not own": {ClusterId: "cl1", Hold: []*pb.ClusterHeldName{{Network: "deplo", Kind: "database", WorkloadId: "d", Name: "db-x", Port: 5432, Origin: "o"}}},
		"a name that is not a name":    {ClusterId: "cl1", Hold: []*pb.ClusterHeldName{{Network: envNet, Kind: "database", WorkloadId: "d", Name: "../x", Port: 5432, Origin: "o"}}},
		"no port":                      {ClusterId: "cl1", Hold: []*pb.ClusterHeldName{{Network: envNet, Kind: "app", WorkloadId: "d", Name: "web", Origin: "o"}}},
		"a source that is not a fingerprint": {ClusterId: "cl1",
			Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "app", WorkloadId: "d", Port: 80, Origin: "o", SourceFingerprints: []string{"zz"}}}},
		"no cluster": {Members: members},
	}
	for name, req := range bad {
		if _, err := a.SetClusterForwarders(context.Background(), req); code(err).String() != "InvalidArgument" {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestForwarders_onlyAnotherNetworksContainerIsTurnedAway(t *testing.T) {
	a, _, fa, _, fpA, _ := memberPair(t)
	fa.subnets[envNet] = "10.9.0.0/16" // the caller below is 127.0.0.1: not on this network
	port := backend(t, "on-a")
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}},
		Hold: []*pb.ClusterHeldName{hold("o", port)}})
	placeOn(t, a, "srvA", dbEntry("srvA"))
	if got := ask(t, fa.spec(t, "db-shop").Target); got != "" {
		t.Fatalf("a caller outside the network was answered %q", got)
	}
}

func TestForwarders_dropWhatTheTableNoLongerNames(t *testing.T) {
	a, _, fa, _, fpA, _ := memberPair(t)
	req := &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}},
		Hold: []*pb.ClusterHeldName{hold("o", 5432)}}
	apply(t, a, req)
	fa.spec(t, "db-shop")
	fa.mu.Lock()
	fa.extra = []string{"deplo-fwd-leftover"}
	fa.mu.Unlock()
	req.Hold = nil
	apply(t, a, req)
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if len(fa.specs) != 0 || fmt.Sprint(fa.removed) == "[]" {
		t.Fatalf("still running %v, removed %v", fa.specs, fa.removed)
	}
}

func TestForwarders_notForAnAgentOutsideACluster(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	_, err := s.SetClusterForwarders(context.Background(), &pb.SetClusterForwardersRequest{ClusterId: "cl1"})
	if code(err).String() != "FailedPrecondition" {
		t.Fatalf("got %v", err)
	}
}

func TestForwarders_advertised(t *testing.T) {
	found := false
	for _, c := range Capabilities {
		found = found || c == "cluster.forwarders"
	}
	if !found {
		t.Fatal("Hello does not advertise cluster.forwarders")
	}
}
