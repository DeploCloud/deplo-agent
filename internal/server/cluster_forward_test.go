package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

const envNet = "deplo-env-e1"

// fakeFwdHost is one host's docker for name forwarders; the sockets are real.
type fakeFwdHost struct {
	mu      sync.Mutex
	specs   map[string]forwardSpec
	removed []string
	extra   []string
	// running is the network each workload's container is on here; unset => the test's envNet.
	running map[string]string
}

func newFakeFwdHost() *fakeFwdHost {
	return &fakeFwdHost{specs: map[string]forwardSpec{}, running: map[string]string{}}
}

func (h *fakeFwdHost) ops() forwardOps {
	return forwardOps{
		network: func(context.Context, string) error { return nil },
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
		// Like docker: a workload answers only on the network its container is on.
		target: func(_ context.Context, id, network string) (string, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			on := h.running[id]
			if on == "" {
				on = envNet
			}
			if on != network {
				return "", fmt.Errorf("%s is not on %s", id, network)
			}
			return "127.0.0.1", nil
		},
		listen: listenForwarder,
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

// ask connects the way a forwarder's container does, on its socket, and returns what the workload answered.
func ask(t *testing.T, s forwardSpec) string {
	t.Helper()
	c, err := net.DialTimeout("unix", filepath.Join(s.Dir, fwdSocket), 3*time.Second)
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

func hold(port uint32) *pb.ClusterHeldName {
	return &pb.ClusterHeldName{Network: envNet, Kind: "database", WorkloadId: "db1", Name: "db-shop", Port: port}
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
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(port)}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(port)},
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: port, SourceFingerprints: []string{fpA}}}})
	placeOn(t, a, "srvA", dbEntry("srvB"))
	placeOn(t, b, "srvB", dbEntry("srvB"))

	if got := ask(t, fa.spec(t, "db-shop")); got != "on-b:PING" {
		t.Fatalf("through the relay it answered %q", got)
	}
	if got := ask(t, fb.spec(t, "db-shop")); got != "on-b:PING" {
		t.Fatalf("on the same member it answered %q", got)
	}
}

func TestForwarders_followAFailoverWithoutANewTable(t *testing.T) {
	a, b, fa, _, fpA, fpB := memberPair(t)
	onA, onB := backend(t, "on-a"), backend(t, "on-b")
	_ = onA
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	// The same port on both members is the workload's; a backend per member answers on it.
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(onB)}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: onB, SourceFingerprints: []string{fpA}}}})
	placeOn(t, a, "srvA", dbEntry("srvB"))
	placeOn(t, b, "srvB", dbEntry("srvB"))
	target := fa.spec(t, "db-shop")
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

const teamBNet = "deplo-env-teamb"

func TestForwarders_receiverRefusesWhatItWasNotHanded(t *testing.T) {
	a, b, _, fb, fpA, fpB := memberPair(t)
	port := backend(t, "on-b")
	other, third := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	// A and a third server are this cluster's members; `other` is a member of another cluster.
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}, {ServerId: "srvC", Fingerprint: third}}
	fb.running["dbB"] = teamBNet
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{
			{Network: envNet, Kind: "database", WorkloadId: "db1", Port: port, SourceFingerprints: []string{fpA, other}},
			{Network: envNet, Kind: "database", WorkloadId: "db2", Port: port, SourceFingerprints: []string{fpA}},
			// Another team's database on the same member: only the third server runs that team's Apps.
			{Network: teamBNet, Kind: "database", WorkloadId: "dbB", Port: port, SourceFingerprints: []string{third}},
		}})
	placeOn(t, b, "srvB", dbEntry("srvB"), lease.Entry{Kind: "database", ID: "db2", Member: "srvA"}, lease.Entry{Kind: "database", ID: "dbB", Member: "srvB"})
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
		{"another team's database, by its own network", src, teamBNet, "database", "dbB", false},
		{"another team's database, by a network the source may use", src, envNet, "database", "dbB", false},
		{"its own database, by another team's network", src, teamBNet, "database", "db1", false},
		{"another Environment's network", src, "deplo-env-e2", "database", "db1", false},
		{"a workload it was not handed", src, envNet, "database", "db9", false},
		{"a workload placed on another member", src, envNet, "database", "db2", false},
		{"an app of the same id", src, envNet, "app", "db1", false},
		{"a member of this cluster that is not the entry's source", wire.Party{Fingerprint: third, Role: wire.RoleMember}, envNet, "database", "db1", false},
		// In the entry's sources, but not in this cluster's roster: another cluster's member.
		{"another cluster's member", wire.Party{Fingerprint: other, Role: wire.RoleMember}, envNet, "database", "db1", false},
		{"a leader", wire.Party{Fingerprint: fpA, Role: wire.RoleLeader}, envNet, "database", "db1", false},
	}
	for _, c := range cases {
		if _, ok := f.authorize(c.from, c.net, c.kind, c.id); ok != c.ok {
			t.Errorf("%s: served=%v, want %v", c.name, ok, c.ok)
		}
	}

	// And on the wire: A asks for the other team's database and is refused before anything is dialed.
	party := a.clusterMember().party
	for _, want := range []string{teamBNet + " database:dbB", envNet + " database:dbB"} {
		conn, err := party.Dial(context.Background(), wire.Party{Fingerprint: fpB, Role: wire.RoleMember})
		if err != nil {
			t.Fatal(err)
		}
		_ = wire.WriteHeader(conn, fwdVerb, want)
		verb, _, err := wire.ReadHeader(conn)
		conn.Close()
		if err != nil || verb != wire.VerbErr {
			t.Fatalf("%q was answered %q, %v", want, verb, err)
		}
	}
}

// A served entry reaches the workload's container on the entry's own network and nowhere else.
func TestForwarders_aServedWorkloadIsDialedOnItsOwnNetworkOnly(t *testing.T) {
	_, b, _, fb, fpA, fpB := memberPair(t)
	port := backend(t, "on-b")
	fb.running["db1"] = teamBNet // the container is on another team's network, whatever the entry says
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1",
		Members: []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}},
		Serve:   []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: port, SourceFingerprints: []string{fpA}}}})
	placeOn(t, b, "srvB", dbEntry("srvB"))
	if _, err := b.forwarders().dialLocal(context.Background(), "db1", envNet, port); err == nil {
		t.Fatal("a container on another network was dialed")
	}
}

func TestForwarders_refuseABadTable(t *testing.T) {
	a, _, fa, _, fpA, _ := memberPair(t)
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}}
	held := func(network, name string, port uint32) *pb.SetClusterForwardersRequest {
		return &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
			Hold: []*pb.ClusterHeldName{{Network: network, Kind: "database", WorkloadId: "d", Name: name, Port: port}}}
	}
	bad := map[string]*pb.SetClusterForwardersRequest{
		"the platform's own network":    held("deplo", "db-x", 5432),
		"the proxy's socket network":    held("deplo-socket", "db-x", 5432),
		"a preview's network":           held("deplo-preview-web__pr-1", "db-x", 5432),
		"a name that is not a name":     held(envNet, "../x", 5432),
		"a name no workload has":        held(envNet, "web", 5432),
		"the panel's name":              held(envNet, "deplo", 5432),
		"the panel database's name":     held(envNet, "postgres", 5432),
		"the proxy's name":              held(envNet, "traefik", 5432),
		"the proxy's container":         held(envNet, "deplo-traefik", 5432),
		"the socket proxy's name":       held(envNet, "docker-socket-proxy", 5432),
		"the socket proxy's other name": held(envNet, "deplo-socket-proxy", 5432),
		"a name with a dot":             held(envNet, "db-x.internal", 5432),
		"no port":                       held(envNet, "db-x", 0),
		"a port that is not one":        held(envNet, "db-x", 70000),
		"a source that is not a fingerprint": {ClusterId: "cl1",
			Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "app", WorkloadId: "d", Port: 80, SourceFingerprints: []string{"zz"}}}},
		"a served workload on the platform's network": {ClusterId: "cl1",
			Serve: []*pb.ClusterServedName{{Network: "deplo", Kind: "app", WorkloadId: "d", Port: 80}}},
		"no cluster": {Members: members},
	}
	for name, req := range bad {
		if _, err := a.SetClusterForwarders(context.Background(), req); code(err).String() != "InvalidArgument" {
			t.Errorf("%s: %v", name, err)
		}
	}
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if len(fa.specs) != 0 {
		t.Fatalf("a refused table started %v", fa.specs)
	}
}

// Each name has a socket of its own, so a forwarder reaches its own Environment's workload and no other.
func TestForwarders_eachNameReachesOnlyItsOwnWorkload(t *testing.T) {
	a, _, fa, _, fpA, _ := memberPair(t)
	mine, theirs := backend(t, "team-a"), backend(t, "team-b")
	fa.running["dbB"] = teamBNet
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}},
		Hold: []*pb.ClusterHeldName{hold(mine), {Network: teamBNet, Kind: "database", WorkloadId: "dbB", Name: "db-shop", Port: theirs}}})
	placeOn(t, a, "srvA", dbEntry("srvA"), lease.Entry{Kind: "database", ID: "dbB", Member: "srvA"})
	var own, other forwardSpec
	eventually(t, "both forwarders", func() bool {
		fa.mu.Lock()
		defer fa.mu.Unlock()
		for _, s := range fa.specs {
			if s.Network == envNet {
				own = s
			} else {
				other = s
			}
		}
		return own.Dir != "" && other.Dir != ""
	})
	if own.Dir == other.Dir || own.Container == other.Container {
		t.Fatalf("two Environments share a forwarder: %+v %+v", own, other)
	}
	if got := ask(t, own); got != "team-a:PING" {
		t.Fatalf("team A's name answered %q", got)
	}
	if got := ask(t, other); got != "team-b:PING" {
		t.Fatalf("team B's name answered %q", got)
	}
	// Only that forwarder's container is handed the socket: nobody but this user reaches the directory above it.
	parent, _ := os.Stat(filepath.Dir(own.Dir))
	if parent.Mode().Perm() != 0o700 {
		t.Fatalf("the sockets' directory is %v", parent.Mode().Perm())
	}
}

// The placement may name a server this member was never handed: it is not dialed.
func TestForwarders_neverDialAMemberOutsideTheRoster(t *testing.T) {
	a, _, fa, _, fpA, _ := memberPair(t)
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}},
		Hold: []*pb.ClusterHeldName{hold(5432)}})
	placeOn(t, a, "srvA", dbEntry("srvB"))
	if got := ask(t, fa.spec(t, "db-shop")); got != "" {
		t.Fatalf("a server outside the roster answered %q", got)
	}
}

func TestForwarders_dropWhatTheTableNoLongerNames(t *testing.T) {
	a, _, fa, _, fpA, _ := memberPair(t)
	req := &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}},
		Hold: []*pb.ClusterHeldName{hold(5432)}}
	apply(t, a, req)
	was := fa.spec(t, "db-shop")
	fa.mu.Lock()
	fa.extra = []string{"deplo_fwd_0123456789abcdef"}
	fa.mu.Unlock()
	req.Hold = nil
	apply(t, a, req)
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if len(fa.specs) != 0 || len(fa.removed) != 2 {
		t.Fatalf("still running %v, removed %v", fa.specs, fa.removed)
	}
	if _, err := os.Stat(was.Dir); !os.IsNotExist(err) {
		t.Fatalf("the dropped name's socket is still there: %v", err)
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
