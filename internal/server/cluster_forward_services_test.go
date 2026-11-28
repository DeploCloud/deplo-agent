package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// askOn connects the way a forwarder's container does after a redirect: it names the port its caller dialed first.
func askOn(t *testing.T, s forwardSpec, port uint32) string {
	t.Helper()
	c, err := net.DialTimeout("unix", filepath.Join(s.Dir, fwdSocket), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprintf(c, "PORT %d\n", port)
	_, _ = io.WriteString(c, "ping")
	out, _ := io.ReadAll(c)
	return string(out)
}

func stackEntry(member string) lease.Entry {
	return lease.Entry{Kind: "app", ID: "stack1", Member: member, Slug: "shop"}
}

func TestForwarders_aComposeServiceIsReachedByNameFromAnotherMember(t *testing.T) {
	a, b, fa, fb, fpA, fpB := memberPair(t)
	port := backend(t, "api-on-b")
	fb.services["stack1"] = "api"
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	held := &pb.ClusterHeldName{Network: envNet, Kind: "app", WorkloadId: "stack1", Name: "api", Port: port, Service: "api"}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{held}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "app", WorkloadId: "stack1", Port: port, Service: "api",
			SourceFingerprints: []string{fpA}}}})
	placeOn(t, a, "srvA", stackEntry("srvB"))
	placeOn(t, b, "srvB", stackEntry("srvB"))
	if got := ask(t, fa.spec(t, "api")); got != "api-on-b:PING" {
		t.Fatalf("the stack's service answered %q", got)
	}
}

func TestForwarders_aServiceIsServedOnlyForItsOwnName(t *testing.T) {
	a, b, fa, fb, fpA, fpB := memberPair(t)
	port := backend(t, "api-on-b")
	fb.services["stack1"] = "api"
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	// a holds "worker" for the stack, but b serves only its "api".
	held := &pb.ClusterHeldName{Network: envNet, Kind: "app", WorkloadId: "stack1", Name: "worker", Port: port, Service: "worker"}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{held}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "app", WorkloadId: "stack1", Port: port, Service: "api",
			SourceFingerprints: []string{fpA}}}})
	placeOn(t, a, "srvA", stackEntry("srvB"))
	placeOn(t, b, "srvB", stackEntry("srvB"))
	if got := ask(t, fa.spec(t, "worker")); got != "" {
		t.Fatalf("a service the receiver was not handed answered %q", got)
	}
}

// The forwarder listens on one port, and every other one is redirected to it: the caller's own port is used.
func TestForwarders_aNameAnswersOnThePortItsCallerDialed(t *testing.T) {
	a, b, fa, _, fpA, fpB := memberPair(t)
	port := backend(t, "on-b")
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	// The entry names port 1, where nothing listens: only the port the caller dialed can answer.
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(1)}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members,
		Serve: []*pb.ClusterServedName{{Network: envNet, Kind: "database", WorkloadId: "db1", Port: 1, SourceFingerprints: []string{fpA}}}})
	placeOn(t, a, "srvA", dbEntry("srvB"))
	placeOn(t, b, "srvB", dbEntry("srvB"))
	if got := askOn(t, fa.spec(t, "db-shop"), port); got != "on-b:PING" {
		t.Fatalf("on the dialed port it answered %q", got)
	}
}

func TestForwarders_aNameOnTheSameMemberAnswersOnTheDialedPortToo(t *testing.T) {
	a, _, fa, _, fpA, _ := memberPair(t)
	port := backend(t, "here")
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}}
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(1)}})
	placeOn(t, a, "srvA", dbEntry("srvA"))
	if got := askOn(t, fa.spec(t, "db-shop"), port); got != "here:PING" {
		t.Fatalf("locally it answered %q", got)
	}
}

func TestForwarders_eachContainerIsRedirectedOncePerProcess(t *testing.T) {
	a, _, fa, _, fpA, _ := memberPair(t)
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}}
	req := &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{hold(5432)}}
	apply(t, a, req)
	apply(t, a, req)
	name := fa.spec(t, "db-shop").Container
	fa.mu.Lock()
	if fmt.Sprint(fa.redirects) != "[1:5432]" {
		t.Fatalf("redirects %v", fa.redirects)
	}
	// The container restarted: its new process lost the redirect with its network.
	fa.pid[name] = 7
	fa.mu.Unlock()
	f := a.forwarders()
	f.dock.Lock()
	f.redirect(context.Background(), f.state())
	f.dock.Unlock()
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if fmt.Sprint(fa.redirects) != "[1:5432 7:5432]" {
		t.Fatalf("after a restart, redirects %v", fa.redirects)
	}
}

func TestForwarders_aComposeNameNeedsItsService(t *testing.T) {
	a, _, _, _, fpA, _ := memberPair(t)
	members := []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}}
	req := func(h *pb.ClusterHeldName) *pb.SetClusterForwardersRequest {
		return &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, Hold: []*pb.ClusterHeldName{h}}
	}
	good := &pb.ClusterHeldName{Network: envNet, Kind: "app", WorkloadId: "s", Name: "api", Port: 80, Service: "api"}
	if _, err := a.SetClusterForwarders(context.Background(), req(good)); err != nil {
		t.Fatalf("a compose service's name was refused: %v", err)
	}
	bad := map[string]*pb.ClusterHeldName{
		"a plain name with no service": {Network: envNet, Kind: "app", WorkloadId: "s", Name: "api", Port: 80},
		"a database with a service":    {Network: envNet, Kind: "database", WorkloadId: "s", Name: "api", Port: 80, Service: "api"},
		"a reserved name":              {Network: envNet, Kind: "app", WorkloadId: "s", Name: "deplo", Port: 80, Service: "deplo"},
		"a service that is not one":    {Network: envNet, Kind: "app", WorkloadId: "s", Name: "api", Port: 80, Service: "../x"},
		"a name with a dot":            {Network: envNet, Kind: "app", WorkloadId: "s", Name: "api.local", Port: 80, Service: "api"},
	}
	for what, h := range bad {
		if _, err := a.SetClusterForwarders(context.Background(), req(h)); code(err).String() != "InvalidArgument" {
			t.Errorf("%s: %v", what, err)
		}
	}
}

func TestFwdArg_keepsTheOldFormForANamesOwnPort(t *testing.T) {
	h := heldName{Network: envNet, Kind: "database", ID: "db1", Port: 5432}
	if got := fwdArg(h, 5432); got != envNet+" database:db1" {
		t.Fatalf("own port: %q", got)
	}
	cases := []struct {
		h    heldName
		port uint32
	}{
		{h, 6379},
		{heldName{Network: envNet, Kind: "app", ID: "stack1", Port: 80, Service: "api"}, 80},
		{heldName{Network: envNet, Kind: "app", ID: "stack1", Port: 80, Service: "api"}, 9000},
	}
	for _, c := range cases {
		network, kind, id, service, port, ok := parseFwdArg(fwdArg(c.h, c.port))
		if !ok || network != c.h.Network || kind != c.h.Kind || id != c.h.ID || service != c.h.Service || port != c.port {
			t.Errorf("%+v on %d read back as %s %s %s %s %d %v", c.h, c.port, network, kind, id, service, port, ok)
		}
	}
	for _, bad := range []string{"", envNet, envNet + " database", envNet + " app:s 0", envNet + " app:s x", envNet + " a:b 1 c d"} {
		if _, _, _, _, _, ok := parseFwdArg(bad); ok {
			t.Errorf("%q was read", bad)
		}
	}
}
