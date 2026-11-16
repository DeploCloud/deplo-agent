package server

import (
	"io"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

var entranceLabels = []string{
	"traefik.enable=true",
	"traefik.tcp.routers.e1.rule=HostSNI(`shop.example.com`)",
	"traefik.tcp.routers.e1.tls.passthrough=true",
}

func appEntry(member string) lease.Entry {
	return lease.Entry{Kind: "app", ID: "prj1", Member: member, Slug: "shop"}
}

// askPort connects as the entrance's container does for a caller that dialed port: the port line, then its bytes.
func askPort(t *testing.T, s forwardSpec, port int) string {
	t.Helper()
	c, err := net.DialTimeout("unix", filepath.Join(s.Dir, fwdSocket), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(c, "PORT "+strconv.Itoa(port)+"\n")
	_, _ = io.WriteString(c, "ping")
	out, _ := io.ReadAll(c)
	return string(out)
}

// entrancePair is two members: A passes the App's domains on, B runs the App. Each one's proxy is a backend
// that names its server, so an answer says which proxy it came from.
func entrancePair(t *testing.T) (a, b *Service, fa *fakeFwdHost, members []*pb.ClusterForwarderMember) {
	t.Helper()
	a, b, fa, _, fpA, fpB := memberPair(t)
	members = []*pb.ClusterForwarderMember{{ServerId: "srvA", Fingerprint: fpA}, {ServerId: "srvB", Fingerprint: fpB}}
	return a, b, fa, members
}

func TestEntrances_passADomainOnToTheProxyOfTheServerThatRunsTheApp(t *testing.T) {
	a, b, fa, members := entrancePair(t)
	onA, onB, web := backend(t, "proxy-a"), backend(t, "proxy-b"), backend(t, "web-b")
	old := proxyWebPort
	proxyWebPort = web
	t.Cleanup(func() { proxyWebPort = old })
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onA,
		Entrances: []*pb.ClusterEntrance{{AppId: "prj1", Labels: entranceLabels}}})
	apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onB,
		ServeEntrances: []string{"prj1"}})
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))

	s := fa.spec(t, entranceAlias("prj1"))
	if s.Network != entranceNetwork || s.Port != entranceTLS || !slices.Equal(s.Labels, entranceLabels) {
		t.Fatalf("the entrance's forwarder is %+v", s)
	}
	if got := ask(t, s); got != "proxy-b:PING" {
		t.Fatalf("its TLS reached %q, want B's internal entrypoint", got)
	}
	if got := askPort(t, s, 80); got != "web-b:PING" {
		t.Fatalf("a certificate check reached %q, want B's port 80", got)
	}
	if got := askPort(t, s, 22); got != "" {
		t.Fatalf("a port that is neither 443 nor 80 was passed on: %q", got)
	}
}

// After a failover the table may still name the entrance on the server the App now runs on: the proxy there hands
// the connection back to itself.
func TestEntrances_answerOnTheirOwnProxyWhileTheAppRunsHere(t *testing.T) {
	a, _, fa, members := entrancePair(t)
	onA := backend(t, "proxy-a")
	apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onA,
		Entrances: []*pb.ClusterEntrance{{AppId: "prj1", Labels: entranceLabels}}})
	placeOn(t, a, "srvA", appEntry("srvA"))
	if got := ask(t, fa.spec(t, entranceAlias("prj1"))); got != "proxy-a:PING" {
		t.Fatalf("it answered %q", got)
	}
}

func TestEntrances_receiverAnswersOnlyAnAppItWasToldToServeAndRuns(t *testing.T) {
	for name, tc := range map[string]struct {
		serve []string
		runs  string
	}{
		"not told to serve it": {nil, "srvB"},
		"another App":          {[]string{"prj2"}, "srvB"},
	} {
		t.Run(name, func(t *testing.T) {
			a, b, fa, members := entrancePair(t)
			onB := backend(t, "proxy-b")
			apply(t, a, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onB,
				Entrances: []*pb.ClusterEntrance{{AppId: "prj1", Labels: entranceLabels}}})
			apply(t, b, &pb.SetClusterForwardersRequest{ClusterId: "cl1", Members: members, EntrancePort: onB,
				ServeEntrances: tc.serve, Entrances: []*pb.ClusterEntrance{{AppId: "prj9", Labels: entranceLabels}}})
			placeOn(t, a, "srvA", appEntry(tc.runs))
			placeOn(t, b, "srvB", appEntry(tc.runs))
			if got := ask(t, fa.spec(t, entranceAlias("prj1"))); got != "" {
				t.Fatalf("the receiver answered %q", got)
			}
		})
	}
}

func TestEntrances_refuseABadTable(t *testing.T) {
	ok := &pb.ClusterEntrance{AppId: "prj1", Labels: entranceLabels}
	for name, req := range map[string]*pb.SetClusterForwardersRequest{
		"a label that is not the proxy's": {ClusterId: "cl1", EntrancePort: 9443,
			Entrances: []*pb.ClusterEntrance{{AppId: "prj1", Labels: []string{"com.example.x=1"}}}},
		"a label value on two lines": {ClusterId: "cl1", EntrancePort: 9443,
			Entrances: []*pb.ClusterEntrance{{AppId: "prj1", Labels: []string{"traefik.enable=true\nx"}}}},
		"no labels":       {ClusterId: "cl1", EntrancePort: 9443, Entrances: []*pb.ClusterEntrance{{AppId: "prj1"}}},
		"an App twice":    {ClusterId: "cl1", EntrancePort: 9443, Entrances: []*pb.ClusterEntrance{ok, ok}},
		"a bad App id":    {ClusterId: "cl1", EntrancePort: 9443, Entrances: []*pb.ClusterEntrance{{AppId: "../x", Labels: entranceLabels}}},
		"no entrypoint":   {ClusterId: "cl1", Entrances: []*pb.ClusterEntrance{ok}},
		"served, no port": {ClusterId: "cl1", ServeEntrances: []string{"prj1"}},
	} {
		if _, err := validateForwarders(req); err == nil {
			t.Errorf("%s: taken", name)
		}
	}
	if _, err := validateForwarders(&pb.SetClusterForwardersRequest{ClusterId: "cl1", EntrancePort: 9443,
		Entrances: []*pb.ClusterEntrance{ok}, ServeEntrances: []string{"prj2"}}); err != nil {
		t.Fatalf("a good table was refused: %v", err)
	}
}

func TestEntrances_theirContainerCarriesTheProxyLabelsAndANameForwarderKeepsItsTag(t *testing.T) {
	name := forwardSpec{Container: "deplo_fwd_0123456789abcdef", Network: envNet, Alias: "db-shop", Port: 5432, Dir: "/x"}
	before := forwarderSpecTag(name)
	name.Labels = nil
	if forwarderSpecTag(name) != before {
		t.Fatal("a name forwarder's tag changed")
	}
	e := forwardSpec{Container: entranceContainer("prj1"), Network: entranceNetwork, Alias: entranceAlias("prj1"),
		Port: entranceTLS, Dir: "/y", Labels: entranceLabels}
	args := strings.Join(forwarderRunArgs("/deplo-agent", e), " ")
	for _, l := range entranceLabels {
		if !strings.Contains(args, "--label "+l) {
			t.Fatalf("the container lacks %q: %s", l, args)
		}
	}
	other := e
	other.Labels = entranceLabels[:1]
	if forwarderSpecTag(other) == forwarderSpecTag(e) {
		t.Fatal("new labels would not replace the container")
	}
	if !fwdContainer.MatchString(entranceContainer("prj1")) {
		t.Fatal("an entrance's container is not one the forwarders list and remove")
	}
}

func TestEntrances_advertised(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.forwarders.entrances") {
		t.Fatal("Hello does not advertise cluster.forwarders.entrances")
	}
}
