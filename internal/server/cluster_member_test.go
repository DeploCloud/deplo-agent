package server

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testrelay"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/link"
)

// bridgeStubbed says a test already answers for Docker's bridge.
var bridgeStubbed bool

func stubBridge(t *testing.T, gw string) {
	t.Helper()
	old, was := bridgeGateway, bridgeStubbed
	bridgeGateway, bridgeStubbed = func(context.Context) string { return gw }, true
	t.Cleanup(func() { bridgeGateway, bridgeStubbed = old, was })
}

func envOf(t *testing.T, dir, role string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "env."+role))
	if err != nil {
		t.Fatalf("the %s was not started: %v", role, err)
	}
	out := map[string]string{}
	for _, l := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		k, v, _ := strings.Cut(l, "=")
		out[k] = v
	}
	return out
}

func TestCluster_advertisesTheMembershipCapability(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster") {
		t.Fatal("Hello does not advertise cluster")
	}
}

func TestInstallCluster_relayAnswersOnlyLoopbackAndTheProxysGateway(t *testing.T) {
	stubBridge(t, "172.17.0.1")
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, true)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "both to start", func() bool { return startsOf(dir) == 2 })
	relay := envOf(t, dir, "relay")
	if relay["DEPLO_RELAY_LISTEN"] != "127.0.0.1:3002,172.17.0.1:3002" {
		t.Fatalf("the relay is told to answer on %q", relay["DEPLO_RELAY_LISTEN"])
	}
	if relay["DEPLO_RELAY_SOCKET"] != relaySocket(dir) {
		t.Fatalf("relay socket %q", relay["DEPLO_RELAY_SOCKET"])
	}
	for _, addr := range strings.Split(relay["DEPLO_RELAY_LISTEN"], ",") {
		if strings.HasPrefix(addr, "0.0.0.0") || strings.HasPrefix(addr, ":") {
			t.Fatalf("the relay would answer everyone: %s", addr)
		}
	}
	leader := envOf(t, dir, "leader")
	if leader["DEPLO_CLUSTER_RELAYS_FILE"] != relaysFile(dir) || leader["DEPLO_RELAY_LISTEN"] != "" {
		t.Fatalf("leader env: %v", leader)
	}
	for _, e := range []map[string]string{relay, leader} {
		if !strings.HasSuffix(e["DEPLO_CLUSTER_CERT"], "agent.crt") || !strings.HasSuffix(e["DEPLO_CLUSTER_CA"], "ca.crt") {
			t.Fatalf("a cluster process was not given the agent's certificate: %v", e)
		}
	}
}

func TestInstallCluster_withoutDockerTheRelayOnlyAnswersLoopback(t *testing.T) {
	stubBridge(t, "")
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, true)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "both to start", func() bool { return startsOf(dir) == 2 })
	if got := envOf(t, dir, "relay")["DEPLO_RELAY_LISTEN"]; got != "127.0.0.1:3002" {
		t.Fatalf("listen = %q", got)
	}
}

func serveOn(t *testing.T, sock string, h http.HandlerFunc) {
	t.Helper()
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Close() })
}

func TestInstallCluster_theLeaderIsHandedTheRelaysItDials(t *testing.T) {
	old := relaysRefresh
	relaysRefresh = 20 * time.Millisecond
	t.Cleanup(func() { relaysRefresh = old })
	stubBridge(t, "")
	s, dir := clusterHostFor(t)
	agentDir := filepath.Dir(dir)
	// The host's own link is loopback, which is the panel's door, not a relay; the other server's is the public panel.
	if err := link.SaveAddresses(agentDir, link.Addresses{Primary: "http://127.0.0.1:3101", Fallback: "https://panel.example.com"}); err != nil {
		t.Fatal(err)
	}
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, true)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the relays file", func() bool {
		raw, _ := os.ReadFile(relaysFile(dir))
		return string(raw) == "http://127.0.0.1:3002\nhttps://panel.example.com\n"
	})

	// The panel moves: the file follows without a restart.
	if err := link.SaveAddresses(agentDir, link.Addresses{Primary: "https://moved.example.com"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the relays file to follow the panel", func() bool {
		raw, _ := os.ReadFile(relaysFile(dir))
		return string(raw) == "http://127.0.0.1:3002\nhttps://moved.example.com\n"
	})
}

func TestClusterCall_relayMethodsGoToTheRelaysSocket(t *testing.T) {
	stubBridge(t, "")
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.ClusterCall(context.Background(), &pb.ClusterCallRequest{Method: "relay.roster"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("no relay on the host: got %v, want FailedPrecondition", err)
	}
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, true)); err != nil {
		t.Fatal(err)
	}
	var viaRelay, viaLeader int
	serveOn(t, relaySocket(dir), func(w http.ResponseWriter, r *http.Request) {
		viaRelay++
		if r.URL.Path != "/call/relay.roster" {
			http.Error(w, r.URL.Path, 400)
			return
		}
		w.Write([]byte("rostered"))
	})
	fakeLeader(t, dir, func(w http.ResponseWriter, r *http.Request) { viaLeader++; w.Write([]byte("led")) })

	got, err := s.ClusterCall(context.Background(), &pb.ClusterCallRequest{Method: "relay.roster", Payload: []byte("{}")})
	if err != nil || string(got.GetPayload()) != "rostered" {
		t.Fatalf("relay call: %q %v", got.GetPayload(), err)
	}
	got, err = s.ClusterCall(context.Background(), &pb.ClusterCallRequest{Method: "place"})
	if err != nil || string(got.GetPayload()) != "led" || viaRelay != 1 || viaLeader != 1 {
		t.Fatalf("leader call: %q %v (relay %d, leader %d)", got.GetPayload(), err, viaRelay, viaLeader)
	}
}

func memberService(t *testing.T) (*Service, *wire.Identity, *testpki.CA) {
	t.Helper()
	agentDir, err := os.MkdirTemp("", "ag")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(agentDir) })
	ca := testpki.New(t)
	id := ca.Provision(t, agentDir, "member")
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(agentDir)
	t.Cleanup(func() { s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{}) })
	return s, id, ca
}

func TestSetClusterMember_needsACertificate(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(t.TempDir())
	_, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
}

func TestSetClusterMember_refusesAnythingButARelayAddress(t *testing.T) {
	s, _, _ := memberService(t)
	for _, bad := range []string{"", "relay.example.com", "ftp://x", "https://u:p@x", "https://x/path", "https://x?q=1", "http://"} {
		_, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{bad}})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%q: got %v, want InvalidArgument", bad, err)
		}
	}
	many := make([]string, maxMemberRelays+1)
	for i := range many {
		many[i] = "https://r.example.com"
	}
	if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: many}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("too many relays: %v", err)
	}
	if st := s.loadMember(); st.Member {
		t.Fatal("a refused request was saved")
	}
}

func TestSetClusterMember_dialsTheRelayAndStopsWhenAskedAndResumesAfterARestart(t *testing.T) {
	stubBridge(t, "")
	s, id, ca := memberService(t)
	me := wire.Party{Fingerprint: id.Fingerprint, Role: wire.RoleMember}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), me)

	resp, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{relay.URL}})
	if err != nil || !resp.GetMember() {
		t.Fatalf("set: %+v %v", resp, err)
	}
	linked := func(want int32) func() bool {
		return func() bool {
			st, err := s.ClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
			return err == nil && st.GetMember() == (want > 0) && st.GetRelayLinks() == want
		}
	}
	eventually(t, "the relay to hold the connection", linked(1))
	if relay.Connected() != 1 {
		t.Fatalf("the relay holds %d connections", relay.Connected())
	}

	// A restart of the agent resumes from what was saved, with no call.
	t.Cleanup(func() { s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{}) })
	again := New(t.TempDir(), t.TempDir(), "/", "")
	again.SetAgentDir(s.agentDir)
	again.StartClusterMember()
	t.Cleanup(func() { again.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{}) })
	st, _ := again.ClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if !st.GetMember() {
		t.Fatal("a restarted agent forgot it is a member")
	}

	if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: false}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the connection to end", linked(0))
	if st := s.loadMember(); st.Member || len(st.Relays) != 0 {
		t.Fatalf("saved %+v after leaving", st)
	}
}

func TestMember_neverListens(t *testing.T) {
	// The agent's cluster side only dials; the relay process is the one thing that answers, behind the proxy.
	raw, err := os.ReadFile("cluster_member.go")
	if err != nil {
		t.Fatal(err)
	}
	// The lease guard answers one stream this member dialed out for: that is not a listener.
	src := strings.ReplaceAll(string(raw), "s.leaseGuard().Serve(from, c)", "")
	for _, banned := range []string{"net.Listen", "ListenAndServe", "ListenTCP", ".Serve("} {
		if strings.Contains(src, banned) {
			t.Fatalf("cluster_member.go uses %s", banned)
		}
	}
}
