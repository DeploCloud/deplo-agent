package server

import (
	"context"
	"encoding/json"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/dnszone"
)

var (
	zfp0 = strings.Repeat("a", 64)
	zfp1 = strings.Repeat("b", 64)
	zfp2 = strings.Repeat("c", 64)
)

// fakeHeard is the private network as a test says it stands: when each peer was last heard.
type fakeHeard struct {
	mu  sync.Mutex
	at  map[string]time.Time
	off bool
}

func (f *fakeHeard) set(fps ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.at = map[string]time.Time{}
	for _, fp := range fps {
		f.at[fp] = time.Now()
	}
}

func (f *fakeHeard) heard() (map[string]time.Time, time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]time.Time{}
	for fp, at := range f.at {
		out[fp] = at
	}
	return out, time.Time{}
}

// zoneService is a provisioned agent whose name server answers on a loopback port; it returns that address.
func zoneService(t *testing.T, dir string, net *fakeHeard) *Service {
	t.Helper()
	m := bootstrap.Paths(dir)
	for _, p := range []string{m.CertPath, m.KeyPath, m.CAPath} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(dir)
	s.zoneHeard = net.heard
	s.zoneConfig = func(c *dnszone.Config) {
		c.Port = 0
		c.Addresses = func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("127.0.0.1")} }
	}
	t.Cleanup(func() { stopZones(s) })
	return s
}

func stopZones(s *Service) {
	if s.zone == nil {
		return
	}
	s.zone.mu.Lock()
	s.zone.st.Clusters = map[string]*zoneCluster{}
	s.zone.ensureLocked()
	s.zone.mu.Unlock()
}

// settled skips the start-up wait: the network has been heard for as long as the test needs.
func settled(s *Service) {
	s.zone.mu.Lock()
	s.zone.since = time.Now().Add(-time.Hour)
	s.zone.mu.Unlock()
}

func zoneRequest() *pb.SetClusterZoneRequest {
	return &pb.SetClusterZoneRequest{
		ClusterId: "cl1", SelfFingerprint: zfp0,
		Zones: []*pb.ClusterZone{{Name: "go.example.com", Names: []string{"shop-example-com.go.example.com", "go.example.com"}}},
		Servers: []*pb.ClusterZoneServer{
			{Fingerprint: zfp0, Addresses: []string{"203.0.113.1", "2001:db8::1"}},
			{Fingerprint: zfp1, Addresses: []string{"203.0.113.2"}},
			{Fingerprint: zfp2, Addresses: []string{"203.0.113.3"}},
		},
		NameServers: []string{zfp0, zfp1}, DropAfterSeconds: 5,
	}
}

func setZone(t *testing.T, s *Service, req *pb.SetClusterZoneRequest) *pb.SetClusterZoneResponse {
	t.Helper()
	resp, err := s.SetClusterZone(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// lookup asks the name server for the A records of a name: the addresses, or "SERVFAIL" and the like.
func lookup(addr, name string) string {
	c := &dns.Client{Timeout: 500 * time.Millisecond}
	m := new(dns.Msg)
	m.SetQuestion(name, dns.TypeA)
	r, _, err := c.Exchange(m, addr)
	if err != nil {
		return "error: " + err.Error()
	}
	if r.Rcode != dns.RcodeSuccess {
		return dns.RcodeToString[r.Rcode]
	}
	var out []string
	for _, rr := range r.Answer {
		out = append(out, rr.(*dns.A).A.String())
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func until(t *testing.T, within time.Duration, want string, got func() string) {
	t.Helper()
	deadline := time.Now().Add(within)
	last := ""
	for time.Now().Before(deadline) {
		if last = got(); last == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("after %v: %q, want %q", within, last, want)
}

func listening(t *testing.T, resp *pb.SetClusterZoneResponse) string {
	t.Helper()
	if len(resp.GetListening()) != 1 {
		t.Fatalf("listening on %v (%s)", resp.GetListening(), resp.GetProblem())
	}
	return resp.GetListening()[0]
}

func TestZone_answersFollowWhatThisServerHears(t *testing.T) {
	heard := &fakeHeard{}
	heard.set(zfp1, zfp2)
	s := zoneService(t, t.TempDir(), heard)
	resp := setZone(t, s, zoneRequest())
	addr := listening(t, resp)
	settled(s)
	q := func() string { return lookup(addr, "shop-example-com.go.example.com.") }
	until(t, 5*time.Second, "203.0.113.1 203.0.113.2 203.0.113.3", q)
	resp = setZone(t, s, zoneRequest())
	if resp.GetState() != dnszone.StateAnswer || resp.GetHeard() != 3 || len(resp.GetAnswering()) != 3 {
		t.Fatalf("reported %s, %d heard, answering %v", resp.GetState(), resp.GetHeard(), resp.GetAnswering())
	}

	// c falls silent: once the removal time has passed, it leaves the answers. Two of three is still a majority.
	heard.set(zfp1)
	until(t, 3*time.Second, "203.0.113.1 203.0.113.2", q)
	// b too: this server hears one of three, so it says it cannot answer rather than name itself alone.
	heard.set()
	until(t, 3*time.Second, "SERVFAIL", q)
	if r := setZone(t, s, zoneRequest()); r.GetState() != dnszone.StateFail || r.GetHeard() != 1 {
		t.Fatalf("alone: %s, %d heard", r.GetState(), r.GetHeard())
	}
	// Back again, in both orders.
	heard.set(zfp2)
	until(t, 3*time.Second, "203.0.113.1 203.0.113.3", q)
	heard.set(zfp1, zfp2)
	until(t, 3*time.Second, "203.0.113.1 203.0.113.2 203.0.113.3", q)

	// Being emptied takes a server out of the answers while it still counts.
	req := zoneRequest()
	req.Servers[1].Emptying = true
	setZone(t, s, req)
	until(t, 3*time.Second, "203.0.113.1 203.0.113.3", q)
	if got := lookup(addr, "example.org."); got != "REFUSED" {
		t.Fatalf("outside the zone: %s", got)
	}
}

func TestZone_anEvenSplitRepeatsTheLastAgreedSet(t *testing.T) {
	heard := &fakeHeard{}
	heard.set(zfp1, zfp2, strings.Repeat("d", 64))
	s := zoneService(t, t.TempDir(), heard)
	req := zoneRequest()
	req.Servers = append(req.Servers, &pb.ClusterZoneServer{Fingerprint: strings.Repeat("d", 64), Addresses: []string{"203.0.113.4"}})
	addr := listening(t, setZone(t, s, req))
	settled(s)
	q := func() string { return lookup(addr, "shop-example-com.go.example.com.") }
	all := "203.0.113.1 203.0.113.2 203.0.113.3 203.0.113.4"
	until(t, 5*time.Second, all, q)
	// Split two and two: this side cannot tell which half carries on, so it names what all four agreed on.
	heard.set(zfp1)
	time.Sleep(2 * zoneTick)
	until(t, 3*time.Second, all, q)
	if r := setZone(t, s, req); r.GetState() != dnszone.StateStale {
		t.Fatalf("split: %s", r.GetState())
	}
}

func TestZone_answersFromDiskAfterARestartWithDeploDown(t *testing.T) {
	dir := t.TempDir()
	heard := &fakeHeard{}
	heard.set(zfp1, zfp2)
	s := zoneService(t, dir, heard)
	addr := listening(t, setZone(t, s, zoneRequest()))
	settled(s)
	until(t, 5*time.Second, "203.0.113.1 203.0.113.2 203.0.113.3", func() string { return lookup(addr, "shop-example-com.go.example.com.") })
	stopZones(s)
	if got := lookup(addr, "shop-example-com.go.example.com."); !strings.HasPrefix(got, "error") {
		t.Fatalf("still answering after the stop: %s", got)
	}
	// A new run of the agent, hearing nobody yet, with what the last one kept on disk.
	writeZoneFile(t, dir)
	again := zoneService(t, dir, &fakeHeard{})
	again.StartClusterZone()
	addrs, _ := again.zone.sup.Listening()
	if len(addrs) != 1 {
		t.Fatalf("listening on %v", addrs)
	}
	// Just started and hearing nobody yet: it answers what it last agreed on, rather than nothing.
	until(t, 5*time.Second, "203.0.113.1 203.0.113.2 203.0.113.3", func() string { return lookup(addrs[0], "shop-example-com.go.example.com.") })
}

// writeZoneFile is what an agent keeps on disk for cl1 once it has agreed on all three servers.
func writeZoneFile(t *testing.T, dir string) {
	t.Helper()
	c, err := validateZone(zoneRequest())
	if err != nil {
		t.Fatal(err)
	}
	c.Serial = 1
	c.Last = &dnszone.Agreed{Servers: []string{zfp0, zfp1, zfp2}, Addrs: dnszone.Addrs{V4: []string{"203.0.113.1", "203.0.113.2", "203.0.113.3"}}}
	raw, _ := json.Marshal(zoneFile{Clusters: map[string]*zoneCluster{"cl1": c}})
	if err := os.WriteFile(filepath.Join(dir, "cluster-zone.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestZone_aTakenPortIsReportedNotACrash(t *testing.T) {
	taken, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	s := zoneService(t, t.TempDir(), &fakeHeard{})
	port := taken.LocalAddr().(*net.UDPAddr).Port
	inner := s.zoneConfig
	s.zoneConfig = func(c *dnszone.Config) { inner(c); c.Port = port }
	resp := setZone(t, s, zoneRequest())
	if len(resp.GetListening()) != 0 || !strings.Contains(resp.GetProblem(), "taken by another program") {
		t.Fatalf("listening %v, problem %q", resp.GetListening(), resp.GetProblem())
	}
}

func TestZone_noZonesStopsAnswering(t *testing.T) {
	heard := &fakeHeard{}
	heard.set(zfp1, zfp2)
	s := zoneService(t, t.TempDir(), heard)
	addr := listening(t, setZone(t, s, zoneRequest()))
	settled(s)
	until(t, 5*time.Second, "203.0.113.1 203.0.113.2 203.0.113.3", func() string { return lookup(addr, "shop-example-com.go.example.com.") })
	resp := setZone(t, s, &pb.SetClusterZoneRequest{ClusterId: "cl1"})
	if len(resp.GetListening()) != 0 || s.zone.sup != nil {
		t.Fatalf("still listening on %v", resp.GetListening())
	}
	if got := lookup(addr, "shop-example-com.go.example.com."); !strings.HasPrefix(got, "error") {
		t.Fatalf("still answering: %s", got)
	}
	raw, _ := os.ReadFile(filepath.Join(s.agentDir, "cluster-zone.json"))
	if strings.Contains(string(raw), "go.example.com") {
		t.Fatalf("the zone is still on disk: %s", raw)
	}
}

func TestZone_refusesATableThatDoesNotHoldTogether(t *testing.T) {
	bad := map[string]func(*pb.SetClusterZoneRequest){
		"no cluster":           func(r *pb.SetClusterZoneRequest) { r.ClusterId = "" },
		"self not a server":    func(r *pb.SetClusterZoneRequest) { r.SelfFingerprint = strings.Repeat("e", 64) },
		"a bad fingerprint":    func(r *pb.SetClusterZoneRequest) { r.Servers[1].Fingerprint = "nope" },
		"a server twice":       func(r *pb.SetClusterZoneRequest) { r.Servers[1].Fingerprint = zfp0 },
		"a loopback address":   func(r *pb.SetClusterZoneRequest) { r.Servers[1].Addresses = []string{"127.0.0.1"} },
		"not an address":       func(r *pb.SetClusterZoneRequest) { r.Servers[1].Addresses = []string{"shop.example.com"} },
		"a TTL of 5 s":         func(r *pb.SetClusterZoneRequest) { r.AnswerTtlSeconds = 5 },
		"a TTL of 301 s":       func(r *pb.SetClusterZoneRequest) { r.AnswerTtlSeconds = 301 },
		"a removal of 4 s":     func(r *pb.SetClusterZoneRequest) { r.DropAfterSeconds = 4 },
		"a removal of 61 s":    func(r *pb.SetClusterZoneRequest) { r.DropAfterSeconds = 61 },
		"a stranger as NS":     func(r *pb.SetClusterZoneRequest) { r.NameServers = []string{strings.Repeat("e", 64)} },
		"five name servers":    func(r *pb.SetClusterZoneRequest) { r.NameServers = []string{zfp0, zfp1, zfp2, zfp0, zfp1} },
		"a name outside":       func(r *pb.SetClusterZoneRequest) { r.Zones[0].Names = []string{"shop.example.org"} },
		"a zone that is not":   func(r *pb.SetClusterZoneRequest) { r.Zones[0].Name = "go example" },
		"the same zone twice":  func(r *pb.SetClusterZoneRequest) { r.Zones = append(r.Zones, r.Zones[0]) },
		"no server at all":     func(r *pb.SetClusterZoneRequest) { r.Servers = nil },
		"nine addresses":       func(r *pb.SetClusterZoneRequest) { r.Servers[1].Addresses = slices.Repeat([]string{"203.0.113.9"}, 9) },
		"a link-local address": func(r *pb.SetClusterZoneRequest) { r.Servers[1].Addresses = []string{"fe80::1"} },
	}
	for what, mutate := range bad {
		req := zoneRequest()
		mutate(req)
		if _, err := validateZone(req); err == nil {
			t.Errorf("%s: accepted", what)
		}
	}
	c, err := validateZone(zoneRequest())
	if err != nil || c.TTL != zoneDefaultTTL || c.DropAfter != 5 || c.Zones[0].Name != "go.example.com." {
		t.Fatalf("%+v %v", c, err)
	}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if _, err := s.SetClusterZone(context.Background(), zoneRequest()); err == nil {
		t.Fatal("an agent with no certificate took a zone")
	}
}

func TestZone_helloSaysItCanAnswerForAZone(t *testing.T) {
	if !slices.Contains(Capabilities, "dns.zone") {
		t.Fatal("Hello does not say dns.zone")
	}
}

// The request the panel renders (lib/deploy/cluster-zone.golden.json in DeploCloud/deplo), kept in testdata too.
func readZoneReference(t *testing.T, path string) *pb.SetClusterZoneRequest {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var req pb.SetClusterZoneRequest
	if err := protojson.Unmarshal(raw, &req); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return &req
}

func TestZone_thePanelsRequestPasses(t *testing.T) {
	req := readZoneReference(t, filepath.Join("testdata", "cluster-zone.golden.json"))
	if _, err := validateZone(req); err != nil {
		t.Fatalf("this agent refuses the request in testdata: %v", err)
	}
	repo := os.Getenv("DEPLO_REPO")
	if repo == "" {
		repo = filepath.Join("..", "..", "..", "deplo")
	}
	path := filepath.Join(repo, "lib", "deploy", "cluster-zone.golden.json")
	if _, err := os.Stat(path); err != nil {
		t.Skip("no panel reference beside this repo (set DEPLO_REPO)")
	}
	if _, err := validateZone(readZoneReference(t, path)); err != nil {
		t.Fatalf("this agent refuses what the panel renders today: %v", err)
	}
}
