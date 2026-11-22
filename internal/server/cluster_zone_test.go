package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/dnszone"
	"github.com/DeploCloud/deplo-agent/internal/dnszone/geotest"
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
	// Just started and hearing nobody yet: it answers what it last agreed on, rather than nothing, but for itself:
	// for the removal time a server just started is named by nobody, itself included.
	until(t, 5*time.Second, "203.0.113.2 203.0.113.3", func() string { return lookup(addrs[0], "shop-example-com.go.example.com.") })
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
	if !slices.Contains(Capabilities, "dns.geo") {
		t.Fatal("Hello does not say dns.geo")
	}
}

// placed is zoneRequest with a in Milan, b in New York and c in Tokyo.
func placed() *pb.SetClusterZoneRequest {
	req := zoneRequest()
	for i, p := range []geotest.Place{geotest.Milan, geotest.NewYork, geotest.Tokyo} {
		req.Servers[i].Located, req.Servers[i].Latitude, req.Servers[i].Longitude = true, p.Lat, p.Lon
	}
	return req
}

func geoRequest(t *testing.T, raw []byte) *pb.SetGeoTableRequest {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(raw)
	_ = zw.Close()
	g, err := dnszone.ParseGeo(raw)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.SetGeoTableRequest{TableGz: buf.Bytes(), Sha256: g.Digest()}
}

// lookupFrom asks for the A records as a resolver for a client in subnet does, in the order they come.
func lookupFrom(addr, name, subnet string) string {
	c := &dns.Client{Timeout: 500 * time.Millisecond}
	m := new(dns.Msg)
	m.SetQuestion(name, dns.TypeA)
	m.SetEdns0(1232, false)
	p := netip.MustParsePrefix(subnet)
	m.IsEdns0().Option = append(m.IsEdns0().Option, &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: 1,
		SourceNetmask: uint8(p.Bits()), Address: net.IP(p.Addr().AsSlice())})
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
	return strings.Join(out, " ")
}

func TestZone_aVisitorIsAnsweredWithTheNearestHealthyServers(t *testing.T) {
	heard := &fakeHeard{}
	heard.set(zfp1, zfp2)
	dir := t.TempDir()
	s := zoneService(t, dir, heard)
	resp := setZone(t, s, placed())
	if resp.GetGeoSha256() != "" {
		t.Fatalf("no table yet, and it says %q", resp.GetGeoSha256())
	}
	addr := listening(t, resp)
	settled(s)
	q := func(subnet string) func() string {
		return func() string { return lookupFrom(addr, "shop-example-com.go.example.com.", subnet) }
	}
	// No location table: every healthy server, whoever asks.
	until(t, 5*time.Second, "203.0.113.1 203.0.113.2 203.0.113.3", func() string { return lookup(addr, "shop-example-com.go.example.com.") })

	got, err := s.SetGeoTable(context.Background(), geoRequest(t, geotest.World()))
	if err != nil || got.GetRelease() != 20261101 {
		t.Fatalf("%v %v", got, err)
	}
	until(t, 5*time.Second, "203.0.113.3 203.0.113.1", q(geotest.TokyoV4))
	until(t, time.Second, "203.0.113.1 203.0.113.2", q(geotest.MilanV4))
	until(t, time.Second, "203.0.113.2 203.0.113.1", q(geotest.NewYorkV4))
	if r := setZone(t, s, placed()); r.GetGeoSha256() != got.GetSha256() {
		t.Fatalf("it says it holds %q, not %q", r.GetGeoSha256(), got.GetSha256())
	}

	// Tokyo's server falls silent: health wins, and a visitor in Tokyo gets the next nearest, still two.
	heard.set(zfp1)
	until(t, 3*time.Second, "203.0.113.1 203.0.113.2", q(geotest.TokyoV4))
	heard.set(zfp1, zfp2)
	until(t, 3*time.Second, "203.0.113.3 203.0.113.1", q(geotest.TokyoV4))

	// One server with no place: nothing to rank it by, so every healthy server again.
	req := placed()
	req.Servers[1].Located = false
	setZone(t, s, req)
	until(t, 3*time.Second, "3", func() string { return strconv.Itoa(len(strings.Fields(q(geotest.TokyoV4)()))) })

	// A new run of the agent answers with the table it kept, Deplo down.
	stopZones(s)
	writeZoneFile(t, dir)
	again := zoneService(t, dir, heard)
	resp = setZone(t, again, placed())
	if resp.GetGeoSha256() != got.GetSha256() {
		t.Fatalf("after a restart it holds %q", resp.GetGeoSha256())
	}
	settled(again)
	until(t, 5*time.Second, "203.0.113.3 203.0.113.1", func() string {
		return lookupFrom(listening(t, resp), "shop-example-com.go.example.com.", geotest.TokyoV4)
	})
}

func TestZone_refusesALocationTableThatDoesNotHoldTogether(t *testing.T) {
	s := zoneService(t, t.TempDir(), &fakeHeard{})
	good := geoRequest(t, geotest.World())
	damaged := geotest.World()
	damaged[50] ^= 1
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(damaged)
	_ = zw.Close()
	bad := map[string]*pb.SetGeoTableRequest{
		"not gzip":        {TableGz: geotest.World(), Sha256: good.Sha256},
		"another digest":  {TableGz: good.TableGz, Sha256: strings.Repeat("0", 64)},
		"a damaged table": {TableGz: buf.Bytes(), Sha256: good.Sha256},
		"empty":           {},
	}
	for what, req := range bad {
		if _, err := s.SetGeoTable(context.Background(), req); err == nil {
			t.Errorf("%s: accepted", what)
		}
	}
	if _, err := os.Stat(filepath.Join(s.agentDir, "dns-geo.bin")); err == nil {
		t.Fatal("a refused table was kept")
	}
	if _, err := s.SetGeoTable(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	if raw, _ := os.ReadFile(filepath.Join(s.agentDir, "dns-geo.bin")); string(raw) != string(geotest.World()) {
		t.Fatal("the table on disk is not the one sent")
	}
	for what, mutate := range map[string]func(*pb.SetClusterZoneRequest){
		"a latitude of 91":    func(r *pb.SetClusterZoneRequest) { r.Servers[0].Latitude = 91 },
		"a longitude of -181": func(r *pb.SetClusterZoneRequest) { r.Servers[1].Longitude = -181 },
		"a latitude of NaN":   func(r *pb.SetClusterZoneRequest) { r.Servers[2].Latitude = math.NaN() },
	} {
		req := placed()
		mutate(req)
		if _, err := validateZone(req); err == nil {
			t.Errorf("%s: accepted", what)
		}
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

func TestZone_aNameIsAnsweredWithItsAppsServerFirstWhileItIsHealthy(t *testing.T) {
	heard := &fakeHeard{}
	heard.set(zfp1, zfp2)
	s := zoneService(t, t.TempDir(), heard)
	req := placed()
	req.Zones[0].Homes = []*pb.ClusterZoneHome{{Name: "Shop-example-com.go.example.com", Fingerprint: zfp2}}
	resp := setZone(t, s, req)
	addr := listening(t, resp)
	settled(s)
	if _, err := s.SetGeoTable(context.Background(), geoRequest(t, geotest.World())); err != nil {
		t.Fatal(err)
	}
	shop := func() string { return lookupFrom(addr, "shop-example-com.go.example.com.", geotest.MilanV4) }
	until(t, 5*time.Second, "203.0.113.3 203.0.113.1 203.0.113.2", shop)
	// The zone's other name is load-balanced: nearest first.
	until(t, time.Second, "203.0.113.1 203.0.113.2", func() string {
		return lookupFrom(addr, "go.example.com.", geotest.MilanV4)
	})
	// Its server falls silent: health wins, and the answer is nearest first, still two.
	heard.set(zfp1)
	until(t, 3*time.Second, "203.0.113.1 203.0.113.2", shop)
	heard.set(zfp1, zfp2)
	until(t, 3*time.Second, "203.0.113.3 203.0.113.1 203.0.113.2", shop)
	// The App moved: Deplo pushes the new home.
	req.Zones[0].Homes[0].Fingerprint = zfp1
	setZone(t, s, req)
	until(t, 3*time.Second, "203.0.113.2 203.0.113.1 203.0.113.3", shop)

	for what, mutate := range map[string]func(*pb.SetClusterZoneRequest){
		"a name not in the zone": func(r *pb.SetClusterZoneRequest) { r.Zones[0].Homes[0].Name = "x.go.example.com" },
		"a server not a member":  func(r *pb.SetClusterZoneRequest) { r.Zones[0].Homes[0].Fingerprint = strings.Repeat("d", 64) },
		"two homes": func(r *pb.SetClusterZoneRequest) {
			r.Zones[0].Homes = append(r.Zones[0].Homes, &pb.ClusterZoneHome{Name: "shop-example-com.go.example.com", Fingerprint: zfp0})
		},
	} {
		bad := placed()
		bad.Zones[0].Homes = []*pb.ClusterZoneHome{{Name: "shop-example-com.go.example.com", Fingerprint: zfp2}}
		mutate(bad)
		if _, err := validateZone(bad); err == nil {
			t.Errorf("%s: accepted", what)
		}
	}
}

// eu-1..3 (22 Nov 2026) still held a cluster of the Deplo they came from: the ident each name server answers, and the
// one it tells Deplo, are its own cluster's, never the stale one's, so Deplo's check of port 53 always matches.
func TestZone_theIdentIsThisServersOwnWhateverClustersItHolds(t *testing.T) {
	heard := &fakeHeard{}
	heard.set(zfp1, zfp2)
	s := zoneService(t, t.TempDir(), heard)
	if err := os.WriteFile(s.memberFile(), []byte(`{"member":true,"cluster":"cl1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stale := zoneRequest()
	stale.ClusterId, stale.SelfFingerprint = "cl0", zfp2
	stale.Zones[0].Name, stale.Zones[0].Names = "lab.test", []string{"lab.test"}
	setZone(t, s, stale)
	addr := listening(t, setZone(t, s, zoneRequest()))
	want := serverIdent(zfp0)
	for i := 0; i < 20; i++ {
		resp, err := s.ProveAddresses(context.Background(), &pb.ProveAddressesRequest{})
		if err != nil || resp.GetDnsIdent() != want {
			t.Fatalf("told Deplo %q, want %q (%v)", resp.GetDnsIdent(), want, err)
		}
	}
	until(t, 5*time.Second, want, func() string {
		c := &dns.Client{Timeout: 500 * time.Millisecond}
		m := new(dns.Msg)
		m.SetQuestion("_deplo-server.go.example.com.", dns.TypeTXT)
		r, _, err := c.Exchange(m, addr)
		if err != nil || len(r.Answer) != 1 {
			return fmt.Sprint(err, r)
		}
		return r.Answer[0].(*dns.TXT).Txt[0]
	})
}
