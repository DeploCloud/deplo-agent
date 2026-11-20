package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/miekg/dns"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/dnszone"
)

const (
	checkValue  = "LHDhK3oGRvkiefQnx7OOczTY5Tic_xZ6HcMOc_gmtoM"
	checkValue2 = "AAAAK3oGRvkiefQnx7OOczTY5Tic_xZ6HcMOc_gmtoM"
)

// lookupTXT asks a name server for the TXT records of a name: the values, or "NXDOMAIN" and the like.
func lookupTXT(addr, name string) string {
	c := &dns.Client{Timeout: 500 * time.Millisecond}
	m := new(dns.Msg)
	m.SetQuestion(name, dns.TypeTXT)
	r, _, err := c.Exchange(m, addr)
	if err != nil {
		return "error: " + err.Error()
	}
	if r.Rcode != dns.RcodeSuccess {
		return dns.RcodeToString[r.Rcode]
	}
	var out []string
	for _, rr := range r.Answer {
		out = append(out, strings.Join(rr.(*dns.TXT).Txt, ""))
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// checkPair is two members of one cluster, each answering for its zone on a loopback port, and a's endpoint.
type checkPair struct {
	a, b       *Service
	fpA, fpB   string
	nsA, nsB   string
	endpoint   string
	secret     string
	requestFor func(self string) *pb.SetClusterZoneRequest
}

func zoneOn(s *Service, other string) {
	s.zoneHeard = func() (map[string]time.Time, time.Time) {
		return map[string]time.Time{other: time.Now()}, time.Time{}
	}
	s.zoneConfig = func(c *dnszone.Config) {
		c.Port = 0
		c.Addresses = func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("127.0.0.1")} }
	}
}

func newCheckPair(t *testing.T) *checkPair {
	t.Helper()
	a, b, fpB := twoMembers(t, newImageHost(), newImageHost())
	id, err := a.clusterIdentity()
	if err != nil {
		t.Fatal(err)
	}
	p := &checkPair{a: a, b: b, fpA: id.Fingerprint, fpB: fpB}
	zoneOn(a, p.fpB)
	zoneOn(b, p.fpA)
	t.Cleanup(func() { stopZones(a); stopZones(b); a.challenge().ensure(false, time.Now()) })
	p.requestFor = func(self string) *pb.SetClusterZoneRequest {
		return &pb.SetClusterZoneRequest{
			ClusterId: "cl1", SelfFingerprint: self,
			Zones: []*pb.ClusterZone{{Name: "go.example.com", Names: []string{"app.go.example.com", "shop-example-com.go.example.com"}}},
			Servers: []*pb.ClusterZoneServer{
				{Fingerprint: p.fpA, Addresses: []string{"203.0.113.1"}},
				{Fingerprint: p.fpB, Addresses: []string{"203.0.113.2"}},
			},
			NameServers: []string{p.fpA, p.fpB}, DropAfterSeconds: 5,
			Challenges: []*pb.ClusterZoneChallenge{
				{Asked: "_acme-challenge.go.example.com", At: "_acme-challenge.go.example.com", OwnerFingerprint: p.fpA},
				{Asked: "_acme-challenge.app.go.example.com", At: "_acme-challenge.app.go.example.com", OwnerFingerprint: p.fpB},
				{Asked: "_acme-challenge.shop.example.com", At: "_acme-challenge.shop-example-com.go.example.com", OwnerFingerprint: p.fpA},
			},
		}
	}
	p.nsA = listening(t, setZone(t, a, p.requestFor(p.fpA)))
	p.nsB = listening(t, setZone(t, b, p.requestFor(p.fpB)))
	for _, s := range []*Service{a, b} {
		settled(s)
		s.zone.mu.Lock()
		s.zone.pushLocked(time.Now())
		s.zone.mu.Unlock()
	}
	for _, ns := range []string{p.nsA, p.nsB} {
		until(t, 3*time.Second, "203.0.113.1 203.0.113.2", func() string { return lookup(ns, "app.go.example.com.") })
	}
	// The proxy's secret, as the agent makes it before the proxy starts.
	if err := ensureChallengeSecret(a.challengeSecretPath()); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(a.challengeSecretPath())
	p.secret = string(raw)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.endpoint = "http://" + ln.Addr().String()
	a.chalListen = func() (net.Listener, uint64, error) { return ln, 1, nil }
	a.challenge().ensure(true, time.Now())
	return p
}

// call is the proxy's HTTP-request DNS provider calling the endpoint.
func (p *checkPair) call(t *testing.T, path, fqdn, value, password string) int {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"fqdn": fqdn, "value": value})
	req, _ := http.NewRequest(http.MethodPost, p.endpoint+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if password != "" {
		req.SetBasicAuth(challengeUser, password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestChallenge_aRecordSetOnOneServerIsServedByAnotherWithinASecond(t *testing.T) {
	p := newCheckPair(t)
	const name = "_acme-challenge.go.example.com."
	if got := lookupTXT(p.nsB, name); got != "NXDOMAIN" {
		t.Fatalf("before the check: %s", got)
	}
	start := time.Now()
	if code := p.call(t, "/present", name, checkValue, p.secret); code != http.StatusOK {
		t.Fatalf("present: %d", code)
	}
	until(t, time.Second, checkValue, func() string { return lookupTXT(p.nsB, name) })
	if took := time.Since(start); took > time.Second {
		t.Fatalf("it took %v", took)
	}
	if got := lookupTXT(p.nsA, name); got != checkValue {
		t.Fatalf("its own name server: %s", got)
	}
	// The zone's own certificate is checked twice at one name: both values stand together, and each goes alone.
	if code := p.call(t, "/present", name, checkValue2, p.secret); code != http.StatusOK {
		t.Fatalf("second present: %d", code)
	}
	until(t, time.Second, checkValue2+" "+checkValue, func() string { return lookupTXT(p.nsB, name) })
	if code := p.call(t, "/cleanup", name, checkValue, p.secret); code != http.StatusOK {
		t.Fatalf("cleanup: %d", code)
	}
	until(t, time.Second, checkValue2, func() string { return lookupTXT(p.nsB, name) })
	p.call(t, "/cleanup", name, checkValue2, p.secret)
	until(t, time.Second, "NXDOMAIN", func() string { return lookupTXT(p.nsB, name) })
	until(t, time.Second, "NXDOMAIN", func() string { return lookupTXT(p.nsA, name) })
}

func TestChallenge_aNameOutsideTheZoneIsAnsweredWhereItsCNAMEPoints(t *testing.T) {
	p := newCheckPair(t)
	if code := p.call(t, "/present", "_acme-challenge.shop.example.com.", checkValue, p.secret); code != http.StatusOK {
		t.Fatalf("present: %d", code)
	}
	until(t, time.Second, checkValue, func() string { return lookupTXT(p.nsB, "_acme-challenge.shop-example-com.go.example.com.") })
	if got := lookupTXT(p.nsB, "_acme-challenge.shop.example.com."); got != "REFUSED" {
		t.Fatalf("the name outside the zone itself: %s", got)
	}
}

func TestChallenge_refusesAnyCallerButTheProxyAndAnyNameButItsOwnChecks(t *testing.T) {
	p := newCheckPair(t)
	for _, c := range []struct {
		why, fqdn, value, password string
		want                       int
	}{
		{"no secret", "_acme-challenge.go.example.com.", checkValue, "", http.StatusUnauthorized},
		{"a wrong secret", "_acme-challenge.go.example.com.", checkValue, strings.Repeat("0", 64), http.StatusUnauthorized},
		{"a name outside the zone", "_acme-challenge.other.org.", checkValue, p.secret, http.StatusForbidden},
		{"a name that is not a check", "app.go.example.com.", checkValue, p.secret, http.StatusForbidden},
		{"a check of the zone with no domain", "_acme-challenge.nothing.go.example.com.", checkValue, p.secret, http.StatusForbidden},
		{"another member's check", "_acme-challenge.app.go.example.com.", checkValue, p.secret, http.StatusForbidden},
		{"a value that is not a check's", "_acme-challenge.go.example.com.", "not a value", p.secret, http.StatusBadRequest},
	} {
		if got := p.call(t, "/present", c.fqdn, c.value, c.password); got != c.want {
			t.Errorf("%s: %d, want %d", c.why, got, c.want)
		}
	}
	time.Sleep(200 * time.Millisecond)
	for _, ns := range []string{p.nsA, p.nsB} {
		for _, n := range []string{"_acme-challenge.go.example.com.", "_acme-challenge.app.go.example.com.", "_acme-challenge.nothing.go.example.com."} {
			if got := lookupTXT(ns, n); got != "NXDOMAIN" {
				t.Errorf("%s at %s: %s", n, ns, got)
			}
		}
	}
}

func TestChallenge_aMemberTakesOnlyTheChecksItsSenderOwnsInThatCluster(t *testing.T) {
	p := newCheckPair(t)
	// Another cluster's zone on b, whose check names a as its owner.
	other := &pb.SetClusterZoneRequest{ClusterId: "cl2", SelfFingerprint: p.fpB,
		Zones:       []*pb.ClusterZone{{Name: "other.example.net", Names: []string{"x.other.example.net"}}},
		Servers:     []*pb.ClusterZoneServer{{Fingerprint: p.fpA, Addresses: []string{"203.0.113.1"}}, {Fingerprint: p.fpB, Addresses: []string{"203.0.113.2"}}},
		NameServers: []string{p.fpB}, DropAfterSeconds: 5,
		Challenges: []*pb.ClusterZoneChallenge{{Asked: "_acme-challenge.x.other.example.net", At: "_acme-challenge.x.other.example.net", OwnerFingerprint: p.fpA}},
	}
	setZone(t, p.b, other)
	z := p.b.zones()
	z.mu.Lock()
	defer z.mu.Unlock()
	body := func(name string) txtBody {
		return txtBody{Seq: time.Now().UnixNano(), TXT: map[string][]string{name: {checkValue}}}
	}
	for _, c := range []struct {
		why, cluster, sender, name string
	}{
		{"another cluster's domain", "cl1", p.fpA, "_acme-challenge.x.other.example.net."},
		{"a check another member owns", "cl1", p.fpA, "_acme-challenge.app.go.example.com."},
		{"a sender outside the cluster", "cl1", strings.Repeat("e", 64), "_acme-challenge.go.example.com."},
		{"a cluster it does not answer for", "cl9", p.fpA, "_acme-challenge.go.example.com."},
		{"a name that is not a check", "cl1", p.fpA, "app.go.example.com."},
	} {
		if err := z.takeTXTLocked(c.cluster, c.sender, body(c.name), time.Now()); err == nil {
			t.Errorf("%s was taken", c.why)
		}
	}
	if err := z.takeTXTLocked("cl1", p.fpA, body("_acme-challenge.go.example.com."), time.Now()); err != nil {
		t.Fatal(err)
	}
	// An older set arriving after a newer one changes nothing.
	old := txtBody{Seq: 1, TXT: map[string][]string{}}
	if err := z.takeTXTLocked("cl1", p.fpA, old, time.Now()); err != nil || len(z.txt["cl1"][p.fpA]) != 1 {
		t.Fatalf("an older set replaced a newer one: %v", err)
	}
}

func TestChallenge_aCheckAMemberRefusesFailsAndIsTakenBack(t *testing.T) {
	p := newCheckPair(t)
	// Deplo told b that the zone's certificate is now b's to renew; a has not heard yet.
	req := p.requestFor(p.fpB)
	req.Challenges[0].OwnerFingerprint = p.fpB
	setZone(t, p.b, req)
	if code := p.call(t, "/present", "_acme-challenge.go.example.com.", checkValue, p.secret); code != http.StatusConflict {
		t.Fatalf("present: %d", code)
	}
	time.Sleep(200 * time.Millisecond)
	if got := lookupTXT(p.nsA, "_acme-challenge.go.example.com."); got != "NXDOMAIN" {
		t.Fatalf("a kept the refused record: %s", got)
	}
}

func TestChallenge_aMemberThatCannotBeReachedDoesNotHoldACheckUp(t *testing.T) {
	p := newCheckPair(t)
	relayOf[p.a].Refuse(true)
	old := txtWait
	txtWait = 300 * time.Millisecond
	t.Cleanup(func() { txtWait = old; relayOf[p.a].Refuse(false) })
	if code := p.call(t, "/present", "_acme-challenge.go.example.com.", checkValue, p.secret); code != http.StatusOK {
		t.Fatalf("present: %d", code)
	}
	if got := lookupTXT(p.nsA, "_acme-challenge.go.example.com."); got != checkValue {
		t.Fatalf("its own name server: %s", got)
	}
}

// The endpoint's socket is made inside the proxy's network: nothing outside it reaches the port. Root only.
func TestChallenge_listensOnlyInTheProxysNetwork(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root, as the agent runs: CI runs it with sudo")
	}
	proxy := exec.Command("unshare", "--net", "sh", "-c", "ip link set lo up; sleep 30")
	if err := proxy.Start(); err != nil {
		t.Skipf("no unshare here: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Process.Kill(); _ = proxy.Wait() })
	tenant := exec.Command("unshare", "--net", "sh", "-c", "ip link set lo up; sleep 30")
	if err := tenant.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tenant.Process.Kill(); _ = tenant.Wait() })
	own, _ := os.Readlink("/proc/self/ns/net")
	for _, p := range []*exec.Cmd{proxy, tenant} {
		eventually(t, "a network of its own", func() bool {
			ns, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/net", p.Process.Pid))
			return err == nil && ns != own
		})
	}
	ln, err := listenInNetwork(proxy.Process.Pid, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	addr := ln.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if c, err := dialInNetwork(ctx, proxy.Process.Pid, addr); err != nil {
		t.Fatalf("the proxy itself cannot reach it: %v", err)
	} else {
		c.Close()
	}
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c.Close()
		t.Fatal("the host's own network reaches it")
	}
	if c, err := dialInNetwork(ctx, tenant.Process.Pid, addr); err == nil {
		c.Close()
		t.Fatal("another container's network reaches it")
	}
	a, _ := os.Readlink(fmt.Sprintf("/proc/%d/ns/net", proxy.Process.Pid))
	if ns, err := netnsOf(proxy.Process.Pid); err != nil || !strings.Contains(a, fmt.Sprint(ns)) {
		t.Fatalf("the proxy's network is %q, read as %d (%v)", a, ns, err)
	}
}

// zoneCertHost is a proxy host that answers for go.example.com, the zone whose certificate it renews or holds.
func zoneCertHost(t *testing.T) *proxyHost {
	t.Helper()
	h := newProxyHost(t)
	zoneOn(h.s, zfp1)
	h.s.zoneConfig = func(c *dnszone.Config) {
		c.Port = 0
		c.Addresses = func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("127.0.0.1")} }
	}
	m := bootstrap.Paths(h.s.agentDir)
	for _, p := range []string{m.CertPath, m.KeyPath, m.CAPath} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { stopZones(h.s) })
	return h
}

func certRequest(cert *pb.ClusterZoneCertificate) *pb.SetClusterZoneRequest {
	req := zoneRequest()
	req.Challenges = []*pb.ClusterZoneChallenge{{Asked: "_acme-challenge.go.example.com", At: "_acme-challenge.go.example.com", OwnerFingerprint: zfp0}}
	if cert != nil {
		cert.Zone = "go.example.com"
		req.Certificates = []*pb.ClusterZoneCertificate{cert}
	}
	return req
}

func routesOf(t *testing.T, h *proxyHost) answerRoutes {
	t.Helper()
	raw, err := os.ReadFile(h.routes)
	if err != nil {
		t.Fatal(err)
	}
	var r answerRoutes
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func orderRouter(r answerRoutes) *answerHTTPRouter {
	if r.HTTP == nil {
		return nil
	}
	if rt, ok := r.HTTP.Routers[zoneRouterName("go.example.com")]; ok {
		return &rt
	}
	return nil
}

func zoneNames() []string { return []string{"go.example.com", "*.go.example.com"} }

func TestZoneCert_aRenewerOrdersOnlyWhenNoValidCertificateExists(t *testing.T) {
	h := zoneCertHost(t)
	// No certificate anywhere: the router that orders it appears, on the internal entrypoint alone.
	resp := setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Order: true}))
	rt := orderRouter(routesOf(t, h))
	if rt == nil || rt.TLS == nil || rt.TLS.CertResolver != zoneResolver || !slices.Equal(rt.EntryPoints, []string{answerEntrypoint}) ||
		rt.TLS.Domains[0].Main != "go.example.com" || !slices.Equal(rt.TLS.Domains[0].SANs, []string{"*.go.example.com"}) {
		t.Fatalf("the order router: %+v", rt)
	}
	if st := resp.GetCertificates(); len(st) != 1 || !st[0].GetOrdering() || st[0].GetSha256() != "" {
		t.Fatalf("reported %v", st)
	}

	// A valid one exists and this server takes the renewal over: it gets the pair and orders nothing until the pair is
	// in its store, which the next start of its proxy makes.
	c, k := testPair(t, time.Now().Add(48*time.Hour), zoneNames()...)
	pc := mustCert(t, c, k)
	setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Sha256: pc.Sum, CertPem: c, KeyPem: k}))
	r := routesOf(t, h)
	if orderRouter(r) != nil {
		t.Fatal("the router orders beside a valid certificate")
	}
	if r.TLS == nil || len(r.TLS.Certificates) != 1 {
		t.Fatal("the pair is not served meanwhile")
	}
	plan := h.s.forwarders().storePlan(context.Background())
	if len(plan.keep) != 1 || !sameNames(plan.keep[0].Names, zoneNames()) {
		t.Fatalf("the next start keeps %v", plan.keep)
	}
	if plan.resolverOf(plan.keep[0], proxyACME{Resolver: "letsencrypt", Resolvers: map[string]bool{zoneResolver: true}}) != zoneResolver {
		t.Fatal("a pair taken over would be renewed over HTTP")
	}
	if _, err := h.s.startProxy(context.Background(), h.s.traefikCompose(), true); err != nil {
		t.Fatal(err)
	}
	if got := h.storeSums(t); !slices.Equal(got, []string{pc.Sum}) {
		t.Fatalf("the store holds %v", got)
	}
	h.s.refreshZoneRoutes()
	if orderRouter(routesOf(t, h)) == nil {
		t.Fatal("the router is not back once the store holds the certificate")
	}
	st, _ := readACMEStore(h.store)
	if len(st.entries(zoneResolver)) != 0 {
		t.Fatal("this proxy has no resolver of that name, yet the pair went under it")
	}
}

func TestZoneCert_aServerThatOnlyHoldsItServesItAsAFileAndDropsItsOwnEntry(t *testing.T) {
	h := zoneCertHost(t)
	c, k := testPair(t, time.Now().Add(48*time.Hour), zoneNames()...)
	pc := mustCert(t, c, k)
	// It renewed it before: its store holds it. Now another server does.
	h.putInStore(t, c, k, zoneNames()...)
	appC, appK := testPair(t, time.Now().Add(48*time.Hour), "app.go.example.com")
	h.putInStore(t, appC, appK, "app.go.example.com")
	resp := setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Sha256: pc.Sum, Covered: []string{"app.go.example.com"}}))
	if orderRouter(routesOf(t, h)) != nil {
		t.Fatal("a holder orders")
	}
	if st := resp.GetCertificates(); len(st) != 1 || st[0].GetSha256() != pc.Sum || st[0].GetKeyPem() != "" {
		t.Fatalf("reported %v", st)
	}
	plan := h.s.forwarders().storePlan(context.Background())
	for _, e := range h.s.forwarders().storeCerts() {
		if !plan.drops(e) {
			t.Fatalf("%v stays in the store", e.Names)
		}
	}
	if _, err := h.s.startProxy(context.Background(), h.s.traefikCompose(), true); err != nil {
		t.Fatal(err)
	}
	if got := h.storeSums(t); len(got) != 0 {
		t.Fatalf("the store still holds %v", got)
	}
	r := routesOf(t, h)
	if r.TLS == nil || len(r.TLS.Certificates) != 1 || mustCert(t, r.TLS.Certificates[0].CertFile, r.TLS.Certificates[0].KeyFile).Sum != pc.Sum {
		t.Fatal("the zone's certificate is not served from the routes file")
	}
}

func TestZoneCert_theRenewerKeepsItsEntryWhateverRunsElsewhere(t *testing.T) {
	h := zoneCertHost(t)
	c, k := testPair(t, time.Now().Add(48*time.Hour), zoneNames()...)
	pc := mustCert(t, c, k)
	h.putInStore(t, c, k, zoneNames()...)
	// The shop App runs on another member: its hostnames' entries go, never the zone's.
	h.runs["prj1"] = false
	h.placedOn(t, "srv_b")
	setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Sha256: pc.Sum, Covered: []string{"go.example.com"}}))
	plan := h.s.forwarders().storePlan(context.Background())
	if plan.drops(pc) {
		t.Fatal("the renewer would drop the zone's certificate")
	}
	if orderRouter(routesOf(t, h)) == nil {
		t.Fatal("no router keeps the renewer's certificate")
	}
}

func TestZoneCert_aNewerPairGoesBackToDeploWithItsKey(t *testing.T) {
	h := zoneCertHost(t)
	oldC, oldK := testPair(t, time.Now().Add(time.Hour), zoneNames()...)
	newC, newK := testPair(t, time.Now().Add(90*24*time.Hour), zoneNames()...)
	h.putInStore(t, newC, newK, zoneNames()...)
	resp := setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Sha256: mustCert(t, oldC, oldK).Sum}))
	st := resp.GetCertificates()
	if len(st) != 1 || st[0].GetSha256() != mustCert(t, newC, newK).Sum || st[0].GetKeyPem() != newK {
		t.Fatalf("reported %v", st)
	}
	resp = setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Sha256: mustCert(t, newC, newK).Sum}))
	if resp.GetCertificates()[0].GetKeyPem() != "" {
		t.Fatal("the key went back although Deplo holds it")
	}
}

func TestZoneCert_aFailedOrderIsSaidAndTriedAgainLater(t *testing.T) {
	h := zoneCertHost(t)
	line := "\x1b[90m2026-11-20T18:00:00Z\x1b[0m \x1b[31mERR\x1b[0m Unable to obtain ACME certificate for domains " +
		"\x1b[36mACME CA=\x1b[0mhttps://pebble:14000/dir \x1b[36mdomains=\x1b[0m[\"go.example.com\",\"*.go.example.com\"] " +
		"\x1b[36merror=\x1b[0m\"unable to generate a certificate for the domains [go.example.com *.go.example.com]: " +
		"acme: error: 400 :: urn:ietf:params:acme:error:dns :: NXDOMAIN looking up TXT for _acme-challenge.go.example.com\" " +
		"\x1b[36mproviderName=\x1b[0mdeplo-dns.acme \x1b[36mrouterName=\x1b[0mdeplo-zone@file"
	logs := ""
	h.s.proxyLogs = func(context.Context, time.Time) (string, error) { return logs, nil }
	setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Order: true}))
	now := time.Now()
	h.s.zones().mu.Lock()
	h.s.zones().tended = time.Time{}
	h.s.zones().mu.Unlock()
	logs = line
	h.s.tendZoneCerts(now.Add(time.Minute))
	resp := setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Order: true}))
	st := resp.GetCertificates()[0]
	if !strings.Contains(st.GetError(), "urn:ietf:params:acme:error:dns") || st.GetOrdering() {
		t.Fatalf("reported %v", st)
	}
	if orderRouter(routesOf(t, h)) != nil {
		t.Fatal("the router stays while the order waits to be tried again")
	}
	// The wait over, the router is back and the proxy orders again.
	h.s.zones().mu.Lock()
	h.s.zones().certs["go.example.com"].retryAt = time.Now().Add(-time.Second)
	h.s.zones().publishViewLocked()
	h.s.zones().mu.Unlock()
	h.s.refreshZoneRoutes()
	if orderRouter(routesOf(t, h)) == nil {
		t.Fatal("the router did not come back")
	}
	if msg, ok := zoneOrderError(line, "other.example.com"); ok {
		t.Fatalf("another zone's error was read: %s", msg)
	}
}

func TestZoneCert_aCertificateThatIsNotTheZonesIsRefused(t *testing.T) {
	c, k := testPair(t, time.Now().Add(48*time.Hour), "app.go.example.com")
	pc := mustCert(t, c, k)
	req := certRequest(&pb.ClusterZoneCertificate{Sha256: pc.Sum, CertPem: c, KeyPem: k})
	if _, err := validateZone(req); err == nil {
		t.Fatal("a certificate of one name was taken as the zone's")
	}
	req = certRequest(&pb.ClusterZoneCertificate{Covered: []string{"elsewhere.org"}})
	if _, err := validateZone(req); err == nil {
		t.Fatal("a name outside the zone was taken as covered")
	}
	req = certRequest(nil)
	req.Challenges[0].At = "_acme-challenge.elsewhere.org"
	if _, err := validateZone(req); err == nil {
		t.Fatal("a check answered outside the zones was taken")
	}
	req = certRequest(nil)
	req.Challenges[0].Asked = "app.go.example.com"
	if _, err := validateZone(req); err == nil {
		t.Fatal("a check that is not one was taken")
	}
}

func TestZoneCert_theKeyIsNeverLoggedNorKeptInTheZoneFile(t *testing.T) {
	h := zoneCertHost(t)
	c, k := testPair(t, time.Now().Add(48*time.Hour), zoneNames()...)
	pc := mustCert(t, c, k)
	out := captureStderr(t, func() {
		setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Sha256: pc.Sum, CertPem: c, KeyPem: k}))
	})
	raw, _ := os.ReadFile(filepath.Join(h.s.agentDir, "cluster-zone.json"))
	for what, where := range map[string]string{"the agent's log": out, "the zone file": string(raw)} {
		if strings.Contains(where, keyBody(k)) {
			t.Fatalf("the key is in %s", what)
		}
	}
	if st, err := os.Stat(h.routes); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("the routes file holding it is %v (%v)", st.Mode(), err)
	}
}

func TestChallenge_advertised(t *testing.T) {
	if !slices.Contains(Capabilities, "dns.challenge") {
		t.Fatal("Hello does not advertise dns.challenge")
	}
}
