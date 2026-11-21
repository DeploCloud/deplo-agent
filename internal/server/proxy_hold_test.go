package server

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// putUnder writes a pair into the store under resolver, as Traefik keeps it.
func (h *proxyHost) putUnder(t *testing.T, resolver, cert, key string, names ...string) {
	t.Helper()
	st, err := readACMEStore(h.store)
	if err != nil {
		t.Fatal(err)
	}
	var e acmeEntry
	e.Domain.Main, e.Domain.SANs = names[0], names[1:]
	e.Certificate, e.Key, e.Store = []byte(cert), []byte(key), "default"
	st.setEntries(resolver, append(st.entries(resolver), e))
	if err := st.write(h.store); err != nil {
		t.Fatal(err)
	}
}

func (h *proxyHost) storeNames(t *testing.T, resolver string) [][]string {
	t.Helper()
	st, err := readACMEStore(h.store)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]string
	for _, e := range st.entries(resolver) {
		out = append(out, mustCert(t, string(e.Certificate), string(e.Key)).Names)
	}
	return out
}

// renewerBeforeACut is the zone's renewer just before its power went: the zone's pair and the shop App's in its
// store, both inside their renewal window, the order router in its routes file.
func renewerBeforeACut(t *testing.T) (*proxyHost, proxyCert, proxyCert) {
	t.Helper()
	h := zoneCertHost(t)
	compose := strings.Replace(proxyCompose, "    volumes:\n",
		"      - --certificatesresolvers.deplo-dns.acme.storage=/acme/acme.json\n    volumes:\n", 1)
	if err := os.WriteFile(h.s.traefikCompose(), []byte(compose), 0o600); err != nil {
		t.Fatal(err)
	}
	zc, zk := testPair(t, time.Now().Add(10*time.Minute), zoneNames()...)
	sc, sk := testPair(t, time.Now().Add(10*time.Minute), shopNames...)
	h.putUnder(t, zoneResolver, zc, zk, zoneNames()...)
	h.putUnder(t, "letsencrypt", sc, sk, shopNames...)
	own, ownK := testPair(t, time.Now().Add(10*time.Minute), "own.example.com")
	h.putUnder(t, "letsencrypt", own, ownK, "own.example.com")
	setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Order: true, Sha256: mustCert(t, zc, zk).Sum}))
	if orderRouter(routesOf(t, h)) == nil {
		t.Fatal("the renewer has no order router to begin with")
	}
	stopZones(h.s)
	return h, mustCert(t, zc, zk), mustCert(t, sc, sk)
}

// backOn is the same server's agent started again on the same directory, after the hold ran: nothing heard yet, and
// a copy of the shop App that Docker started again running here.
func backOn(t *testing.T, h *proxyHost) *proxyHost {
	t.Helper()
	s := newTestService(t)
	s.SetAgentDir(h.s.agentDir)
	back := &proxyHost{s: s, store: h.store, routes: h.routes, runs: h.runs}
	s.fwdOps, s.zoneConfig, s.zoneHeard = h.s.fwdOps, h.s.zoneConfig, h.s.zoneHeard
	s.traefikApply = func(context.Context, string, bool) error { back.applied++; return nil }
	s.forwarders().st = h.s.forwarders().st
	s.zones()
	t.Cleanup(func() { stopZones(s) })
	return back
}

func TestProxyHold_aServerBackFromAPowerCutRenewsNothingItMayNoLongerOwn(t *testing.T) {
	h, zone, shop := renewerBeforeACut(t)
	n, err := ProxyHold(h.s.agentDir)
	if err != nil || n != 2 {
		t.Fatalf("held %d (%v), want the zone's pair and the shop App's", n, err)
	}
	if got := h.storeNames(t, zoneResolver); len(got) != 0 {
		t.Fatalf("the store still renews %v over DNS", got)
	}
	if got := h.storeNames(t, "letsencrypt"); len(got) != 1 || got[0][0] != "own.example.com" {
		t.Fatalf("the store holds %v, want only the certificate of this server's own App", got)
	}
	r := routesOf(t, h)
	if r.TLS == nil || len(r.TLS.Certificates) != 2 {
		t.Fatal("the held pairs are not served from the routes file")
	}
	for k, rt := range r.HTTP.Routers {
		if rt.TLS != nil && rt.TLS.CertResolver != "" {
			t.Fatalf("router %s still orders", k)
		}
	}
	if r.TCP == nil || len(r.TCP.Routers) != 1 {
		t.Fatal("the hold took the passed-on routes with it")
	}
	if n, err := ProxyHold(h.s.agentDir); err != nil || n != 0 {
		t.Fatalf("a second hold moved %d (%v)", n, err)
	}

	// Its agent starts again and hears nothing for a while: it puts nothing back, drops nothing, orders nothing.
	b := backOn(t, h)
	now := time.Now()
	for i := 0; i < 5; i++ {
		at := now.Add(time.Duration(i) * time.Minute)
		b.s.restartForRenewal(context.Background(), at)
		b.s.dropStaleEntries(context.Background(), at)
		b.s.forwarders().pruneFiles(context.Background(), at)
	}
	b.s.refreshZoneRoutes()
	if b.applied != 0 || orderRouter(routesOf(t, b)) != nil {
		t.Fatalf("restarts %d, order router %v: want neither before the cluster speaks", b.applied, orderRouter(routesOf(t, b)))
	}
	if plan := b.s.forwarders().storePlan(context.Background()); len(plan.keep) != 0 {
		t.Fatalf("the next start would put back %v", plan.keep)
	}

	// The cluster moved both while it was away: they stay files, and its proxy is never handed them to renew.
	setZone(t, b.s, certRequest(&pb.ClusterZoneCertificate{Sha256: zone.Sum}))
	b.placedOn(t, "srv_b")
	b.s.restartForRenewal(context.Background(), now.Add(6*time.Minute))
	b.s.dropStaleEntries(context.Background(), now.Add(6*time.Minute))
	if b.applied != 0 || len(b.storeNames(t, zoneResolver)) != 0 || len(b.storeNames(t, "letsencrypt")) != 1 {
		t.Fatal("a pair the cluster moved went back into the store")
	}
	_ = shop
}

func TestProxyHold_theRenewerStillRenewsOnceTheClusterSaysItIsStillItsOwn(t *testing.T) {
	h, zone, shop := renewerBeforeACut(t)
	if _, err := ProxyHold(h.s.agentDir); err != nil {
		t.Fatal(err)
	}
	b := backOn(t, h)
	setZone(t, b.s, certRequest(&pb.ClusterZoneCertificate{Renew: true, Order: true, Sha256: zone.Sum}))
	b.placedOn(t, "srv_a")
	// Both are inside their renewal window: one restart at once puts them back, each under the resolver that renews it.
	b.s.restartForRenewal(context.Background(), time.Now())
	if b.applied != 1 {
		t.Fatalf("restarts %d, want one", b.applied)
	}
	if got := b.storeNames(t, zoneResolver); len(got) != 1 || !sameNames(got[0], zone.Names) {
		t.Fatalf("over DNS the store renews %v", got)
	}
	if got := b.storeNames(t, "letsencrypt"); len(got) != 2 || !slices.ContainsFunc(got, func(n []string) bool { return sameNames(n, shop.Names) }) {
		t.Fatalf("over HTTP the store renews %v", got)
	}
	b.s.refreshZoneRoutes()
	if orderRouter(routesOf(t, b)) == nil {
		t.Fatal("the renewer's order router is not back")
	}
}

// #266: weeks away, the App's certificate expired; a copy Docker started again here must not order a new one.
func TestProxyHold_anExpiredPairStaysWhileAStaleCopyOfItsAppRuns(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(time.Minute), shopNames...)
	h.putUnder(t, "letsencrypt", c, k, shopNames...)
	h.s.refreshZoneRoutes()
	if n, err := ProxyHold(h.s.agentDir); err != nil || n != 1 {
		t.Fatalf("held %d (%v)", n, err)
	}
	b := backOn(t, h)
	later := time.Now().Add(time.Hour)
	b.s.forwarders().pruneFiles(context.Background(), later)
	if len(b.s.forwarders().installedCerts()) != 1 {
		t.Fatal("the file that stops an order went while a stale copy of its App runs")
	}
	b.runs["prj1"] = false
	b.s.forwarders().pruneFiles(context.Background(), later)
	if len(b.s.forwarders().installedCerts()) != 0 {
		t.Fatal("an expired pair stays once nothing here can ask for it")
	}
}

func TestProxyHold_aServerWithNothingTheClusterMovesIsLeftAlone(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(time.Hour), "own.example.com")
	h.putUnder(t, "letsencrypt", c, k, "own.example.com")
	before, _ := os.ReadFile(h.store)
	if n, err := ProxyHold(h.s.agentDir); err != nil || n != 0 {
		t.Fatalf("held %d (%v)", n, err)
	}
	after, _ := os.ReadFile(h.store)
	if string(before) != string(after) {
		t.Fatal("the store was rewritten with nothing to hold")
	}
	if _, err := os.Stat(h.routes); !os.IsNotExist(err) {
		t.Fatal("a routes file appeared with nothing to hold")
	}
	if n, err := ProxyHold(t.TempDir()); err != nil || n != 0 {
		t.Fatalf("a server with no proxy: %d (%v)", n, err)
	}
}

func TestProxyHold_runsOncePerBootBeforeDockerAndNeverHoldsItUp(t *testing.T) {
	u := proxyHoldUnitText("/usr/local/bin/deplo-agent", "/var/lib/deplo-agent")
	for _, want := range []string{"Before=docker.service", "Type=oneshot", "RemainAfterExit=yes", "TimeoutStartSec=30",
		"ExecStart=/usr/local/bin/deplo-agent proxy-hold --agent-dir /var/lib/deplo-agent", "WantedBy=docker.service"} {
		if !strings.Contains(u, want) {
			t.Fatalf("the unit lacks %q:\n%s", want, u)
		}
	}
	if strings.Contains(u, "Requires=") || strings.Contains(u, "RequiredBy=") {
		t.Fatal("a failed hold would keep Docker from starting")
	}
}

// #273: a name whose _acme-challenge CNAME now points into the zone renews over DNS from its next renewal, and back
// over HTTP once the CNAME is gone; neither move restarts the proxy before the quiet hour ahead of the window.
func TestProxyCerts_aStoredPairMovesToTheWayItsNamesAreCheckedNow(t *testing.T) {
	h := zoneCertHost(t)
	compose := strings.Replace(proxyCompose, "    volumes:\n",
		"      - --certificatesresolvers.deplo-dns.acme.storage=/acme/acme.json\n    volumes:\n", 1)
	if err := os.WriteFile(h.s.traefikCompose(), []byte(compose), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.Local)
	c, k := testPair(t, now.Add(48*time.Hour), shopNames...)
	h.putUnder(t, "letsencrypt", c, k, shopNames...)
	req := certRequest(nil)
	for _, n := range shopNames {
		at := "_acme-challenge." + strings.ReplaceAll(n, ".", "-") + ".go.example.com"
		req.Challenges = append(req.Challenges, &pb.ClusterZoneChallenge{Asked: "_acme-challenge." + n, At: at, OwnerFingerprint: zfp0})
	}
	setZone(t, h.s, req)
	if got := h.s.forwarders().storePlan(context.Background()).misfiled; len(got) != 1 {
		t.Fatalf("misfiled %v, want the shop's pair", got)
	}
	deadline := mustCert(t, c, k).NotAfter.Add(-renewBefore(time.Hour))
	at, urgent := restartAt(now, deadline, 4)
	if urgent || !at.After(now) {
		t.Fatalf("the move is due at %v (urgent %v)", at, urgent)
	}
	h.s.restartForRenewal(context.Background(), now)
	h.s.restartForRenewal(context.Background(), at.Add(-time.Minute))
	if h.applied != 0 {
		t.Fatal("the proxy restarted outside the quiet hour before the window")
	}
	h.s.restartForRenewal(context.Background(), at)
	if h.applied != 1 || len(h.storeNames(t, "letsencrypt")) != 0 || len(h.storeNames(t, zoneResolver)) != 1 {
		t.Fatalf("restarts %d, over HTTP %v, over DNS %v", h.applied, h.storeNames(t, "letsencrypt"), h.storeNames(t, zoneResolver))
	}
	h.s.restartForRenewal(context.Background(), at.Add(2*time.Hour))
	if h.applied != 1 {
		t.Fatal("the proxy restarted again with nothing left to move")
	}

	// The CNAME is gone: the same pair goes back under the resolver that checks over HTTP.
	setZone(t, h.s, certRequest(nil))
	h.s.restartForRenewal(context.Background(), at.Add(renewRetry))
	if h.applied != 2 || len(h.storeNames(t, "letsencrypt")) != 1 || len(h.storeNames(t, zoneResolver)) != 0 {
		t.Fatalf("restarts %d, over HTTP %v, over DNS %v", h.applied, h.storeNames(t, "letsencrypt"), h.storeNames(t, zoneResolver))
	}
}
