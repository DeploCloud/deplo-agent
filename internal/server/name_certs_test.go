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

// #228: a name's own certificate over DNS follows the zone's model: one server renews it, every other one holds it
// as a file, and another takes the renewal over.
const shopAt = "_acme-challenge.shop-example-com.go.example.com"

func nameCertRequest(owner string, cert *pb.ClusterZoneCertificate) *pb.SetClusterZoneRequest {
	req := zoneRequest()
	req.Challenges = []*pb.ClusterZoneChallenge{{Asked: "_acme-challenge.shop.example.com", At: shopAt, OwnerFingerprint: owner}}
	if cert != nil {
		cert.Zone, cert.Names = "go.example.com", []string{"shop.example.com"}
		req.Certificates = []*pb.ClusterZoneCertificate{cert}
	}
	return req
}

func nameRouter(r answerRoutes) *answerHTTPRouter {
	if r.HTTP == nil {
		return nil
	}
	if rt, ok := r.HTTP.Routers[zoneRouterName(zoneCertRole{Names: []string{"shop.example.com"}}.key())]; ok {
		return &rt
	}
	return nil
}

func dnsProxy(t *testing.T, h *proxyHost) {
	t.Helper()
	compose := strings.Replace(proxyCompose, "    volumes:\n",
		"      - --certificatesresolvers.deplo-dns.acme.storage=/acme/acme.json\n    volumes:\n", 1)
	if err := os.WriteFile(h.s.traefikCompose(), []byte(compose), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestNameCert_oneServerRenewsItAndEveryOtherServesItAsAFile(t *testing.T) {
	renewer, holder := zoneCertHost(t), zoneCertHost(t)
	dnsProxy(t, renewer)
	dnsProxy(t, holder)

	// None exists anywhere: the renewer's proxy orders it, over DNS, for that name alone.
	resp := setZone(t, renewer.s, nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Renew: true, Order: true}))
	rt := nameRouter(routesOf(t, renewer))
	if rt == nil || rt.TLS.CertResolver != zoneResolver || rt.TLS.Domains[0].Main != "shop.example.com" || len(rt.TLS.Domains[0].SANs) != 0 {
		t.Fatalf("the order router: %+v", rt)
	}
	if st := resp.GetCertificates(); len(st) != 1 || !st[0].GetOrdering() || !slices.Equal(st[0].GetNames(), []string{"shop.example.com"}) {
		t.Fatalf("reported %v", st)
	}

	// The proxy got it: the renewer hands it back with its key, and keeps renewing it.
	c, k := testPair(t, time.Now().Add(48*time.Hour), "shop.example.com")
	pc := mustCert(t, c, k)
	renewer.putUnder(t, zoneResolver, c, k, "shop.example.com")
	resp = setZone(t, renewer.s, nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Renew: true, Order: true}))
	if st := resp.GetCertificates()[0]; st.GetSha256() != pc.Sum || st.GetKeyPem() != k {
		t.Fatalf("reported %v", st)
	}
	if nameRouter(routesOf(t, renewer)) == nil {
		t.Fatal("no router keeps the renewer's certificate")
	}
	if plan := renewer.s.forwarders().storePlan(context.Background()); plan.drops(pc) {
		t.Fatal("the renewer would drop it")
	}

	// Every other server gets it from Deplo as a file, orders nothing, and drops an entry of its own.
	old, oldK := testPair(t, time.Now().Add(time.Hour), "shop.example.com")
	holder.putUnder(t, zoneResolver, old, oldK, "shop.example.com")
	setZone(t, holder.s, nameCertRequest(zfp1, &pb.ClusterZoneCertificate{Sha256: pc.Sum, CertPem: c, KeyPem: k}))
	r := routesOf(t, holder)
	if nameRouter(r) != nil {
		t.Fatal("a holder orders")
	}
	if r.TLS == nil || !slices.ContainsFunc(r.TLS.Certificates, func(f answerTLSFile) bool { return f.CertFile == c }) {
		t.Fatal("the holder does not serve it from its routes file")
	}
	if _, err := holder.s.startProxy(context.Background(), holder.s.traefikCompose(), true); err != nil {
		t.Fatal(err)
	}
	if got := holder.storeNames(t, zoneResolver); len(got) != 0 {
		t.Fatalf("the holder still renews %v", got)
	}

	// The renewer went: the holder takes it over with the pair it holds, and orders nothing until it is in its store.
	setZone(t, holder.s, nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Renew: true, Order: true, Sha256: pc.Sum}))
	if nameRouter(routesOf(t, holder)) != nil {
		t.Fatal("the new renewer orders beside a valid certificate")
	}
	holder.s.restartForRenewal(context.Background(), pc.NotAfter.Add(-renewBefore(time.Hour)))
	if got := holder.storeNames(t, zoneResolver); len(got) != 1 || !sameNames(got[0], pc.Names) {
		t.Fatalf("the new renewer's store renews %v", got)
	}
	holder.s.refreshZoneRoutes()
	if nameRouter(routesOf(t, holder)) == nil {
		t.Fatal("the new renewer's router is not there once its store holds it")
	}
}

func TestNameCert_refusesANameTheClusterDoesNotCheckOrAPairOfOtherNames(t *testing.T) {
	h := zoneCertHost(t)
	req := nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Renew: true})
	req.Challenges = nil
	if _, err := h.s.SetClusterZone(context.Background(), req); err == nil {
		t.Fatal("a certificate of a name the cluster does not check was taken")
	}
	c, k := testPair(t, time.Now().Add(time.Hour), "other.example.com")
	req = nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Sha256: mustCert(t, c, k).Sum, CertPem: c, KeyPem: k})
	if _, err := h.s.SetClusterZone(context.Background(), req); err == nil {
		t.Fatal("a pair of other names was taken")
	}
	both := nameCertRequest(zfp0, &pb.ClusterZoneCertificate{})
	both.Certificates = append(both.Certificates, &pb.ClusterZoneCertificate{Zone: "go.example.com", Names: []string{"shop.example.com"}})
	if _, err := h.s.SetClusterZone(context.Background(), both); err == nil {
		t.Fatal("one certificate listed twice was taken")
	}
	// The zone's own and a name's own sit side by side.
	beside := nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Renew: true})
	beside.Certificates = append(beside.Certificates, &pb.ClusterZoneCertificate{Zone: "go.example.com", Renew: true})
	beside.Challenges = append(beside.Challenges, &pb.ClusterZoneChallenge{Asked: "_acme-challenge.go.example.com",
		At: "_acme-challenge.go.example.com", OwnerFingerprint: zfp0})
	if resp := setZone(t, h.s, beside); len(resp.GetCertificates()) != 2 {
		t.Fatalf("reported %v", resp.GetCertificates())
	}
}

func TestNameCert_advertised(t *testing.T) {
	if !slices.Contains(Capabilities, "dns.certificates") {
		t.Fatal("Hello does not advertise dns.certificates")
	}
}
