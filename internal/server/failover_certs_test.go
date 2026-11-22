package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// A server takes a moved App only once it can answer every one of its domains with a valid certificate.
func TestFailoverCerts_aServerWithoutTheAppsCertificateRefusesItsStart(t *testing.T) {
	h := zoneCertHost(t)
	dnsProxy(t, h)
	c, k := testPair(t, time.Now().Add(48*time.Hour), "shop.example.com")
	pc := mustCert(t, c, k)

	// Deplo says the certificate exists, but this server has not been handed it yet.
	setZone(t, h.s, nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Sha256: pc.Sum}))
	if got := h.s.certsMissing("prj1", nil, time.Now()); !slices.Equal(got, []string{"shop.example.com"}) {
		t.Fatalf("missing %v", got)
	}
	err := h.s.startFromBundle(context.Background(), &lease.Start{Slug: "shop", ComposeYAML: "services: {}", ProjectID: "prj1"})
	if err == nil || !strings.Contains(err.Error(), "no valid certificate for shop.example.com") {
		t.Fatalf("start: %v", err)
	}

	// The bundle's own pair counts, and so does the one Deplo hands every server.
	if got := h.s.certsMissing("prj1", bundleCerts([2]string{c, k}), time.Now()); len(got) != 0 {
		t.Fatalf("with the bundle's pair, missing %v", got)
	}
	setZone(t, h.s, nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Sha256: pc.Sum, CertPem: c, KeyPem: k}))
	if got := h.s.certsMissing("prj1", nil, time.Now()); len(got) != 0 {
		t.Fatalf("holding it, missing %v", got)
	}
	// Past its date it counts as none.
	if got := h.s.certsMissing("prj1", nil, time.Now().Add(72*time.Hour)); len(got) != 1 {
		t.Fatalf("expired, missing %v", got)
	}
}

// A name no certificate exists for yet holds no start up: its App's own server orders it as before.
func TestFailoverCerts_aNameWithNoCertificateAnywhereDoesNotHoldTheStart(t *testing.T) {
	h := zoneCertHost(t)
	dnsProxy(t, h)
	setZone(t, h.s, nameCertRequest(zfp0, &pb.ClusterZoneCertificate{Renew: true, Order: true}))
	if got := h.s.certsMissing("prj1", nil, time.Now()); len(got) != 0 {
		t.Fatalf("missing %v", got)
	}
}
