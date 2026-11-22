package clusterlink

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testrelay"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

func withRotations(p *Party, rs ...wire.Rotation) {
	p.cfg.Rotations = func() []wire.Rotation { return rs }
}

// A peer whose table still names b by the certificate b rotated away reaches it: the relay knows b by its new one.
func TestRotation_aPeerNamedByItsOldCertificateIsReachedByItsNew(t *testing.T) {
	ca := testpki.New(t)
	a, old, next := ca.Leaf(t, "a"), ca.Leaf(t, "b"), ca.Leaf(t, "b")
	member := func(id *wire.Identity) wire.Party {
		return wire.Party{Fingerprint: id.Fingerprint, Role: wire.RoleMember}
	}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), member(a), member(next))
	got := make(chan wire.Party, 1)
	runParty(t, relay.URL, next, wire.RoleMember, func(f wire.Party, c net.Conn) { got <- f; c.Close() })
	p := runParty(t, relay.URL, a, wire.RoleMember, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if c, err := p.Dial(ctx, member(old)); err == nil {
		c.Close()
		t.Fatal("reached with no rotation announced")
	}
	withRotations(p, wire.Rotation{From: old.Fingerprint, To: next.Fingerprint})
	c, err := p.Dial(ctx, member(old))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if f := <-got; f.Fingerprint != a.Fingerprint {
		t.Fatalf("b heard %s", f.Key())
	}
}

// The new certificate is also the server the old one named; the old one is never taken for the new (ADR-0050).
func TestRotation_aRetiredCertificateIsNeverTakenForItsReplacement(t *testing.T) {
	ca := testpki.New(t)
	b, old, next := ca.Leaf(t, "b"), ca.Leaf(t, "a"), ca.Leaf(t, "a")
	member := func(id *wire.Identity) wire.Party {
		return wire.Party{Fingerprint: id.Fingerprint, Role: wire.RoleMember}
	}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), member(b), member(old), member(next))
	got := make(chan wire.Party, 2)
	pb := runParty(t, relay.URL, b, wire.RoleMember, func(f wire.Party, c net.Conn) { got <- f; c.Close() })
	withRotations(pb, wire.Rotation{From: old.Fingerprint, To: next.Fingerprint})
	for _, id := range []*wire.Identity{next, old} {
		c, err := runParty(t, relay.URL, id, wire.RoleMember, nil).Dial(context.Background(), member(b))
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
	renewed, copied := <-got, <-got
	if !renewed.Is(next.Fingerprint) || !renewed.Is(old.Fingerprint) || !slices.Equal(renewed.Previous, []string{old.Fingerprint}) {
		t.Fatalf("the renewed key came as %+v", renewed)
	}
	if copied.Is(next.Fingerprint) || len(copied.Previous) != 0 {
		t.Fatalf("the retired key came as %+v", copied)
	}
}
