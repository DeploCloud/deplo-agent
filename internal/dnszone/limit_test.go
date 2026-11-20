package dnszone

import (
	"net/netip"
	"testing"
	"time"
)

func TestLimiter_holdsBackANetworkPastItsBudget(t *testing.T) {
	l := newLimiter(10, 20)
	now := time.Now()
	a := netip.MustParseAddr("198.51.100.7")
	sent, slipped, dropped := 0, 0, 0
	for range 100 {
		switch l.allow(a, now) {
		case send:
			sent++
		case slip:
			slipped++
		case drop:
			dropped++
		}
	}
	if sent != 20 || slipped != 40 || dropped != 40 {
		t.Fatalf("sent %d, truncated %d, dropped %d", sent, slipped, dropped)
	}
	// Another address of the same /24 shares the budget; another network has its own.
	if l.allow(netip.MustParseAddr("198.51.100.200"), now) == send {
		t.Fatal("the same /24 got a fresh budget")
	}
	if l.allow(netip.MustParseAddr("198.51.101.7"), now) != send {
		t.Fatal("another network was held back")
	}
	// The budget comes back with time.
	if l.allow(a, now.Add(time.Second)) != send {
		t.Fatal("no budget a second later")
	}
	tr, dr, n := l.take()
	if tr != 40 || dr < 40 || n != 1 {
		t.Fatalf("reported %d truncated, %d dropped, %d networks", tr, dr, n)
	}
	if tr, dr, _ := l.take(); tr+dr != 0 {
		t.Fatal("a report is made once")
	}
}

func TestLimiter_IPv6NetworksAreA56(t *testing.T) {
	l := newLimiter(1, 1)
	now := time.Now()
	if l.allow(netip.MustParseAddr("2001:db8:0:100::1"), now) != send {
		t.Fatal("first answer held back")
	}
	if l.allow(netip.MustParseAddr("2001:db8:0:1ff::2"), now) == send {
		t.Fatal("the same /56 got a fresh budget")
	}
	if l.allow(netip.MustParseAddr("2001:db8:0:200::1"), now) != send {
		t.Fatal("another /56 was held back")
	}
	if network(netip.MustParseAddr("::ffff:198.51.100.7")) != network(netip.MustParseAddr("198.51.100.9")) {
		t.Fatal("an IPv4-mapped address is its IPv4 network")
	}
}

func TestLimiter_forgetsQuietNetworks(t *testing.T) {
	l := newLimiter(1, 1)
	now := time.Now()
	l.allow(netip.MustParseAddr("198.51.100.7"), now)
	l.allow(netip.MustParseAddr("203.0.113.7"), now.Add(2*limitIdle))
	if len(l.buckets) != 1 {
		t.Fatalf("%d networks kept", len(l.buckets))
	}
}
