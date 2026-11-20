package dnszone

import (
	"net"
	"net/netip"
	"strings"
	"testing"

	"github.com/miekg/dns"

	"github.com/DeploCloud/deplo-agent/internal/dnszone/geotest"
)

func at(p geotest.Place) Point { return Point{Lat: p.Lat, Lon: p.Lon} }

var (
	rome      = Point{41.9, 12.5}
	paris     = Point{48.86, 2.35}
	frankfurt = Point{50.11, 8.68}
	amsterdam = Point{52.37, 4.9}
)

func names(ps []place) string {
	var out []string
	for _, p := range ps {
		out = append(out, p.addrs.v4[0].String())
	}
	return strings.Join(out, " ")
}

func places(at ...Point) []place {
	out := make([]place, len(at))
	for i, p := range at {
		out[i] = place{addrs: addrSet{v4: []netip.Addr{netip.AddrFrom4([4]byte{203, 0, 113, byte(i + 1)})}}, at: p}
	}
	return out
}

func TestNearest_theNearestFirstAndNeverAlone(t *testing.T) {
	world := places(at(geotest.Milan), at(geotest.NewYork), at(geotest.Tokyo))
	for _, tc := range []struct {
		from Point
		want string
	}{
		{rome, "203.0.113.1 203.0.113.2"},
		{at(geotest.NewYork), "203.0.113.2 203.0.113.1"},
		{at(geotest.Tokyo), "203.0.113.3 203.0.113.1"},
	} {
		if got := names(nearest(world, tc.from)); got != tc.want {
			t.Errorf("from %v: %s, want %s", tc.from, got, tc.want)
		}
	}
	// Servers about as near as the nearest are answered beside it; a far one is not, past the second.
	europe := places(frankfurt, amsterdam, at(geotest.NewYork))
	if got := names(nearest(europe, paris)); got != "203.0.113.2 203.0.113.1" {
		t.Errorf("from Paris: %s", got)
	}
	// One server is all there is; none is none.
	if got := names(nearest(places(frankfurt), at(geotest.Tokyo))); got != "203.0.113.1" {
		t.Errorf("one server: %s", got)
	}
	if got := nearest(nil, paris); len(got) != 0 {
		t.Errorf("no server: %v", got)
	}
}

func TestNearest_serversInOnePlaceShareTheLoad(t *testing.T) {
	two := places(frankfurt, Point{50.12, 8.7}, at(geotest.Tokyo))
	firsts := map[string]int{}
	for range 200 {
		got := nearest(two, paris)
		if len(got) != 2 || got[0].at.Lat == geotest.Tokyo.Lat || got[1].at.Lat == geotest.Tokyo.Lat {
			t.Fatalf("from Paris: %s", names(got))
		}
		firsts[got[0].addrs.v4[0].String()]++
	}
	if len(firsts) != 2 {
		t.Fatalf("the same one of two servers in Frankfurt always came first: %v", firsts)
	}
}

// geoTable is testTable with each server placed: .1 in Milan, .2 in New York, .3 in Tokyo; .1 also has the IPv6.
func geoTable() *Table {
	tb := testTable()
	tb.Zones[0].Near = []Place{
		{Addrs: Addrs{V4: []string{"203.0.113.1"}, V6: []string{"2001:db8::1"}}, Lat: geotest.Milan.Lat, Lon: geotest.Milan.Lon},
		{Addrs: Addrs{V4: []string{"203.0.113.2"}}, Lat: geotest.NewYork.Lat, Lon: geotest.NewYork.Lon},
		{Addrs: Addrs{V4: []string{"203.0.113.3"}}, Lat: geotest.Tokyo.Lat, Lon: geotest.Tokyo.Lon},
	}
	return tb
}

// withSubnet is a query whose resolver says its client is in subnet.
func withSubnet(q *dns.Msg, subnet string) *dns.Msg {
	if q.IsEdns0() == nil {
		q.SetEdns0(1232, false)
	}
	if subnet == "" {
		return q
	}
	p := netip.MustParsePrefix(subnet)
	e := &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: 1, SourceNetmask: uint8(p.Bits()), Address: net.IP(p.Addr().AsSlice())}
	if p.Addr().Is6() {
		e.Family = 2
	}
	q.IsEdns0().Option = append(q.IsEdns0().Option, e)
	return q
}

func answerSubnetOf(r *dns.Msg) *dns.EDNS0_SUBNET {
	if opt := r.IsEdns0(); opt != nil {
		for _, o := range opt.Option {
			if e, ok := o.(*dns.EDNS0_SUBNET); ok {
				return e
			}
		}
	}
	return nil
}

// inOrder is the addresses of an answer as they come.
func inOrder(rrs []dns.RR) string {
	var out []string
	for _, rr := range rrs {
		switch v := rr.(type) {
		case *dns.A:
			out = append(out, v.A.String())
		case *dns.AAAA:
			out = append(out, v.AAAA.String())
		}
	}
	return strings.Join(out, " ")
}

func TestReply_placesTheVisitorByItsSubnet(t *testing.T) {
	c := mustCompile(t, geoTable())
	g := world(t)
	resolver := visitor{from: netip.MustParseAddr("10.0.0.53"), geo: g}
	for subnet, want := range map[string]string{
		geotest.MilanV4:   "203.0.113.1 203.0.113.2",
		geotest.NewYorkV4: "203.0.113.2 203.0.113.1",
		geotest.TokyoV4:   "203.0.113.3 203.0.113.1",
		geotest.MilanV6:   "203.0.113.1 203.0.113.2",
	} {
		r := c.reply(withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), subnet), true, resolver)
		if got := inOrder(r.Answer); got != want {
			t.Errorf("a visitor in %s: %s, want %s", subnet, got, want)
		}
		e := answerSubnetOf(r)
		if e == nil || int(e.SourceScope) != netip.MustParsePrefix(subnet).Bits() {
			t.Errorf("%s: the answer's subnet is %v, it holds for that subnet only", subnet, e)
		}
	}
	// AAAA: only one server has IPv6, so it is all there is to answer.
	r := c.reply(withSubnet(query("shop-example-com.go.example.com.", dns.TypeAAAA), geotest.TokyoV4), true, resolver)
	if got := inOrder(r.Answer); got != "2001:db8::1" {
		t.Errorf("AAAA from Tokyo: %s", got)
	}
}

func TestReply_withoutASubnetTheResolversAddressPlacesTheVisitor(t *testing.T) {
	c := mustCompile(t, geoTable())
	g := world(t)
	inTokyo := visitor{from: netip.MustParseAddr("192.0.2.53"), geo: g}
	r := c.reply(query("shop-example-com.go.example.com.", dns.TypeA), true, inTokyo)
	if got := inOrder(r.Answer); got != "203.0.113.3 203.0.113.1" {
		t.Fatalf("a resolver in Tokyo: %s", got)
	}
	if r.IsEdns0() != nil {
		t.Fatal("an answer to a query with no EDNS carries EDNS")
	}
	// EDNS without a subnet, and a subnet of /0 (a client that asked not to be placed): the resolver's address.
	for _, q := range []*dns.Msg{
		withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), ""),
		withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), "0.0.0.0/0"),
	} {
		r = c.reply(q, true, inTokyo)
		if got := inOrder(r.Answer); got != "203.0.113.3 203.0.113.1" {
			t.Errorf("%v: %s", q.IsEdns0(), got)
		}
		if e := answerSubnetOf(r); e != nil && e.SourceScope != 0 {
			t.Errorf("%v: scope %d, the answer holds for every client of this resolver", q.IsEdns0(), e.SourceScope)
		}
	}
}

func TestReply_nowhereToPlaceTheVisitorAnswersEveryServer(t *testing.T) {
	c := mustCompile(t, geoTable())
	g := world(t)
	all := "203.0.113.1 203.0.113.2 203.0.113.3"
	// A subnet the table has no place for: every server, for that subnet only; the resolver's address is not used.
	r := c.reply(withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), geotest.NowhereV4), true,
		visitor{from: netip.MustParseAddr("192.0.2.53"), geo: g})
	if got := strings.Join(addrsIn(r.Answer), " "); got != all {
		t.Errorf("an unplaced subnet: %s", got)
	}
	if e := answerSubnetOf(r); e == nil || e.SourceScope != 24 {
		t.Errorf("an unplaced subnet: the answer's subnet is %v", e)
	}
	// No subnet and a resolver with no place (the lab's own, 10.0.0.53): every server.
	r = c.reply(query("shop-example-com.go.example.com.", dns.TypeA), true, visitor{from: netip.MustParseAddr("10.0.0.53"), geo: g})
	if got := strings.Join(addrsIn(r.Answer), " "); got != all {
		t.Errorf("an unplaced resolver: %s", got)
	}
	// No table at all, or a zone whose servers have no places: every server, and the subnet's scope is 0.
	r = c.reply(withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), geotest.MilanV4), true, visitor{})
	if got := strings.Join(addrsIn(r.Answer), " "); got != all {
		t.Errorf("no table: %s", got)
	}
	plain := mustCompile(t, testTable())
	r = plain.reply(withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), geotest.MilanV4), true, visitor{geo: g})
	if got := strings.Join(addrsIn(r.Answer), " "); got != all {
		t.Errorf("servers with no place: %s", got)
	}
	if e := answerSubnetOf(r); e == nil || e.SourceScope != 0 {
		t.Errorf("servers with no place: the answer's subnet is %v, it holds for every client", e)
	}
	// What is not a server's address does not depend on the visitor either.
	r = c.reply(withSubnet(query("go.example.com.", dns.TypeNS), geotest.MilanV4), true, visitor{geo: g})
	if e := answerSubnetOf(r); e == nil || e.SourceScope != 0 || len(r.Answer) != 2 {
		t.Errorf("NS: %v %v", r.Answer, e)
	}
}

func TestReply_onlyTheHealthyAreEverPlaced(t *testing.T) {
	// Health decides the answer's servers; the places only order them, so a place outside the answer is refused.
	tb := geoTable()
	tb.Zones[0].Servers.V4 = []string{"203.0.113.2", "203.0.113.3"}
	tb.Zones[0].Servers.V6 = nil
	if _, err := compile(tb); err == nil {
		t.Fatal("a place naming a server health left out was taken")
	}
	// Milan's server stopped: a visitor in Milan gets the next nearest first, and still two.
	tb.Zones[0].Near = tb.Zones[0].Near[1:]
	c := mustCompile(t, tb)
	r := c.reply(withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), geotest.MilanV4), true, visitor{geo: world(t)})
	if got := inOrder(r.Answer); got != "203.0.113.2 203.0.113.3" {
		t.Fatalf("Milan's server down: %s", got)
	}
	// A place that leaves a server of the answer out is refused too: it would never be named.
	tb = geoTable()
	tb.Zones[0].Near = tb.Zones[0].Near[:2]
	if _, err := compile(tb); err == nil {
		t.Fatal("places that leave a server out were taken")
	}
	for what, mutate := range map[string]func(*Table){
		"off the globe":    func(tb *Table) { tb.Zones[0].Near[0].Lat = 91 },
		"no address":       func(tb *Table) { tb.Zones[0].Near[1].V4 = nil },
		"an address twice": func(tb *Table) { tb.Zones[0].Near[2].V4 = []string{"203.0.113.2"} },
	} {
		tb := geoTable()
		mutate(tb)
		if _, err := compile(tb); err == nil {
			t.Errorf("%s: accepted", what)
		}
	}
}

func TestReply_aMalformedSubnetIsRefused(t *testing.T) {
	c := mustCompile(t, geoTable())
	g := world(t)
	bad := map[string]*dns.EDNS0_SUBNET{
		"bits past the prefix": {Code: dns.EDNS0SUBNET, Family: 1, SourceNetmask: 24, Address: net.ParseIP("198.51.100.7")},
		"a v4 prefix of 33":    {Code: dns.EDNS0SUBNET, Family: 1, SourceNetmask: 33, Address: net.ParseIP("198.51.100.0")},
		"a v6 prefix of 129":   {Code: dns.EDNS0SUBNET, Family: 2, SourceNetmask: 129, Address: net.ParseIP("2001:db8::")},
		"family 3":             {Code: dns.EDNS0SUBNET, Family: 3, SourceNetmask: 8, Address: net.ParseIP("198.51.100.0")},
		"family 0 with a mask": {Code: dns.EDNS0SUBNET, Family: 0, SourceNetmask: 8, Address: net.ParseIP("0.0.0.0")},
	}
	for what, e := range bad {
		q := withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), "")
		q.IsEdns0().Option = append(q.IsEdns0().Option, e)
		r := c.reply(q, true, visitor{geo: g})
		if r.Rcode != dns.RcodeFormatError || len(r.Answer) != 0 {
			t.Errorf("%s: rcode %d, %d answers", what, r.Rcode, len(r.Answer))
		}
	}
	// Two subnets in one query: which one is the client?
	q := withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), geotest.MilanV4)
	q = withSubnet(q, geotest.TokyoV4)
	if r := c.reply(q, true, visitor{geo: g}); r.Rcode != dns.RcodeFormatError {
		t.Errorf("two subnets: rcode %d", r.Rcode)
	}
}
