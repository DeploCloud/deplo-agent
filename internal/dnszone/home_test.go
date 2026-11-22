package dnszone

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/miekg/dns"

	"github.com/DeploCloud/deplo-agent/internal/dnszone/geotest"
)

// homeTable is geoTable with shop's App on the New York server (.2); go.example.com's App runs everywhere.
func homeTable() *Table {
	tb := geoTable()
	tb.Zones[0].Homes = []Home{{Name: "shop-example-com.go.example.com", Addrs: Addrs{V4: []string{"203.0.113.2"}}}}
	return tb
}

func TestReply_aNameIsAnsweredWithItsAppsServerFirst(t *testing.T) {
	c := mustCompile(t, homeTable())
	g := world(t)
	for subnet, want := range map[string]string{
		// Its server, then the others nearest first: never alone.
		geotest.MilanV4:   "203.0.113.2 203.0.113.1 203.0.113.3",
		geotest.TokyoV4:   "203.0.113.2 203.0.113.3 203.0.113.1",
		geotest.NewYorkV4: "203.0.113.2 203.0.113.1 203.0.113.3",
	} {
		r := c.reply(withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), subnet), true, visitor{geo: g})
		if got := inOrder(r.Answer); got != want {
			t.Errorf("shop from %s: %s, want %s", subnet, got, want)
		}
	}
	// A load-balanced App's name keeps nearest first.
	r := c.reply(withSubnet(query("go.example.com.", dns.TypeA), geotest.TokyoV4), true, visitor{geo: g})
	if got := inOrder(r.Answer); got != "203.0.113.3 203.0.113.1" {
		t.Errorf("go.example.com from Tokyo: %s", got)
	}
	// Its server has no IPv6: AAAA is answered as before.
	r = c.reply(withSubnet(query("shop-example-com.go.example.com.", dns.TypeAAAA), geotest.TokyoV4), true, visitor{geo: g})
	if got := inOrder(r.Answer); got != "2001:db8::1" {
		t.Errorf("AAAA: %s", got)
	}
	// Nowhere to place the visitor: its server first, then every other healthy one.
	for range 50 {
		r = c.reply(query("shop-example-com.go.example.com.", dns.TypeA), true, visitor{from: netip.MustParseAddr("10.0.0.53"), geo: g})
		got := strings.Fields(inOrder(r.Answer))
		if len(got) != 3 || got[0] != "203.0.113.2" {
			t.Fatalf("unplaced: %v", got)
		}
	}
}

func TestCompile_aHomeNamesOnlyAHealthyServerOfOneOfItsNames(t *testing.T) {
	for what, mutate := range map[string]func(*Table){
		"a name not in the zone's": func(tb *Table) { tb.Zones[0].Homes[0].Name = "other.go.example.com" },
		"a server not answered":    func(tb *Table) { tb.Zones[0].Homes[0].V4 = []string{"203.0.113.9"} },
		"no address":               func(tb *Table) { tb.Zones[0].Homes[0].V4 = nil },
		"two homes": func(tb *Table) {
			tb.Zones[0].Homes = append(tb.Zones[0].Homes, Home{Name: "shop-example-com.go.example.com", Addrs: Addrs{V4: []string{"203.0.113.1"}}})
		},
	} {
		tb := homeTable()
		mutate(tb)
		if _, err := compile(tb); err == nil {
			t.Errorf("%s: accepted", what)
		}
	}
}
