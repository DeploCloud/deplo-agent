package dnszone

import (
	"net/netip"
	"testing"

	"github.com/miekg/dns"

	"github.com/DeploCloud/deplo-agent/internal/dnszone/geotest"
)

// FuzzReply drives the path a packet from the internet takes: the library's own checks, the parse, the answer and its
// packing. It must never panic, and a UDP answer never outgrows the bound a resolver could be flooded with.
func FuzzReply(f *testing.F) {
	for _, q := range []*dns.Msg{
		query("shop-example-com.go.example.com.", dns.TypeA),
		query("go.example.com.", dns.TypeNS),
		query("go.example.com.", dns.TypeANY),
		query("nothing.go.example.com.", dns.TypeAAAA),
		query("example.org.", dns.TypeA),
	} {
		raw, _ := q.Pack()
		f.Add(raw)
		q.SetEdns0(4096, true)
		raw, _ = q.Pack()
		f.Add(raw)
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0, 0x0c, 0, 1, 0, 1})
	tb := bigTable()
	tb.Zones = append(tb.Zones, geoTable().Zones[0])
	tb.Zones[1].Name = "near.example.com"
	tb.Zones[1].Names = []string{"shop.near.example.com"}
	c, err := compile(tb)
	if err != nil {
		f.Fatal(err)
	}
	g, err := ParseGeo(geotest.World())
	if err != nil {
		f.Fatal(err)
	}
	for _, s := range []string{geotest.MilanV4, geotest.MilanV6, "0.0.0.0/0"} {
		raw, _ := withSubnet(query("shop.near.example.com.", dns.TypeA), s).Pack()
		f.Add(raw)
	}
	v := visitor{from: netip.MustParseAddr("192.0.2.53"), geo: g}
	f.Fuzz(func(t *testing.T, raw []byte) {
		req := new(dns.Msg)
		var h dns.Header
		if len(raw) >= 12 {
			h = dns.Header{Id: uint16(raw[0])<<8 | uint16(raw[1]), Bits: uint16(raw[2])<<8 | uint16(raw[3]),
				Qdcount: uint16(raw[4])<<8 | uint16(raw[5]), Ancount: uint16(raw[6])<<8 | uint16(raw[7]),
				Nscount: uint16(raw[8])<<8 | uint16(raw[9]), Arcount: uint16(raw[10])<<8 | uint16(raw[11])}
		}
		if len(raw) < 12 || dns.DefaultMsgAcceptFunc(h) != dns.MsgAccept {
			return
		}
		if err := req.Unpack(raw); err != nil {
			return
		}
		for _, udp := range []bool{true, false} {
			resp := c.reply(req, udp, v)
			out, err := resp.Pack()
			if err != nil {
				t.Fatalf("an answer that does not pack: %v", err)
			}
			if udp && len(out) > maxUDP {
				t.Fatalf("a %d-byte UDP answer to a %d-byte query", len(out), len(raw))
			}
		}
	})
}

// FuzzClientSubnet drives the client subnet option a resolver sends, whatever its bytes: the library's parse, the
// check, the placing and the answer. It must never panic, never answer a malformed one, and never return a scope
// longer than the subnet sent.
func FuzzClientSubnet(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 1, 24, 0, 198, 51, 100},
		{0, 2, 48, 0, 0x20, 0x01, 0x0d, 0xb8, 0x10, 0},
		{0, 1, 0, 0},
		{0, 0, 0, 0},
		{0, 1, 24, 0, 198, 51, 100, 7},
		{0, 1, 33, 0, 1, 2, 3, 4, 5},
		{0, 3, 8, 0, 1},
		{0, 1, 24, 24, 198, 51, 100},
		{0, 2, 129, 0},
		{},
	} {
		f.Add(seed, uint16(dns.TypeA))
	}
	c, err := compile(geoTable())
	if err != nil {
		f.Fatal(err)
	}
	g, err := ParseGeo(geotest.World())
	if err != nil {
		f.Fatal(err)
	}
	v := visitor{from: netip.MustParseAddr("192.0.2.53"), geo: g}
	f.Fuzz(func(t *testing.T, option []byte, qtype uint16) {
		q := query("shop-example-com.go.example.com.", qtype)
		q.SetEdns0(1232, false)
		q.IsEdns0().Option = append(q.IsEdns0().Option, &dns.EDNS0_LOCAL{Code: dns.EDNS0SUBNET, Data: option})
		raw, err := q.Pack()
		if err != nil {
			return
		}
		req := new(dns.Msg)
		if err := req.Unpack(raw); err != nil {
			return // the library answers FORMERR itself, before any of ours runs
		}
		sub, bad := clientSubnet(req)
		resp := c.reply(req, true, v)
		out, err := resp.Pack()
		if err != nil {
			t.Fatalf("an answer that does not pack: %v", err)
		}
		if len(out) > maxUDP {
			t.Fatalf("a %d-byte UDP answer", len(out))
		}
		if bad {
			if resp.Rcode != dns.RcodeFormatError || len(resp.Answer) != 0 {
				t.Fatalf("a malformed subnet was answered: rcode %d, %v", resp.Rcode, resp.Answer)
			}
			return
		}
		if sub == nil {
			return
		}
		if sub.prefix.Masked() != sub.prefix {
			t.Fatalf("a subnet with bits past its prefix was taken: %v", sub.prefix)
		}
		e := answerSubnetOf(resp)
		if resp.Rcode != dns.RcodeBadVers && e == nil {
			t.Fatal("the subnet did not come back")
		}
		if e != nil && e.SourceScope > e.SourceNetmask {
			t.Fatalf("an answer for /%d claimed /%d", e.SourceNetmask, e.SourceScope)
		}
	})
}
