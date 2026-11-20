package dnszone

import (
	"testing"

	"github.com/miekg/dns"
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
	c, err := compile(bigTable())
	if err != nil {
		f.Fatal(err)
	}
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
			resp := c.reply(req, udp)
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
