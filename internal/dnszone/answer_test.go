package dnszone

import (
	"sort"
	"strings"
	"testing"

	"github.com/miekg/dns"
)

func testTable() *Table {
	return &Table{Zones: []Zone{{
		Name:  "Go.Example.com",
		Names: []string{"shop-example-com.go.example.com", "go.example.com", "a.b.go.example.com"},
		NameServers: []Addrs{
			{V4: []string{"203.0.113.1"}, V6: []string{"2001:db8::1"}},
			{V4: []string{"203.0.113.2"}},
		},
		Servers: Addrs{V4: []string{"203.0.113.1", "203.0.113.2", "203.0.113.3"}, V6: []string{"2001:db8::1"}},
		TTL:     30,
		Serial:  7,
	}}}
}

func mustCompile(t *testing.T, tb *Table) *compiled {
	t.Helper()
	c, err := compile(tb)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func query(name string, qtype uint16) *dns.Msg {
	m := new(dns.Msg)
	m.SetQuestion(name, qtype)
	m.RecursionDesired = true
	return m
}

func addrsIn(rrs []dns.RR) []string {
	var out []string
	for _, rr := range rrs {
		switch v := rr.(type) {
		case *dns.A:
			out = append(out, v.A.String())
		case *dns.AAAA:
			out = append(out, v.AAAA.String())
		}
	}
	sort.Strings(out)
	return out
}

func TestReply_namesEveryServer(t *testing.T) {
	c := mustCompile(t, testTable())
	r := c.reply(query("SHOP-example-com.go.example.com.", dns.TypeA), true, visitor{})
	if r.Rcode != dns.RcodeSuccess || !r.Authoritative || r.RecursionAvailable {
		t.Fatalf("rcode %d aa %v ra %v", r.Rcode, r.Authoritative, r.RecursionAvailable)
	}
	if got := strings.Join(addrsIn(r.Answer), " "); got != "203.0.113.1 203.0.113.2 203.0.113.3" {
		t.Fatalf("A: %s", got)
	}
	if r.Answer[0].Header().Ttl != 30 {
		t.Fatalf("ttl %d", r.Answer[0].Header().Ttl)
	}
	// The question comes back as it was asked, case included.
	if r.Question[0].Name != "SHOP-example-com.go.example.com." {
		t.Fatalf("question %q", r.Question[0].Name)
	}
	r = c.reply(query("go.example.com.", dns.TypeAAAA), true, visitor{})
	if got := strings.Join(addrsIn(r.Answer), " "); got != "2001:db8::1" {
		t.Fatalf("AAAA at the apex: %s", got)
	}
}

func TestReply_shufflesTheServers(t *testing.T) {
	c := mustCompile(t, testTable())
	firsts := map[string]bool{}
	for range 200 {
		r := c.reply(query("shop-example-com.go.example.com.", dns.TypeA), true, visitor{})
		firsts[r.Answer[0].(*dns.A).A.String()] = true
	}
	if len(firsts) < 2 {
		t.Fatalf("the same server came first every time: %v", firsts)
	}
}

func TestReply_zoneRecords(t *testing.T) {
	c := mustCompile(t, testTable())
	r := c.reply(query("go.example.com.", dns.TypeNS), true, visitor{})
	if len(r.Answer) != 2 || r.Answer[0].(*dns.NS).Ns != "ns1.go.example.com." || r.Answer[0].Header().Ttl != nsTTL {
		t.Fatalf("NS: %v", r.Answer)
	}
	if got := strings.Join(addrsIn(r.Extra), " "); got != "2001:db8::1 203.0.113.1 203.0.113.2" {
		t.Fatalf("glue: %s", got)
	}
	r = c.reply(query("go.example.com.", dns.TypeSOA), true, visitor{})
	soa, ok := r.Answer[0].(*dns.SOA)
	if !ok || soa.Ns != "ns1.go.example.com." || soa.Serial != 7 || soa.Minttl != negTTL {
		t.Fatalf("SOA: %v", r.Answer)
	}
	r = c.reply(query("ns2.go.example.com.", dns.TypeA), true, visitor{})
	if got := strings.Join(addrsIn(r.Answer), " "); got != "203.0.113.2" {
		t.Fatalf("ns2: %s", got)
	}
	r = c.reply(query("ns2.go.example.com.", dns.TypeAAAA), true, visitor{})
	if r.Rcode != dns.RcodeSuccess || len(r.Answer) != 0 || len(r.Ns) != 1 {
		t.Fatalf("ns2 has no IPv6: rcode %d %v %v", r.Rcode, r.Answer, r.Ns)
	}
}

func TestReply_nothingElse(t *testing.T) {
	c := mustCompile(t, testTable())
	cases := []struct {
		name   string
		qtype  uint16
		rcode  int
		answer int
		aa     bool
	}{
		{"nothing.go.example.com.", dns.TypeA, dns.RcodeNameError, 0, true},
		{"x.shop-example-com.go.example.com.", dns.TypeA, dns.RcodeNameError, 0, true},
		{"b.go.example.com.", dns.TypeA, dns.RcodeSuccess, 0, true}, // leads to a.b: exists, holds nothing
		{"shop-example-com.go.example.com.", dns.TypeMX, dns.RcodeSuccess, 0, true},
		{"shop-example-com.go.example.com.", dns.TypeTXT, dns.RcodeSuccess, 0, true},
		{"example.com.", dns.TypeA, dns.RcodeRefused, 0, false},
		{"deplo.build.", dns.TypeA, dns.RcodeRefused, 0, false},
		{"go.example.com.", dns.TypeAXFR, dns.RcodeRefused, 0, false},
		{"go.example.com.", dns.TypeIXFR, dns.RcodeRefused, 0, false},
	}
	for _, tc := range cases {
		r := c.reply(query(tc.name, tc.qtype), true, visitor{})
		if r.Rcode != tc.rcode || len(r.Answer) != tc.answer || r.Authoritative != tc.aa {
			t.Errorf("%s %s: rcode %d answers %d aa %v", tc.name, dns.TypeToString[tc.qtype], r.Rcode, len(r.Answer), r.Authoritative)
		}
		if tc.rcode == dns.RcodeNameError || tc.rcode == dns.RcodeSuccess && tc.answer == 0 {
			if len(r.Ns) != 1 || r.Ns[0].Header().Rrtype != dns.TypeSOA || r.Ns[0].Header().Ttl != negTTL {
				t.Errorf("%s: a negative answer carries the SOA: %v", tc.name, r.Ns)
			}
		}
		if r.RecursionAvailable {
			t.Errorf("%s: no recursion is offered", tc.name)
		}
	}
}

func TestReply_noANY(t *testing.T) {
	c := mustCompile(t, testTable())
	r := c.reply(query("shop-example-com.go.example.com.", dns.TypeANY), true, visitor{})
	if len(r.Answer) != 1 {
		t.Fatalf("ANY: %v", r.Answer)
	}
	if h, ok := r.Answer[0].(*dns.HINFO); !ok || h.Cpu != "RFC8482" {
		t.Fatalf("ANY: %v", r.Answer)
	}
	r = c.reply(query("go.example.com.", dns.TypeANY), true, visitor{})
	if len(r.Answer) != 1 || len(r.Extra) != 0 {
		t.Fatalf("ANY at the apex: %v %v", r.Answer, r.Extra)
	}
}

func TestReply_failAnswersServfail(t *testing.T) {
	tb := testTable()
	tb.Zones[0].Fail = true
	c := mustCompile(t, tb)
	for _, q := range []uint16{dns.TypeA, dns.TypeNS, dns.TypeSOA} {
		r := c.reply(query("shop-example-com.go.example.com.", q), true, visitor{})
		if r.Rcode != dns.RcodeServerFailure || len(r.Answer) != 0 {
			t.Fatalf("%s: rcode %d %v", dns.TypeToString[q], r.Rcode, r.Answer)
		}
	}
	var none *compiled
	if r := none.reply(query("shop-example-com.go.example.com.", dns.TypeA), true, visitor{}); r.Rcode != dns.RcodeServerFailure {
		t.Fatalf("no table yet: rcode %d", r.Rcode)
	}
}

func TestReply_otherClassesAndOpcodes(t *testing.T) {
	c := mustCompile(t, testTable())
	q := query("version.bind.", dns.TypeTXT)
	q.Question[0].Qclass = dns.ClassCHAOS
	if r := c.reply(q, true, visitor{}); r.Rcode != dns.RcodeRefused {
		t.Fatalf("CHAOS: rcode %d", r.Rcode)
	}
	q = query("go.example.com.", dns.TypeSOA)
	q.Opcode = dns.OpcodeUpdate
	if r := c.reply(q, true, visitor{}); r.Rcode != dns.RcodeNotImplemented {
		t.Fatalf("UPDATE: rcode %d", r.Rcode)
	}
	q = query("go.example.com.", dns.TypeSOA)
	q.Question = append(q.Question, q.Question[0])
	if r := c.reply(q, true, visitor{}); r.Rcode != dns.RcodeFormatError {
		t.Fatalf("two questions: rcode %d", r.Rcode)
	}
	q = query("go.example.com.", dns.TypeSOA)
	q.SetEdns0(4096, false)
	q.IsEdns0().SetVersion(1)
	r := c.reply(q, true, visitor{})
	if _, err := r.Pack(); err != nil || r.Rcode != dns.RcodeBadVers {
		t.Fatalf("EDNS version 1: rcode %d %v", r.Rcode, err)
	}
}

// bigTable names 60 servers, whose A records alone do not fit 512 bytes.
func bigTable() *Table {
	tb := testTable()
	tb.Zones[0].Servers = Addrs{}
	for i := range 60 {
		tb.Zones[0].Servers.V4 = append(tb.Zones[0].Servers.V4, "198.51.100."+itoa(i+1))
		tb.Zones[0].Servers.V6 = append(tb.Zones[0].Servers.V6, "2001:db8::"+itoa(i+1))
	}
	return tb
}

func itoa(i int) string {
	const d = "0123456789"
	if i < 10 {
		return d[i : i+1]
	}
	return itoa(i/10) + d[i%10:i%10+1]
}

func TestReply_truncatesOverUDP(t *testing.T) {
	c := mustCompile(t, bigTable())
	r := c.reply(query("shop-example-com.go.example.com.", dns.TypeAAAA), true, visitor{})
	raw, err := r.Pack()
	if err != nil {
		t.Fatal(err)
	}
	if !r.Truncated || len(raw) > 512 {
		t.Fatalf("no EDNS: truncated %v, %d bytes", r.Truncated, len(raw))
	}
	q := query("shop-example-com.go.example.com.", dns.TypeAAAA)
	q.SetEdns0(65000, false)
	r = c.reply(q, true, visitor{})
	raw, _ = r.Pack()
	if !r.Truncated || len(raw) > maxUDP {
		t.Fatalf("a resolver offering 65000: truncated %v, %d bytes", r.Truncated, len(raw))
	}
	if opt := r.IsEdns0(); opt == nil || opt.UDPSize() != maxUDP {
		t.Fatalf("the answer offers %v", opt)
	}
	r = c.reply(query("shop-example-com.go.example.com.", dns.TypeAAAA), false, visitor{})
	if r.Truncated || len(r.Answer) != 60 {
		t.Fatalf("over TCP: truncated %v, %d answers", r.Truncated, len(r.Answer))
	}
}

func TestCompile_refusesWhatDoesNotHoldTogether(t *testing.T) {
	bad := []func(*Table){
		func(tb *Table) { tb.Zones[0].Name = "" },
		func(tb *Table) { tb.Zones[0].Name = "bad name.com" },
		func(tb *Table) { tb.Zones[0].Names = []string{"shop.other.com"} },
		func(tb *Table) { tb.Zones[0].TTL = 0 },
		func(tb *Table) { tb.Zones[0].Servers.V4 = []string{"127.0.0.1"} },
		func(tb *Table) { tb.Zones[0].Servers.V4 = []string{"2001:db8::1"} },
		func(tb *Table) { tb.Zones[0].Servers.V6 = []string{"fe80::1"} },
		func(tb *Table) { tb.Zones[0].Servers.V4 = []string{"0.0.0.0"} },
		func(tb *Table) { tb.Zones = append(tb.Zones, tb.Zones[0]) },
		func(tb *Table) { tb.Zones[0].NameServers = make([]Addrs, 5) },
		func(tb *Table) {
			for range maxAddrs + 1 {
				tb.Zones[0].Servers.V4 = append(tb.Zones[0].Servers.V4, "203.0.113.9")
			}
		},
	}
	for i, mutate := range bad {
		tb := testTable()
		mutate(tb)
		if _, err := compile(tb); err == nil {
			t.Errorf("case %d: accepted", i)
		}
	}
	if _, err := compile(nil); err == nil {
		t.Error("no table: accepted")
	}
}

func txtTable() *Table {
	tb := testTable()
	tb.Zones[0].TXT = []TXT{
		{Name: "_acme-challenge.go.example.com", Values: []string{"apex-value", "wildcard-value"}},
		{Name: "_acme-challenge.deep.x.go.example.com", Values: []string{"deep-value"}},
	}
	return tb
}

func txtsIn(rrs []dns.RR) []string {
	var out []string
	for _, rr := range rrs {
		if v, ok := rr.(*dns.TXT); ok {
			out = append(out, strings.Join(v.Txt, ""))
		}
	}
	sort.Strings(out)
	return out
}

func TestReply_answersACertificateCheck(t *testing.T) {
	c := mustCompile(t, txtTable())
	r := c.reply(query("_ACME-challenge.go.example.com.", dns.TypeTXT), true, visitor{})
	if r.Rcode != dns.RcodeSuccess || !r.Authoritative {
		t.Fatalf("rcode %d aa %v", r.Rcode, r.Authoritative)
	}
	if got := strings.Join(txtsIn(r.Answer), " "); got != "apex-value wildcard-value" {
		t.Fatalf("TXT: %s", got)
	}
	if r.Answer[0].Header().Ttl != txtTTL {
		t.Fatalf("ttl %d", r.Answer[0].Header().Ttl)
	}
	// The check's name has no address, and a name between it and the zone exists with nothing.
	if r := c.reply(query("_acme-challenge.go.example.com.", dns.TypeA), true, visitor{}); r.Rcode != dns.RcodeSuccess || len(r.Answer) != 0 {
		t.Fatalf("A of a check: rcode %d, %d answers", r.Rcode, len(r.Answer))
	}
	if r := c.reply(query("x.go.example.com.", dns.TypeA), true, visitor{}); r.Rcode != dns.RcodeSuccess || len(r.Answer) != 0 {
		t.Fatalf("a name above a deep check: rcode %d", r.Rcode)
	}
	// A server's name has no TXT, and a check that is not there is no name at all.
	if r := c.reply(query("go.example.com.", dns.TypeTXT), true, visitor{}); r.Rcode != dns.RcodeSuccess || len(r.Answer) != 0 {
		t.Fatalf("TXT of a server's name: rcode %d, %d answers", r.Rcode, len(r.Answer))
	}
	if r := c.reply(query("_acme-challenge.shop-example-com.go.example.com.", dns.TypeTXT), true, visitor{}); r.Rcode != dns.RcodeNameError {
		t.Fatalf("a check not under way: rcode %d", r.Rcode)
	}
}

func TestReply_aServerThatHearsTooFewAnswersNoCheck(t *testing.T) {
	tb := txtTable()
	tb.Zones[0].Fail = true
	c := mustCompile(t, tb)
	if r := c.reply(query("_acme-challenge.go.example.com.", dns.TypeTXT), true, visitor{}); r.Rcode != dns.RcodeServerFailure {
		t.Fatalf("rcode %d", r.Rcode)
	}
}

func TestCompile_takesOnlyCertificateChecksInsideTheZone(t *testing.T) {
	bad := [][]TXT{
		{{Name: "go.example.com", Values: []string{"v"}}},
		{{Name: "www.go.example.com", Values: []string{"v"}}},
		{{Name: "_acme-challenge.other.com", Values: []string{"v"}}},
		{{Name: "_acme-challenge.go.example.com", Values: nil}},
		{{Name: "_acme-challenge.go.example.com", Values: []string{`a"b`}}},
		{{Name: "_acme-challenge.go.example.com", Values: []string{strings.Repeat("v", 256)}}},
		{{Name: "_acme-challenge.go.example.com", Values: []string{"v"}}, {Name: "_ACME-challenge.go.example.com", Values: []string{"w"}}},
	}
	for i, txt := range bad {
		tb := testTable()
		tb.Zones[0].TXT = txt
		if err := CheckTable(tb); err == nil {
			t.Errorf("table %d was taken", i)
		}
	}
	if err := CheckTable(txtTable()); err != nil {
		t.Fatal(err)
	}
}
