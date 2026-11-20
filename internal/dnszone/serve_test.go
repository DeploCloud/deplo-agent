package dnszone

import (
	"crypto/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/miekg/dns"
)

// serveLocal runs a Server on a loopback port, UDP and TCP, and returns that address.
func serveLocal(t *testing.T, tb *Table, before ...func(*Server)) (*Server, string) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer()
	for _, f := range before {
		f(s)
	}
	if tb != nil {
		if err := s.SetTable(tb); err != nil {
			t.Fatal(err)
		}
	}
	s.Serve([]net.PacketConn{pc}, []net.Listener{ln})
	t.Cleanup(s.Shutdown)
	return s, addr
}

func exchange(t *testing.T, net, addr string, m *dns.Msg) *dns.Msg {
	t.Helper()
	c := &dns.Client{Net: net, Timeout: 2 * time.Second}
	r, _, err := c.Exchange(m, addr)
	if err != nil {
		t.Fatalf("%s %s: %v", net, m.Question[0].Name, err)
	}
	return r
}

func TestServer_answersOverUDPAndTCP(t *testing.T) {
	_, addr := serveLocal(t, testTable())
	for _, n := range []string{"udp", "tcp"} {
		r := exchange(t, n, addr, query("shop-example-com.go.example.com.", dns.TypeA))
		if r.Rcode != dns.RcodeSuccess || len(r.Answer) != 3 || !r.Authoritative {
			t.Fatalf("%s: rcode %d, %d answers", n, r.Rcode, len(r.Answer))
		}
		if r := exchange(t, n, addr, query("example.org.", dns.TypeA)); r.Rcode != dns.RcodeRefused {
			t.Fatalf("%s outside the zone: rcode %d", n, r.Rcode)
		}
	}
}

func TestServer_aLongAnswerFallsBackToTCP(t *testing.T) {
	_, addr := serveLocal(t, bigTable())
	q := query("shop-example-com.go.example.com.", dns.TypeAAAA)
	r := exchange(t, "udp", addr, q)
	if !r.Truncated {
		t.Fatalf("UDP: %d answers, not truncated", len(r.Answer))
	}
	r = exchange(t, "tcp", addr, q)
	if r.Truncated || len(r.Answer) != 60 {
		t.Fatalf("TCP: truncated %v, %d answers", r.Truncated, len(r.Answer))
	}
}

func TestServer_noTableYetAnswersServfail(t *testing.T) {
	s, addr := serveLocal(t, nil)
	if r := exchange(t, "udp", addr, query("shop-example-com.go.example.com.", dns.TypeA)); r.Rcode != dns.RcodeServerFailure {
		t.Fatalf("rcode %d", r.Rcode)
	}
	if err := s.SetTable(testTable()); err != nil {
		t.Fatal(err)
	}
	if r := exchange(t, "udp", addr, query("shop-example-com.go.example.com.", dns.TypeA)); r.Rcode != dns.RcodeSuccess {
		t.Fatalf("with a table: rcode %d", r.Rcode)
	}
	// A table that does not hold together leaves the last one answering.
	bad := testTable()
	bad.Zones[0].TTL = 0
	if err := s.SetTable(bad); err == nil {
		t.Fatal("a bad table was taken")
	}
	if r := exchange(t, "udp", addr, query("shop-example-com.go.example.com.", dns.TypeA)); len(r.Answer) != 3 {
		t.Fatalf("after a bad table: %v", r.Answer)
	}
}

func TestServer_rateLimitsAFlood(t *testing.T) {
	_, addr := serveLocal(t, testTable(), func(s *Server) { s.lim = newLimiter(5, 10) })
	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	q := query("shop-example-com.go.example.com.", dns.TypeA)
	const sent = 200
	for i := range sent {
		q.Id = uint16(i + 1)
		raw, _ := q.Pack()
		if _, err := conn.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	full, truncated := 0, 0
	buf := make([]byte, 2048)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := conn.Read(buf)
		if err != nil {
			break
		}
		var r dns.Msg
		if err := r.Unpack(buf[:n]); err != nil {
			t.Fatal(err)
		}
		if r.Truncated {
			if len(r.Answer) != 0 {
				t.Fatal("a truncated answer carried records")
			}
			truncated++
		} else {
			full++
		}
	}
	if full < 10 || full > 20 || truncated < 50 || full+truncated > sent-50 {
		t.Fatalf("of %d queries: %d answered, %d truncated", sent, full, truncated)
	}
	// TCP cannot be forged, so it is never held back.
	if r := exchange(t, "tcp", addr, q); len(r.Answer) != 3 {
		t.Fatalf("TCP after the flood: %v", r.Answer)
	}
}

func TestServer_survivesMalformedPackets(t *testing.T) {
	_, addr := serveLocal(t, testTable())
	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	good, _ := query("shop-example-com.go.example.com.", dns.TypeA).Pack()
	junk := [][]byte{{}, {0}, good[:5], good[:12], append(append([]byte{}, good...), 0xff, 0xff)}
	for i := range 300 {
		b := make([]byte, 1+i%600)
		_, _ = rand.Read(b)
		junk = append(junk, b)
		// The header of a query, then nonsense.
		c := append(append([]byte{}, good[:12]...), b...)
		junk = append(junk, c)
	}
	for _, b := range junk {
		_, _ = conn.Write(b)
		_ = conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		buf := make([]byte, 4096)
		if n, err := conn.Read(buf); err == nil && n > len(b)+512 {
			t.Fatalf("%d bytes of junk drew %d bytes back", len(b), n)
		}
	}
	tc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tc.Write([]byte{0xff, 0xff, 1, 2, 3})
	_ = tc.Close()
	if r := exchange(t, "udp", addr, query("shop-example-com.go.example.com.", dns.TypeA)); len(r.Answer) != 3 {
		t.Fatalf("after the junk: %v", r.Answer)
	}
	if r := exchange(t, "tcp", addr, query("shop-example-com.go.example.com.", dns.TypeA)); len(r.Answer) != 3 {
		t.Fatalf("TCP after the junk: %v", r.Answer)
	}
}

func TestServer_capsTCPConnections(t *testing.T) {
	_, addr := serveLocal(t, testTable())
	var held []net.Conn
	for range maxTCPConns + 20 {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	// An idle connection is closed after tcpIdle, which frees room for a new one.
	start := time.Now()
	c := &dns.Client{Net: "tcp", Timeout: 4*ioTimeout + tcpIdle}
	r, _, err := c.Exchange(query("shop-example-com.go.example.com.", dns.TypeA), addr)
	if err != nil || len(r.Answer) != 3 {
		t.Fatalf("past the cap: %v %v", r, err)
	}
	for _, c := range held {
		_ = c.Close()
	}
	t.Logf("a new connection past the cap was answered after %v", time.Since(start).Round(time.Millisecond))
}

func TestName_canonical(t *testing.T) {
	for in, want := range map[string]string{"Go.Example.COM": "go.example.com.", "a.b.": "a.b.", "_acme-challenge.x.com": "_acme-challenge.x.com."} {
		if got, err := Name(in); err != nil || got != want {
			t.Errorf("%q: %q %v", in, got, err)
		}
	}
	for _, in := range []string{"", ".", "a b.com", "a..com", "é.com", strings.Repeat("a", 64) + ".com"} {
		if _, err := Name(in); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}
