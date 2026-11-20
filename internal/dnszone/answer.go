package dnszone

import (
	"math/rand/v2"
	"net/netip"

	"github.com/miekg/dns"
)

const (
	// negTTL is how long a resolver keeps "no such name": a name added a moment ago appears quickly.
	negTTL = 30
	// nsTTL is the registrar's side, which changes only when a name server leaves.
	nsTTL = 86400
	// maxUDP is the largest answer sent over UDP whatever a resolver offers (DNS flag day 2020); past it, TCP.
	maxUDP = 1232
	// maxTCP is a whole DNS message.
	maxTCP = dns.MaxMsgSize
)

// visitor is who asks: the address the query came from, and the location table to place it with.
type visitor struct {
	from netip.Addr
	geo  *Geo
}

// where places the visitor: by the client subnet a resolver sent, else by the resolver's own address.
type where struct {
	at Point
	ok bool
	// bySubnet: the place, or the lack of one, came from the client subnet, so the answer holds for that subnet only.
	bySubnet bool
}

func (v visitor) place(s *subnet) where {
	if s != nil && s.prefix.Bits() > 0 {
		at, ok := v.geo.Locate(s.prefix.Addr())
		return where{at: at, ok: ok, bySubnet: true}
	}
	at, ok := v.geo.Locate(v.from)
	return where{at: at, ok: ok}
}

// reply is the answer to one query; udp bounds its size and sets TC past it.
func (c *compiled) reply(req *dns.Msg, udp bool, v visitor) *dns.Msg {
	resp := new(dns.Msg)
	if req.Opcode != dns.OpcodeQuery {
		return resp.SetRcode(req, dns.RcodeNotImplemented)
	}
	if len(req.Question) != 1 {
		return resp.SetRcode(req, dns.RcodeFormatError)
	}
	resp.SetReply(req)
	resp.RecursionAvailable = false
	resp.Compress = true
	size := 512
	var sub *subnet
	if opt := req.IsEdns0(); opt != nil {
		resp.SetEdns0(maxUDP, false)
		if opt.Version() != 0 {
			resp.Rcode = dns.RcodeBadVers
			return resp
		}
		size = max(512, min(int(opt.UDPSize()), maxUDP))
		var bad bool
		if sub, bad = clientSubnet(req); bad {
			resp.Rcode = dns.RcodeFormatError
			return resp
		}
	}
	if !udp {
		size = maxTCP
	}
	tailored := c.answer(resp, req.Question[0], v.place(sub))
	if sub != nil {
		// RFC 7871: the subnet comes back, with the scope the answer holds for; 0 is every client.
		scope := uint8(0)
		if tailored {
			scope = uint8(sub.prefix.Bits())
		}
		opt := resp.IsEdns0()
		opt.Option = append(opt.Option, answerSubnet(sub, scope))
	}
	resp.Truncate(size)
	return resp
}

// answer fills resp for one question; resp is already a reply to it. tailored is an answer that depends on the
// client subnet the visitor was placed by.
func (c *compiled) answer(resp *dns.Msg, q dns.Question, at where) (tailored bool) {
	if c == nil {
		resp.Rcode = dns.RcodeServerFailure
		return false
	}
	if q.Qclass != dns.ClassINET && q.Qclass != dns.ClassANY {
		resp.Rcode = dns.RcodeRefused
		return false
	}
	qname := dns.CanonicalName(q.Name)
	z := c.zoneFor(qname)
	if z == nil {
		// Authoritative only: nothing outside the zones, and no recursion.
		resp.Rcode = dns.RcodeRefused
		return false
	}
	if z.fail {
		resp.Rcode = dns.RcodeServerFailure
		return false
	}
	resp.Authoritative = true
	switch q.Qtype {
	case dns.TypeAXFR, dns.TypeIXFR:
		resp.Authoritative = false
		resp.Rcode = dns.RcodeRefused
		return false
	}
	exists, answer, extra := z.records(qname, q.Qtype, at)
	if !exists {
		resp.Rcode = dns.RcodeNameError
		resp.Ns = []dns.RR{z.soa(negTTL)}
		return false
	}
	if q.Qtype == dns.TypeANY {
		// RFC 8482: one small record, never everything the name has.
		resp.Answer = []dns.RR{&dns.HINFO{Hdr: hdr(qname, dns.TypeHINFO, z.ttl), Cpu: "RFC8482"}}
		return false
	}
	tailored = at.bySubnet && len(z.near) > 0 && z.names[qname] && (q.Qtype == dns.TypeA || q.Qtype == dns.TypeAAAA)
	if len(answer) == 0 {
		resp.Ns = []dns.RR{z.soa(negTTL)}
		return tailored
	}
	resp.Answer = answer
	resp.Extra = append(resp.Extra, extra...)
	return tailored
}

// records is what qname holds of qtype, and the glue an NS answer carries; exists is false for NXDOMAIN.
func (z *zone) records(qname string, qtype uint16, at where) (exists bool, answer, extra []dns.RR) {
	if qname == z.name {
		switch qtype {
		case dns.TypeSOA:
			return true, []dns.RR{z.soa(negTTL)}, nil
		case dns.TypeNS:
			for _, ns := range z.ns {
				answer = append(answer, &dns.NS{Hdr: hdr(z.name, dns.TypeNS, nsTTL), Ns: ns.name})
				extra = append(extra, addrRecords(ns.name, ns.addrs.v4, nsTTL, false)...)
				extra = append(extra, addrRecords(ns.name, ns.addrs.v6, nsTTL, false)...)
			}
			return true, answer, extra
		}
		if z.names[qname] {
			return true, z.serverRecords(qname, qtype, at), nil
		}
		return true, nil, nil
	}
	for _, ns := range z.ns {
		if ns.name == qname {
			switch qtype {
			case dns.TypeA:
				return true, addrRecords(qname, ns.addrs.v4, nsTTL, false), nil
			case dns.TypeAAAA:
				return true, addrRecords(qname, ns.addrs.v6, nsTTL, false), nil
			}
			return true, nil, nil
		}
	}
	if z.names[qname] {
		return true, z.serverRecords(qname, qtype, at), nil
	}
	return z.inner[qname], nil, nil
}

func (z *zone) serverRecords(qname string, qtype uint16, at where) []dns.RR {
	if qtype != dns.TypeA && qtype != dns.TypeAAAA {
		return nil
	}
	v6 := qtype == dns.TypeAAAA
	if len(z.near) == 0 || !at.ok {
		// Nowhere to place the visitor, or nothing to place the servers by: every healthy server, never a guess.
		if v6 {
			return addrRecords(qname, z.servers.v6, z.ttl, true)
		}
		return addrRecords(qname, z.servers.v4, z.ttl, true)
	}
	var of []place
	for _, p := range z.near {
		if v6 && len(p.addrs.v6) > 0 || !v6 && len(p.addrs.v4) > 0 {
			of = append(of, p)
		}
	}
	var out []dns.RR
	for _, p := range nearest(of, at.at) {
		if v6 {
			out = append(out, addrRecords(qname, p.addrs.v6, z.ttl, false)...)
		} else {
			out = append(out, addrRecords(qname, p.addrs.v4, z.ttl, false)...)
		}
	}
	return out
}

// addrRecords are A or AAAA records; shuffled, so resolvers spread visitors over the servers.
func addrRecords(name string, addrs []netip.Addr, ttl uint32, shuffle bool) []dns.RR {
	out := make([]dns.RR, 0, len(addrs))
	for _, a := range addrs {
		if a.Is4() {
			out = append(out, &dns.A{Hdr: hdr(name, dns.TypeA, ttl), A: a.AsSlice()})
		} else {
			out = append(out, &dns.AAAA{Hdr: hdr(name, dns.TypeAAAA, ttl), AAAA: a.AsSlice()})
		}
	}
	if shuffle {
		rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	}
	return out
}

func (z *zone) soa(ttl uint32) dns.RR {
	mname := z.name
	if len(z.ns) > 0 {
		mname = z.ns[0].name
	}
	return &dns.SOA{
		Hdr: hdr(z.name, dns.TypeSOA, ttl), Ns: mname, Mbox: "hostmaster." + z.name,
		Serial: z.serial, Refresh: 3600, Retry: 600, Expire: 1209600, Minttl: negTTL,
	}
}

func hdr(name string, t uint16, ttl uint32) dns.RR_Header {
	return dns.RR_Header{Name: name, Rrtype: t, Class: dns.ClassINET, Ttl: ttl}
}
