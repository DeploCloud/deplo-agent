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

// reply is the answer to one query; udp bounds its size and sets TC past it.
func (c *compiled) reply(req *dns.Msg, udp bool) *dns.Msg {
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
	if opt := req.IsEdns0(); opt != nil {
		resp.SetEdns0(maxUDP, false)
		if opt.Version() != 0 {
			resp.Rcode = dns.RcodeBadVers
			return resp
		}
		size = max(512, min(int(opt.UDPSize()), maxUDP))
	}
	if !udp {
		size = maxTCP
	}
	c.answer(resp, req.Question[0])
	resp.Truncate(size)
	return resp
}

// answer fills resp for one question; resp is already a reply to it.
func (c *compiled) answer(resp *dns.Msg, q dns.Question) {
	if c == nil {
		resp.Rcode = dns.RcodeServerFailure
		return
	}
	if q.Qclass != dns.ClassINET && q.Qclass != dns.ClassANY {
		resp.Rcode = dns.RcodeRefused
		return
	}
	qname := dns.CanonicalName(q.Name)
	z := c.zoneFor(qname)
	if z == nil {
		// Authoritative only: nothing outside the zones, and no recursion.
		resp.Rcode = dns.RcodeRefused
		return
	}
	if z.fail {
		resp.Rcode = dns.RcodeServerFailure
		return
	}
	resp.Authoritative = true
	switch q.Qtype {
	case dns.TypeAXFR, dns.TypeIXFR:
		resp.Authoritative = false
		resp.Rcode = dns.RcodeRefused
		return
	}
	exists, answer, extra := z.records(qname, q.Qtype)
	if !exists {
		resp.Rcode = dns.RcodeNameError
		resp.Ns = []dns.RR{z.soa(negTTL)}
		return
	}
	if q.Qtype == dns.TypeANY {
		// RFC 8482: one small record, never everything the name has.
		resp.Answer = []dns.RR{&dns.HINFO{Hdr: hdr(qname, dns.TypeHINFO, z.ttl), Cpu: "RFC8482"}}
		return
	}
	if len(answer) == 0 {
		resp.Ns = []dns.RR{z.soa(negTTL)}
		return
	}
	resp.Answer = answer
	resp.Extra = append(resp.Extra, extra...)
}

// records is what qname holds of qtype, and the glue an NS answer carries; exists is false for NXDOMAIN.
func (z *zone) records(qname string, qtype uint16) (exists bool, answer, extra []dns.RR) {
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
			return true, z.serverRecords(qname, qtype), nil
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
		return true, z.serverRecords(qname, qtype), nil
	}
	return z.inner[qname], nil, nil
}

func (z *zone) serverRecords(qname string, qtype uint16) []dns.RR {
	switch qtype {
	case dns.TypeA:
		return addrRecords(qname, z.servers.v4, z.ttl, true)
	case dns.TypeAAAA:
		return addrRecords(qname, z.servers.v6, z.ttl, true)
	}
	return nil
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
