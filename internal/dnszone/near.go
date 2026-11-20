package dnszone

import (
	"math"
	"math/rand/v2"
	"net"
	"net/netip"
	"sort"

	"github.com/miekg/dns"
)

const (
	// alsoNearKm: a server this much farther than the nearest is still about as near, and answered beside it.
	alsoNearKm = 500
	// sameKm: servers this close to each other count as one place, and are shuffled among themselves.
	sameKm      = 50
	earthKm     = 6371.0
	minAnswered = 2
)

// distanceKm is the great-circle distance between two points.
func distanceKm(a, b Point) float64 {
	rad := math.Pi / 180
	dLat, dLon := (b.Lat-a.Lat)*rad, (b.Lon-a.Lon)*rad
	h := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(a.Lat*rad)*math.Cos(b.Lat*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthKm * math.Asin(math.Sqrt(min(1, h)))
}

// place is one server of an answer, where it is.
type place struct {
	addrs addrSet
	at    Point
}

// nearest is the places to answer a visitor at p with, nearest first: the nearest, every one about as near, and at
// least two whenever two are there, so a server that stops between answers still leaves the visitor another.
func nearest(places []place, p Point) []place {
	type ranked struct {
		place
		km   float64
		tick float64
	}
	rs := make([]ranked, len(places))
	for i, pl := range places {
		rs[i] = ranked{place: pl, km: distanceKm(p, pl.at), tick: rand.Float64()}
	}
	// Servers in one place share the load: by distance in steps of sameKm, shuffled within a step.
	sort.Slice(rs, func(i, j int) bool {
		si, sj := math.Floor(rs[i].km/sameKm), math.Floor(rs[j].km/sameKm)
		if si != sj {
			return si < sj
		}
		return rs[i].tick < rs[j].tick
	})
	out := make([]place, 0, len(rs))
	for i, r := range rs {
		if i >= minAnswered && r.km > rs[0].km+alsoNearKm {
			break
		}
		out = append(out, r.place)
	}
	return out
}

// subnet is what a resolver said of its client in EDNS Client Subnet (RFC 7871).
type subnet struct {
	opt    *dns.EDNS0_SUBNET
	prefix netip.Prefix
}

// clientSubnet reads the query's client subnet. bad is a malformed one (FORMERR); a /0 or none places nobody.
func clientSubnet(req *dns.Msg) (s *subnet, bad bool) {
	opt := req.IsEdns0()
	if opt == nil {
		return nil, false
	}
	for _, o := range opt.Option {
		e, ok := o.(*dns.EDNS0_SUBNET)
		if !ok {
			continue
		}
		if s != nil {
			return nil, true
		}
		var a netip.Addr
		switch e.Family {
		case 0:
			if e.SourceNetmask != 0 {
				return nil, true
			}
			a = netip.IPv4Unspecified()
		case 1:
			ip := e.Address.To4()
			if ip == nil || e.SourceNetmask > 32 {
				return nil, true
			}
			a = netip.AddrFrom4([4]byte(ip))
		case 2:
			ip := e.Address.To16()
			if ip == nil || e.SourceNetmask > 128 {
				return nil, true
			}
			a = netip.AddrFrom16([16]byte(ip))
		default:
			return nil, true
		}
		pfx := netip.PrefixFrom(a, int(e.SourceNetmask))
		// RFC 7871 §6: no address bit may be set past the source prefix.
		if pfx.Masked().Addr() != a {
			return nil, true
		}
		s = &subnet{opt: e, prefix: pfx}
	}
	return s, false
}

// answerSubnet is the client subnet option an answer carries back: the query's, with the scope it holds for.
func answerSubnet(s *subnet, scope uint8) *dns.EDNS0_SUBNET {
	return &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: s.opt.Family, SourceNetmask: s.opt.SourceNetmask,
		SourceScope: scope, Address: net.IP(s.prefix.Addr().AsSlice())}
}
