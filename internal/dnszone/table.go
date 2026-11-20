// Package dnszone is the name server of a cluster's DNS zone (ADR-0049): the unprivileged process that answers on
// port 53, the table it answers from, and the supervisor the agent starts it with.
package dnszone

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"

	"github.com/miekg/dns"
)

// Limits on one table, so a bad one is refused whole rather than half applied.
const (
	maxZones       = 16
	maxNamesInZone = 4096
	maxNames       = 8192
	maxNameServers = 4
	maxAddrs       = 64
	// MaxTableBytes bounds one table on the wire between the agent and the process.
	MaxTableBytes = 8 << 20
)

// Table is everything the name server process is given: the zones, and how to answer each one now.
type Table struct {
	Zones []Zone `json:"zones"`
}

// Zone is one zone delegated to a cluster, with the servers to answer its names with now.
type Zone struct {
	// Name is the zone itself, "go.example.com".
	Name string `json:"name"`
	// Names are answered with Servers; the zone itself may be one of them.
	Names []string `json:"names"`
	// NameServers are ns1, ns2, ... of the zone, in that order.
	NameServers []Addrs `json:"ns"`
	// Fail answers SERVFAIL for every name of the zone: this server hears too few of its cluster.
	Fail    bool  `json:"fail,omitempty"`
	Servers Addrs `json:"servers"`
	// Near is Servers one server at a time, where each is, so a visitor is answered with the nearest; empty answers
	// every one of Servers. It names exactly the addresses of Servers, so geography never adds a server health left out.
	Near   []Place `json:"near,omitempty"`
	TTL    uint32  `json:"ttl"`
	Serial uint32  `json:"serial"`
}

// Place is one server of an answer and where it is, in degrees.
type Place struct {
	Addrs
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// Addrs are one server's or one set's IPv4 and IPv6 addresses.
type Addrs struct {
	V4 []string `json:"v4,omitempty"`
	V6 []string `json:"v6,omitempty"`
}

// Name makes a DNS name canonical: lower case, fully qualified. It refuses anything that is not one.
func Name(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" || s == "." {
		return "", errors.New("a name is required")
	}
	s = dns.CanonicalName(s)
	if _, ok := dns.IsDomainName(s); !ok || len(s) > 254 {
		return "", fmt.Errorf("%q is not a DNS name", raw)
	}
	for _, label := range dns.SplitDomainName(s) {
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
				return "", fmt.Errorf("%q is not a DNS name", raw)
			}
		}
	}
	return s, nil
}

// ipsOf parses addresses of one family; a server's address is a unicast one, never loopback or unspecified.
func ipsOf(raw []string, v6 bool) ([]netip.Addr, error) {
	if len(raw) > maxAddrs {
		return nil, fmt.Errorf("at most %d addresses", maxAddrs)
	}
	out := make([]netip.Addr, 0, len(raw))
	seen := map[netip.Addr]bool{}
	for _, s := range raw {
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("%q is not an address", s)
		}
		a = a.Unmap()
		if a.Is6() != v6 || a.Zone() != "" || !a.IsGlobalUnicast() {
			return nil, fmt.Errorf("%q is not a server's address", s)
		}
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out, nil
}

type addrSet struct{ v4, v6 []netip.Addr }

func parseAddrs(a Addrs) (addrSet, error) {
	v4, err := ipsOf(a.V4, false)
	if err != nil {
		return addrSet{}, err
	}
	v6, err := ipsOf(a.V6, true)
	if err != nil {
		return addrSet{}, err
	}
	return addrSet{v4: v4, v6: v6}, nil
}

// zone is one Zone made ready to answer from.
type zone struct {
	name    string
	names   map[string]bool
	inner   map[string]bool // names with no records of their own that lead to one: NODATA, not NXDOMAIN
	ns      []nameServer
	fail    bool
	servers addrSet
	near    []place
	ttl     uint32
	serial  uint32
}

type nameServer struct {
	name  string
	addrs addrSet
}

// compiled is a whole Table made ready to answer from.
type compiled struct {
	zones map[string]*zone
}

func compile(t *Table) (*compiled, error) {
	if t == nil {
		return nil, errors.New("no table")
	}
	if len(t.Zones) > maxZones {
		return nil, fmt.Errorf("at most %d zones", maxZones)
	}
	c := &compiled{zones: map[string]*zone{}}
	total := 0
	for _, raw := range t.Zones {
		name, err := Name(raw.Name)
		if err != nil {
			return nil, err
		}
		if c.zones[name] != nil {
			return nil, fmt.Errorf("zone %s is listed twice", name)
		}
		if len(raw.Names) > maxNamesInZone {
			return nil, fmt.Errorf("zone %s: at most %d names", name, maxNamesInZone)
		}
		if total += len(raw.Names); total > maxNames {
			return nil, fmt.Errorf("at most %d names in all", maxNames)
		}
		if len(raw.NameServers) > maxNameServers {
			return nil, fmt.Errorf("zone %s: at most %d name servers", name, maxNameServers)
		}
		if raw.TTL == 0 || raw.TTL > 86400 {
			return nil, fmt.Errorf("zone %s: a TTL of %d s", name, raw.TTL)
		}
		z := &zone{name: name, names: map[string]bool{}, inner: map[string]bool{}, fail: raw.Fail, ttl: raw.TTL, serial: raw.Serial}
		if z.servers, err = parseAddrs(raw.Servers); err != nil {
			return nil, fmt.Errorf("zone %s: %w", name, err)
		}
		if z.near, err = compileNear(raw.Near, z.servers); err != nil {
			return nil, fmt.Errorf("zone %s: %w", name, err)
		}
		for i, ns := range raw.NameServers {
			a, err := parseAddrs(ns)
			if err != nil {
				return nil, fmt.Errorf("zone %s, name server %d: %w", name, i+1, err)
			}
			z.ns = append(z.ns, nameServer{name: fmt.Sprintf("ns%d.%s", i+1, name), addrs: a})
		}
		for _, rn := range raw.Names {
			n, err := Name(rn)
			if err != nil {
				return nil, err
			}
			if !dns.IsSubDomain(name, n) {
				return nil, fmt.Errorf("%s is not inside zone %s", n, name)
			}
			z.names[n] = true
			// Every name between it and the zone exists too, with nothing of its own.
			for p := parent(n); p != "" && p != name && dns.IsSubDomain(name, p); p = parent(p) {
				z.inner[p] = true
			}
		}
		c.zones[name] = z
	}
	return c, nil
}

// compileNear checks that the places name exactly the servers of the answer, each once, on the globe.
func compileNear(raw []Place, servers addrSet) ([]place, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > maxAddrs {
		return nil, fmt.Errorf("at most %d places", maxAddrs)
	}
	want := map[netip.Addr]bool{}
	for _, a := range append(append([]netip.Addr{}, servers.v4...), servers.v6...) {
		want[a] = true
	}
	seen := map[netip.Addr]bool{}
	out := make([]place, 0, len(raw))
	for i, r := range raw {
		if math.IsNaN(r.Lat) || math.IsNaN(r.Lon) || r.Lat < -90 || r.Lat > 90 || r.Lon < -180 || r.Lon > 180 {
			return nil, fmt.Errorf("place %d is off the globe", i+1)
		}
		a, err := parseAddrs(r.Addrs)
		if err != nil {
			return nil, fmt.Errorf("place %d: %w", i+1, err)
		}
		if len(a.v4)+len(a.v6) == 0 {
			return nil, fmt.Errorf("place %d has no address", i+1)
		}
		for _, ip := range append(append([]netip.Addr{}, a.v4...), a.v6...) {
			if !want[ip] || seen[ip] {
				return nil, fmt.Errorf("place %d names %s, which the answer does not, or twice", i+1, ip)
			}
			seen[ip] = true
		}
		out = append(out, place{addrs: a, at: Point{Lat: r.Lat, Lon: r.Lon}})
	}
	if len(seen) != len(want) {
		return nil, errors.New("the places leave out a server of the answer")
	}
	return out, nil
}

// CheckTable refuses a table the name server would refuse, with the reason.
func CheckTable(t *Table) error {
	_, err := compile(t)
	return err
}

// parent is a name with its first label off, or "" past the root.
func parent(n string) string {
	i := strings.IndexByte(n, '.')
	if i < 0 || i == len(n)-1 {
		return ""
	}
	return n[i+1:]
}

// zoneFor is the zone a name falls in, the deepest one when zones nest; nil when none.
func (c *compiled) zoneFor(qname string) *zone {
	for n := qname; n != ""; n = parent(n) {
		if z := c.zones[n]; z != nil {
			return z
		}
	}
	return nil
}
