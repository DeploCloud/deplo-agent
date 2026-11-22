// Package dnszone is the name server of a cluster's DNS zone (ADR-0049): the unprivileged process that answers on
// port 53, the table it answers from, and the supervisor the agent starts it with.
package dnszone

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"slices"
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
	// A certificate check record (ADR-0049 phase 3): a name per domain at most, a few values each.
	maxTXTNames  = 4096
	maxTXTValues = 8
	maxTXTLen    = 255
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
	Near []Place `json:"near,omitempty"`
	// Homes are the names answered with one server first, the one that runs their App: only healthy servers, by
	// addresses of Servers, so a home never adds a server health left out.
	Homes []Home `json:"homes,omitempty"`
	// TXT are the certificate checks under way: "_acme-challenge." names inside the zone. Fail answers them SERVFAIL
	// too, so a check is read on the bigger side of a split only.
	TXT    []TXT  `json:"txt,omitempty"`
	TTL    uint32 `json:"ttl"`
	Serial uint32 `json:"serial"`
}

// TXT is one certificate check's record: the name and the values a certificate authority reads there.
type TXT struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// Home is a name answered with this server first, then the others.
type Home struct {
	Name string `json:"name"`
	Addrs
}

// ChallengeLabel is the first label of every name a TXT may have.
const ChallengeLabel = "_acme-challenge."

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
	homes   map[string]addrSet
	txt     map[string][]string
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
		z := &zone{name: name, names: map[string]bool{}, inner: map[string]bool{}, txt: map[string][]string{}, fail: raw.Fail,
			ttl: raw.TTL, serial: raw.Serial}
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
			z.addInner(n)
		}
		if err := z.compileTXT(raw.TXT); err != nil {
			return nil, err
		}
		if err := z.compileHomes(raw.Homes); err != nil {
			return nil, err
		}
		c.zones[name] = z
	}
	return c, nil
}

// addInner marks every name between n and the zone as existing, with nothing of its own.
func (z *zone) addInner(n string) {
	for p := parent(n); p != "" && p != z.name && dns.IsSubDomain(z.name, p); p = parent(p) {
		z.inner[p] = true
	}
}

// compileTXT takes only certificate check records inside the zone, each name once.
func (z *zone) compileTXT(raw []TXT) error {
	if len(raw) > maxTXTNames {
		return fmt.Errorf("zone %s: at most %d check records", z.name, maxTXTNames)
	}
	for _, t := range raw {
		n, err := Name(t.Name)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(n, ChallengeLabel) || !dns.IsSubDomain(z.name, n) || n == z.name {
			return fmt.Errorf("%s is not a certificate check of zone %s", n, z.name)
		}
		if _, dup := z.txt[n]; dup {
			return fmt.Errorf("%s is listed twice", n)
		}
		if len(t.Values) == 0 || len(t.Values) > maxTXTValues {
			return fmt.Errorf("%s: 1 to %d values", n, maxTXTValues)
		}
		for _, v := range t.Values {
			if !TXTValue(v) {
				return fmt.Errorf("%s: a value is not a check's", n)
			}
		}
		z.txt[n] = slices.Clone(t.Values)
		z.addInner(n)
	}
	return nil
}

// compileHomes takes a home only for a name of the zone, each once, by addresses the answer names.
func (z *zone) compileHomes(raw []Home) error {
	if len(raw) > maxNamesInZone {
		return fmt.Errorf("zone %s: at most %d homes", z.name, maxNamesInZone)
	}
	healthy := map[netip.Addr]bool{}
	for _, a := range append(append([]netip.Addr{}, z.servers.v4...), z.servers.v6...) {
		healthy[a] = true
	}
	z.homes = map[string]addrSet{}
	for _, h := range raw {
		n, err := Name(h.Name)
		if err != nil {
			return err
		}
		if !z.names[n] {
			return fmt.Errorf("zone %s: %s has a home but is not one of its names", z.name, n)
		}
		if _, dup := z.homes[n]; dup {
			return fmt.Errorf("zone %s: %s has two homes", z.name, n)
		}
		a, err := parseAddrs(h.Addrs)
		if err != nil {
			return fmt.Errorf("zone %s, home of %s: %w", z.name, n, err)
		}
		if len(a.v4)+len(a.v6) == 0 {
			return fmt.Errorf("zone %s: the home of %s has no address", z.name, n)
		}
		for _, ip := range append(append([]netip.Addr{}, a.v4...), a.v6...) {
			if !healthy[ip] {
				return fmt.Errorf("zone %s: the home of %s names %s, which the answer does not", z.name, n, ip)
			}
		}
		z.homes[n] = a
	}
	return nil
}

// TXTValue says whether v is what a check record holds: printable ASCII, no quote or backslash, at most 255 bytes.
func TXTValue(v string) bool {
	if v == "" || len(v) > maxTXTLen {
		return false
	}
	for _, r := range v {
		if r < 0x21 || r > 0x7e || r == '"' || r == '\\' {
			return false
		}
	}
	return true
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
