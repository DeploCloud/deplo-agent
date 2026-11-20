package dnszone

import (
	"net"
	"net/netip"
	"strings"
)

// clat is an IPv6-only server's own way out (192.0.0.0/24), never a way in.
var clat = netip.MustParsePrefix("192.0.0.0/24")

// ListenAddresses are the addresses a name server answers on: every address of the host's own interfaces but loopback,
// link-local and Docker's, private ones included, since a server behind NAT receives its public traffic on those.
func ListenAddresses() []netip.Addr {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []netip.Addr
	seen := map[netip.Addr]bool{}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || !hostIface(ifc.Name) {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(ipn.IP)
			if !ok {
				continue
			}
			ip = ip.Unmap()
			if !ip.IsGlobalUnicast() || clat.Contains(ip) || seen[ip] {
				continue
			}
			seen[ip] = true
			out = append(out, ip)
		}
	}
	return out
}

// hostIface is false for loopback, Docker's interfaces and Deplo's own (the DNS cache's deplo-dns0, the CLAT).
func hostIface(name string) bool {
	for _, p := range []string{"veth", "docker", "br-", "deplo-", "deplo_", "clat"} {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	return name != "lo"
}
