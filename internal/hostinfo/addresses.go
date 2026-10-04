package hostinfo

import (
	"net"
	"strings"
)

var cgnat = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

// PublicAddresses lists the addresses this host answers on from the internet, read from its own interfaces.
func PublicAddresses() []string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var addrs []net.Addr
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || containerIface(ifc.Name) {
			continue
		}
		if a, err := ifc.Addrs(); err == nil {
			addrs = append(addrs, a...)
		}
	}
	return publicOf(addrs)
}

// publicOf keeps global unicast only: never private, carrier-grade NAT, loopback or link-local.
func publicOf(addrs []net.Addr) []string {
	seen := map[string]bool{}
	var out []string
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipnet.IP
		if !ip.IsGlobalUnicast() || ip.IsPrivate() || cgnat.Contains(ip) {
			continue
		}
		s := ip.String()
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func containerIface(name string) bool {
	return name == "lo" ||
		strings.HasPrefix(name, "veth") ||
		strings.HasPrefix(name, "docker") ||
		strings.HasPrefix(name, "br-")
}
