// Package netguard refuses the addresses an untrusted destination must never reach: loopback,
// private, link-local and reserved ranges.
package netguard

import (
	"context"
	"fmt"
	"net"
)

// LookupIP is swapped by tests that need a resolver they control.
var LookupIP = func(ctx context.Context, host string) ([]net.IP, error) {
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

// AllowedIPs resolves host and returns its addresses, or refuses the lot when any one of them is disallowed.
func AllowedIPs(ctx context.Context, host string) ([]net.IP, error) {
	ips := []net.IP{net.ParseIP(host)}
	if ips[0] == nil {
		resolved, err := LookupIP(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("host %q: cannot resolve: %w", host, err)
		}
		ips = resolved
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("host %q has no address", host)
	}
	for _, ip := range ips {
		if reason := BlockedReason(ip); reason != "" {
			return nil, fmt.Errorf("host %q resolves to a disallowed %s address %s; refusing to connect (SSRF guard)", host, reason, ip)
		}
	}
	return ips, nil
}

// Dial vets EVERY host it is asked for and dials the address it vetted, so a rebinding resolver
// cannot swap it after the check.
func Dial(base func(ctx context.Context, network, addr string) (net.Conn, error)) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := AllowedIPs(ctx, host)
		if err != nil {
			return nil, err
		}
		return base(ctx, network, net.JoinHostPort(ips[0].String(), port))
	}
}

func cidrs(cs ...string) []*net.IPNet {
	var out []*net.IPNet
	for _, c := range cs {
		_, n, _ := net.ParseCIDR(c)
		out = append(out, n)
	}
	return out
}

var (
	blockedNets = cidrs("0.0.0.0/8", "100.64.0.0/10", "198.18.0.0/15", "240.0.0.0/4", "fec0::/10")
	nat64       = cidrs("64:ff9b::/96", "64:ff9b:1::/48")
	sixToFour   = cidrs("2002::/16")[0]
	teredo      = cidrs("2001::/32")[0]
)

// embeddedIPv4 is the IPv4 a translated or tunnelled IPv6 delivers to, where it carries one.
func embeddedIPv4(ip net.IP) net.IP {
	b := ip.To16()
	if b == nil || ip.To4() != nil {
		return nil
	}
	zero := func(bs []byte) bool {
		for _, x := range bs {
			if x != 0 {
				return false
			}
		}
		return true
	}
	switch {
	case nat64[0].Contains(ip), nat64[1].Contains(ip):
		return net.IP(b[12:16])
	case sixToFour.Contains(ip):
		return net.IP(b[2:6])
	case teredo.Contains(ip):
		return net.IPv4(^b[12], ^b[13], ^b[14], ^b[15])
	case zero(b[:12]), zero(b[:8]) && b[8] == 0xff && b[9] == 0xff && zero(b[10:12]):
		return net.IP(b[12:16])
	}
	return nil
}

// BlockedReason names the range ip falls in, or returns "" when it may be dialed.
func BlockedReason(ip net.IP) string {
	switch {
	case ip.IsLoopback():
		return "loopback"
	case ip.IsUnspecified():
		return "unspecified"
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		return "link-local"
	case ip.IsPrivate():
		return "private"
	case ip.IsMulticast():
		return "multicast"
	}
	// NAT64 included: the gateway delivers to the IPv4 inside, so that is what is judged.
	if v4 := embeddedIPv4(ip); v4 != nil {
		return BlockedReason(v4)
	}
	for _, n := range blockedNets {
		if n.Contains(ip) {
			return "reserved"
		}
	}
	return ""
}
