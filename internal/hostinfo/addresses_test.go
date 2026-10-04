package hostinfo

import (
	"net"
	"reflect"
	"testing"
)

func cidr(t *testing.T, s string) net.Addr {
	t.Helper()
	ip, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatal(err)
	}
	n.IP = ip
	return n
}

func TestPublicOf_keepsOnlyAddressesTheInternetCanReach(t *testing.T) {
	in := []net.Addr{
		cidr(t, "203.0.113.10/24"),
		cidr(t, "2001:db8:1::10/64"),
		cidr(t, "10.0.0.5/8"),
		cidr(t, "172.17.0.1/16"),
		cidr(t, "192.168.1.2/24"),
		cidr(t, "100.64.1.1/10"),
		cidr(t, "127.0.0.1/8"),
		cidr(t, "::1/128"),
		cidr(t, "fe80::1/64"),
		cidr(t, "fd00::1/64"),
		cidr(t, "203.0.113.10/32"),
	}
	got := publicOf(in)
	want := []string{"203.0.113.10", "2001:db8:1::10"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("publicOf = %v, want %v", got, want)
	}
}

func TestContainerIface_skipsBridgesAndVeths(t *testing.T) {
	for _, name := range []string{"lo", "docker0", "br-1a2b3c", "veth12ab"} {
		if !containerIface(name) {
			t.Errorf("%s should be skipped", name)
		}
	}
	for _, name := range []string{"eth0", "ens3", "enp1s0", "wg0"} {
		if containerIface(name) {
			t.Errorf("%s should be read", name)
		}
	}
}
