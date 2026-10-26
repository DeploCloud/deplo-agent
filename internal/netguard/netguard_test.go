package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
)

func fakeResolver(t *testing.T, answers map[string]string) {
	t.Helper()
	orig := LookupIP
	LookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		if ip, ok := answers[host]; ok {
			return []net.IP{net.ParseIP(ip)}, nil
		}
		return nil, fmt.Errorf("no such host %q", host)
	}
	t.Cleanup(func() { LookupIP = orig })
}

// A virtual-host bucket dials <bucket>.<endpoint>, a name the endpoint check never saw: the dial itself must vet it.
func TestDial_vetsEveryHost(t *testing.T) {
	fakeResolver(t, map[string]string{"evil.test": "8.8.8.8", "xyz.evil.test": "127.0.0.1"})
	var dialed []string
	dial := Dial(func(_ context.Context, _, addr string) (net.Conn, error) {
		dialed = append(dialed, addr)
		return nil, errors.New("not dialing in a test")
	})
	if _, err := dial(context.Background(), "tcp", "xyz.evil.test:9000"); err == nil || !strings.Contains(err.Error(), "SSRF guard") {
		t.Fatalf("host resolving to loopback: want an SSRF-guard error, got %v", err)
	}
	_, _ = dial(context.Background(), "tcp", "evil.test:9000")
	if len(dialed) != 1 || dialed[0] != "8.8.8.8:9000" {
		t.Fatalf("want only the vetted address dialed, got %v", dialed)
	}
}

func TestAllowedIPs_refusesAMixedAnswer(t *testing.T) {
	orig := LookupIP
	LookupIP = func(context.Context, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("8.8.8.8"), net.ParseIP("10.0.0.5")}, nil
	}
	t.Cleanup(func() { LookupIP = orig })
	if _, err := AllowedIPs(context.Background(), "mixed.test"); err == nil {
		t.Fatal("one private address in the answer must refuse the host")
	}
}

func TestBlockedReason_reservedRanges(t *testing.T) {
	for _, ip := range []string{"100.64.0.1", "100.127.255.254", "0.1.2.3", "198.18.0.1", "198.19.255.1", "240.0.0.1", "255.255.255.255", "64:ff9b::7f00:1",
		"::ffff:169.254.169.254", "169.254.169.254", "fd00::1", "fe80::1", "::1", "10.1.2.3", "172.17.0.1", "192.168.1.1"} {
		if BlockedReason(net.ParseIP(ip)) == "" {
			t.Errorf("%s should be refused", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "100.128.0.1", "198.20.0.1", "2606:4700::1111"} {
		if r := BlockedReason(net.ParseIP(ip)); r != "" {
			t.Errorf("%s refused as %s", ip, r)
		}
	}
}

func TestBlockedReason_judgesTheIPv4InsideATranslatedAddress(t *testing.T) {
	for _, ip := range []string{"64:ff9b::a00:1", "64:ff9b:1::7f00:1", "2002:7f00:1::1", "2001:0:0:0:0:0:80ff:fffe",
		"::127.0.0.1", "::ffff:0:a00:1", "fec0::1", "ff02::1"} {
		if BlockedReason(net.ParseIP(ip)) == "" {
			t.Errorf("%s should be refused", ip)
		}
	}
	// A provider's own NAT64 is how an IPv6-only server reaches an IPv4-only S3 or asset host.
	for _, ip := range []string{"64:ff9b::808:808", "64:ff9b::34d9:3b4a", "2002:808:808::1"} {
		if r := BlockedReason(net.ParseIP(ip)); r != "" {
			t.Errorf("%s refused as %s", ip, r)
		}
	}
}
