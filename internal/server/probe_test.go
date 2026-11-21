package server

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

const (
	probeHeader    = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	listen3000v4   = probeHeader + "   0: 00000000:0BB8 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 1 1\n"
	listen3000v6   = probeHeader + "   0: 00000000000000000000000000000000:0BB8 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 1 1\n"
	connected3000  = probeHeader + "   0: 0100007F:9C40 0100007F:0BB8 01 00000000:00000000 00:00000000 00000000     0        0 1 1\n"
	listen8080only = probeHeader + "   0: 00000000:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 1 1\n"
)

func probeWith(t *testing.T, tcp, tcp6 string, args ...string) int {
	t.Helper()
	dir := t.TempDir()
	if tcp != "" {
		mustWrite(t, filepath.Join(dir, "tcp"), tcp)
	}
	if tcp6 != "" {
		mustWrite(t, filepath.Join(dir, "tcp6"), tcp6)
	}
	was := probeProcNet
	probeProcNet = dir
	t.Cleanup(func() { probeProcNet = was })
	return Probe(args)
}

// The probe answers what the shell loop it replaces answered (deplo#281), with no shell to run it.
func TestProbe_passesOnceSomethingListensOnThePort(t *testing.T) {
	cases := []struct {
		name      string
		tcp, tcp6 string
		port      string
		want      int
	}{
		{"IPv4", listen3000v4, "", "3000", 0},
		{"IPv6 only", "", listen3000v6, "3000", 0},
		{"nothing yet", probeHeader, probeHeader, "3000", 1},
		{"a connection to the port is no listener", connected3000, "", "3000", 1},
		{"another port", listen8080only, "", "3000", 1},
		{"that other port", listen8080only, "", "8080", 0},
		{"no table at all", "", "", "3000", 1},
		{"not a port", listen3000v4, "", "http", 2},
		{"port 0", listen3000v4, "", "0", 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := probeWith(t, c.tcp, c.tcp6, c.port); got != c.want {
				t.Fatalf("probe %s = %d, want %d", c.port, got, c.want)
			}
		})
	}
	if got := probeWith(t, listen3000v4, "", "3000", "8080"); got != 2 {
		t.Fatalf("two ports = %d, want 2", got)
	}
}

func TestPlaceProbe_putsThisBinaryWhereAContainerMountsIt(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "deplo-agent")
	mustWrite(t, exe, "v1")
	agentBinaryAt(t, exe)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if err := s.PlaceProbe(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(s.stackDir, ".deplo-probe")
	placed := filepath.Join(dir, "deplo-agent")
	for _, p := range []string{dir, placed} {
		fi, err := os.Stat(p)
		if err != nil || fi.Mode().Perm() != 0o755 {
			t.Fatalf("%s: %v %v, want 0755 so any user of any image runs it", p, fi, err)
		}
	}
	if b, _ := os.ReadFile(placed); string(b) != "v1" {
		t.Fatalf("placed %q", b)
	}
	// A self-update renames a new binary over the old one: the next start places that one.
	next := exe + ".new"
	mustWrite(t, next, "v2")
	if err := os.Rename(next, exe); err != nil {
		t.Fatal(err)
	}
	if err := s.PlaceProbe(); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(placed); string(b) != "v2" {
		t.Fatalf("after an update placed %q, want v2", b)
	}
	entries, _ := os.ReadDir(dir)
	if names := len(entries); names != 1 {
		t.Fatalf("the folder holds %d files, want only the probe", names)
	}
}

func TestCapabilities_advertisesTheProbe(t *testing.T) {
	if !slices.Contains(Capabilities, "deploy.probe") {
		t.Fatal("Hello must say deploy.probe, or the panel keeps the shell health check")
	}
}
