package hostinfo

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The parsers are the whole risk surface of this package: everything else is a syscall.

func TestParseCPUInfo(t *testing.T) {
	tests := []struct {
		name              string
		in                string
		model             string
		physical, logical int
	}{
		{
			// The case that matters: 2 physical cores presented as 4 logical ones.
			// Reporting 4 "cores" is the classic spec-sheet lie.
			name: "hyperthreaded x86 reports physical cores, not threads",
			in: cpuBlock("0", "AMD Ryzen 5 5600X 6-Core Processor", "0", "0") +
				cpuBlock("1", "AMD Ryzen 5 5600X 6-Core Processor", "0", "1") +
				cpuBlock("2", "AMD Ryzen 5 5600X 6-Core Processor", "0", "0") +
				cpuBlock("3", "AMD Ryzen 5 5600X 6-Core Processor", "0", "1"),
			model: "AMD Ryzen 5 5600X 6-Core Processor", physical: 2, logical: 4,
		},
		{
			name: "two sockets are counted apart even with identical core ids",
			in: cpuBlock("0", "Intel(R) Xeon(R) Silver 4210", "0", "0") +
				cpuBlock("1", "Intel(R) Xeon(R) Silver 4210", "1", "0"),
			model: "Intel(R) Xeon(R) Silver 4210", physical: 2, logical: 2,
		},
		{
			// Many ARM boards and VMs publish no physical/core id at all. Falling
			// back to the logical count beats reporting 0 cores.
			name:  "no topology keys falls back to the logical count",
			in:    "processor\t: 0\nModel\t\t: Raspberry Pi 5\n\nprocessor\t: 1\nModel\t\t: Raspberry Pi 5\n\n",
			model: "Raspberry Pi 5", physical: 2, logical: 2,
		},
		{
			name: "no trailing blank line still closes the last block",
			in: cpuBlock("0", "QEMU Virtual CPU", "0", "0") +
				strings.TrimSuffix(cpuBlock("1", "QEMU Virtual CPU", "0", "1"), "\n"),
			model: "QEMU Virtual CPU", physical: 2, logical: 2,
		},
		{name: "empty input yields zeroes, never a panic", in: "", model: "", physical: 0, logical: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			model, physical, logical := parseCPUInfo(strings.NewReader(tc.in))
			if model != tc.model {
				t.Errorf("model = %q, want %q", model, tc.model)
			}
			if physical != tc.physical {
				t.Errorf("physical cores = %d, want %d", physical, tc.physical)
			}
			if logical != tc.logical {
				t.Errorf("logical cores = %d, want %d", logical, tc.logical)
			}
		})
	}
}

func cpuBlock(processor, model, pkg, core string) string {
	return "processor\t: " + processor + "\n" +
		"model name\t: " + model + "\n" +
		"physical id\t: " + pkg + "\n" +
		"core id\t\t: " + core + "\n\n"
}

func TestParseOSRelease(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{
			name: "prefers PRETTY_NAME and strips its quotes",
			in:   "NAME=\"Ubuntu\"\nVERSION=\"24.04.1 LTS (Noble Numbat)\"\nPRETTY_NAME=\"Ubuntu 24.04.1 LTS\"\n",
			want: "Ubuntu 24.04.1 LTS",
		},
		{
			// Alpine's file has no PRETTY_NAME on some releases.
			name: "falls back to NAME + VERSION",
			in:   "NAME=\"Alpine Linux\"\nVERSION=3.20.3\nID=alpine\n",
			want: "Alpine Linux 3.20.3",
		},
		{name: "NAME alone is better than nothing", in: "NAME=Debian\n", want: "Debian"},
		{name: "an unreadable file yields empty, not a guess", in: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseOSRelease(strings.NewReader(tc.in)); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestZoneFromPath(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"absolute symlink target", "/usr/share/zoneinfo/Europe/Rome", "Europe/Rome"},
		{"relative symlink target", "../usr/share/zoneinfo/Europe/Rome", "Europe/Rome"},
		{"single-segment zone", "/usr/share/zoneinfo/UTC", "UTC"},
		{"three-segment zone", "/usr/share/zoneinfo/America/Argentina/Salta", "America/Argentina/Salta"},
		{"not a zoneinfo path at all", "/etc/localtime", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := zoneFromPath(tc.in); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// resolveZone is what stands between a name off the wire and a relink of
// /etc/localtime, so its refusals are the security-relevant assertions here.
func TestResolveZoneRefusesAnythingOutsideZoneinfo(t *testing.T) {
	dir := t.TempDir()
	zoneDir := filepath.Join(dir, "zoneinfo")
	if err := os.MkdirAll(filepath.Join(zoneDir, "Europe"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(zoneDir, "Europe", "Rome"), []byte("TZif"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A secret next to the zone tree, and a symlink inside the tree pointing at
	// it — the escape a string-prefix check would wave through.
	secret := filepath.Join(dir, "shadow")
	if err := os.WriteFile(secret, []byte("root:x:"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(zoneDir, "Sneaky")); err != nil {
		t.Fatal(err)
	}

	prev := ZoneinfoDir
	ZoneinfoDir = zoneDir
	t.Cleanup(func() { ZoneinfoDir = prev })

	if _, err := resolveZone("Europe/Rome"); err != nil {
		t.Fatalf("a real zone must resolve: %v", err)
	}
	if !KnownTimezone("Europe/Rome") {
		t.Error("KnownTimezone must agree with resolveZone")
	}

	for _, bad := range []string{
		"",                    // nothing given
		"../shadow",           // lexical traversal
		"Europe/../../shadow", // traversal mid-path
		"/etc/shadow",         // absolute
		"Sneaky",              // symlink escape: inside the tree, points out of it
		"Europe",              // a directory is a path, not a zone
		"Mars/Olympus",        // simply not there
	} {
		t.Run("refuses "+bad, func(t *testing.T) {
			if _, err := resolveZone(bad); err == nil {
				t.Errorf("resolveZone(%q) must fail", bad)
			}
			if KnownTimezone(bad) {
				t.Errorf("KnownTimezone(%q) must be false", bad)
			}
		})
	}
}

// Collect must never panic or block on a host where some source is missing —
// it is called from an interactive RPC.
func TestCollectIsBestEffort(t *testing.T) {
	got := Collect("/")
	if got.DiskTotalBytes <= 0 {
		t.Error("statfs on / must report a total size")
	}
	if got.TimeUnixMs <= 0 {
		t.Error("the clock must always be reported")
	}
	// -720..+840 minutes is the full range of real UTC offsets.
	if got.UTCOffsetMinutes < -720 || got.UTCOffsetMinutes > 840 {
		t.Errorf("implausible UTC offset %d", got.UTCOffsetMinutes)
	}
}
