// Package hostinfo answers "what IS this host?" - the neofetch question, as opposed to
// the gauge question hostmetrics answers. Putting them in one package would mean every
// "show me the hardware" click paid for a 1s CPU sample.
package hostinfo

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ZoneinfoDir is where IANA zone files live. A variable so tests can point it at
// a fixture tree instead of the runner's real one.
var ZoneinfoDir = "/usr/share/zoneinfo"

// Info mirrors the proto HostInfoResponse, minus the fields the caller supplies
// (Docker's version/root dir, the Traefik stack file, the control-plane container).
type Info struct {
	CPUModel         string
	CPUCores         int // physical, deduplicated by (physical id, core id)
	CPUThreads       int // logical processors
	MemTotalBytes    int64
	DiskTotalBytes   int64
	DiskUsedBytes    int64
	OSPretty         string
	Kernel           string
	Arch             string
	UptimeSec        int64
	Timezone         string
	TimeUnixMs       int64
	UTCOffsetMinutes int32
}

// Collect reads the host. dataDir is the filesystem to measure (empty => "/"),
// matching hostmetrics.Collect's contract.
func Collect(dataDir string) Info {
	if dataDir == "" {
		dataDir = "/"
	}
	info := Info{
		MemTotalBytes: memTotal(),
		OSPretty:      osPretty(),
		UptimeSec:     uptimeSec(),
	}
	info.DiskUsedBytes, info.DiskTotalBytes = diskBytes(dataDir)
	info.CPUModel, info.CPUCores, info.CPUThreads = cpuInfo()
	info.Kernel, info.Arch = unameInfo()
	info.Timezone, info.TimeUnixMs, info.UTCOffsetMinutes = clock()
	return info
}

// ---- CPU ------------------------------------------------------------------

func cpuInfo() (model string, physical, logical int) {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return "", 0, 0
	}
	defer f.Close()
	return parseCPUInfo(f)
}

// parseCPUInfo reads /proc/cpuinfo's key : value blocks, one per LOGICAL processor.
func parseCPUInfo(r io.Reader) (model string, physical, logical int) {
	seen := map[string]bool{}
	var pkg, core string
	// flush closes the block that just ended, recording its (physical, core) pair.
	flush := func() {
		if pkg != "" || core != "" {
			seen[pkg+"/"+core] = true
		}
		pkg, core = "", ""
	}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		switch key {
		case "processor":
			logical++
		case "model name", "Model", "cpu model":
			// "Model"/"cpu model" are the ARM and MIPS spellings. First one wins:
			// every block repeats it, and on a big.LITTLE board the blocks differ,
			// where any single answer is a simplification anyway.
			if model == "" {
				model = value
			}
		case "physical id":
			pkg = value
		case "core id":
			core = value
		}
	}
	flush()
	physical = len(seen)
	if physical == 0 {
		physical = logical
	}
	return model, physical, logical
}

// ---- OS -------------------------------------------------------------------

func osPretty() string {
	f, err := os.Open("/etc/os-release")
	if err != nil {
		return ""
	}
	defer f.Close()
	return parseOSRelease(f)
}

// parseOSRelease pulls PRETTY_NAME out of the shell-style os-release format,
// falling back to NAME + VERSION when it is absent (Alpine's minimal file).
func parseOSRelease(r io.Reader) string {
	fields := map[string]string{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		key, value, ok := strings.Cut(sc.Text(), "=")
		if !ok {
			continue
		}
		fields[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), `"'`)
	}
	if p := fields["PRETTY_NAME"]; p != "" {
		return p
	}
	name := fields["NAME"]
	if v := fields["VERSION"]; v != "" && name != "" {
		return name + " " + v
	}
	return name
}

// unameInfo returns the kernel release and machine arch via the syscall rather
// than shelling out to `uname` - the agent must work on a host with a minimal
// PATH, and an exec for two strings is a process we don't need.
func unameInfo() (kernel, arch string) {
	var u syscall.Utsname
	if err := syscall.Uname(&u); err != nil {
		return "", ""
	}
	return charsToString(u.Release[:]), charsToString(u.Machine[:])
}

// charsToString turns a NUL-terminated C char array from Utsname into a Go
// string. The element type is int8 on amd64 and uint8 on arm64, so the
// conversion goes through a type parameter rather than being written twice.
func charsToString[T int8 | uint8](chars []T) string {
	b := make([]byte, 0, len(chars))
	for _, c := range chars {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	return string(b)
}

// ---- Clock ----------------------------------------------------------------

// clock reports the host's IANA zone, its current wall time, and its offset from UTC in
// MINUTES, not hours, because Kathmandu is +345 and Kolkata +330.
func clock() (tz string, unixMs int64, offsetMinutes int32) {
	now := time.Now()
	tz = readTimezone()
	loc := time.Local
	if tz != "" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc = l
		}
	}
	_, offsetSec := now.In(loc).Zone()
	return tz, now.UnixMilli(), int32(offsetSec / 60)
}

// readTimezone resolves the host's IANA zone name from the /etc/localtime
// symlink (the systemd convention), falling back to /etc/timezone (Debian's
// plain-text file) where localtime is a copy rather than a link.
func readTimezone() string {
	if target, err := os.Readlink("/etc/localtime"); err == nil {
		if tz := zoneFromPath(target); tz != "" {
			return tz
		}
	}
	if b, err := os.ReadFile("/etc/timezone"); err == nil {
		if tz := strings.TrimSpace(string(b)); tz != "" {
			return tz
		}
	}
	return ""
}

// zoneFromPath turns a zoneinfo path into its IANA name:
// "/usr/share/zoneinfo/Europe/Rome" and the relative
// "../usr/share/zoneinfo/Europe/Rome" both yield "Europe/Rome".
func zoneFromPath(p string) string {
	p = filepath.Clean(p)
	const marker = "zoneinfo/"
	i := strings.LastIndex(p, marker)
	if i < 0 {
		return ""
	}
	return strings.TrimPrefix(p[i+len(marker):], "/")
}

// ---- Shared readers -------------------------------------------------------

func memTotal() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, _ := strconv.ParseInt(fields[1], 10, 64)
			return kb * 1024
		}
	}
	return 0
}

func diskBytes(path string) (used, total int64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0
	}
	bsize := int64(st.Bsize)
	total = int64(st.Blocks) * bsize
	used = total - int64(st.Bavail)*bsize
	if used < 0 {
		used = 0
	}
	return used, total
}

func uptimeSec() int64 {
	b, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(fields[0], 64)
	return int64(v)
}
