package server

import (
	"encoding/hex"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// procRoot is the host's /proc: a container's sockets are read through its main process.
var procRoot = "/proc"

type listener struct {
	port     int32
	loopback bool
}

// containerListeners reads the TCP sockets listening in pid's network namespace.
func containerListeners(pid int) []listener {
	if pid <= 0 {
		return nil
	}
	var out []listener
	for _, f := range []string{"tcp", "tcp6"} {
		b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "net", f))
		if err == nil {
			out = append(out, parseListeners(string(b))...)
		}
	}
	return out
}

// parseListeners reads the LISTEN rows of a /proc/net/tcp or tcp6 table.
func parseListeners(table string) []listener {
	var out []listener
	for _, line := range strings.Split(table, "\n") {
		f := strings.Fields(line)
		if len(f) < 4 || f[3] != "0A" {
			continue
		}
		hexIP, hexPort, ok := strings.Cut(f[1], ":")
		port, err := strconv.ParseUint(hexPort, 16, 16)
		ip := procIP(hexIP)
		if !ok || err != nil || ip == nil || port == 0 {
			continue
		}
		out = append(out, listener{port: int32(port), loopback: ip.IsLoopback()})
	}
	return out
}

// procIP decodes the kernel's address: 32-bit words in host (little-endian) order.
func procIP(h string) net.IP {
	b, err := hex.DecodeString(h)
	if err != nil || (len(b) != 4 && len(b) != 16) {
		return nil
	}
	for i := 0; i < len(b); i += 4 {
		b[i], b[i+1], b[i+2], b[i+3] = b[i+3], b[i+2], b[i+1], b[i]
	}
	return net.IP(b)
}

// splitListeners sorts listeners into the ports reachable from outside the container and the
// ones bound to its loopback only.
func splitListeners(ls []listener) (public, loopback []int32) {
	for _, l := range ls {
		if !l.loopback && !slices.Contains(public, l.port) {
			public = append(public, l.port)
		}
	}
	for _, l := range ls {
		if l.loopback && !slices.Contains(public, l.port) && !slices.Contains(loopback, l.port) {
			loopback = append(loopback, l.port)
		}
	}
	slices.Sort(public)
	slices.Sort(loopback)
	return public, loopback
}

// Ports a process opens next to its real one: debuggers, profilers, metrics exporters.
var sidePorts = map[int32]bool{5005: true, 6060: true, 9090: true, 9091: true, 9100: true, 9229: true, 9464: true}

// Ports of services that never answer a browser: an app's own SSH, mail, DNS, database or queue.
var nonHTTPPorts = map[int32]bool{22: true, 25: true, 53: true, 110: true, 143: true, 465: true, 587: true,
	993: true, 995: true, 1883: true, 3306: true, 5432: true, 5672: true, 6379: true, 11211: true, 27017: true}

// pickPort is the port to route to when the app did not open the expected one: the lowest that
// answers HTTP, else the lowest that is neither a side port nor a known non-web one, else the lowest.
func pickPort(public []int32, speaksHTTP func(int32) bool) int32 {
	if speaksHTTP != nil {
		for _, p := range public {
			if !sidePorts[p] && speaksHTTP(p) {
				return p
			}
		}
	}
	for _, p := range public {
		if !sidePorts[p] && !nonHTTPPorts[p] {
			return p
		}
	}
	for _, p := range public {
		if !sidePorts[p] {
			return p
		}
	}
	if len(public) > 0 {
		return public[0]
	}
	return 0
}

// speaksHTTP sends a plain GET and reads whether the reply is HTTP; an SSH banner or a database's is not.
func speaksHTTP(ip string, port int32) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, strconv.Itoa(int(port))), time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")); err != nil {
		return false
	}
	buf := make([]byte, 5)
	n, _ := io.ReadFull(conn, buf)
	return n == 5 && string(buf) == "HTTP/"
}
