package dnszone

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

// EnvSockets names what the process is handed, one letter per descriptor from 3: "u" UDP, "t" TCP, "g" the location
// table, open for reading only.
const EnvSockets = "DEPLO_DNS_SOCKETS"

// Main is `deplo-agent dns`: it answers on the sockets it was handed, from the tables read on stdin, until stdin ends.
func Main(stdin io.Reader, stderr io.Writer) int {
	logf := func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) }
	if os.Getuid() == 0 || os.Geteuid() == 0 {
		logf("refusing to answer DNS as root")
		return 2
	}
	if err := lockDown(); err != nil {
		logf("cannot lock the process down: %v", err)
		return 2
	}
	udp, tcp, geo, err := inherited(os.Getenv(EnvSockets))
	if err != nil {
		logf("%v", err)
		return 2
	}
	srv := NewServer()
	if geo != nil {
		g, err := readGeo(geo)
		if err != nil {
			logf("the location table is refused, every healthy server is answered: %v", err)
		} else {
			srv.SetGeo(g)
		}
	}
	srv.Serve(udp, tcp)
	defer srv.Shutdown()

	stop := make(chan struct{})
	defer close(stop)
	go report(srv.lim, logf, stop)

	sc := bufio.NewScanner(stdin)
	sc.Buffer(make([]byte, 64<<10), MaxTableBytes)
	for sc.Scan() {
		var t Table
		if err := json.Unmarshal(sc.Bytes(), &t); err != nil {
			logf("a table that is not JSON: %v", err)
			continue
		}
		if err := srv.SetTable(&t); err != nil {
			logf("a table refused, the last one stays: %v", err)
		}
	}
	if err := sc.Err(); err != nil {
		logf("reading tables: %v", err)
		return 1
	}
	return 0
}

// inherited turns the descriptors from 3 on into sockets, and the location table when one is handed.
func inherited(kinds string) ([]net.PacketConn, []net.Listener, *os.File, error) {
	var udp []net.PacketConn
	var tcp []net.Listener
	var geo *os.File
	for i, k := range strings.Split(kinds, ",") {
		if k == "" {
			continue
		}
		f := os.NewFile(uintptr(3+i), "dns-"+k)
		if f == nil {
			return nil, nil, nil, fmt.Errorf("descriptor %d is missing", i)
		}
		switch k {
		case "u":
			pc, err := net.FilePacketConn(f)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("socket %d: %v", i, err)
			}
			udp = append(udp, pc)
		case "t":
			ln, err := net.FileListener(f)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("socket %d: %v", i, err)
			}
			tcp = append(tcp, ln)
		case "g":
			if geo != nil {
				return nil, nil, nil, fmt.Errorf("descriptor %d: a second location table", i)
			}
			geo = f
			continue
		default:
			return nil, nil, nil, fmt.Errorf("descriptor %d: unknown kind %q", i, k)
		}
		_ = f.Close()
	}
	if len(udp)+len(tcp) == 0 {
		return nil, nil, nil, fmt.Errorf("no socket to answer on")
	}
	return udp, tcp, geo, nil
}

// readGeo reads the location table it was handed, at most MaxGeoBytes of it, and lets the descriptor go.
func readGeo(f *os.File) (*Geo, error) {
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, MaxGeoBytes+1))
	if err != nil {
		return nil, err
	}
	return ParseGeo(raw)
}

// report says once a minute how many answers the rate limit held back, when any.
func report(l *limiter, logf func(string, ...any), stop <-chan struct{}) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if tr, dr, n := l.take(); tr+dr > 0 {
				logf("rate limit: %d answers sent truncated and %d dropped, to %d networks", tr, dr, n)
			}
		}
	}
}
