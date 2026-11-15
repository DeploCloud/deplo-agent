// Package forwarder is the process inside a name forwarder's container: it listens on the
// workload's port and hands every connection to this host's agent, which decides where it goes (#200).
// The agent redirects every TCP port of the container to that one listener, so the name answers on all of them.
package forwarder

import (
	"flag"
	"fmt"
	"io"
	"net"
	"time"
)

// Main runs `deplo-agent forward --listen :port --to /path/to/the/agent's/socket`.
func Main(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("forward", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", "", "address to listen on inside the container")
	to := fs.String("to", "", "the agent's socket for this name")
	if err := fs.Parse(args); err != nil || *listen == "" || *to == "" {
		fmt.Fprintln(stderr, "forward: --listen and --to are required")
		return 2
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "forward: %v\n", err)
		return 1
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			return 1
		}
		go Relay(c, *to)
	}
}

// HeaderVerb opens what a forwarder sends the agent: the port its caller dialed, 0 when it cannot tell.
const HeaderVerb = "PORT"

// Relay joins c to a new connection to the agent's socket until either side closes.
func Relay(c net.Conn, socket string) {
	up, err := net.DialTimeout("unix", socket, 10*time.Second)
	if err != nil {
		c.Close()
		return
	}
	if _, err := fmt.Fprintf(up, "%s %d\n", HeaderVerb, OriginalPort(c)); err != nil {
		c.Close()
		up.Close()
		return
	}
	Join(c, up)
}

// Join copies both ways until both directions are done. A side that only stopped sending still gets its answer:
// the end of one direction closes the other end's sending side, and both ends only when that cannot be done.
func Join(a, b net.Conn) {
	done := make(chan bool, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		cw, ok := dst.(interface{ CloseWrite() error })
		done <- ok && cw.CloseWrite() == nil
	}
	go cp(a, b)
	go cp(b, a)
	if !<-done {
		a.Close()
		b.Close()
	}
	<-done
	a.Close()
	b.Close()
}
