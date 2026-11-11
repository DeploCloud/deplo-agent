// Package forwarder is the process inside a name forwarder's container: it listens on the
// workload's port and hands every connection to this host's agent, which decides where it goes (#200).
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

// Relay joins c to a new connection to the agent's socket until either side closes.
func Relay(c net.Conn, socket string) {
	up, err := net.DialTimeout("unix", socket, 10*time.Second)
	if err != nil {
		c.Close()
		return
	}
	Join(c, up)
}

// Join copies both ways and closes both ends when one direction finishes.
func Join(a, b net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	a.Close()
	b.Close()
	<-done
}
