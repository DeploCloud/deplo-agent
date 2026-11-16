//go:build linux

package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// dialInNetwork connects to addr from inside the network of process pid. The thread that enters it is never
// handed back: it ends with its goroutine, so no other goroutine ever runs in that network.
func dialInNetwork(ctx context.Context, pid int, addr string) (net.Conn, error) {
	type result struct {
		c   net.Conn
		err error
	}
	done := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		ns, err := os.Open(fmt.Sprintf("/proc/%d/ns/net", pid))
		if err != nil {
			done <- result{nil, err}
			return
		}
		defer ns.Close()
		if err := unix.Setns(int(ns.Fd()), unix.CLONE_NEWNET); err != nil {
			done <- result{nil, err}
			return
		}
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", addr)
		done <- result{c, err}
	}()
	r := <-done
	return r.c, r.err
}
