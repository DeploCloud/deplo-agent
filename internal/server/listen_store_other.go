//go:build !linux

package server

import (
	"net"
	"os"
	"sync/atomic"
)

func keptUnixListener(_, path string) (net.Listener, error) {
	_ = os.Remove(path)
	return net.Listen("unix", path)
}

func forgetKept(string)        {}
func dropUnclaimedKept(string) {}
func keptForExec() string      { return "" }

var restarting atomic.Bool

// DrainVisitors stops taking visitors before the agent exits.
func DrainVisitors() { restarting.Store(true) }
