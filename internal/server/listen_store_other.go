//go:build !linux

package server

import (
	"net"
	"os"
)

func keptUnixListener(_, path string) (net.Listener, error) {
	_ = os.Remove(path)
	return net.Listen("unix", path)
}

func forgetKept(string)        {}
func dropUnclaimedKept(string) {}
func keptForExec() string      { return "" }
