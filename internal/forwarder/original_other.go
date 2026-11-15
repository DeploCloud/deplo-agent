//go:build !linux

package forwarder

import "net"

// OriginalPort cannot tell where a connection was redirected from off Linux.
func OriginalPort(net.Conn) int { return 0 }
