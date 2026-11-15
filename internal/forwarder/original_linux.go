//go:build linux

package forwarder

import (
	"net"

	"golang.org/x/sys/unix"
)

// OriginalPort is the port a connection was dialed on before a redirect brought it to this listener; 0 when
// it cannot tell.
func OriginalPort(c net.Conn) int {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return 0
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return 0
	}
	port := 0
	_ = raw.Control(func(fd uintptr) {
		// SO_ORIGINAL_DST answers a sockaddr_in; its port is bytes 2 and 3, network order.
		m, err := unix.GetsockoptIPv6Mreq(int(fd), unix.SOL_IP, unix.SO_ORIGINAL_DST)
		if err == nil {
			port = int(m.Multiaddr[2])<<8 | int(m.Multiaddr[3])
		}
	})
	return port
}
