package mesh

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"sync"
	"syscall"

	"golang.zx2c4.com/wireguard/tun"
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
)

// netTun is WireGuard's netstack TUN with a Close that is safe against a packet still on its way out:
// upstream closes the channel a writer may be sending on, and that writer panics.
type netTun struct {
	ep       *channel.Endpoint
	stack    *stack.Stack
	events   chan tun.Event
	incoming chan *buffer.View
	done     chan struct{}
	once     sync.Once
	mtu      int
}

func newNetTUN(addr netip.Addr, mtu int) (*netTun, error) {
	t := &netTun{
		ep: channel.New(1024, uint32(mtu), ""),
		stack: stack.New(stack.Options{
			NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
			TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol, icmp.NewProtocol6, icmp.NewProtocol4},
			HandleLocal:        true,
		}),
		events:   make(chan tun.Event, 1),
		incoming: make(chan *buffer.View),
		done:     make(chan struct{}),
		mtu:      mtu,
	}
	sack := tcpip.TCPSACKEnabled(true)
	if err := t.stack.SetTransportProtocolOption(tcp.ProtocolNumber, &sack); err != nil {
		return nil, fmt.Errorf("could not enable TCP SACK: %v", err)
	}
	t.ep.AddNotify(t)
	if err := t.stack.CreateNIC(1, t.ep); err != nil {
		return nil, fmt.Errorf("CreateNIC: %v", err)
	}
	_, proto := fullAddr(netip.AddrPortFrom(addr, 0))
	pa := tcpip.ProtocolAddress{Protocol: proto, AddressWithPrefix: tcpip.AddrFromSlice(addr.AsSlice()).WithPrefix()}
	if err := t.stack.AddProtocolAddress(1, pa, stack.AddressProperties{}); err != nil {
		return nil, fmt.Errorf("AddProtocolAddress(%v): %v", addr, err)
	}
	route := header.IPv6EmptySubnet
	if addr.Is4() {
		route = header.IPv4EmptySubnet
	}
	t.stack.AddRoute(tcpip.Route{Destination: route, NIC: 1})
	t.events <- tun.EventUp
	return t, nil
}

func (t *netTun) Name() (string, error)    { return "go", nil }
func (t *netTun) File() *os.File           { return nil }
func (t *netTun) Events() <-chan tun.Event { return t.events }
func (t *netTun) MTU() (int, error)        { return t.mtu, nil }
func (t *netTun) BatchSize() int           { return 1 }

func (t *netTun) Read(buf [][]byte, sizes []int, offset int) (int, error) {
	select {
	case view := <-t.incoming:
		n, err := view.Read(buf[0][offset:])
		view.Release()
		if err != nil {
			return 0, err
		}
		sizes[0] = n
		return 1, nil
	case <-t.done:
		return 0, os.ErrClosed
	}
}

func (t *netTun) Write(bufs [][]byte, offset int) (int, error) {
	for _, b := range bufs {
		packet := b[offset:]
		if len(packet) == 0 {
			continue
		}
		pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: buffer.MakeWithData(packet)})
		switch packet[0] >> 4 {
		case 4:
			t.ep.InjectInbound(header.IPv4ProtocolNumber, pkb)
		case 6:
			t.ep.InjectInbound(header.IPv6ProtocolNumber, pkb)
		default:
			pkb.DecRef()
			return 0, syscall.EAFNOSUPPORT
		}
	}
	return len(bufs), nil
}

// WriteNotify hands the stack's next outbound packet to WireGuard, or drops it once the TUN is closed.
func (t *netTun) WriteNotify() {
	pkt := t.ep.Read()
	if pkt == nil {
		return
	}
	view := pkt.ToView()
	pkt.DecRef()
	select {
	case t.incoming <- view:
	case <-t.done:
		view.Release()
	}
}

// Close never closes a channel a writer sends on: done releases the writers, and the stack stops after.
func (t *netTun) Close() error {
	t.once.Do(func() {
		close(t.done)
		t.stack.RemoveNIC(1)
		t.stack.Close()
		t.ep.Close()
		close(t.events)
	})
	return nil
}

func fullAddr(a netip.AddrPort) (tcpip.FullAddress, tcpip.NetworkProtocolNumber) {
	proto := ipv6.ProtocolNumber
	if a.Addr().Is4() {
		proto = ipv4.ProtocolNumber
	}
	return tcpip.FullAddress{NIC: 1, Addr: tcpip.AddrFromSlice(a.Addr().AsSlice()), Port: a.Port()}, proto
}

func (t *netTun) DialContextTCPAddrPort(ctx context.Context, a netip.AddrPort) (*gonet.TCPConn, error) {
	fa, pn := fullAddr(a)
	return gonet.DialContextTCP(ctx, t.stack, fa, pn)
}

func (t *netTun) ListenTCPAddrPort(a netip.AddrPort) (*gonet.TCPListener, error) {
	fa, pn := fullAddr(a)
	return gonet.ListenTCP(t.stack, fa, pn)
}

func (t *netTun) ListenUDPAddrPort(a netip.AddrPort) (*gonet.UDPConn, error) {
	fa, pn := fullAddr(a)
	return gonet.DialUDP(t.stack, &fa, nil, pn)
}
