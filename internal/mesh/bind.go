package mesh

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.zx2c4.com/wireguard/conn"
)

// endpointPrefix names a peer, never an address: the bind decides each packet's path, so the device never roams.
const endpointPrefix = "deplo:"

type endpoint struct{ p *peer }

func (e *endpoint) ClearSrc()           {}
func (e *endpoint) SrcToString() string { return "" }
func (e *endpoint) DstToString() string { return endpointPrefix + e.p.Fingerprint }

// DstToBytes keys WireGuard's cookie under load; the peer's key is what this endpoint stands for.
func (e *endpoint) DstToBytes() []byte { return e.p.PublicKey[:] }
func (e *endpoint) DstIP() netip.Addr  { return netip.Addr{} }
func (e *endpoint) SrcIP() netip.Addr  { return netip.Addr{} }

type inbound struct {
	p *peer
	b []byte
}

// bind is WireGuard's transport: one UDP socket for direct paths, and relay streams for the rest.
type bind struct {
	m      *Mesh
	mu     sync.Mutex
	udp    atomic.Pointer[net.UDPConn]
	done   chan struct{}
	in     chan inbound
	actual atomic.Uint32
}

func newBind(m *Mesh) *bind { return &bind{m: m, in: make(chan inbound, 1024)} }

func (b *bind) port() uint16 { return uint16(b.actual.Load()) }

func (b *bind) Open(port uint16) ([]conn.ReceiveFunc, uint16, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.udp.Load() != nil {
		return nil, 0, conn.ErrBindAlreadyOpen
	}
	c, err := net.ListenUDP("udp", &net.UDPAddr{Port: int(port)})
	if err != nil {
		return nil, 0, err
	}
	actual := uint16(c.LocalAddr().(*net.UDPAddr).Port)
	b.udp.Store(c)
	b.done = make(chan struct{})
	b.actual.Store(uint32(actual))
	return []conn.ReceiveFunc{b.receiveUDP(c), b.receiveRelay(b.done)}, actual, nil
}

func (b *bind) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.udp.Swap(nil)
	if c == nil {
		return nil
	}
	close(b.done)
	return c.Close()
}

func (b *bind) SetMark(uint32) error { return nil }
func (b *bind) BatchSize() int       { return 1 }

func (b *bind) ParseEndpoint(s string) (conn.Endpoint, error) {
	fp, ok := strings.CutPrefix(s, endpointPrefix)
	if !ok {
		return nil, fmt.Errorf("%q is not a peer of this network", s)
	}
	b.m.mu.RLock()
	p := b.m.peers[fp]
	b.m.mu.RUnlock()
	if p == nil {
		return nil, fmt.Errorf("%q is not a peer of this network", s)
	}
	return p.ep, nil
}

// receiveUDP hands WireGuard only what came from an address a peer proved it holds; probes are answered here.
func (b *bind) receiveUDP(c *net.UDPConn) conn.ReceiveFunc {
	return func(packets [][]byte, sizes []int, eps []conn.Endpoint) (int, error) {
		for {
			n, from, err := c.ReadFromUDPAddrPort(packets[0])
			if err != nil {
				return 0, err
			}
			from = norm(from)
			if isProbe(packets[0][:n]) {
				b.m.handleProbe(packets[0][:n], from)
				continue
			}
			p := b.m.peerAt(from)
			if p == nil || !b.m.takeInitiation(p, packets[0][:n]) {
				continue
			}
			sizes[0], eps[0] = n, p.ep
			return 1, nil
		}
	}
}

func (b *bind) receiveRelay(done chan struct{}) conn.ReceiveFunc {
	return func(packets [][]byte, sizes []int, eps []conn.Endpoint) (int, error) {
		for {
			select {
			case in := <-b.in:
				if !b.m.takeInitiation(in.p, in.b) {
					continue
				}
				sizes[0], eps[0] = copy(packets[0], in.b), in.p.ep
				return 1, nil
			case <-done:
				return 0, net.ErrClosed
			}
		}
	}
}

// deliver queues a packet a relay stream carried; it says false once the network is gone.
func (b *bind) deliver(p *peer, pkt []byte) bool {
	select {
	case b.in <- inbound{p, pkt}:
		return true
	case <-b.m.stop:
		return false
	}
}

func (b *bind) sendUDP(pkt []byte, to netip.AddrPort) error {
	c := b.udp.Load()
	if c == nil {
		return net.ErrClosed
	}
	if b.m.blockUDP.Load() {
		return nil
	}
	_, err := c.WriteToUDPAddrPort(pkt, to)
	return err
}

// Send takes the direct path while one answers, and the relay otherwise. A packet with no path yet is dropped:
// WireGuard and TCP both send again.
func (b *bind) Send(bufs [][]byte, ep conn.Endpoint) error {
	e, ok := ep.(*endpoint)
	if !ok {
		return conn.ErrWrongEndpointType
	}
	if bufs = e.p.withoutStaleInits(bufs); len(bufs) == 0 {
		return nil
	}
	// In flight from here, held or sent: one of p's arriving while it waits for a path has crossed it.
	e.p.noteSent(bufs)
	addr, link, direct := b.m.routeOrHold(e.p, bufs)
	if !direct && link == nil {
		b.m.relayFor(e.p)
		return nil
	}
	if direct {
		for _, buf := range bufs {
			if err := b.sendUDP(buf, addr); err != nil {
				return err
			}
		}
		return nil
	}
	for _, buf := range bufs {
		if err := link.write(buf); err != nil {
			link.c.Close()
			return nil
		}
	}
	return nil
}

// relayLink frames the tunnel's packets on a stream: a 2-byte length, then the packet.
type relayLink struct {
	c    net.Conn
	wmu  sync.Mutex
	used atomic.Int64
}

func newRelayLink(c net.Conn) *relayLink {
	l := &relayLink{c: c}
	l.used.Store(time.Now().UnixNano())
	return l
}

func (l *relayLink) lastUsed() time.Time { return time.Unix(0, l.used.Load()) }

func (l *relayLink) write(pkt []byte) error {
	if len(pkt) > maxFrame {
		return errors.New("packet too large for the relay")
	}
	frame := make([]byte, 2+len(pkt))
	binary.BigEndian.PutUint16(frame, uint16(len(pkt)))
	copy(frame[2:], pkt)
	l.wmu.Lock()
	defer l.wmu.Unlock()
	_ = l.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := l.c.Write(frame)
	if err == nil {
		l.used.Store(time.Now().UnixNano())
	}
	return err
}

// read hands each packet to deliver until the stream ends or deliver says stop.
func (l *relayLink) read(deliver func([]byte) bool) {
	var hdr [2]byte
	for {
		if _, err := io.ReadFull(l.c, hdr[:]); err != nil {
			return
		}
		n := int(binary.BigEndian.Uint16(hdr[:]))
		if n == 0 || n > maxFrame {
			return
		}
		pkt := make([]byte, n)
		if _, err := io.ReadFull(l.c, pkt); err != nil {
			return
		}
		l.used.Store(time.Now().UnixNano())
		if !deliver(pkt) {
			return
		}
	}
}
