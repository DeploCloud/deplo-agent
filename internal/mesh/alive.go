package mesh

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"
)

// Each run of the network says which run it is, to every peer every aliveEvery and at the start of every stream, so a
// stream belongs to one run of each end. A stream to a run that ended, or fell quiet, is closed: a dead run refuses
// nothing, so a reader would otherwise wait on it forever.
const (
	alivePort = 7001
	aliveLen  = 4 + 8
	greetLen  = 4 + 8
)

var (
	aliveEvery     = 2 * time.Second
	aliveDeadAfter = 15 * time.Second
	greetWithin    = 10 * time.Second
)

var (
	aliveMagic = [4]byte{'D', 'P', 'L', 'A'}
	greetMagic = [4]byte{'D', 'P', 'L', 'G'}
)

func newIncarnation() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint64(b[:])
}

// liveness is what this end last heard from one peer's run.
type liveness struct {
	incarnation uint64
	at          time.Time
}

// tracked is a stream inside the tunnel, filed under the peer's run that answered it.
type tracked struct {
	net.Conn
	m    *Mesh
	fp   string
	inc  uint64
	once sync.Once
}

func (c *tracked) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.m.untrack(c) })
	return err
}

// CloseWrite keeps a half-close working through the wrapper.
func (c *tracked) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return c.Close()
}

// greet opens a stream: each end says which run it is before anything else is sent.
func (m *Mesh) greet(ctx context.Context, c net.Conn) (uint64, error) {
	deadline := time.Now().Add(greetWithin)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.SetDeadline(deadline)
	defer c.SetDeadline(time.Time{})
	var out [greetLen]byte
	copy(out[:], greetMagic[:])
	binary.BigEndian.PutUint64(out[4:], m.incarnation)
	if _, err := c.Write(out[:]); err != nil {
		return 0, err
	}
	var in [greetLen]byte
	if _, err := io.ReadFull(c, in[:]); err != nil {
		return 0, err
	}
	if [4]byte(in[:4]) != greetMagic {
		return 0, errors.New("the other end is not on this cluster's network")
	}
	return binary.BigEndian.Uint64(in[4:]), nil
}

// open greets a new stream and files it under the peer's run that answered it.
func (m *Mesh) open(ctx context.Context, fp string, c net.Conn) (net.Conn, error) {
	inc, err := m.greet(ctx, c)
	if err != nil {
		c.Close()
		return nil, err
	}
	if fp == "" {
		return c, nil
	}
	t := &tracked{Conn: c, m: m, fp: fp, inc: inc}
	m.connsMu.Lock()
	if m.conns[fp] == nil {
		m.conns[fp] = map[*tracked]bool{}
	}
	m.conns[fp][t] = true
	m.connsMu.Unlock()
	m.heard(fp, inc)
	return t, nil
}

func (m *Mesh) untrack(t *tracked) {
	m.connsMu.Lock()
	delete(m.conns[t.fp], t)
	if len(m.conns[t.fp]) == 0 {
		delete(m.conns, t.fp)
	}
	m.connsMu.Unlock()
}

// reap closes a peer's streams, every one or those of runs but `keep`; it says how many.
func (m *Mesh) reap(fp string, keep ...uint64) int {
	m.connsMu.Lock()
	list := make([]*tracked, 0, len(m.conns[fp]))
	for t := range m.conns[fp] {
		if len(keep) == 0 || t.inc != keep[0] {
			list = append(list, t)
		}
	}
	m.connsMu.Unlock()
	for _, t := range list {
		t.Close()
	}
	return len(list)
}

// trackedListener greets each stream it accepts, apart from the others, and files it under the peer's run.
type trackedListener struct {
	net.Listener
	m      *Mesh
	ready  chan net.Conn
	failed chan struct{}
	err    error
}

func (m *Mesh) trackListener(ln net.Listener) *trackedListener {
	l := &trackedListener{Listener: ln, m: m, ready: make(chan net.Conn), failed: make(chan struct{})}
	go l.loop()
	return l
}

func (l *trackedListener) loop() {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			l.err = err
			close(l.failed)
			return
		}
		go func() {
			t, err := l.m.open(context.Background(), l.m.peerByAddr(c.RemoteAddr()), c)
			if err != nil {
				return
			}
			select {
			case l.ready <- t:
			case <-l.failed:
				t.Close()
			}
		}()
	}
}

func (l *trackedListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ready:
		return c, nil
	case <-l.failed:
		return nil, l.err
	}
}

func (m *Mesh) peerByAddr(a net.Addr) string {
	ap, err := netip.ParseAddrPort(a.String())
	if err != nil {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for fp, p := range m.peers {
		if p.Address == ap.Addr().Unmap() {
			return fp
		}
	}
	return ""
}

// listenAlive hears the peers say they are alive inside the tunnel; a peer is known by the address only it sends from.
func (m *Mesh) listenAlive() error {
	c, err := m.tnet.ListenUDPAddrPort(netip.AddrPortFrom(m.cfg.Address, alivePort))
	if err != nil {
		return err
	}
	m.alive = c
	go func() {
		buf := make([]byte, 64)
		for {
			n, from, err := c.ReadFrom(buf)
			if err != nil {
				return
			}
			if n != aliveLen || [4]byte(buf[:4]) != aliveMagic {
				continue
			}
			if fp := m.peerByAddr(from); fp != "" {
				m.heard(fp, binary.BigEndian.Uint64(buf[4:aliveLen]))
			}
		}
	}()
	return nil
}

// heard records a peer's run. A run heard for the first time ends every stream another run answered.
func (m *Mesh) heard(fp string, incarnation uint64) {
	m.aliveMu.Lock()
	was, known := m.lives[fp]
	m.lives[fp] = liveness{incarnation: incarnation, at: time.Now()}
	m.aliveMu.Unlock()
	if known && was.incarnation == incarnation {
		return
	}
	if n := m.reap(fp, incarnation); n > 0 {
		m.cfg.Logf("cluster network: %s restarted, %d connections to its earlier run are closed", short(fp), n)
	}
}

// sayAlive tells every peer this run is alive, and closes the streams of a peer that stopped saying it.
func (m *Mesh) sayAlive(now time.Time) {
	if m.alive == nil {
		return
	}
	msg := make([]byte, aliveLen)
	copy(msg, aliveMagic[:])
	binary.BigEndian.PutUint64(msg[4:], m.incarnation)
	m.mu.RLock()
	addrs := map[string]netip.Addr{}
	for fp, p := range m.peers {
		addrs[fp] = p.Address
	}
	m.mu.RUnlock()
	for _, a := range addrs {
		_, _ = m.alive.WriteTo(msg, net.UDPAddrFromAddrPort(netip.AddrPortFrom(a, alivePort)))
	}
	var quiet []string
	m.aliveMu.Lock()
	for fp, l := range m.lives {
		if _, still := addrs[fp]; !still {
			delete(m.lives, fp)
			continue
		}
		// Every stream's peer is heard as it opens, so a stream outlives its peer's silence by aliveDeadAfter at most.
		if now.Sub(l.at) >= aliveDeadAfter {
			quiet = append(quiet, fp)
			delete(m.lives, fp)
		}
	}
	m.aliveMu.Unlock()
	for _, fp := range quiet {
		m.cfg.Logf("cluster network: %s fell quiet, its connections are closed", short(fp))
		m.reap(fp)
	}
}

func short(fp string) string {
	if len(fp) > 12 {
		return fp[:12]
	}
	return fp
}
