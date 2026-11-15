package mesh

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"net/netip"
	"sync"
	"time"
)

// Every end tells each peer, inside the tunnel, that it is alive, which run of it this is and how long that run has
// been up. A connection to a peer that restarted, or that went quiet, would otherwise wait forever on a read: the
// peer's new network stack never heard of it and a reader sends nothing that could be refused.
const (
	alivePort = 7001
	aliveLen  = 4 + 8 + 8
	// aliveSlack is how much earlier than a run's start, as its peer reckons it, a connection may be and still be
	// that run's: the difference between two trips through the tunnel, never the time a restart takes.
	aliveSlack = 250 * time.Millisecond
)

var (
	aliveEvery     = 2 * time.Second
	aliveDeadAfter = 15 * time.Second
)

var aliveMagic = [4]byte{'D', 'P', 'L', 'A'}

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

// tracked is a connection inside the tunnel, kept so it can be closed when its peer's run ends.
type tracked struct {
	net.Conn
	m    *Mesh
	fp   string
	at   time.Time
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

func (m *Mesh) track(fp string, c net.Conn) net.Conn {
	t := &tracked{Conn: c, m: m, fp: fp, at: time.Now()}
	m.connsMu.Lock()
	if m.conns[fp] == nil {
		m.conns[fp] = map[*tracked]bool{}
	}
	m.conns[fp][t] = true
	m.connsMu.Unlock()
	return t
}

func (m *Mesh) untrack(t *tracked) {
	m.connsMu.Lock()
	delete(m.conns[t.fp], t)
	if len(m.conns[t.fp]) == 0 {
		delete(m.conns, t.fp)
	}
	m.connsMu.Unlock()
}

// reap closes every connection to a peer's run that is over.
func (m *Mesh) reap(fp string) { m.reapBefore(fp, time.Time{}) }

// reapBefore closes a peer's connections made before `before`, or all of them for a zero time; it says how many.
func (m *Mesh) reapBefore(fp string, before time.Time) int {
	m.connsMu.Lock()
	list := make([]*tracked, 0, len(m.conns[fp]))
	for t := range m.conns[fp] {
		if before.IsZero() || t.at.Before(before) {
			list = append(list, t)
		}
	}
	m.connsMu.Unlock()
	for _, t := range list {
		t.Close()
	}
	return len(list)
}

// trackedListener files each accepted connection under the peer whose tunnel address it came from.
type trackedListener struct {
	net.Listener
	m *Mesh
}

func (l *trackedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if p := l.m.peerByAddr(c.RemoteAddr()); p != "" {
		return l.m.track(p, c), nil
	}
	return c, nil
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

// listenAlive answers the peers' liveness inside the tunnel; a peer is known by the address only it can send from.
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
				up := time.Duration(binary.BigEndian.Uint64(buf[12:aliveLen])) * time.Millisecond
				m.heard(fp, binary.BigEndian.Uint64(buf[4:12]), up)
			}
		}
	}()
	return nil
}

// heard records a peer's run. A run heard for the first time ends every connection made before it started, which
// can only belong to a run that is over, heard from or not.
func (m *Mesh) heard(fp string, incarnation uint64, up time.Duration) {
	now := time.Now()
	m.aliveMu.Lock()
	was, known := m.lives[fp]
	m.lives[fp] = liveness{incarnation: incarnation, at: now}
	m.aliveMu.Unlock()
	if known && was.incarnation == incarnation {
		return
	}
	if n := m.reapBefore(fp, now.Add(-up-aliveSlack)); n > 0 {
		m.cfg.Logf("cluster network: %s restarted, %d connections to its earlier run are closed", short(fp), n)
	}
}

// sayAlive tells every peer this run is alive, and closes the connections of a peer that stopped saying it, or that
// has not said it since they were made.
func (m *Mesh) sayAlive(now time.Time) {
	if m.alive == nil {
		return
	}
	msg := make([]byte, aliveLen)
	copy(msg, aliveMagic[:])
	binary.BigEndian.PutUint64(msg[4:12], m.incarnation)
	binary.BigEndian.PutUint64(msg[12:], uint64(time.Since(m.started).Milliseconds()))
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
	heard := map[string]bool{}
	m.aliveMu.Lock()
	for fp, l := range m.lives {
		if _, still := addrs[fp]; !still {
			delete(m.lives, fp)
			continue
		}
		if now.Sub(l.at) >= aliveDeadAfter {
			quiet = append(quiet, fp)
			delete(m.lives, fp)
			continue
		}
		heard[fp] = true
	}
	m.aliveMu.Unlock()
	for _, fp := range quiet {
		m.cfg.Logf("cluster network: %s fell quiet, its connections are closed", short(fp))
		m.reap(fp)
	}
	for _, fp := range m.unheard(heard) {
		if n := m.reapBefore(fp, now.Add(-aliveDeadAfter)); n > 0 {
			m.cfg.Logf("cluster network: %s has not said it is alive, %d connections to it are closed", short(fp), n)
		}
	}
}

// unheard names the peers holding connections that no run of theirs has been heard for.
func (m *Mesh) unheard(heard map[string]bool) []string {
	m.connsMu.Lock()
	defer m.connsMu.Unlock()
	var out []string
	for fp := range m.conns {
		if !heard[fp] {
			out = append(out, fp)
		}
	}
	return out
}

func short(fp string) string {
	if len(fp) > 12 {
		return fp[:12]
	}
	return fp
}
