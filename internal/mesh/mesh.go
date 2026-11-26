package mesh

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.zx2c4.com/wireguard/device"
)

const (
	// MTU is the IPv6 minimum, so a path through odd encapsulation still carries every packet whole.
	MTU = 1280
	// keepalive holds each session open, so a peer whose packets stop is noticed and rekeyed.
	keepalive = 25
	// A direct path is pinged every second and is gone after 4 s without an answer: well inside the 5 s a
	// cluster lease survives.
	pingLive   = time.Second
	pingOther  = time.Second
	liveFor    = 4 * time.Second
	waitPong   = 5 * time.Second
	learnedFor = 2 * time.Minute
	// A relay path is dialed again at most this often, and closed after this long unused.
	relayRedial = 2 * time.Second
	relayIdle   = time.Minute
	// A session that shook hands this recently is carrying packets; WireGuard rekeys every 2 minutes.
	handshakeFresh = 3 * time.Minute
	maxPongs       = 20
	maxFrame       = MTU + 256
)

// Peer is another server on the network.
type Peer struct {
	// Fingerprint names the peer's agent certificate, which is how relays and streams know it.
	Fingerprint string
	PublicKey   Key
	Address     netip.Addr
	// Endpoints are where it may answer directly; with none it is reached through a relay only.
	Endpoints []netip.AddrPort
	// Next is the key it is about to switch to: taken once it says so under its current one, never before.
	Next Key
}

// Config is this server's side of the network.
type Config struct {
	PrivateKey Key
	// Port is the UDP port to answer on; 0 picks one.
	Port    uint16
	Address netip.Addr
	// Relay opens a stream to a peer through a cluster relay; the tunnel's packets ride it when no direct path answers.
	Relay func(ctx context.Context, fingerprint string) (net.Conn, error)
	Logf  func(format string, args ...any)
	// GreetPort is a port every peer answers on: a new peer is greeted there at once, so this run hears it within
	// a round trip, not at its next alive (a restarted name server stayed silent up to 2 s, deplo ADR-0049).
	GreetPort uint16
}

// PeerStatus is how the tunnel to one peer stands.
type PeerStatus struct {
	Fingerprint   string
	Path          string // "direct", "relay" or "none"
	Endpoint      string
	LastHandshake time.Time
}

// Path names.
const (
	PathDirect = "direct"
	PathRelay  = "relay"
	PathNone   = "none"
)

// Mesh is one server's end of the network.
type Mesh struct {
	cfg  Config
	pub  Key
	dev  *device.Device
	tnet *netTun
	bind *bind

	mu     sync.RWMutex
	peers  map[string]*peer
	byKey  map[Key]*peer
	byAddr map[netip.AddrPort]*peer
	stop   chan struct{}
	closed bool
	// loop is run(), which Close waits for: nothing of a closed network is still running when it returns.
	loop sync.WaitGroup
	// blockUDP drops every datagram this end sends: a test's way to cut the direct path.
	blockUDP atomic.Bool

	// incarnation names this run of the network; peers close what they hold of an earlier one (alive.go).
	incarnation uint64
	alive       net.PacketConn
	aliveAt     time.Time
	aliveMu     sync.Mutex
	lives       map[string]liveness
	// heardAt outlives lives' reaping: a name server's removal time may be longer than aliveDeadAfter.
	heardAt map[string]time.Time
	started time.Time
	connsMu sync.Mutex
	conns   map[string]map[*tracked]bool
}

type pathInfo struct {
	at  time.Time
	rtt time.Duration
}

type sentPing struct {
	addr netip.AddrPort
	at   time.Time
}

type peer struct {
	Peer
	pair     [32]byte
	ep       *endpoint
	learned  map[netip.AddrPort]time.Time
	live     map[netip.AddrPort]pathInfo
	waiting  map[uint64]sentPing
	lastPing map[netip.AddrPort]time.Time
	current  netip.AddrPort

	relay        *relayLink
	relayDialing bool
	relayTried   time.Time
	// pending holds what was sent before any path answered: WireGuard would otherwise wait 5 s to try again.
	pending   [][]byte
	pendingAt time.Time

	pongs      int
	pongWindow time.Time
	// sentInit is when WireGuard last handed over an initiation for p, held or sent, until p answers it (0 then);
	// tookInit is when this end last took one of p's. Unix nanoseconds.
	sentInit atomic.Int64
	tookInit atomic.Int64
	// heldInits are p's initiations while it has a next key: one made under that key goes in once it is taken.
	heldInits []heldInit
}

type heldInit struct {
	b  []byte
	at time.Time
}

const (
	maxPending = 32
	// crossWindow is how close two initiations must be to cross: then only the lower key's goes on.
	crossWindow = time.Second
	pendingFor  = 3 * time.Second
)

// queue keeps a packet until a path answers; the caller holds m.mu.
func (p *peer) queue(bufs [][]byte, now time.Time) {
	if now.Sub(p.pendingAt) > pendingFor {
		p.pending = nil
	}
	p.pendingAt = now
	for _, b := range bufs {
		p.pending = append(p.pending, append([]byte(nil), b...))
	}
	if over := len(p.pending) - maxPending; over > 0 {
		p.pending = p.pending[over:]
	}
}

// takePending is what is still worth sending; the caller holds m.mu.
func (p *peer) takePending(now time.Time) [][]byte {
	out := p.pending
	p.pending = nil
	if now.Sub(p.pendingAt) > pendingFor {
		return nil
	}
	return out
}

// Start brings this server's end of the network up.
func Start(cfg Config) (*Mesh, error) {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if !cfg.Address.IsValid() {
		return nil, errors.New("the network needs an address for this server")
	}
	pub, err := cfg.PrivateKey.Public()
	if err != nil {
		return nil, err
	}
	tnet, err := newNetTUN(cfg.Address, MTU)
	if err != nil {
		return nil, err
	}
	m := &Mesh{
		cfg: cfg, pub: pub, tnet: tnet,
		peers: map[string]*peer{}, byKey: map[Key]*peer{}, byAddr: map[netip.AddrPort]*peer{},
		stop:        make(chan struct{}),
		incarnation: newIncarnation(),
		lives:       map[string]liveness{},
		heardAt:     map[string]time.Time{},
		started:     time.Now(),
		conns:       map[string]map[*tracked]bool{},
	}
	m.bind = newBind(m)
	logger := &device.Logger{
		Verbosef: func(string, ...any) {},
		Errorf:   func(f string, a ...any) { cfg.Logf("cluster network: "+f, a...) },
	}
	m.dev = device.NewDevice(tnet, m.bind, logger)
	if err := m.dev.IpcSet(fmt.Sprintf("private_key=%s\nlisten_port=%d\n", cfg.PrivateKey.hex(), cfg.Port)); err != nil {
		m.dev.Close()
		return nil, err
	}
	if err := m.dev.Up(); err != nil {
		m.dev.Close()
		return nil, err
	}
	if err := m.listenAlive(); err != nil {
		cfg.Logf("cluster network: no liveness inside the tunnel: %v", err)
	}
	m.loop.Add(1)
	go func() {
		defer m.loop.Done()
		m.run()
	}()
	return m, nil
}

// Heard is when each peer was last heard alive, by certificate fingerprint; one never heard is absent.
func (m *Mesh) Heard() map[string]time.Time {
	m.aliveMu.Lock()
	defer m.aliveMu.Unlock()
	out := make(map[string]time.Time, len(m.heardAt))
	for fp, at := range m.heardAt {
		out[fp] = at
	}
	return out
}

// Started is when this run of the network began.
func (m *Mesh) Started() time.Time { return m.started }

// PublicKey is what peers know this server by.
func (m *Mesh) PublicKey() Key {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pub
}

// Address is this server's address inside the tunnel.
func (m *Mesh) Address() netip.Addr { return m.cfg.Address }

// Port is the UDP port this server answers on.
func (m *Mesh) Port() uint16 { return m.bind.port() }

// SetPeers replaces the peers. A peer that stays keeps its session; one whose key changed starts over. A key named
// twice is the first entry's.
func (m *Mesh) SetPeers(peers []Peer) error {
	var ipc strings.Builder
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return net.ErrClosed
	}
	seen := map[string]bool{}
	var fresh []string
	// One key is one peer, the first entry: WireGuard knows a peer by its key, the bind by the addresses it sends from.
	keys := map[Key]bool{}
	for _, np := range peers {
		if np.Fingerprint == "" || np.PublicKey.IsZero() || np.PublicKey == m.pub || !np.Address.IsValid() ||
			seen[np.Fingerprint] || keys[np.PublicKey] {
			continue
		}
		keys[np.PublicKey] = true
		p := m.peers[np.Fingerprint]
		if p != nil && p.PublicKey != np.PublicKey {
			fmt.Fprintf(&ipc, "public_key=%s\nremove=true\n", p.PublicKey.hex())
			m.dropLocked(p)
			p = nil
		}
		if p == nil {
			pair, err := pairKey(m.cfg.PrivateKey, np.PublicKey)
			if err != nil {
				continue
			}
			p = &peer{
				Peer: np, pair: pair,
				learned: map[netip.AddrPort]time.Time{}, live: map[netip.AddrPort]pathInfo{},
				waiting: map[uint64]sentPing{}, lastPing: map[netip.AddrPort]time.Time{},
			}
			p.ep = &endpoint{p: p}
			m.peers[np.Fingerprint], m.byKey[np.PublicKey] = p, p
			fresh = append(fresh, np.Fingerprint)
		} else {
			p.Address, p.Endpoints, p.Next = np.Address, np.Endpoints, np.Next
		}
		seen[np.Fingerprint] = true
		fmt.Fprintf(&ipc, "public_key=%s\nreplace_allowed_ips=true\nallowed_ip=%s\nendpoint=%s\npersistent_keepalive_interval=%d\n",
			np.PublicKey.hex(), netip.PrefixFrom(np.Address, np.Address.BitLen()), endpointPrefix+np.Fingerprint, keepalive)
	}
	var gone []string
	for fp, p := range m.peers {
		if !seen[fp] {
			// A key that moved to another certificate (a renewal) keeps its session under the new one.
			if !keys[p.PublicKey] {
				fmt.Fprintf(&ipc, "public_key=%s\nremove=true\n", p.PublicKey.hex())
			}
			m.dropLocked(p)
			gone = append(gone, fp)
		}
	}
	m.reindexLocked()
	m.mu.Unlock()
	// A server taken off the network takes its connections with it.
	for _, fp := range gone {
		m.reap(fp)
	}
	if ipc.Len() == 0 {
		return nil
	}
	err := m.dev.IpcSet(ipc.String())
	// Look for direct paths, and say this run is alive, now rather than at the next tick.
	go func() {
		now := time.Now()
		m.tick(now)
		m.sayAlive(now)
	}()
	for _, fp := range fresh {
		go m.greetPeer(fp)
	}
	return err
}

// greetPeer opens and closes one stream to a new peer: its greeting is the peer heard.
func (m *Mesh) greetPeer(fp string) {
	if m.cfg.GreetPort == 0 {
		return
	}
	// Tried every 100 ms: a first SYN lost while the path is still being found is sent again only after a second.
	for end := time.Now().Add(greetWithin); time.Now().Before(end); {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		c, err := m.Dial(ctx, fp, m.cfg.GreetPort)
		cancel()
		if err == nil {
			c.Close()
			return
		}
		m.mu.RLock()
		gone := m.closed || m.peers[fp] == nil
		m.mu.RUnlock()
		if gone {
			return
		}
	}
}

// dropLocked forgets a peer; the caller holds m.mu.
func (m *Mesh) dropLocked(p *peer) {
	delete(m.peers, p.Fingerprint)
	if m.byKey[p.PublicKey] == p {
		delete(m.byKey, p.PublicKey)
	}
	if p.relay != nil {
		p.relay.c.Close()
		p.relay = nil
	}
}

// reindexLocked rebuilds which source address belongs to which peer; the caller holds m.mu.
func (m *Mesh) reindexLocked() {
	m.byAddr = map[netip.AddrPort]*peer{}
	for _, p := range m.peers {
		for _, a := range p.Endpoints {
			m.byAddr[norm(a)] = p
		}
		for a := range p.learned {
			m.byAddr[a] = p
		}
		for a := range p.live {
			m.byAddr[a] = p
		}
	}
}

func norm(a netip.AddrPort) netip.AddrPort { return netip.AddrPortFrom(a.Addr().Unmap(), a.Port()) }

// Has says whether a peer is on the network.
func (m *Mesh) Has(fingerprint string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.peers[fingerprint] != nil
}

// Listen answers TCP connections inside the tunnel on this server's address. Nothing outside the tunnel reaches it.
func (m *Mesh) Listen(port uint16) (net.Listener, error) {
	ln, err := m.tnet.ListenTCPAddrPort(netip.AddrPortFrom(m.cfg.Address, port))
	if err != nil {
		return nil, err
	}
	return m.trackListener(ln), nil
}

// ServeRelay carries the tunnel's packets over a relay stream a peer opened, until it ends.
func (m *Mesh) ServeRelay(fingerprint string, c net.Conn) {
	m.mu.RLock()
	p := m.peers[fingerprint]
	m.mu.RUnlock()
	if p == nil {
		c.Close()
		return
	}
	m.attach(p, c)
}

// Status is how the tunnel to each peer stands.
func (m *Mesh) Status() []PeerStatus {
	handshakes := map[Key]time.Time{}
	if raw, err := m.dev.IpcGet(); err == nil {
		var cur Key
		var sec, nsec int64
		flush := func() {
			if !cur.IsZero() && sec > 0 {
				handshakes[cur] = time.Unix(sec, nsec)
			}
		}
		sc := bufio.NewScanner(strings.NewReader(raw))
		for sc.Scan() {
			k, v, _ := strings.Cut(sc.Text(), "=")
			switch k {
			case "public_key":
				flush()
				cur, sec, nsec = Key{}, 0, 0
				if b, err := hexKey(v); err == nil {
					cur = b
				}
			case "last_handshake_time_sec":
				sec, _ = strconv.ParseInt(v, 10, 64)
			case "last_handshake_time_nsec":
				nsec, _ = strconv.ParseInt(v, 10, 64)
			}
		}
		flush()
	}
	now := time.Now()
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]PeerStatus, 0, len(m.peers))
	for _, p := range m.peers {
		st := PeerStatus{Fingerprint: p.Fingerprint, Path: PathNone, LastHandshake: handshakes[p.PublicKey]}
		if a, ok := p.directLocked(now); ok {
			st.Path, st.Endpoint = PathDirect, a.String()
		} else if p.relay != nil && !st.LastHandshake.IsZero() && now.Sub(st.LastHandshake) < handshakeFresh {
			// Only a relay that is up: a peer cut off read "relay" for 3 minutes, and Deplo moved the leader (c46).
			st.Path = PathRelay
		}
		out = append(out, st)
	}
	return out
}

// Close takes this server off the network.
func (m *Mesh) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.stop)
	for _, p := range m.peers {
		if p.relay != nil {
			p.relay.c.Close()
		}
	}
	m.mu.Unlock()
	if m.alive != nil {
		m.alive.Close()
	}
	// Streams end while the netstack can still carry their last packets; untrack takes connsMu, so not under it.
	m.connsMu.Lock()
	var open []*tracked
	for _, set := range m.conns {
		for c := range set {
			open = append(open, c)
		}
	}
	m.connsMu.Unlock()
	for _, c := range open {
		c.Close()
	}
	// Down first: WireGuard's receivers end before it closes the netstack, which panics when a packet still comes in.
	_ = m.dev.Down()
	m.dev.Close()
	m.loop.Wait()
	return nil
}

func (m *Mesh) run() {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-t.C:
			m.tick(now)
			if now.Sub(m.aliveAt) >= aliveEvery {
				m.aliveAt = now
				m.sayAlive(now)
			}
		}
	}
}

// Rotate moves this end to a new key and closes nothing: a peer that holds it as this end's next key takes it once
// told, within a round trip, and the others once Deplo hands it to them.
func (m *Mesh) Rotate(key Key) error {
	pub, err := key.Public()
	if err != nil {
		return err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return net.ErrClosed
	}
	var out []outPing
	for _, p := range m.peers {
		// Under the pair the peer still holds.
		pkt := probe{kind: probeMoved, sender: m.pub, nonce: randomNonce()}.encode(p.pair)
		for a := range p.targetsLocked() {
			out = append(out, outPing{a, pkt})
		}
		if pair, err := pairKey(key, p.PublicKey); err == nil {
			p.pair = pair
		}
	}
	m.cfg.PrivateKey, m.pub = key, pub
	m.mu.Unlock()
	if err := m.dev.IpcSet(fmt.Sprintf("private_key=%s\n", key.hex())); err != nil {
		return err
	}
	// Said three times: a datagram may be lost.
	go func() {
		for i := 0; i < 3; i++ {
			for _, o := range out {
				m.bind.sendUDP(o.pkt, o.to)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	return nil
}

// takeNextLocked makes p's next key its key; the caller holds m.mu, hands ipc to the device, then delivers again
// the initiations p sent meanwhile: lost, they cost 5 s before WireGuard tries again.
func (m *Mesh) takeNextLocked(p *peer) (ipc string, again [][]byte) {
	pair, err := pairKey(m.cfg.PrivateKey, p.Next)
	if err != nil {
		return "", nil
	}
	old := p.PublicKey
	if m.byKey[old] == p {
		delete(m.byKey, old)
	}
	p.PublicKey, p.Next, p.pair = p.Next, Key{}, pair
	m.byKey[p.PublicKey] = p
	for _, h := range p.heldInits {
		if time.Since(h.at) < crossWindow {
			again = append(again, h.b)
		}
	}
	p.heldInits = nil
	// No keepalive yet: one now would cross the peer's own handshake and cost 5 s; Deplo's next table sets it.
	return fmt.Sprintf("public_key=%s\nremove=true\npublic_key=%s\nreplace_allowed_ips=true\nallowed_ip=%s\nendpoint=%s\n",
		old.hex(), p.PublicKey.hex(), netip.PrefixFrom(p.Address, p.Address.BitLen()), endpointPrefix+p.Fingerprint), again
}

// peerAt is the peer a datagram from this address belongs to.
func (m *Mesh) peerAt(a netip.AddrPort) *peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byAddr[a]
}

// routeOrHold is where a packet to p goes now: a direct address, or the relay. With neither, it is queued under
// the same lock a path coming up flushes the queue with, so nothing is queued after the flush and left there.
func (m *Mesh) routeOrHold(p *peer, bufs [][]byte) (netip.AddrPort, *relayLink, bool) {
	now := time.Now()
	m.mu.RLock()
	a, direct := p.directLocked(now)
	link := p.relay
	m.mu.RUnlock()
	if direct || link != nil {
		return a, link, direct
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if a, direct = p.directLocked(now); direct || p.relay != nil {
		return a, p.relay, direct
	}
	p.queue(bufs, now)
	return netip.AddrPort{}, nil, false
}

// relayFor starts a relay stream to p when none is up.
func (m *Mesh) relayFor(p *peer) {
	if m.cfg.Relay == nil {
		return
	}
	now := time.Now()
	m.mu.Lock()
	if m.closed || p.relay != nil || p.relayDialing || now.Sub(p.relayTried) < relayRedial || m.peers[p.Fingerprint] != p {
		m.mu.Unlock()
		return
	}
	p.relayDialing, p.relayTried = true, now
	m.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		c, err := m.cfg.Relay(ctx, p.Fingerprint)
		cancel()
		m.mu.Lock()
		p.relayDialing = false
		m.mu.Unlock()
		if err != nil {
			return
		}
		m.attach(p, c)
	}()
}

// attach makes c p's relay path and reads packets from it until it ends.
func (m *Mesh) attach(p *peer, c net.Conn) {
	l := newRelayLink(c)
	m.mu.Lock()
	if m.closed || m.peers[p.Fingerprint] != p {
		m.mu.Unlock()
		c.Close()
		return
	}
	p.relay = l
	var flush [][]byte
	if _, direct := p.directLocked(time.Now()); !direct {
		flush = p.takePending(time.Now())
	}
	m.mu.Unlock()
	flush = p.withoutStaleInits(flush)
	p.noteSent(flush)
	for _, b := range flush {
		if l.write(b) != nil {
			break
		}
	}
	l.read(func(b []byte) bool { return m.bind.deliver(p, b) })
	m.mu.Lock()
	if p.relay == l {
		p.relay = nil
	}
	m.mu.Unlock()
	c.Close()
}

func hexKey(s string) (Key, error) {
	var k Key
	if len(s) != 64 {
		return k, errors.New("not a key")
	}
	for i := 0; i < 32; i++ {
		v, err := strconv.ParseUint(s[2*i:2*i+2], 16, 8)
		if err != nil {
			return k, err
		}
		k[i] = byte(v)
	}
	return k, nil
}
