package mesh

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"
)

const servicePort = 7000

var (
	fpA = strings.Repeat("a", 64)
	fpB = strings.Repeat("b", 64)
)

// fakeRelay is the cluster relay as the tunnel sees it: a stream from one peer to the other.
type fakeRelay struct {
	mu     sync.Mutex
	meshes map[string]*Mesh
	off    bool
}

func (r *fakeRelay) dial(from string) func(ctx context.Context, to string) (net.Conn, error) {
	return func(ctx context.Context, to string) (net.Conn, error) {
		r.mu.Lock()
		peer, off := r.meshes[to], r.off
		r.mu.Unlock()
		if peer == nil || off {
			return nil, io.ErrClosedPipe
		}
		a, b := net.Pipe()
		go peer.ServeRelay(from, b)
		return a, nil
	}
}

type side struct {
	m   *Mesh
	key Key
	fp  string
}

func start(t *testing.T, r *fakeRelay, fp string, addr string) side {
	t.Helper()
	key, err := NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{PrivateKey: key, Address: netip.MustParseAddr(addr), Logf: t.Logf}
	if r != nil {
		cfg.Relay = r.dial(fp)
	}
	m, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	if r != nil {
		r.mu.Lock()
		if r.meshes == nil {
			r.meshes = map[string]*Mesh{}
		}
		r.meshes[fp] = m
		r.mu.Unlock()
	}
	return side{m: m, key: key, fp: fp}
}

func (s side) as(direct bool) Peer {
	p := Peer{Fingerprint: s.fp, PublicKey: s.m.PublicKey(), Address: s.m.Address()}
	if direct {
		p.Endpoints = []netip.AddrPort{netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), s.m.Port())}
	}
	return p
}

func connect(t *testing.T, a, b side, direct bool) {
	t.Helper()
	if err := a.m.SetPeers([]Peer{b.as(direct)}); err != nil {
		t.Fatal(err)
	}
	if err := b.m.SetPeers([]Peer{a.as(direct)}); err != nil {
		t.Fatal(err)
	}
}

// echo answers every connection on the service port with what it was sent.
func echo(t *testing.T, s side) {
	t.Helper()
	ln, err := s.m.Listen(servicePort)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
}

func roundTrip(t *testing.T, c net.Conn, msg string) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != msg {
		t.Fatalf("got %q, want %q", got, msg)
	}
}

func dial(t *testing.T, from, to side) net.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := from.m.Dial(ctx, to.fp, servicePort)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func pathTo(m *Mesh, fp string) string {
	for _, st := range m.Status() {
		if st.Fingerprint == fp {
			return st.Path
		}
	}
	return ""
}

func waitPath(t *testing.T, m *Mesh, fp, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if pathTo(m, fp) == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the path to %s is %q, want %q", fp[:4], pathTo(m, fp), want)
}

func TestMesh_reachesAPeerDirectly(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	roundTrip(t, dial(t, a, b), "over the tunnel")
	waitPath(t, a.m, fpB, PathDirect)
	waitPath(t, b.m, fpA, PathDirect)
}

// Both ends start at once, so their first handshakes cross: the network must still be up in well under
// WireGuard's 5 s retry.
func TestMesh_comesUpAtOnceWhenBothEndsStartTogether(t *testing.T) {
	for i := 0; i < 5; i++ {
		a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
		echo(t, b)
		done := make(chan struct{})
		go func() { _ = b.m.SetPeers([]Peer{a.as(true)}); close(done) }()
		_ = a.m.SetPeers([]Peer{b.as(true)})
		<-done
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		c, err := a.m.Dial(ctx, fpB, servicePort)
		cancel()
		if err != nil {
			t.Fatalf("round %d: the network was not up within 3 s: %v", i, err)
		}
		roundTrip(t, c, "at once")
		c.Close()
		a.m.Close()
		b.m.Close()
	}
}

func TestMesh_reachesAPeerThroughTheRelayWhenNoDirectPathAnswers(t *testing.T) {
	r := &fakeRelay{}
	a, b := start(t, r, fpA, "fdde:9100::a"), start(t, r, fpB, "fdde:9100::b")
	connect(t, a, b, false)
	echo(t, b)
	roundTrip(t, dial(t, a, b), "through the relay")
	waitPath(t, a.m, fpB, PathRelay)
}

func TestMesh_movesAnOpenConnectionToADirectPathOnceOneAnswers(t *testing.T) {
	r := &fakeRelay{}
	a, b := start(t, r, fpA, "fdde:9100::a"), start(t, r, fpB, "fdde:9100::b")
	connect(t, a, b, false)
	echo(t, b)
	c := dial(t, a, b)
	roundTrip(t, c, "first through the relay")
	connect(t, a, b, true)
	waitPath(t, a.m, fpB, PathDirect)
	roundTrip(t, c, "then directly, on the same connection")
}

func TestMesh_fallsBackToTheRelayWhenTheDirectPathStops(t *testing.T) {
	r := &fakeRelay{}
	a, b := start(t, r, fpA, "fdde:9100::a"), start(t, r, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	c := dial(t, a, b)
	roundTrip(t, c, "directly")
	waitPath(t, a.m, fpB, PathDirect)
	a.m.blockUDP.Store(true)
	b.m.blockUDP.Store(true)
	waitPath(t, a.m, fpB, PathRelay)
	roundTrip(t, c, "the same connection, through the relay")
}

func TestMesh_aPeerNotOnTheNetworkIsNotReached(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	// b knows a, a does not know b: nothing a receives from b gets in.
	if err := b.m.SetPeers([]Peer{a.as(true)}); err != nil {
		t.Fatal(err)
	}
	echo(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if c, err := b.m.Dial(ctx, fpA, servicePort); err == nil {
		c.Close()
		t.Fatal("reached a server that does not have this one as a peer")
	}
	if _, err := a.m.Dial(ctx, fpB, servicePort); err == nil {
		t.Fatal("dialed a server that is not a peer")
	}
}

func TestMesh_forgedProbesAreIgnored(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	if err := a.m.SetPeers([]Peer{b.as(false)}); err != nil {
		t.Fatal(err)
	}
	stranger, _ := NewPrivateKey()
	key, _ := pairKey(stranger, a.m.PublicKey())
	forged := probe{kind: probePong, sender: b.m.PublicKey(), nonce: 1}.encode(key)
	to := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), a.m.Port())
	c, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(to))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, pkt := range [][]byte{forged, bytes.Repeat([]byte{4}, 100), {probeMagic}} {
		if _, err := c.Write(pkt); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	if got := pathTo(a.m, fpB); got != PathNone {
		t.Fatalf("a forged probe made the path %q", got)
	}
}

func TestPairKey_isTheSameFromBothSides(t *testing.T) {
	a, _ := NewPrivateKey()
	b, _ := NewPrivateKey()
	pa, _ := a.Public()
	pb, _ := b.Public()
	k1, err1 := pairKey(a, pb)
	k2, err2 := pairKey(b, pa)
	if err1 != nil || err2 != nil || k1 != k2 {
		t.Fatal("the two sides of a pair do not share a probe key")
	}
}

func TestKey_roundTripsThroughItsText(t *testing.T) {
	k, _ := NewPrivateKey()
	got, err := ParseKey(k.String())
	if err != nil || got != k {
		t.Fatal("a key does not read back")
	}
	if _, err := ParseKey("short"); err == nil {
		t.Fatal("a short key was read")
	}
}

// Closed while a peer still sends to it, a network shuts down with no race and no panic in WireGuard's netstack:
// its receivers stop before the netstack does.
func TestMesh_closingWhileAPeerStillSendsToIt(t *testing.T) {
	for round := 0; round < 20; round++ {
		a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
		connect(t, a, b, true)
		echo(t, a)
		c := dial(t, b, a)
		roundTrip(t, c, "before the close")
		stop := make(chan struct{})
		var wg sync.WaitGroup
		// A stream that keeps sending, and dials to a port nothing listens on: a's netstack answers each packet,
		// which is the write the race was in.
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1024)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = c.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
				if _, err := c.Write(buf); err != nil {
					return
				}
			}
		}()
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
					if d, err := b.m.Dial(ctx, fpA, servicePort+1); err == nil {
						d.Close()
					}
					cancel()
				}
			}()
		}
		time.Sleep(time.Duration(10+round%5*10) * time.Millisecond)
		a.m.Close()
		close(stop)
		wg.Wait()
		b.m.Close()
	}
}

// A server another Deplo once had in a cluster keeps that cluster's table: the same machines, under the certificates
// they had there, with the same tunnel keys. One key is one peer, the first entry: a second one under another
// certificate stole the first one's tunnel address and the answers to its probes, so the two ends fell quiet.
func TestMesh_oneKeyNamedTwiceIsOnePeer(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	stale := b.as(true)
	stale.Fingerprint, stale.Address = strings.Repeat("c", 64), netip.MustParseAddr("fdde:9100::c")
	if err := a.m.SetPeers([]Peer{b.as(true), stale}); err != nil {
		t.Fatal(err)
	}
	if err := b.m.SetPeers([]Peer{a.as(true)}); err != nil {
		t.Fatal(err)
	}
	echo(t, b)
	roundTrip(t, dial(t, a, b), "past the stale entry")
	waitPath(t, a.m, fpB, PathDirect)
	if a.m.Has(stale.Fingerprint) {
		t.Fatal("the stale entry is still a peer")
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := b.m.Heard()[fpA]; ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("b never heard a")
}

// A renewed certificate names the same key under a new fingerprint: the tunnel stays up under the new one.
func TestMesh_aKeyThatMovesToANewCertificateStaysUp(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	roundTrip(t, dial(t, a, b), "before the renewal")
	renewed := b.as(true)
	renewed.Fingerprint = strings.Repeat("c", 64)
	if err := a.m.SetPeers([]Peer{renewed}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := a.m.Dial(ctx, renewed.Fingerprint, servicePort)
	if err != nil {
		t.Fatalf("the tunnel did not stay up under the new certificate: %v", err)
	}
	defer c.Close()
	roundTrip(t, c, "after the renewal")
}

// c46: a member's power cut read as "reaches it through the relay" for 3 minutes, and Deplo moved the leader.
func TestMesh_aPeerThatWentSilentIsNotReadAsRelayed(t *testing.T) {
	r := &fakeRelay{}
	a, b := start(t, r, fpA, "fdde:9100::a"), start(t, r, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	roundTrip(t, dial(t, a, b), "directly")
	waitPath(t, a.m, fpB, PathDirect)
	r.mu.Lock()
	r.off = true
	r.mu.Unlock()
	b.m.Close()
	waitPath(t, a.m, fpB, PathNone)
}

// c46 printed eu-1's private key: a key replaced while a connection runs keeps it, and peers take only the named one.
func TestMesh_aKeyRotatedInPlaceKeepsItsConnections(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	c := dial(t, a, b)
	roundTrip(t, c, "under the first key")
	next, err := NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	nextPub, _ := next.Public()
	peer := b.as(true)
	peer.Next = nextPub
	if err := a.m.SetPeers([]Peer{peer}); err != nil {
		t.Fatal(err)
	}
	// WireGuard takes one initiation per peer each 20 ms: a rotation comes long after the last one.
	time.Sleep(100 * time.Millisecond)
	if err := b.m.Rotate(next); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	roundTrip(t, c, "under the next key, on the same connection")
	if took := time.Since(at); took > 2*time.Second {
		t.Fatalf("the connection stalled %v across the rotation", took)
	}
	a.m.mu.RLock()
	took := a.m.peers[fpB].PublicKey == nextPub
	a.m.mu.RUnlock()
	if !took {
		t.Fatal("the peer still knows the old key")
	}
}

// c46: after eu-1's agent was killed, visitors carried to it got their answer 4 to 5 s after it was back.
func TestMesh_aDialReachesAPeerWithinASecondOfItsReturn(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	roundTrip(t, dial(t, a, b), "before")
	port, addr := b.m.Port(), b.m.Address()
	b.m.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := make(chan net.Conn, 1)
	go func() {
		c, err := a.m.Dial(ctx, fpB, servicePort)
		if err != nil {
			t.Log(err)
		}
		got <- c
	}()
	time.Sleep(3500 * time.Millisecond)
	m, err := Start(Config{PrivateKey: b.key, Address: addr, Port: port, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	echo(t, side{m: m, key: b.key, fp: fpB})
	back := time.Now()
	if err := m.SetPeers([]Peer{a.as(true)}); err != nil {
		t.Fatal(err)
	}
	c := <-got
	if c == nil {
		t.Fatal("the dial never reached the peer")
	}
	c.Close()
	if late := time.Since(back); late > 1500*time.Millisecond {
		t.Fatalf("the dial reached the peer %v after it was back", late)
	}
}

// c48: a renewal sent while a member's agent was down reached it 2 s after its return, at its dial's next SYN.
func TestMesh_aDialReachesAPeerAsSoonAsItsNextRunIsHeard(t *testing.T) {
	was := dialEvery
	dialEvery = time.Minute
	t.Cleanup(func() { dialEvery = was })
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	roundTrip(t, dial(t, a, b), "before")
	port, addr := b.m.Port(), b.m.Address()
	b.m.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := make(chan net.Conn, 1)
	go func() {
		c, _ := a.m.Dial(ctx, fpB, servicePort)
		got <- c
	}()
	// Back between TCP's own tries of the first SYN, 1 and 3 s after it.
	time.Sleep(1200 * time.Millisecond)
	m, err := Start(Config{PrivateKey: b.key, Address: addr, Port: port, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	echo(t, side{m: m, key: b.key, fp: fpB})
	back := time.Now()
	if err := m.SetPeers([]Peer{a.as(true)}); err != nil {
		t.Fatal(err)
	}
	c := <-got
	if c == nil {
		t.Fatal("the dial never reached the peer")
	}
	roundTrip(t, c, "after")
	c.Close()
	if late := time.Since(back); late > 500*time.Millisecond {
		t.Fatalf("the dial reached the peer %v after it was back", late)
	}
}

// c50: a new member learns its peers before they learn it; once they do, their ping opens its path, not its own next one.
func TestMesh_aMemberThePeersLearnLastReachesThemAtOnce(t *testing.T) {
	// Its own pings never open the path here: only the peer's can.
	was := pingOther
	pingOther = time.Minute
	t.Cleanup(func() { pingOther = was })
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	echo(t, b)
	if err := a.m.SetPeers([]Peer{b.as(true)}); err != nil {
		t.Fatal(err)
	}
	// Its handshakes go unanswered meanwhile: WireGuard tries again only 5 s after each.
	time.Sleep(2500 * time.Millisecond)
	learned := time.Now()
	if err := b.m.SetPeers([]Peer{a.as(true)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := a.m.Dial(ctx, fpB, servicePort)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, "joined")
	c.Close()
	t.Logf("reached %v after the peer learned it", time.Since(learned))
	waitPath(t, a.m, fpB, PathDirect)
	waitPath(t, b.m, fpA, PathDirect)
	t.Logf("direct both ways %v after", time.Since(learned))
	if late := time.Since(learned); late > 500*time.Millisecond {
		t.Fatalf("the member reached its peer %v after the peer learned it", late)
	}
}
