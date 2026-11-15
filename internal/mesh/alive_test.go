package mesh

import (
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"
)

// waitHeard waits until a has heard b's run say it is alive.
func waitHeard(t *testing.T, a, b side) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		a.m.aliveMu.Lock()
		_, ok := a.m.lives[b.fp]
		a.m.aliveMu.Unlock()
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("a never heard that b is alive")
}

// readEnds waits for a read on c to end: by the connection closing, not by the test's own deadline.
func readEnds(t *testing.T, c net.Conn, within time.Duration) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(within))
	_, err := c.Read(make([]byte, 1))
	var ne net.Error
	if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
		t.Fatalf("the connection was still open after %v: %v", within, err)
	}
}

func TestMesh_aPeerThatRestartsEndsTheConnectionsToItsOldRun(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	c := dial(t, a, b)
	roundTrip(t, c, "before the restart")
	waitHeard(t, a, b)
	// b's agent restarts: the same key, address and port, a new run with a new network stack.
	port := b.m.Port()
	b.m.Close()
	again, err := Start(Config{PrivateKey: b.key, Port: port, Address: b.m.Address(), Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { again.Close() })
	if err := again.SetPeers([]Peer{a.as(true)}); err != nil {
		t.Fatal(err)
	}
	readEnds(t, c, 10*time.Second)
}

func TestMesh_aPeerThatFallsQuietEndsItsConnections(t *testing.T) {
	every, dead := aliveEvery, aliveDeadAfter
	aliveEvery, aliveDeadAfter = 200*time.Millisecond, 1500*time.Millisecond
	t.Cleanup(func() { aliveEvery, aliveDeadAfter = every, dead })
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	c := dial(t, a, b)
	roundTrip(t, c, "while both are up")
	waitHeard(t, a, b)
	b.m.Close()
	readEnds(t, c, 8*time.Second)
}

func TestMesh_aConnectionToAPeerThatStaysUpIsNeverClosed(t *testing.T) {
	every, dead := aliveEvery, aliveDeadAfter
	aliveEvery, aliveDeadAfter = 200*time.Millisecond, 1500*time.Millisecond
	t.Cleanup(func() { aliveEvery, aliveDeadAfter = every, dead })
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	c := dial(t, a, b)
	// Quiet for longer than a dead peer is waited for: only the peer's liveness counts, not the traffic.
	time.Sleep(3 * time.Second)
	roundTrip(t, c, "after a quiet while")
}

func TestMesh_aServerTakenOffTheNetworkTakesItsConnections(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	c := dial(t, a, b)
	roundTrip(t, c, "while it is a peer")
	if err := a.m.SetPeers(nil); err != nil {
		t.Fatal(err)
	}
	readEnds(t, c, 5*time.Second)
}

// A run that ends before it ever said it was alive, a second after the stream to it opened: the stream said which run
// it was, so it ends when the next run is heard.
func TestMesh_aStreamToARunThatNeverSaidItIsAliveEndsWhenTheNextRunIsHeard(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	b.m.alive.Close()
	connect(t, a, b, true)
	echo(t, b)
	c := dial(t, a, b)
	roundTrip(t, c, "to the old run")
	time.Sleep(time.Second)
	port := b.m.Port()
	b.m.Close()
	again, err := Start(Config{PrivateKey: b.key, Port: port, Address: b.m.Address(), Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { again.Close() })
	if err := again.SetPeers([]Peer{a.as(true)}); err != nil {
		t.Fatal(err)
	}
	readEnds(t, c, 10*time.Second)
}

// A connection made to a run that is already up is that run's: hearing the run for the first time keeps it.
func TestMesh_firstHearingAPeerKeepsTheConnectionsToThatRun(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	waitHeard(t, a, b)
	c := dial(t, a, b)
	a.m.aliveMu.Lock()
	delete(a.m.lives, fpB)
	a.m.aliveMu.Unlock()
	waitHeard(t, a, b)
	roundTrip(t, c, "still that run's")
}

// A run that never says it is alive is not one: the connections to it end once a live one would have said it.
func TestMesh_aPeerThatNeverSaysItIsAliveLosesItsConnections(t *testing.T) {
	every, dead := aliveEvery, aliveDeadAfter
	aliveEvery, aliveDeadAfter = 200*time.Millisecond, 1500*time.Millisecond
	t.Cleanup(func() { aliveEvery, aliveDeadAfter = every, dead })
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	b.m.alive.Close()
	connect(t, a, b, true)
	echo(t, b)
	c := dial(t, a, b)
	roundTrip(t, c, "while it is new")
	readEnds(t, c, 8*time.Second)
}

// A stream opened to a run not heard yet, while the run before is the one last heard, is the new run's.
func TestMesh_aStreamBelongsToTheRunThatAnsweredIt(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	echo(t, b)
	a.m.aliveMu.Lock()
	a.m.lives[fpB] = liveness{incarnation: b.m.incarnation + 1, at: time.Now()}
	a.m.aliveMu.Unlock()
	c := dial(t, a, b)
	if tc, ok := c.(*tracked); !ok || tc.inc != b.m.incarnation {
		t.Fatalf("the stream was filed as %#v", c)
	}
	waitHeard(t, a, b)
	time.Sleep(2 * aliveEvery)
	roundTrip(t, c, "the run that answered it")
}

func TestMesh_anAcceptedConnectionIsFiledUnderItsPeer(t *testing.T) {
	a, b := start(t, nil, fpA, "fdde:9100::a"), start(t, nil, fpB, "fdde:9100::b")
	connect(t, a, b, true)
	got := make(chan net.Conn, 1)
	ln, err := b.m.Listen(servicePort)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		if c, err := ln.Accept(); err == nil {
			got <- c
		}
	}()
	dial(t, a, b)
	select {
	case c := <-got:
		if tc, ok := c.(*tracked); !ok || tc.fp != fpA || tc.inc != a.m.incarnation {
			t.Fatalf("the accepted connection was filed as %#v", c)
		}
		c.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was accepted")
	}
	if b.m.peerByAddr(net.TCPAddrFromAddrPort(netip.MustParseAddrPort("[fdde:9100::a]:1"))) != fpA {
		t.Fatal("a peer's tunnel address does not name it")
	}
}
