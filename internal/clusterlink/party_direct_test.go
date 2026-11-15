package clusterlink

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testrelay"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

func echoParty(t *testing.T, relay string, id *wire.Identity, seen chan<- string) *Party {
	return runParty(t, relay, id, wire.RoleMember, func(f wire.Party, c net.Conn) {
		defer c.Close()
		seen <- f.Key()
		_, _ = io.Copy(c, c)
	})
}

// directTo is a cluster network that reaches `to` by handing the stream straight to its Serve.
func directTo(to *Party) func(context.Context, wire.Party) (net.Conn, error) {
	return func(context.Context, wire.Party) (net.Conn, error) {
		mine, theirs := net.Pipe()
		go to.Answer(theirs)
		return mine, nil
	}
}

func dialer(t *testing.T, relay string, id *wire.Identity, direct func(context.Context, wire.Party) (net.Conn, error)) *Party {
	t.Helper()
	p := New(Config{
		Role:     wire.RoleMember,
		Identity: func() (*wire.Identity, error) { return id, nil },
		Relays:   func() []string { return []string{relay} },
		Direct:   direct,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for p.Connected() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return p
}

func roundTrip(t *testing.T, c net.Conn) {
	t.Helper()
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("read %q %v", buf, err)
	}
}

func TestDial_takesTheClusterNetworkWhenItAnswers(t *testing.T) {
	ca := testpki.New(t)
	a, b := ca.Leaf(t, "a"), ca.Leaf(t, "b")
	pa, pb := wire.Party{Fingerprint: a.Fingerprint, Role: wire.RoleMember}, wire.Party{Fingerprint: b.Fingerprint, Role: wire.RoleMember}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), pa, pb)
	seen := make(chan string, 1)
	target := echoParty(t, relay.URL, b, seen)
	relay.Refuse(true)
	c, err := dialer(t, relay.URL, a, directTo(target)).Dial(context.Background(), pb)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c)
	if got := <-seen; got != pa.Key() {
		t.Fatalf("the peer saw %s, want %s", got, pa.Key())
	}
	if relay.Streams() != 0 {
		t.Fatal("the stream went through the relay")
	}
}

func TestDial_triesARelayBesideANetworkThatDoesNotAnswer(t *testing.T) {
	ca := testpki.New(t)
	a, b := ca.Leaf(t, "a"), ca.Leaf(t, "b")
	pa, pb := wire.Party{Fingerprint: a.Fingerprint, Role: wire.RoleMember}, wire.Party{Fingerprint: b.Fingerprint, Role: wire.RoleMember}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), pa, pb)
	echoParty(t, relay.URL, b, make(chan string, 1))
	hang := func(ctx context.Context, _ wire.Party) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	c, err := dialer(t, relay.URL, a, hang).Dial(ctx, pb)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c)
	if took := time.Since(started); took < directHead || took > directHead+3*time.Second {
		t.Fatalf("the relay was tried after %v", took)
	}
}

func TestDial_aNetworkThatReachesTheWrongPeerFailsItsPin(t *testing.T) {
	ca := testpki.New(t)
	a, b, c := ca.Leaf(t, "a"), ca.Leaf(t, "b"), ca.Leaf(t, "c")
	pa, pb, pc := wire.Party{Fingerprint: a.Fingerprint, Role: wire.RoleMember},
		wire.Party{Fingerprint: b.Fingerprint, Role: wire.RoleMember}, wire.Party{Fingerprint: c.Fingerprint, Role: wire.RoleMember}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), pa, pb, pc)
	toB, toC := make(chan string, 1), make(chan string, 1)
	echoParty(t, relay.URL, b, toB)
	wrong := echoParty(t, relay.URL, c, toC)
	conn, err := dialer(t, relay.URL, a, directTo(wrong)).Dial(context.Background(), pb)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, conn)
	select {
	case <-toB:
	case <-toC:
		t.Fatal("a stream pinned to b was handed to c")
	case <-time.After(5 * time.Second):
		t.Fatal("b never got the stream")
	}
}
