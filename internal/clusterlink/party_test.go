package clusterlink

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testrelay"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

func runParty(t *testing.T, relay string, id *wire.Identity, role wire.Role, accept func(wire.Party, net.Conn)) *Party {
	t.Helper()
	p := New(Config{
		Role:     role,
		Identity: func() (*wire.Identity, error) { return id, nil },
		Relays:   func() []string { return []string{relay} },
		Accept:   accept,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for p.Connected() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.Connected() != 1 {
		t.Fatal("the party did not connect to the relay")
	}
	return p
}

func TestMemberDialsOutAndReachesAPeerMutualTLSPinned(t *testing.T) {
	ca := testpki.New(t)
	a, b, c := ca.Leaf(t, "a"), ca.Leaf(t, "b"), ca.Leaf(t, "c")
	pa := wire.Party{Fingerprint: a.Fingerprint, Role: wire.RoleMember}
	pb := wire.Party{Fingerprint: b.Fingerprint, Role: wire.RoleLeader}
	pc := wire.Party{Fingerprint: c.Fingerprint, Role: wire.RoleMember}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), pa, pb, pc)

	from := make(chan string, 1)
	runParty(t, relay.URL, b, wire.RoleLeader, func(f wire.Party, conn net.Conn) {
		defer conn.Close()
		from <- f.Key()
		buf := make([]byte, 8)
		n, _ := conn.Read(buf)
		_, _ = conn.Write(buf[:n])
	})
	member := runParty(t, relay.URL, a, wire.RoleMember, nil)
	runParty(t, relay.URL, c, wire.RoleMember, func(_ wire.Party, conn net.Conn) { conn.Close() })

	conn, err := member.Dial(context.Background(), pb)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("ping"))
	buf := make([]byte, 8)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buf)
	if err != nil || string(buf[:n]) != "ping" {
		t.Fatalf("read %q %v", buf[:n], err)
	}
	if got := <-from; got != pa.Key() {
		t.Fatalf("the peer saw %s, want %s", got, pa.Key())
	}

	// The relay would hand a stream for b to whoever it likes; a pinned dial to the wrong
	// certificate behind b's name fails the handshake instead of delivering data.
	wrong := wire.Party{Fingerprint: c.Fingerprint, Role: wire.RoleLeader}
	relay.Misdirect(wrong, pb)
	if bad, err := member.Dial(context.Background(), wrong); err == nil {
		bad.Close()
		t.Fatal("a stream pinned to c ended at b")
	}
}

func TestPartyOnlyKeepsRelaysItIsToldOf(t *testing.T) {
	ca := testpki.New(t)
	a := ca.Leaf(t, "a")
	pa := wire.Party{Fingerprint: a.Fingerprint, Role: wire.RoleMember}
	relay := testrelay.New(t, ca.Leaf(t, "relay"), pa)
	var mu sync.Mutex
	urls := []string{relay.URL}
	p := New(Config{
		Role:     wire.RoleMember,
		Identity: func() (*wire.Identity, error) { return a, nil },
		Relays:   func() []string { mu.Lock(); defer mu.Unlock(); return urls },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	wait := func(want int) {
		t.Helper()
		deadline := time.Now().Add(8 * time.Second)
		for p.Connected() != want && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if p.Connected() != want {
			t.Fatalf("connected = %d, want %d", p.Connected(), want)
		}
	}
	wait(1)
	mu.Lock()
	urls = nil
	mu.Unlock()
	wait(0)
}

func TestDialWithNoRelayFailsClearly(t *testing.T) {
	ca := testpki.New(t)
	a := ca.Leaf(t, "a")
	p := New(Config{Role: wire.RoleMember, Identity: func() (*wire.Identity, error) { return a, nil }, Relays: func() []string { return nil }})
	if _, err := p.Dial(context.Background(), wire.Party{Fingerprint: strings.Repeat("a", 64), Role: wire.RoleMember}); err == nil {
		t.Fatal("dialed with no relay")
	}
}

// An agent never accepts an incoming connection for the cluster (ADR-0044): nothing here may listen.
func TestClusterLinkNeverListens(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range pkgs {
		for name, f := range pkg.Files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && (strings.HasPrefix(sel.Sel.Name, "Listen") || sel.Sel.Name == "Serve" || sel.Sel.Name == "ServeTLS") {
					t.Errorf("%s: %s listens", fset.Position(sel.Pos()), sel.Sel.Name)
				}
				return true
			})
		}
	}
}
