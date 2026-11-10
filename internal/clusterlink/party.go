// Package clusterlink is the agent's side of the cluster relay: it only dials out, never listens (ADR-0044).
package clusterlink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

const (
	dialTimeout = 15 * time.Second
	minBackoff  = time.Second
	maxBackoff  = 30 * time.Second
	// refresh is how often the relay list is read again.
	refresh = 5 * time.Second
)

// Config says who this party is and which relays it knows.
type Config struct {
	Role wire.Role
	// Identity is read per connection and per stream, so a renewed certificate counts.
	Identity func() (*wire.Identity, error)
	// Relays are base addresses (http or https, no path); read again every few seconds.
	Relays func() []string
	// Accept receives each stream a peer opened to this party, after mutual TLS pinned to that peer.
	Accept func(from wire.Party, c net.Conn)
	Logf   func(format string, args ...any)
}

// Party keeps one connection to every relay it knows.
type Party struct {
	cfg Config
	mu  sync.Mutex
	up  map[string]*yamux.Session // relay address -> its live connection
}

// New returns a party; Run starts it.
func New(cfg Config) *Party {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Party{cfg: cfg, up: map[string]*yamux.Session{}}
}

// Connected is how many relays hold a connection from this party.
func (p *Party) Connected() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.up)
}

// Run dials every relay the config names and keeps each connection until ctx ends.
func (p *Party) Run(ctx context.Context) {
	running := map[string]context.CancelFunc{}
	defer func() {
		for _, stop := range running {
			stop()
		}
	}()
	for {
		wanted := map[string]bool{}
		for _, u := range p.cfg.Relays() {
			if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
				wanted[u] = true
			}
		}
		for u := range wanted {
			if running[u] == nil {
				rctx, stop := context.WithCancel(ctx)
				running[u] = stop
				go p.keep(rctx, u)
			}
		}
		for u, stop := range running {
			if !wanted[u] {
				stop()
				delete(running, u)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(refresh):
		}
	}
}

func (p *Party) keep(ctx context.Context, relay string) {
	backoff := minBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := p.hold(ctx, relay)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > 30*time.Second {
			backoff = minBackoff
		}
		p.cfg.Logf("deplo-cluster: relay %s: %v (retrying)", relay, err)
		select {
		case <-ctx.Done():
		case <-time.After(backoff/2 + rand.N(backoff/2+1)):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// hold keeps one connection to the relay until it ends.
func (p *Party) hold(ctx context.Context, relay string) error {
	id, err := p.cfg.Identity()
	if err != nil {
		return err
	}
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	ws, _, err := websocket.Dial(dctx, relay+wire.Path, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			// Trust is the mutual TLS inside, never the outer certificate (ADR-0040).
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
			DisableKeepAlives: true,
		}},
		HTTPHeader:   http.Header{wire.CertHeader: {id.Fingerprint}, wire.RoleHeader: {string(p.cfg.Role)}},
		Subprotocols: []string{wire.Subprotocol},
	})
	if err != nil {
		return err
	}
	if ws.Subprotocol() != wire.Subprotocol {
		ws.CloseNow()
		return fmt.Errorf("does not speak %s", wire.Subprotocol)
	}
	ws.SetReadLimit(1 << 20)
	cctx, stop := context.WithCancel(ctx)
	defer stop()
	nc := websocket.NetConn(cctx, ws, websocket.MessageBinary)
	defer nc.Close()
	tc := tls.Client(nc, id.ClientConfig(""))
	if err := tc.HandshakeContext(dctx); err != nil {
		return err
	}
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard
	mux, err := yamux.Client(tc, cfg)
	if err != nil {
		return err
	}
	defer mux.Close()
	p.set(relay, mux)
	defer p.unset(relay, mux)
	p.cfg.Logf("deplo-cluster: relay %s is up", relay)
	for {
		st, err := mux.AcceptStream()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("connection ended: %w", err)
		}
		go p.accepted(st)
	}
}

func (p *Party) set(relay string, m *yamux.Session) {
	p.mu.Lock()
	p.up[relay] = m
	p.mu.Unlock()
}

func (p *Party) unset(relay string, m *yamux.Session) {
	p.mu.Lock()
	if p.up[relay] == m {
		delete(p.up, relay)
	}
	p.mu.Unlock()
}

// accepted runs the end-to-end handshake on a stream the relay opened, and hands it over only for the peer it named.
func (p *Party) accepted(st *yamux.Stream) {
	fail := func() { st.Close() }
	st.SetReadDeadline(time.Now().Add(wire.HeaderTimeout))
	verb, arg, err := wire.ReadHeader(st)
	if err != nil || verb != wire.VerbFrom {
		fail()
		return
	}
	from, err := wire.ParseParty(arg)
	id, ierr := p.cfg.Identity()
	if err != nil || ierr != nil || p.cfg.Accept == nil {
		fail()
		return
	}
	tc := tls.Server(st, id.ServerConfig())
	if err := tc.Handshake(); err != nil || wire.PeerFingerprint(tc.ConnectionState()) != from.Fingerprint {
		fail()
		return
	}
	st.SetReadDeadline(time.Time{})
	p.cfg.Accept(from, tc)
}

// Dial opens a stream to another party through whichever relay answers, mutual TLS end to end and pinned to `to`.
func (p *Party) Dial(ctx context.Context, to wire.Party) (net.Conn, error) {
	id, err := p.cfg.Identity()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	links := make([]*yamux.Session, 0, len(p.up))
	for _, m := range p.up {
		links = append(links, m)
	}
	p.mu.Unlock()
	if len(links) == 0 {
		return nil, errors.New("no relay is connected")
	}
	var last error
	for _, m := range links {
		c, err := p.dialVia(ctx, m, id, to)
		if err == nil {
			return c, nil
		}
		last = err
	}
	return nil, last
}

func (p *Party) dialVia(ctx context.Context, m *yamux.Session, id *wire.Identity, to wire.Party) (net.Conn, error) {
	st, err := m.OpenStream()
	if err != nil {
		return nil, err
	}
	dl := time.Now().Add(wire.HeaderTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(dl) {
		dl = d
	}
	st.SetDeadline(dl)
	if err := wire.WriteHeader(st, wire.VerbConnect, to.Key()); err != nil {
		st.Close()
		return nil, err
	}
	verb, arg, err := wire.ReadHeader(st)
	if err != nil || verb != wire.VerbOK {
		st.Close()
		if err == nil {
			err = fmt.Errorf("the relay refused: %s", arg)
		}
		return nil, err
	}
	tc := tls.Client(st, id.ClientConfig(to.Fingerprint))
	if err := tc.HandshakeContext(ctx); err != nil {
		st.Close()
		return nil, err
	}
	st.SetDeadline(time.Time{})
	return tc, nil
}
