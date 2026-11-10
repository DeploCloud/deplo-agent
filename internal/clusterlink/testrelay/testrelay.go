// Package testrelay is a minimal relay for tests, speaking the contract in deplo-cluster internal/relay.
package testrelay

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// Relay holds one cluster of every party it was told of.
type Relay struct {
	id    *wire.Identity
	mu    sync.Mutex
	allow map[string]bool
	live  map[string]*yamux.Session
	// URL is where parties dial.
	URL string
	srv *httptest.Server
}

func New(t testing.TB, id *wire.Identity, parties ...wire.Party) *Relay {
	f := &Relay{id: id, allow: map[string]bool{}, live: map[string]*yamux.Session{}}
	for _, p := range parties {
		f.allow[p.Key()] = true
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	f.URL = f.srv.URL
	t.Cleanup(f.srv.Close)
	return f
}

func (f *Relay) serve(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{wire.Subprotocol}})
	if err != nil {
		return
	}
	nc := websocket.NetConn(r.Context(), ws, websocket.MessageBinary)
	defer nc.Close()
	tc := tls.Server(nc, f.id.ServerConfig())
	if tc.Handshake() != nil {
		return
	}
	key := wire.Party{Fingerprint: wire.PeerFingerprint(tc.ConnectionState()), Role: wire.Role(r.Header.Get(wire.RoleHeader))}.Key()
	f.mu.Lock()
	ok := f.allow[key]
	f.mu.Unlock()
	if !ok {
		return
	}
	mux, err := yamux.Server(tc, nil)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.live[key] = mux
	f.mu.Unlock()
	for {
		st, err := mux.AcceptStream()
		if err != nil {
			return
		}
		go f.forward(key, st)
	}
}

func (f *Relay) forward(from string, st *yamux.Stream) {
	defer st.Close()
	_, arg, err := wire.ReadHeader(st)
	if err != nil {
		return
	}
	f.mu.Lock()
	target := f.live[arg]
	ok := f.allow[arg]
	f.mu.Unlock()
	if target == nil || !ok {
		_ = wire.WriteHeader(st, wire.VerbErr, "not connected")
		return
	}
	out, err := target.OpenStream()
	if err != nil {
		return
	}
	defer out.Close()
	_ = wire.WriteHeader(out, wire.VerbFrom, from)
	_ = wire.WriteHeader(st, wire.VerbOK, "")
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(out, st); out.Close(); done <- struct{}{} }()
	go func() { _, _ = io.Copy(st, out); st.Close(); done <- struct{}{} }()
	<-done
	<-done
}

// Allow adds a party to the cluster.
func (f *Relay) Allow(p wire.Party) {
	f.mu.Lock()
	f.allow[p.Key()] = true
	f.mu.Unlock()
}

// Misdirect hands streams for `name` to whoever holds `to`: a relay that lies about who is on the other end.
func (f *Relay) Misdirect(name, to wire.Party) {
	f.mu.Lock()
	f.allow[name.Key()] = true
	f.live[name.Key()] = f.live[to.Key()]
	f.mu.Unlock()
}

// Connected is how many parties hold a connection.
func (f *Relay) Connected() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.live)
}
