package link

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// agentCert writes a leaf certificate like the agent's and returns its path and DER.
func agentCert(t *testing.T) (string, []byte) {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7),
		Subject:      pkix.Name{CommonName: "deplo-agent"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "agent.crt")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, der
}

// doorLink is one link as the panel's door saw it. The door reads all the time, as the real one does.
type doorLink struct {
	req   *http.Request
	ws    *websocket.Conn
	in    *io.PipeReader // what the agent sent
	pings atomic.Int32
	ended chan struct{}
	err   error // why the link ended, once ended is closed
}

func (dl *doorLink) send(t *testing.T, p string) {
	t.Helper()
	if err := dl.ws.Write(context.Background(), websocket.MessageBinary, []byte(p)); err != nil {
		t.Fatal(err)
	}
}

func (dl *doorLink) waitEnded(t *testing.T) error {
	t.Helper()
	select {
	case <-dl.ended:
		return dl.err
	case <-time.After(10 * time.Second):
		t.Fatal("the link never ended")
		return nil
	}
}

type doorOpts struct {
	tls bool
	// refuse answers an attempt (counted from 1) before any upgrade when it returns true.
	refuse func(attempt int, w http.ResponseWriter) bool
	// mute stops answering pings.
	mute bool
}

// door answers like the panel's link door and hands over every link it accepts.
func door(t *testing.T, o doorOpts) (*httptest.Server, <-chan *doorLink) {
	t.Helper()
	links := make(chan *doorLink, 16)
	var attempts atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(attempts.Add(1))
		if o.refuse != nil && o.refuse(n, w) {
			return
		}
		if r.URL.Path != Path {
			http.NotFound(w, r)
			return
		}
		dl := &doorLink{req: r, ended: make(chan struct{})}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{Subprotocol},
			OnPingReceived: func(context.Context, []byte) bool {
				dl.pings.Add(1)
				return !o.mute
			},
		})
		if err != nil {
			return
		}
		pr, pw := io.Pipe()
		dl.ws, dl.in = c, pr
		links <- dl
		for {
			_, rd, err := c.Reader(context.Background())
			if err != nil {
				dl.err = err
				break
			}
			if _, err := io.Copy(pw, rd); err != nil {
				dl.err = err
				break
			}
		}
		pw.CloseWithError(dl.err)
		close(dl.ended)
	})
	var srv *httptest.Server
	if o.tls {
		srv = httptest.NewTLSServer(h)
	} else {
		srv = httptest.NewServer(h)
	}
	t.Cleanup(srv.Close)
	return srv, links
}

// run starts Run; stop ends it and waits for it to return, and the test's end stops it too.
func run(t *testing.T, cfg Config) (l *Listener, stop func()) {
	t.Helper()
	l = NewListener()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, cfg, l); close(done) }()
	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("Run did not return after its context ended")
		}
		l.Close()
	}
	t.Cleanup(stop)
	return l, stop
}

// accept is Listener.Accept bounded, so an agent that never links fails the test rather than hanging it.
func accept(t *testing.T, l *Listener) net.Conn {
	t.Helper()
	got := make(chan net.Conn, 1)
	go func() {
		if c, err := l.Accept(); err == nil {
			got <- c
		}
	}()
	select {
	case c := <-got:
		return c
	case <-time.After(10 * time.Second):
		t.Fatal("the agent never handed a link to its listener")
		return nil
	}
}

func nextLink(t *testing.T, links <-chan *doorLink) *doorLink {
	t.Helper()
	select {
	case dl := <-links:
		return dl
	case <-time.After(10 * time.Second):
		t.Fatal("the agent never linked")
		return nil
	}
}

func TestRun_linksToThePrimaryAndNamesItsServer(t *testing.T) {
	certPath, der := agentCert(t)
	srv, links := door(t, doorOpts{})
	l, _ := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})

	conn := accept(t, l)
	dl := nextLink(t, links)
	sum := sha256.Sum256(der)
	if got, want := dl.req.Header.Get(CertHeader), hex.EncodeToString(sum[:]); got != want {
		t.Errorf("claim = %q, want the sha256 of the agent's certificate %q", got, want)
	}
	if got := dl.req.URL.Path; got != "/api/agent/link" {
		t.Errorf("path = %q", got)
	}
	if got := dl.ws.Subprotocol(); got != "deplo-agent-link.v1" {
		t.Errorf("subprotocol = %q", got)
	}

	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(dl.in, buf); err != nil || string(buf) != "hello" {
		t.Fatalf("door read %q, %v", buf, err)
	}
	dl.send(t, "world")
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "world" {
		t.Fatalf("agent read %q, %v", buf, err)
	}
}

func TestRun_linksWhateverThePanelsOuterCertificate(t *testing.T) {
	certPath, _ := agentCert(t)
	srv, links := door(t, doorOpts{tls: true}) // self-signed: trust is the mTLS inside the link
	l, _ := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})
	accept(t, l)
	nextLink(t, links)
}

func TestRun_stoppingClosesTheLinkAsGoingAway(t *testing.T) {
	certPath, _ := agentCert(t)
	srv, links := door(t, doorOpts{})
	l, stop := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})
	accept(t, l)
	dl := nextLink(t, links)
	stop()
	if got := websocket.CloseStatus(dl.waitEnded(t)); got != websocket.StatusGoingAway {
		t.Fatalf("close status = %v, want going away (1001)", got)
	}
}

// fakeWaits lets every reconnect wait pass at once and returns the waits Run asked for so far.
func fakeWaits(t *testing.T) func() []time.Duration {
	t.Helper()
	var mu sync.Mutex
	var waits []time.Duration
	prev := after
	after = func(d time.Duration) <-chan time.Time {
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	t.Cleanup(func() { after = prev })
	return func() []time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Duration(nil), waits...)
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func refuseWith(status int) func(int, http.ResponseWriter) bool {
	return func(_ int, w http.ResponseWriter) bool {
		w.WriteHeader(status)
		return true
	}
}

func TestRun_backsOffWithJitterFromAboutOneSecondToThirty(t *testing.T) {
	waits := fakeWaits(t)
	certPath, _ := agentCert(t)
	srv, _ := door(t, doorOpts{refuse: refuseWith(http.StatusServiceUnavailable)})
	_, stop := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})
	waitFor(t, "a dozen reconnect waits", func() bool { return len(waits()) >= 12 })
	stop()

	got := waits()[:12]
	if got[0] < 500*time.Millisecond || got[0] > time.Second {
		t.Errorf("first wait = %v, want about 1 s", got[0])
	}
	for i, w := range got {
		if w < 500*time.Millisecond || w > 30*time.Second {
			t.Errorf("wait %d = %v, outside about 1 s to 30 s", i, w)
		}
	}
	late := got[8:]
	for i, w := range late {
		if w < 15*time.Second {
			t.Errorf("late wait %d = %v, want the cap of about 30 s", i, w)
		}
	}
	if late[0] == late[1] && late[1] == late[2] && late[2] == late[3] {
		t.Errorf("late waits %v are all equal: no jitter", late)
	}
}

// standIn reads a link the way grpc.Server does and closes it once the panel ends it.
func standIn(c net.Conn) {
	go func() {
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
	}()
}

func TestRun_aLinkThatLastedStartsTheBackoffOver(t *testing.T) {
	waits := fakeWaits(t)
	prev := healthyAfter
	healthyAfter = 50 * time.Millisecond
	t.Cleanup(func() { healthyAfter = prev })
	certPath, _ := agentCert(t)
	// Refused six times, so the wait has grown to its cap; linked on the seventh; refused after.
	srv, links := door(t, doorOpts{refuse: func(n int, w http.ResponseWriter) bool {
		if n == 7 {
			return false
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}})
	l, stop := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})
	standIn(accept(t, l))
	dl := nextLink(t, links)
	time.Sleep(4 * healthyAfter)
	_ = dl.ws.Close(4000, "replaced by a newer link")
	waitFor(t, "the wait after the link", func() bool { return len(waits()) >= 7 })
	stop()

	got := waits()
	if got[5] < 15*time.Second {
		t.Fatalf("waits %v never reached the cap", got)
	}
	if got[6] > time.Second {
		t.Errorf("wait after a link that lasted = %v, want about 1 s again", got[6])
	}
}

func TestRun_aLinkThePanelDropsAtOnceKeepsBackingOff(t *testing.T) {
	waits := fakeWaits(t)
	certPath, _ := agentCert(t)
	srv, links := door(t, doorOpts{})
	l, stop := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			standIn(c)
		}
	}()
	go func() {
		for {
			select {
			case dl := <-links:
				_ = dl.ws.Close(4001, "handshake failed")
			case <-t.Context().Done():
				return
			}
		}
	}()
	waitFor(t, "eight reconnect waits", func() bool { return len(waits()) >= 8 })
	stop()

	if got := waits()[7]; got < 15*time.Second {
		t.Errorf("eighth wait = %v, want the cap of about 30 s: a link refused at once is no reason to hurry", got)
	}
}

func TestRun_waitsAsLongAsThePanelAsks(t *testing.T) {
	waits := fakeWaits(t)
	certPath, _ := agentCert(t)
	srv, _ := door(t, doorOpts{refuse: func(n int, w http.ResponseWriter) bool {
		w.Header().Set("Retry-After", map[int]string{1: "7", 2: "86400"}[n])
		w.WriteHeader(http.StatusTooManyRequests)
		return true
	}})
	_, stop := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})
	waitFor(t, "two reconnect waits", func() bool { return len(waits()) >= 2 })
	stop()

	got := waits()
	if got[0] < 7*time.Second {
		t.Errorf("wait after Retry-After: 7 = %v, want at least 7 s", got[0])
	}
	if got[1] < 30*time.Second || got[1] > 10*time.Minute {
		t.Errorf("wait after Retry-After: 86400 = %v, want it honoured up to a sane cap", got[1])
	}
}

// shortPings makes the link ping every 20 ms with a 100 ms pong deadline.
func shortPings(t *testing.T) {
	t.Helper()
	prevPing, prevPong := pingInterval, pongTimeout
	pingInterval, pongTimeout = 20*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { pingInterval, pongTimeout = prevPing, prevPong })
}

func TestRun_pingsTheLinkWhileItIsQuiet(t *testing.T) {
	shortPings(t)
	certPath, _ := agentCert(t)
	srv, links := door(t, doorOpts{})
	l, _ := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})
	standIn(accept(t, l))
	dl := nextLink(t, links)
	waitFor(t, "three pings", func() bool { return dl.pings.Load() >= 3 })
	select {
	case <-dl.ended:
		t.Fatalf("an answered ping ended the link: %v", dl.err)
	default:
	}
}

func TestRun_dropsALinkWhosePingsGoUnanswered(t *testing.T) {
	shortPings(t)
	certPath, _ := agentCert(t)
	srv, links := door(t, doorOpts{mute: true})
	l, _ := run(t, Config{URL: srv.URL, CertFile: certPath, Logf: t.Logf})
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			standIn(c)
		}
	}()
	first := nextLink(t, links)
	first.waitEnded(t)
	nextLink(t, links) // and it links again
}
