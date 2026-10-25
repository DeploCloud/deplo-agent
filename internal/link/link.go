// Package link keeps the agent link: a WebSocket the agent opens to its panel and serves gRPC on.
package link

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// The door's wire protocol (ADR-0040).
const (
	Path        = "/api/agent/link"
	Subprotocol = "deplo-agent-link.v1"
	CertHeader  = "Deplo-Agent-Cert"
)

const (
	dialTimeout = 15 * time.Second
	minBackoff  = time.Second
	maxBackoff  = 30 * time.Second
	// maxRetryAfter bounds a Retry-After, so a misconfigured proxy cannot park a server for days.
	maxRetryAfter = 5 * time.Minute
)

// Package vars so tests can stand in for time.
var (
	after = time.After
	// pingInterval stays under common proxy idle timeouts; gRPC keepalives stop when no call is open.
	pingInterval = 20 * time.Second
	pongTimeout  = 10 * time.Second
	// healthyAfter is how long a link has to last before the next reconnect starts from minBackoff.
	healthyAfter = 30 * time.Second
)

// Listener hands each new link to grpc.Server.Serve; Accept blocks across reconnects until Close.
type Listener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

// NewListener returns a Listener for Run to feed and grpc.Server.Serve to drain.
func NewListener() *Listener {
	return &Listener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *Listener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *Listener) Addr() net.Addr { return linkAddr{} }

type linkAddr struct{}

func (linkAddr) Network() string { return "agent-link" }
func (linkAddr) String() string  { return "agent-link" }

// Over reports whether addr, a call's peer address, is an agent link rather than the agent's own port.
func Over(addr net.Addr) bool {
	return addr != nil && addr.Network() == linkAddr{}.Network()
}

// Config is where to link and how the agent names itself there.
type Config struct {
	// Addresses is the saved pair: each attempt dials its primary, then its fallback.
	Addresses *Store
	// CertFile is the agent's leaf; its sha256 names the server at the door, read per dial so a renewal counts.
	CertFile string
	Logf     func(format string, args ...any)
}

// Run keeps one link open to the saved pair until ctx ends or l is closed; with no primary it waits for one.
func Run(ctx context.Context, cfg Config, l *Listener) {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	backoff, lastErr := minBackoff, ""
	for {
		a, replaced := cfg.Addresses.watch()
		wait := time.Duration(-1)
		if a.Primary != "" {
			up, retryAfter, err := cfg.attempt(ctx, l, a)
			if ctx.Err() != nil || l.isClosed() {
				return
			}
			// A refusal repeats every 30 s for as long as it lasts: log it when it changes.
			if up > 0 {
				cfg.Logf("deplo-agent: agent link ended after %s: %v", up.Round(time.Second), err)
			} else if err.Error() != lastErr {
				cfg.Logf("deplo-agent: agent link: %v (retrying)", err)
			}
			lastErr = err.Error()
			if up >= healthyAfter {
				backoff = minBackoff
			}
			wait = max(jitter(backoff), retryAfter)
		}
		var waited <-chan time.Time
		if wait >= 0 {
			waited = after(wait)
		}
		select {
		case <-ctx.Done():
			return
		case <-l.closed:
			return
		case <-replaced:
			// A new pair is dialed at once, from a fresh backoff.
			backoff, lastErr = minBackoff, ""
			continue
		case <-waited:
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// attempt dials the primary, then the fallback, and serves the first that links until that link ends.
func (cfg Config) attempt(ctx context.Context, l *Listener, a Addresses) (up, retryAfter time.Duration, err error) {
	addresses := []string{a.Primary}
	if a.Fallback != "" && a.Fallback != a.Primary {
		addresses = append(addresses, a.Fallback)
	}
	var failed []error
	for _, address := range addresses {
		ws, err := cfg.dial(ctx, l, address)
		if err == nil {
			up, err := cfg.serve(ctx, l, ws, address)
			return up, 0, err
		}
		var r *refusedError
		if errors.As(err, &r) {
			retryAfter = max(retryAfter, r.retryAfter)
		}
		failed = append(failed, fmt.Errorf("%s: %w", address, err))
		if ctx.Err() != nil || l.isClosed() {
			break
		}
	}
	return 0, retryAfter, errors.Join(failed...)
}

// jitter spreads a wait over its upper half, so servers that dropped together do not return together.
func jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d/2+1)
}

func (l *Listener) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

// serve hands the link to l and returns how long it was up once it has ended.
func (cfg Config) serve(ctx context.Context, l *Listener, ws *websocket.Conn, address string) (time.Duration, error) {
	linkCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &conn{Conn: websocket.NetConn(linkCtx, ws, websocket.MessageBinary), closed: make(chan struct{})}
	select {
	case l.conns <- c:
	case <-l.closed:
		ws.Close(websocket.StatusGoingAway, "agent stopping")
		return 0, net.ErrClosed
	case <-ctx.Done():
		ws.Close(websocket.StatusGoingAway, "agent stopping")
		return 0, ctx.Err()
	}
	cfg.Logf("deplo-agent: agent link to %s is up", address)
	started := time.Now()
	unanswered := make(chan error, 1)
	go ping(linkCtx, ws, unanswered)
	select {
	case <-c.closed:
		return time.Since(started), errors.New("link closed")
	case err := <-unanswered:
		ws.CloseNow()
		return time.Since(started), err
	case <-ctx.Done():
		ws.Close(websocket.StatusGoingAway, "agent stopping")
		return time.Since(started), ctx.Err()
	}
}

// ping pings the panel until linkCtx ends, and reports the first ping left without a pong.
func ping(linkCtx context.Context, ws *websocket.Conn, unanswered chan<- error) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-linkCtx.Done():
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(linkCtx, pongTimeout)
		err := ws.Ping(pctx)
		cancel()
		if err != nil && linkCtx.Err() == nil {
			unanswered <- fmt.Errorf("the panel stopped answering: %w", err)
			return
		}
	}
}

func (cfg Config) dial(ctx context.Context, l *Listener, address string) (*websocket.Conn, error) {
	fp, err := fingerprint(cfg.CertFile)
	if err != nil {
		return nil, err
	}
	u, err := linkURL(address)
	if err != nil {
		return nil, err
	}
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	go func() {
		select {
		case <-l.closed:
			cancel()
		case <-dctx.Done():
		}
	}()
	ws, resp, err := websocket.Dial(dctx, u, &websocket.DialOptions{
		HTTPClient:   &http.Client{Transport: transport()},
		HTTPHeader:   http.Header{CertHeader: []string{fp}},
		Subprotocols: []string{Subprotocol},
	})
	if err != nil {
		if resp != nil {
			return nil, refused(resp)
		}
		return nil, err
	}
	if ws.Subprotocol() != Subprotocol {
		ws.CloseNow()
		return nil, fmt.Errorf("the panel does not speak %s", Subprotocol)
	}
	return ws, nil
}

// refusedError is the door answering without upgrading; retryAfter is how long it asked the agent to stay away.
type refusedError struct {
	status     int
	retryAfter time.Duration
}

func (e *refusedError) Error() string {
	return fmt.Sprintf("the panel answered HTTP %d", e.status)
}

func refused(resp *http.Response) *refusedError {
	e := &refusedError{status: resp.StatusCode}
	if secs, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && secs > 0 {
		e.retryAfter = time.Duration(min(secs, int(maxRetryAfter/time.Second))) * time.Second
	}
	return e
}

func transport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		// ADR-0040: trust is the mTLS inside the link, never the panel's outer certificate.
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout: 10 * time.Second,
		DisableKeepAlives:   true,
	}
}

func linkURL(base string) (string, error) {
	b := strings.TrimRight(strings.TrimSpace(base), "/")
	lower := strings.ToLower(b)
	if !strings.HasPrefix(lower, "https://") && !strings.HasPrefix(lower, "http://") {
		return "", fmt.Errorf("not an http(s) address: %q", base)
	}
	return b + Path, nil
}

// fingerprint is the sha256 of the agent's leaf certificate, the value the panel pins.
func fingerprint(certFile string) (string, error) {
	raw, err := os.ReadFile(certFile)
	if err != nil {
		return "", err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return "", errors.New("no PEM block in the agent certificate")
	}
	sum := sha256.Sum256(block.Bytes)
	return hex.EncodeToString(sum[:]), nil
}

// conn is one link as grpc.Server sees it; closed fires once gRPC is done with it.
type conn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (c *conn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

// A call that came over the link says so, wherever the WebSocket under it came from.
func (c *conn) RemoteAddr() net.Addr { return linkAddr{} }
func (c *conn) LocalAddr() net.Addr  { return linkAddr{} }
