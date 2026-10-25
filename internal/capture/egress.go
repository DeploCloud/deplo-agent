package capture

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/netguard"
)

// Egress is the browser's only way out: an HTTP proxy on a unix socket. The deployment's own host
// always goes to this server's proxy, and nothing that is not a public address is ever dialed.
type Egress struct {
	// OwnHost is the deployment's host, never resolved through DNS.
	OwnHost string
	// Proxy maps "80" and "443" to where this server's proxy listens.
	Proxy map[string]string
	// Local lists this server's own addresses; nil reads the network interfaces.
	Local func() ([]net.IP, error)
	// DialContext is the dialer the vetted addresses go through; nil means a net.Dialer.
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)

	refused   atomic.Int64
	localOnce sync.Once
	local     []net.IP
	localErr  error
}

var errRefused = errors.New("refused")

// Refused counts the connections the policy turned down.
func (e *Egress) Refused() int64 { return e.refused.Load() }

func (e *Egress) dial(ctx context.Context, hostport string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil, err
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	proxy, viaProxy := e.Proxy[port]
	if viaProxy && host == strings.ToLower(e.OwnHost) {
		return e.base(ctx, proxy)
	}
	ips := []net.IP{net.ParseIP(host)}
	if ips[0] == nil {
		if ips, err = netguard.LookupIP(ctx, host); err != nil {
			return nil, err
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("%s has no address", host)
	}
	local := e.localIPs()
	// Another app on this server: reached through its proxy, the way any visitor reaches it.
	if viaProxy && allLocal(ips, local) {
		return e.base(ctx, proxy)
	}
	for _, ip := range ips {
		if reason := netguard.BlockedReason(ip); reason != "" {
			return nil, e.refuse(host, ip, reason)
		}
		if contains(local, ip) {
			return nil, e.refuse(host, ip, "this server's own")
		}
	}
	return e.base(ctx, net.JoinHostPort(ips[0].String(), port))
}

func (e *Egress) refuse(host string, ip net.IP, reason string) error {
	e.refused.Add(1)
	return fmt.Errorf("%w: %s resolves to a %s address %s", errRefused, host, reason, ip)
}

func (e *Egress) base(ctx context.Context, addr string) (net.Conn, error) {
	if e.DialContext != nil {
		return e.DialContext(ctx, "tcp", addr)
	}
	d := net.Dialer{Timeout: 10 * time.Second}
	return d.DialContext(ctx, "tcp", addr)
}

func (e *Egress) localIPs() []net.IP {
	e.localOnce.Do(func() {
		if e.Local != nil {
			e.local, e.localErr = e.Local()
			return
		}
		e.local, e.localErr = interfaceIPs()
	})
	if e.localErr != nil {
		log.Printf("deplo-agent: screenshot: reading this server's addresses: %v", e.localErr)
	}
	return e.local
}

func interfaceIPs() ([]net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	var out []net.IP
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok {
			out = append(out, n.IP)
		}
	}
	return out, nil
}

func contains(set []net.IP, ip net.IP) bool {
	for _, s := range set {
		if s.Equal(ip) {
			return true
		}
	}
	return false
}

func allLocal(ips, local []net.IP) bool {
	for _, ip := range ips {
		if !contains(local, ip) {
			return false
		}
	}
	return true
}

// Serve answers proxy requests on l until ctx ends, then closes every tunnel it opened.
func (e *Egress) Serve(ctx context.Context, l net.Listener) {
	forward := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL = pr.In.URL
			pr.Out.Host = ""
		},
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         func(ctx context.Context, _, addr string) (net.Conn, error) { return e.dial(ctx, addr) },
			TLSHandshakeTimeout: 10 * time.Second,
			MaxIdleConns:        16,
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadGateway)
		},
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodConnect:
				e.tunnel(ctx, w, r)
			case r.URL.IsAbs() && r.URL.Scheme == "http":
				forward.ServeHTTP(w, r)
			default:
				http.Error(w, "not a proxy request", http.StatusBadRequest)
			}
		}),
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          log.New(io.Discard, "", 0),
	}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	_ = srv.Serve(l)
}

func (e *Egress) tunnel(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	up, err := e.dial(r.Context(), r.Host)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = up.Close()
		http.Error(w, "cannot tunnel", http.StatusInternalServerError)
		return
	}
	c, buf, err := hj.Hijack()
	if err != nil {
		_ = up.Close()
		return
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close(); _ = up.Close() })
	defer stop()
	defer c.Close()
	defer up.Close()
	if _, err := c.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		return
	}
	splice(c, up, buf.Reader)
}

// splice copies both ways until either side ends; pending holds bytes the client sent early.
func splice(client, up net.Conn, pending *bufio.Reader) {
	done := make(chan struct{}, 2)
	go func() {
		if pending != nil && pending.Buffered() > 0 {
			_, _ = io.CopyN(up, pending, int64(pending.Buffered()))
		}
		_, _ = io.Copy(up, client)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, up)
		done <- struct{}{}
	}()
	<-done
}
