package dnszone

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"
)

const (
	maxTCPConns   = 128
	tcpQueries    = 64
	tcpIdle       = 5 * time.Second
	ioTimeout     = 2 * time.Second
	udpReadBuffer = 1500
)

// Server answers from the latest table on the sockets it is handed.
type Server struct {
	table   atomic.Pointer[compiled]
	geo     atomic.Pointer[Geo]
	lim     *limiter
	mu      sync.Mutex
	servers []*dns.Server
	wg      sync.WaitGroup
}

// NewServer is a server with no table yet: it answers SERVFAIL until it has one.
func NewServer() *Server {
	return &Server{lim: newLimiter(limitRate, limitBurst)}
}

// SetTable replaces what the server answers from; a table that does not hold together is refused whole.
func (s *Server) SetTable(t *Table) error {
	c, err := compile(t)
	if err != nil {
		return err
	}
	s.table.Store(c)
	return nil
}

// SetGeo is the location table visitors are placed with; nil answers every healthy server.
func (s *Server) SetGeo(g *Geo) { s.geo.Store(g) }

// Serve answers on every socket until Shutdown.
func (s *Server) Serve(udp []net.PacketConn, tcp []net.Listener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pc := range udp {
		s.start(&dns.Server{PacketConn: pc, Net: "udp", Handler: s, UDPSize: udpReadBuffer,
			ReadTimeout: ioTimeout, WriteTimeout: ioTimeout})
	}
	for _, ln := range tcp {
		s.start(&dns.Server{Listener: newCapped(ln, maxTCPConns), Net: "tcp",
			Handler: s, ReadTimeout: ioTimeout, WriteTimeout: ioTimeout, MaxTCPQueries: tcpQueries,
			IdleTimeout: func() time.Duration { return tcpIdle }})
	}
}

func (s *Server) start(srv *dns.Server) {
	s.servers = append(s.servers, srv)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = srv.ActivateAndServe()
	}()
}

// Shutdown stops answering and closes every socket.
func (s *Server) Shutdown() {
	s.mu.Lock()
	list := s.servers
	s.servers = nil
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, srv := range list {
		_ = srv.ShutdownContext(ctx)
	}
	s.wg.Wait()
}

// ServeDNS answers one query.
func (s *Server) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	udp := false
	if _, ok := w.LocalAddr().(*net.UDPAddr); ok {
		udp = true
	}
	if udp {
		switch s.lim.allow(addrOf(w.RemoteAddr()), time.Now()) {
		case drop:
			return
		case slip:
			tc := new(dns.Msg)
			if len(req.Question) == 1 {
				tc.SetReply(req)
			} else {
				tc.SetRcode(req, dns.RcodeFormatError)
			}
			tc.Truncated = true
			_ = w.WriteMsg(tc)
			return
		}
	}
	_ = w.WriteMsg(s.table.Load().reply(req, udp, visitor{from: addrOf(w.RemoteAddr()), geo: s.geo.Load()}))
}

func addrOf(a net.Addr) netip.Addr {
	switch v := a.(type) {
	case *net.UDPAddr:
		ip, _ := netip.AddrFromSlice(v.IP)
		return ip.Unmap()
	case *net.TCPAddr:
		ip, _ := netip.AddrFromSlice(v.IP)
		return ip.Unmap()
	}
	return netip.Addr{}
}

// capped holds at most cap(slots) connections open at once; past it, Accept waits for one to close.
type capped struct {
	net.Listener
	slots  chan struct{}
	closed chan struct{}
	once   sync.Once
}

func newCapped(ln net.Listener, n int) *capped {
	return &capped{Listener: ln, slots: make(chan struct{}, n), closed: make(chan struct{})}
}

func (l *capped) Close() error {
	l.once.Do(func() { close(l.closed) })
	return l.Listener.Close()
}

func (l *capped) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.closed:
		return nil, net.ErrClosed
	}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &slotConn{Conn: c, release: func() { <-l.slots }}, nil
}

type slotConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *slotConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
