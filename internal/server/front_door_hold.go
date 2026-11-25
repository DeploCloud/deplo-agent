package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/forwarder"
)

// While this server's proxy restarts, its ports 80 and 443 lead to the agent: a visitor of a cluster App goes where
// the App answers, as the proxy would have passed it, and any other waits for the new proxy (c41: 8-10 s unanswered).

var (
	// holdRedirect sends this server's own ports to the agent's (front port -> hold port); stop lets new visitors
	// through again, drop ends it once the visitors it sent are gone. A var so tests redirect nothing.
	holdRedirect = redirectFrontDoor
	// frontDoorDial reaches the proxy's own port inside its network.
	frontDoorDial = func(ctx context.Context, port uint32) (net.Conn, error) { return dialProxyLoopback(ctx, port) }
	// frontDoorWait is how long a release waits for the new proxy, holdSettle how long it lets it read its routes.
	frontDoorWait = 60 * time.Second
	holdSettle    = 500 * time.Millisecond
	// holdLinger keeps the hold's listeners a moment past the redirect's end, for visitors already on their way.
	holdLinger = time.Second
)

var frontDoorPorts = []uint32{80, 443}

type frontHold struct {
	mu   sync.Mutex
	held []heldVisitor
	open bool
	// live counts the visitors it took that are still connected; drop runs once none is and the hold ended.
	live int
	drop func()
	// pass takes a visitor of that hostname for a cluster App, and says whether it did.
	pass func(c net.Conn, host string) bool
	// standby, when one runs, takes every other visitor at once instead of holding it.
	standby standbyDialer
}

type heldVisitor struct {
	c    net.Conn
	port uint32
}

// holdFrontDoor holds this server's visitors while its proxy restarts; its release, once the new proxy is up, hands
// over the ones it kept. A hold that could not start is said once and costs nothing: visitors are turned away.
func (s *Service) holdFrontDoor(standby standbyDialer) (release func()) {
	return holdFrontDoorPassing(s.forwarders().passHeld, standby)
}

func holdFrontDoorPassing(pass func(net.Conn, string) bool, standby standbyDialer) (release func()) {
	h := &frontHold{open: true, pass: pass, standby: standby}
	redirect := map[uint16]uint16{}
	var lns []net.Listener
	for _, port := range frontDoorPorts {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: could not hold visitors while the proxy restarts: %v\n", err)
			closeAll(lns)
			return func() {}
		}
		lns = append(lns, ln)
		redirect[uint16(port)] = uint16(ln.Addr().(*net.TCPAddr).Port)
		go h.accept(ln, port)
	}
	stop, drop, err := holdRedirect(redirect)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not hold visitors while the proxy restarts: %v\n", err)
		closeAll(lns)
		return func() {}
	}
	h.drop = drop
	var once sync.Once
	return func() {
		once.Do(func() {
			h.release(stop, lns)
		})
	}
}

func (h *frontHold) accept(ln net.Listener, port uint32) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		h.mu.Lock()
		h.live++
		h.mu.Unlock()
		go h.take(&tracked{Conn: c, done: h.gone}, port)
	}
}

// gone is one visitor the hold took that closed: the last one after the release ends the redirect.
func (h *frontHold) gone() {
	h.mu.Lock()
	h.live--
	last := h.live == 0 && !h.open
	h.mu.Unlock()
	if last {
		h.drop()
	}
}

// take passes a TLS visitor of a cluster App on as the proxy would have, or holds it for the new proxy.
func (h *frontHold) take(c net.Conn, port uint32) {
	if port == 443 && h.pass != nil {
		hello, host := peekServerName(c)
		c = &bufferedConn{Conn: c, r: bufio.NewReader(io.MultiReader(bytes.NewReader(hello), c))}
		if host != "" && h.pass(c, host) {
			return
		}
	}
	h.mu.Lock()
	if h.open && h.standby == nil {
		h.held = append(h.held, heldVisitor{c, port})
		h.mu.Unlock()
		return
	}
	dial := frontDoorDial
	if h.open {
		dial = h.standby
	}
	h.mu.Unlock()
	handOverVia(dial, c, port)
}

// release waits for the new proxy's front door and its routes, lets new visitors reach it, then hands over every
// visitor held.
func (h *frontHold) release(stop func(), lns []net.Listener) {
	ctx, cancel := context.WithTimeout(context.Background(), frontDoorWait)
	defer cancel()
	for ctx.Err() == nil {
		if c, err := frontDoorDial(ctx, 443); err == nil {
			c.Close()
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	time.Sleep(holdSettle)
	stop()
	h.mu.Lock()
	h.open = false
	held, idle := h.held, h.live == 0
	h.held = nil
	h.mu.Unlock()
	for _, v := range held {
		go handOver(v.c, v.port)
	}
	time.AfterFunc(holdLinger, func() { closeAll(lns) })
	if idle {
		h.drop()
	}
}

func handOver(c net.Conn, port uint32) { handOverVia(frontDoorDial, c, port) }

func handOverVia(dial func(context.Context, uint32) (net.Conn, error), c net.Conn, port uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	up, err := dial(ctx, port)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: a visitor held on port %s while the proxy restarted could not reach it: %v\n", strconv.Itoa(int(port)), err)
		c.Close()
		return
	}
	forwarder.Join(c, up)
}

// tracked is a visitor the hold took; its first Close says so.
type tracked struct {
	net.Conn
	once sync.Once
	done func()
}

func (t *tracked) Close() error {
	err := t.Conn.Close()
	t.once.Do(t.done)
	return err
}

func (t *tracked) CloseWrite() error {
	if cw, ok := t.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

var errPeeked = errors.New("peeked")

// peekServerName reads a TLS visitor's first message and the name it asks for; the bytes read go back in front.
func peekServerName(c net.Conn) ([]byte, string) {
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	var read bytes.Buffer
	var name string
	_ = tls.Server(helloOnly{io.TeeReader(c, &read)}, &tls.Config{GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
		name = h.ServerName
		return nil, errPeeked
	}}).Handshake()
	return read.Bytes(), name
}

// helloOnly is a connection a TLS server reads a ClientHello from and never writes to.
type helloOnly struct{ r io.Reader }

func (h helloOnly) Read(p []byte) (int, error)     { return h.r.Read(p) }
func (helloOnly) Write([]byte) (int, error)        { return 0, errPeeked }
func (helloOnly) Close() error                     { return nil }
func (helloOnly) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (helloOnly) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (helloOnly) SetDeadline(time.Time) error      { return nil }
func (helloOnly) SetReadDeadline(time.Time) error  { return nil }
func (helloOnly) SetWriteDeadline(time.Time) error { return nil }

// proxyHeader is the PROXY v1 line a server's answer entrypoint expects before a visitor's TLS.
func proxyHeader(c net.Conn) string {
	from, _ := c.RemoteAddr().(*net.TCPAddr)
	to, _ := c.LocalAddr().(*net.TCPAddr)
	if from == nil || to == nil {
		return "PROXY UNKNOWN\r\n"
	}
	family := "TCP6"
	if from.IP.To4() != nil && to.IP.To4() != nil {
		family = "TCP4"
		from.IP, to.IP = from.IP.To4(), to.IP.To4()
	}
	return fmt.Sprintf("PROXY %s %s %s %d %d\r\n", family, from.IP, to.IP, from.Port, to.Port)
}

// proxyGoingDown starts holding this server's visitors as its proxy stops, once per restart: none is passed to the
// proxy until proxyBack.
func (s *Service) proxyGoingDown() {
	s.frontMu.Lock()
	defer s.frontMu.Unlock()
	if s.frontRelease != nil {
		s.forwarders().proxyStopping(nil)
		return
	}
	dial, end, err := standbyUp(context.Background(), s.agentDir, s.forwarders().state().AnswerPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: no standby proxy: visitors wait for the new one (%v)\n", err)
	}
	if dial != nil {
		dial, end = countedStandby(dial, end)
	}
	s.forwarders().proxyStopping(dial)
	s.frontRelease = s.holdFrontDoor(dial)
	s.frontStandbyEnd = end
}

// standbyDrain bounds how long the standby outlives the restart for a visitor it still carries.
var standbyDrain = 30 * time.Second

// countedStandby counts the visitors passed to the standby: it ends once none is left, or after standbyDrain (c43: a
// standby stopped under the visitors handed to it the moment before reset them).
func countedStandby(dial standbyDialer, end func()) (standbyDialer, func()) {
	var open atomic.Int64
	counted := func(ctx context.Context, port uint32) (net.Conn, error) {
		c, err := dial(ctx, port)
		if err != nil {
			return nil, err
		}
		open.Add(1)
		return &tracked{Conn: c, done: func() { open.Add(-1) }}, nil
	}
	return counted, func() {
		for until := time.Now().Add(standbyDrain); open.Load() > 0 && time.Now().Before(until); {
			time.Sleep(100 * time.Millisecond)
		}
		end()
	}
}

// proxyBack is the proxy started again: its visitors wait proxySettle more, and the hold hands over what it kept.
func (s *Service) proxyBack() {
	s.forwarders().proxyRestarted()
	s.frontMu.Lock()
	release, end := s.frontRelease, s.frontStandbyEnd
	s.frontRelease, s.frontStandbyEnd = nil, nil
	s.frontMu.Unlock()
	if release != nil {
		release()
	}
	if end != nil {
		// The standby serves until the new proxy is settled, then lets what it carries finish.
		go func() {
			time.Sleep(proxySettle)
			s.forwarders().standbyGone()
			end()
		}()
	}
}

// ClearFrontDoorHold ends what an agent that stopped mid-restart left holding this server's ports.
func ClearFrontDoorHold() { clearFrontDoorHold() }

func closeAll(lns []net.Listener) {
	for _, ln := range lns {
		ln.Close()
	}
}
