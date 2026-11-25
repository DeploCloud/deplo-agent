package server

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Tests hold nothing on the machine that runs them, except where one swaps in its own redirect.
func init() {
	holdRedirect = func(map[uint16]uint16) (func(), func(), error) { return nil, nil, errors.New("tests hold no port") }
}

// c41: every App went unanswered 8-10 s while its server's proxy restarted. A visitor that arrives meanwhile is held,
// and handed to the new proxy once it routes, nothing of it lost.
func TestFrontDoor_aVisitorArrivingWhileTheProxyRestartsIsHeldAndHandedOver(t *testing.T) {
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	go func() {
		for {
			c, err := proxy.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 64)
				n, _ := c.Read(buf)
				_, _ = c.Write([]byte(strings.ToUpper(string(buf[:n]))))
			}()
		}
	}()
	var up atomic.Bool
	var redirected map[uint16]uint16
	undone, dropped := make(chan struct{}), make(chan struct{})
	redirect, dial, settle := holdRedirect, frontDoorDial, holdSettle
	t.Cleanup(func() { holdRedirect, frontDoorDial, holdSettle = redirect, dial, settle })
	holdRedirect = func(p map[uint16]uint16) (func(), func(), error) {
		redirected = p
		return func() { close(undone) }, func() { close(dropped) }, nil
	}
	frontDoorDial = func(ctx context.Context, port uint32) (net.Conn, error) {
		if !up.Load() {
			return nil, errors.New("connection refused")
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", proxy.Addr().String())
	}
	holdSettle = 10 * time.Millisecond

	release := holdFrontDoorPassing(nil)
	if redirected[443] == 0 || redirected[80] == 0 {
		t.Fatalf("ports 80 and 443 are not held: %v", redirected)
	}
	c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(int(redirected[443])))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "hello")
	released := make(chan struct{})
	go func() { release(); close(released) }()
	time.Sleep(200 * time.Millisecond)
	select {
	case <-undone:
		t.Fatal("the hold ended before the new proxy answered")
	default:
	}
	up.Store(true)
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	got := make([]byte, 5)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "HELLO" {
		t.Fatalf("the held visitor read %q (%v)", got, err)
	}
	<-released
	select {
	case <-undone:
	default:
		t.Fatal("the redirect still takes new visitors")
	}
	// The redirect itself stays while a visitor it sent is connected: removing it loses that visitor's replies.
	select {
	case <-dropped:
		t.Fatal("the redirect was removed under a visitor it sent")
	case <-time.After(100 * time.Millisecond):
	}
	c.Close()
	select {
	case <-dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("the redirect stayed after its last visitor left")
	}
}

// The decision of c42: a visitor held while this server's proxy restarts is passed at once to another server that
// runs its App, its first TLS message intact; only an App this server alone runs waits for the new proxy.
func TestFrontDoor_aHeldVisitorOfAnAppElsewhereIsPassedOnAtOnce(t *testing.T) {
	var redirected map[uint16]uint16
	redirect, dial := holdRedirect, frontDoorDial
	t.Cleanup(func() { holdRedirect, frontDoorDial = redirect, dial })
	holdRedirect = func(p map[uint16]uint16) (func(), func(), error) { redirected = p; return func() {}, func() {}, nil }
	frontDoorDial = func(context.Context, uint32) (net.Conn, error) { return nil, errors.New("the proxy is down") }
	passed := make(chan string, 2)
	release := holdFrontDoorPassing(func(c net.Conn, host string) bool {
		if host != "shop.example" {
			return false
		}
		_, again := peekServerName(c)
		passed <- again
		c.Close()
		return true
	})
	defer func() { go release() }()
	visit := func(name string) {
		c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(int(redirected[443])))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		go func() { _ = tls.Client(c, &tls.Config{ServerName: name, InsecureSkipVerify: true}).Handshake() }()
	}
	visit("shop.example")
	visit("other.example")
	select {
	case got := <-passed:
		if got != "shop.example" {
			t.Fatalf("the visitor passed on reads %q, its first message not intact", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a visitor of an App another server runs was not passed on while the proxy is down")
	}
	select {
	case got := <-passed:
		t.Fatalf("a visitor of %q was passed on, though no other server runs its App", got)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestFrontDoor_thePassedVisitorCarriesItsAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		if c, err := ln.Accept(); err == nil {
			defer c.Close()
			_, _ = io.Copy(io.Discard, c)
		}
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	local := c.LocalAddr().(*net.TCPAddr).Port
	want := "PROXY TCP4 127.0.0.1 127.0.0.1 " + strconv.Itoa(local) + " " + strconv.Itoa(ln.Addr().(*net.TCPAddr).Port) + "\r\n"
	// From the server's side the visitor is the remote end.
	if got := proxyHeader(swapped{c}); got != want {
		t.Fatalf("PROXY header %q, want %q", got, want)
	}
}

type swapped struct{ net.Conn }

func (s swapped) LocalAddr() net.Addr  { return s.Conn.RemoteAddr() }
func (s swapped) RemoteAddr() net.Addr { return s.Conn.LocalAddr() }
