package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Tests hold nothing on the machine that runs them, except where one swaps in its own redirect.
func init() {
	holdRedirect = func(map[uint16]uint16) (func(), func(), error) { return nil, nil, errors.New("tests hold no port") }
	standbyUp = func(context.Context, string, uint32) (standbyDialer, func(), error) {
		return nil, nil, errors.New("tests run no standby")
	}
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

	release := holdFrontDoorPassing(nil, nil)
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
	}, nil)
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

// c42: an App on one server waited its proxy's whole restart (3-8 s on one core). With a standby proxy running, a
// visitor arriving meanwhile is answered by it at once, never held for the new proxy.
func TestFrontDoor_aStandbyProxyAnswersAtOnceWhileTheProxyRestarts(t *testing.T) {
	standby, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer standby.Close()
	go func() {
		for {
			c, err := standby.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 64)
				n, _ := c.Read(buf)
				_, _ = c.Write([]byte("standby:" + string(buf[:n])))
			}()
		}
	}()
	var redirected map[uint16]uint16
	redirect, dial := holdRedirect, frontDoorDial
	t.Cleanup(func() { holdRedirect, frontDoorDial = redirect, dial })
	holdRedirect = func(p map[uint16]uint16) (func(), func(), error) { redirected = p; return func() {}, func() {}, nil }
	frontDoorDial = func(context.Context, uint32) (net.Conn, error) { return nil, errors.New("the proxy is down") }
	release := holdFrontDoorPassing(nil, func(ctx context.Context, port uint32) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", standby.Addr().String())
	})
	defer func() { go release() }()
	c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(int(redirected[80])))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET")
	_ = c.SetDeadline(time.Now().Add(time.Second))
	got := make([]byte, 11)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "standby:GET" {
		t.Fatalf("a visitor while the proxy restarts read %q (%v), not the standby's answer", got, err)
	}
}

func TestStandbyRunArgs_noPortNoAliasAndACopyOfWhatTheProxyWrites(t *testing.T) {
	var p proxyShape
	p.Image = "sha256:abc"
	p.Config.Cmd = []string{"--providers.docker=true", "--providers.file.directory=/deplo-dynamic"}
	p.Config.Entrypoint = []string{"/entrypoint.sh"}
	p.HostConfig.Binds = []string{"/t/acme:/acme:rw", "/t/answers.yml:/deplo-dynamic/deplo-answers.yml:ro"}
	p.NetworkSettings.Networks = map[string]json.RawMessage{"deplo": nil, "traefik_deplo-socket": nil}
	args, copies, networks := standbyRunArgs(p, "/c")
	line := strings.Join(args, " ")
	for _, bad := range []string{"-p", "--publish", "--network-alias", "/t/acme:/acme"} {
		for _, a := range args {
			if a == bad || strings.HasPrefix(a, bad) {
				t.Fatalf("the standby has %q: %s", bad, line)
			}
		}
	}
	for _, want := range []string{"--name " + standbyContainer, "--restart no", "-v /c/0:/acme", "-v /t/answers.yml:/deplo-dynamic/deplo-answers.yml:ro",
		"--network deplo", "-v /c/dynamic:/deplo-dynamic:ro", "--entrypoint /entrypoint.sh sha256:abc --providers.docker=true"} {
		if !strings.Contains(line, want) {
			t.Fatalf("want %q in %s", want, line)
		}
	}
	// c43: compose copies its configs into the proxy (the answer transport, the certificates): the standby lacked them.
	if len(copies) != 2 || copies[0] != [2]string{"/t/acme", "/c/0"} || copies[1] != [2]string{"container:/deplo-dynamic", "/c/dynamic"} || len(networks) != 2 {
		t.Fatalf("copies %v networks %v", copies, networks)
	}
}

// c43: recreating the whole file recreated the socket proxy too, and the standby answering meanwhile lost every route it
// reads through it (404 for 3 s on eu-4). Only the proxy's service is recreated; the rest only if it changed.
func TestTraefikUpArgs_recreatesOnlyTheProxy(t *testing.T) {
	got := traefikUpArgs("/t/docker-compose.yml", "traefik")
	if len(got) != 2 || !strings.Contains(strings.Join(got[0], " "), "--force-recreate --no-deps --timeout 1 traefik") ||
		strings.Contains(strings.Join(got[1], " "), "--force-recreate") {
		t.Fatalf("%q", got)
	}
	if old := traefikUpArgs("/t/docker-compose.yml", ""); len(old) != 1 || !strings.Contains(strings.Join(old[0], " "), "--force-recreate --remove-orphans") {
		t.Fatalf("with no service known: %q", old)
	}
}

// The standby counts ready once it answers a routed domain as the proxy does, on the answer entrypoint behind its
// PROXY header for a cluster server.
func TestStatusVia_readsAProxysAnswerBehindTheProxyHeader(t *testing.T) {
	certs := httptest.NewTLSServer(http.NotFoundHandler())
	defer certs.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				br := bufio.NewReader(c)
				line := ""
				if b, _ := br.Peek(1); len(b) == 1 && b[0] == 'P' {
					line, _ = br.ReadString('\n')
				}
				tc := tls.Server(&bufferedConn{Conn: c, r: br}, &tls.Config{Certificates: certs.TLS.Certificates})
				req, err := http.ReadRequest(bufio.NewReader(tc))
				status := "404 Not Found"
				if err == nil && strings.HasPrefix(line, "PROXY TCP4 ") && req.Host == "shop.example" {
					status = "200 OK"
				}
				_, _ = io.WriteString(tc, "HTTP/1.1 "+status+"\r\nContent-Length: 0\r\n\r\n")
			}()
		}
	}()
	dial := func(ctx context.Context, _ uint32) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", ln.Addr().String())
	}
	if got := statusVia(context.Background(), dial, 8443, "PROXY TCP4 127.0.0.1 127.0.0.1 1 443\r\n", "shop.example"); got != 200 {
		t.Fatalf("behind the header: %d", got)
	}
	if got := statusVia(context.Background(), dial, 443, "", "shop.example"); got != 404 {
		t.Fatalf("without it: %d", got)
	}
}
