package forwarder

import (
	"bufio"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRelay_carriesBothWays(t *testing.T) {
	dir, err := os.MkdirTemp("", "fw")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	up, err := net.Listen("unix", filepath.Join(dir, "s"))
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	header := make(chan string, 1)
	go func() {
		c, _ := up.Accept()
		r := bufio.NewReader(c)
		line, _ := r.ReadString('\n')
		header <- line
		b := make([]byte, 16)
		n, _ := r.Read(b)
		_, _ = c.Write([]byte(strings.ToUpper(string(b[:n]))))
		c.Close()
	}()
	front, back := net.Pipe()
	go Relay(back, up.Addr().String())
	_ = front.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = front.Write([]byte("hi"))
	out, _ := io.ReadAll(front)
	if string(out) != "HI" {
		t.Fatalf("got %q", out)
	}
	// A pipe has no address a redirect could have changed, so the agent is told it cannot tell.
	if got := <-header; got != "PORT 0\n" {
		t.Fatalf("the agent was told %q", got)
	}
}

// Off a redirect a TCP connection reads as the port it reached, or as unknown; never as another port.
func TestOriginalPort_isTheListenersOwnWithoutARedirect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err == nil {
			time.Sleep(200 * time.Millisecond)
			c.Close()
		}
	}()
	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, own := OriginalPort(c), ln.Addr().(*net.TCPAddr).Port
	if got != 0 && got != own {
		t.Fatalf("the original port reads %d, the listener is on %d", got, own)
	}
}

func TestMain_needsBothAddresses(t *testing.T) {
	var sb strings.Builder
	if code := Main([]string{"--listen", ":1"}, &sb); code != 2 {
		t.Fatalf("exit %d", code)
	}
}

// A client that sends its request and closes its sending side still gets the answer.
func TestJoin_aHalfClosedClientStillGetsItsAnswer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// The workload: reads the whole request, takes a moment, answers.
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		req, _ := io.ReadAll(c)
		time.Sleep(100 * time.Millisecond)
		c.Write([]byte("answer to " + string(req)))
	}()
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer front.Close()
	go func() {
		c, err := front.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			c.Close()
			return
		}
		Join(c, up)
	}()
	client, err := net.Dial("tcp", front.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.Write([]byte("ping"))
	client.(*net.TCPConn).CloseWrite()
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	got, _ := io.ReadAll(client)
	if string(got) != "answer to ping" {
		t.Fatalf("the client read %q", got)
	}
}
