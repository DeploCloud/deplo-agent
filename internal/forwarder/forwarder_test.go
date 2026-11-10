package forwarder

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestRelay_carriesBothWays(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	go func() {
		c, _ := up.Accept()
		b := make([]byte, 16)
		n, _ := c.Read(b)
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
}

func TestMain_needsBothAddresses(t *testing.T) {
	var sb strings.Builder
	if code := Main([]string{"--listen", ":1"}, &sb); code != 2 {
		t.Fatalf("exit %d", code)
	}
}
