package server

import (
	"io"
	"testing"
	"time"
)

func TestAttachPipesEndsWhenInputCloses(t *testing.T) {
	bin, args := attachBin, attachArgs
	attachBin, attachArgs = "cat", func(string) []string { return nil }
	t.Cleanup(func() { attachBin, attachArgs = bin, args })

	a, err := newAttachPipes("unused")
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	a.write([]byte("hi\n"))
	a.closeInput()

	got := make(chan string)
	go func() {
		var out []byte
		buf := make([]byte, 64)
		for {
			n, err := a.read(buf)
			out = append(out, buf[:n]...)
			if err == io.EOF {
				got <- string(out)
				return
			}
		}
	}()
	select {
	case out := <-got:
		if out != "hi\n" {
			t.Fatalf("output %q", out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("attach kept running after its input closed")
	}
}
