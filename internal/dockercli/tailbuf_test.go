package dockercli

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTailBufKeepsTheEnd(t *testing.T) {
	b := NewTailBuf(10)
	b.Write([]byte("0123456789"))
	if got := b.String(); got != "0123456789" {
		t.Fatalf("exact fit = %q, want the whole thing", got)
	}
	if b.over {
		t.Fatal("an exact fit must not report truncation")
	}

	b.Write([]byte("ABCDE"))
	got := b.String()
	if !strings.HasSuffix(got, "56789ABCDE") {
		t.Fatalf("tail = %q, want it to end with the LAST 10 bytes", got)
	}
	if !strings.Contains(got, "earlier output trimmed") {
		t.Fatalf("truncated output must say so, got %q", got)
	}
}

func TestTailBufSingleOversizedWrite(t *testing.T) {
	b := NewTailBuf(4)
	b.Write([]byte(strings.Repeat("x", 1<<20) + "END!"))
	if !strings.HasSuffix(b.String(), "END!") {
		t.Fatalf("want the end of the giant write, got %q", b.String())
	}
	if len(b.buf) != 4 {
		t.Fatalf("retained %d bytes, want the ceiling of 4", len(b.buf))
	}
}

func TestTailBufNeverCutsARuneInHalf(t *testing.T) {
	b := NewTailBuf(3)
	b.Write([]byte("ééé"))
	got := b.String()
	if !utf8.ValidString(got) {
		t.Fatalf("tail is not valid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, "é") {
		t.Fatalf("tail = %q, want it to end on a whole rune", got)
	}
}

func TestTailBufKeepsTextAroundAStrayByte(t *testing.T) {
	b := NewTailBuf(64)
	b.Write([]byte("ok\xff\xfe\xfdend\n"))
	got := b.String()
	if !utf8.ValidString(got) || !strings.HasPrefix(got, "ok") || !strings.HasSuffix(got, "end\n") {
		t.Fatalf("tail = %q, want ok...end with the stray bytes replaced", got)
	}
}
