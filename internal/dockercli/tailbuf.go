package dockercli

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// TailBuf is an io.Writer that keeps only the last max bytes written to it.
type TailBuf struct {
	buf  []byte
	max  int
	over bool
}

// NewTailBuf returns a TailBuf that retains at most max bytes.
func NewTailBuf(max int) *TailBuf { return &TailBuf{max: max} }

func (t *TailBuf) Write(p []byte) (int, error) {
	n := len(p)
	if len(t.buf)+n <= t.max {
		t.buf = append(t.buf, p...)
		return n, nil
	}
	t.over = true
	if n >= t.max {
		t.buf = append(t.buf[:0], p[n-t.max:]...)
		return n, nil
	}
	t.buf = append(t.buf, p...)
	t.buf = append(t.buf[:0], t.buf[len(t.buf)-t.max:]...)
	return n, nil
}

// String returns the retained tail, prefixed with a note when anything was dropped.
func (t *TailBuf) String() string {
	b := t.buf
	for len(b) > 0 && !utf8.RuneStart(b[0]) {
		b = b[1:]
	}
	text := strings.ToValidUTF8(string(b), "\uFFFD")
	if !t.over {
		return text
	}
	kept := fmt.Sprintf("%d KiB", t.max>>10)
	if t.max < 1<<10 {
		kept = fmt.Sprintf("%d bytes", t.max)
	}
	return fmt.Sprintf("[deplo] earlier output trimmed - showing the last %s\n%s", kept, text)
}
