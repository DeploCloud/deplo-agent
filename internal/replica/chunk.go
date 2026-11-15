package replica

import (
	"crypto/sha256"
)

// A file travels as content-defined chunks (FastCDC): a chunk ends where the bytes before it say, not at a fixed
// offset, so inserting or appending shifts no boundary further on, and only chunks the receiver lacks are sent.
const (
	minChunk = 16 << 10
	avgChunk = 64 << 10
	maxChunk = 256 << 10
	sumLen   = 16
)

// Normalized chunking: harder to cut before the average size, easier after it. The top bits of the gear hash
// depend on the last 64 bytes only, so a boundary is found again right after a change.
const (
	maskS = uint64(1<<18-1) << (64 - 18)
	maskL = uint64(1<<14-1) << (64 - 14)
)

var gear [256]uint64

func init() {
	// A fixed table: both sides must cut every file in the same places.
	x := uint64(0x9e3779b97f4a7c15)
	for i := range gear {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		gear[i] = z ^ (z >> 31)
	}
}

// cut says where the chunk that starts b ends; b holds at least maxChunk bytes unless the file ends within it.
func cut(b []byte) int {
	n := len(b)
	if n <= minChunk {
		return n
	}
	if n > maxChunk {
		n = maxChunk
	}
	var h uint64
	i := minChunk
	for ; i < n && i < avgChunk; i++ {
		h = h<<1 + gear[b[i]]
		if h&maskS == 0 {
			return i + 1
		}
	}
	for ; i < n; i++ {
		h = h<<1 + gear[b[i]]
		if h&maskL == 0 {
			return i + 1
		}
	}
	return n
}

// chunk names one piece of a file by what it holds.
type chunk struct {
	sum  [sumLen]byte
	size uint32
}

func sumOf(b []byte) [sumLen]byte {
	full := sha256.Sum256(b)
	var s [sumLen]byte
	copy(s[:], full[:sumLen])
	return s
}

// chunker cuts a stream pushed through it; emit sees each chunk once, and must not keep the slice.
type chunker struct {
	buf []byte
}

func (c *chunker) write(p []byte, emit func([]byte) error) error {
	c.buf = append(c.buf, p...)
	for len(c.buf) >= maxChunk {
		n := cut(c.buf)
		if err := emit(c.buf[:n]); err != nil {
			return err
		}
		c.buf = append(c.buf[:0], c.buf[n:]...)
	}
	return nil
}

func (c *chunker) flush(emit func([]byte) error) error {
	for len(c.buf) > 0 {
		n := cut(c.buf)
		if err := emit(c.buf[:n]); err != nil {
			return err
		}
		c.buf = c.buf[n:]
	}
	c.buf = nil
	return nil
}

// listOf is the chunks of a file, as a receiver holds it.
type listOf struct {
	c   chunker
	out []chunk
}

func (l *listOf) Write(p []byte) (int, error) {
	err := l.c.write(p, func(b []byte) error {
		l.out = append(l.out, chunk{sumOf(b), uint32(len(b))})
		return nil
	})
	return len(p), err
}

func (l *listOf) done() []chunk {
	_ = l.c.flush(func(b []byte) error {
		l.out = append(l.out, chunk{sumOf(b), uint32(len(b))})
		return nil
	})
	return l.out
}

// where says, for each chunk a receiver holds, where it starts in its copy of the file.
func where(chunks []chunk) map[[sumLen]byte]int64 {
	at := make(map[[sumLen]byte]int64, len(chunks))
	var off int64
	for _, c := range chunks {
		if _, ok := at[c.sum]; !ok {
			at[c.sum] = off
		}
		off += int64(c.size)
	}
	return at
}

func (w *writer) chunks(list []chunk) error {
	if err := w.uvarint(uint64(len(list))); err != nil {
		return err
	}
	for _, c := range list {
		if _, err := w.w.Write(c.sum[:]); err != nil {
			return err
		}
		if err := w.uvarint(uint64(c.size)); err != nil {
			return err
		}
	}
	return nil
}

// maxChunks bounds one file's list: a file of size n never has more than n/minChunk+1 of them.
func (r *reader) chunks(size int64) ([]chunk, error) {
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if size < 0 || n > uint64(size/minChunk+2) {
		return nil, errBadChunks
	}
	out := make([]chunk, n)
	for i := range out {
		if _, err := readFull(r, out[i].sum[:]); err != nil {
			return nil, err
		}
		sz, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		if sz == 0 || sz > maxChunk {
			return nil, errBadChunks
		}
		out[i].size = uint32(sz)
	}
	return out, nil
}
