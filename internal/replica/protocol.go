// Package replica keeps a copy of an App's stored data on another member of its cluster, as it changes (ADR-0046
// in DeploCloud/deplo). The member that runs the App sends, every other one receives into a copy of its own.
package replica

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// The stream, after the sink's manifest: a hello, a snapshot between S and E, then changes and beats until either
// side closes it. Every path is relative to one of the roots the hello names, and never leaves it.
const (
	opHello    = 'H'
	opSnapshot = 'S'
	opEnd      = 'E'
	opFile     = 'F'
	opDir      = 'D'
	opLink     = 'L'
	opKeep     = 'K'
	opDelete   = 'X'
	// A beat below zero is the size of the next record's file, as -(size+1): a receiver that predates it reads a beat.
	opBeat = 'B'
	// What a receiver sends after its manifest: beats, then Q when it stops the stream on purpose.
	opStop = 'Q'
	// The sink's manifest, sent first: one M per entry it holds, then Z.
	opManifest = 'M'
	opManEnd   = 'Z'
)

// A file record is its metadata, then pieces until its end: a range the receiver already holds (of this file or
// another one), or bytes it does not. The end carries the size and the SHA-256 the rebuilt file must have.
const (
	pieceCopy = 'c'
	pieceNew  = 'n'
	pieceEnd  = 'z'
)

// version is the stream's: both sides must speak the same one.
const version = 2

var errBadChunks = errors.New("replica: a file's chunk list is not one")

func readFull(r *reader, b []byte) (int, error) { return io.ReadFull(r.r, b) }

const (
	kindFile = 'f'
	kindDir  = 'd'
	kindLink = 'l'
)

// maxPath bounds one path on the wire; maxRoots the roots of one App.
const (
	maxPath  = 4096
	maxRoots = 64
)

// Entry is what both sides compare to decide whether a path must travel again.
type Entry struct {
	Kind   byte
	Mode   uint32
	UID    uint32
	GID    uint32
	MTime  int64
	Size   int64
	Target string
	// Chunks is what a file holds, as the receiver has it; never compared.
	Chunks []chunk
}

// Same says the two would be indistinguishable after a copy.
func (e Entry) Same(o Entry) bool {
	// A directory's time moves with every write below it: it is never what makes one travel again.
	return e.Kind == o.Kind && e.Mode == o.Mode && e.UID == o.UID && e.GID == o.GID &&
		(e.Kind == kindDir || e.MTime == o.MTime) && (e.Kind != kindFile || e.Size == o.Size) && e.Target == o.Target
}

// Key names one path of one root.
type Key struct {
	Root uint8
	Path string
}

// cleanRel refuses what could point outside a root: absolute paths, "..", empty segments, NUL.
func cleanRel(p string) (string, error) {
	if p == "" || len(p) > maxPath || strings.ContainsRune(p, 0) || strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("replica: bad path %q", p)
	}
	c := path.Clean(p)
	if c != p || c == "." || c == ".." || strings.HasPrefix(c, "../") {
		return "", fmt.Errorf("replica: bad path %q", p)
	}
	return c, nil
}

type writer struct{ w *bufio.Writer }

func newWriter(w io.Writer) *writer { return &writer{bufio.NewWriterSize(w, 64<<10)} }

func (w *writer) byte(b byte) error { return w.w.WriteByte(b) }

func (w *writer) uvarint(v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	_, err := w.w.Write(buf[:binary.PutUvarint(buf[:], v)])
	return err
}

func (w *writer) varint(v int64) error {
	var buf [binary.MaxVarintLen64]byte
	_, err := w.w.Write(buf[:binary.PutVarint(buf[:], v)])
	return err
}

func (w *writer) str(s string) error {
	if err := w.uvarint(uint64(len(s))); err != nil {
		return err
	}
	_, err := w.w.WriteString(s)
	return err
}

func (w *writer) head(op byte, k Key) error {
	if err := w.byte(op); err != nil {
		return err
	}
	if err := w.byte(k.Root); err != nil {
		return err
	}
	return w.str(k.Path)
}

func (w *writer) meta(e Entry) error {
	for _, v := range []uint64{uint64(e.Mode), uint64(e.UID), uint64(e.GID)} {
		if err := w.uvarint(v); err != nil {
			return err
		}
	}
	return w.varint(e.MTime)
}

func (w *writer) flush() error { return w.w.Flush() }

type reader struct{ r *bufio.Reader }

func newReader(r io.Reader) *reader { return &reader{bufio.NewReaderSize(r, 64<<10)} }

func (r *reader) byte() (byte, error) { return r.r.ReadByte() }

func (r *reader) uvarint() (uint64, error) { return binary.ReadUvarint(r.r) }

func (r *reader) varint() (int64, error) { return binary.ReadVarint(r.r) }

func (r *reader) str(limit int) (string, error) {
	n, err := r.uvarint()
	if err != nil {
		return "", err
	}
	if n > uint64(limit) {
		return "", errors.New("replica: a field is too long")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func (r *reader) key(roots int) (Key, error) {
	root, err := r.byte()
	if err != nil {
		return Key{}, err
	}
	if int(root) >= roots {
		return Key{}, errors.New("replica: a path names no root")
	}
	p, err := r.str(maxPath)
	if err != nil {
		return Key{}, err
	}
	p, err = cleanRel(p)
	return Key{root, p}, err
}

func (r *reader) meta(e *Entry) error {
	var vals [3]uint64
	for i := range vals {
		v, err := r.uvarint()
		if err != nil {
			return err
		}
		if v > 1<<32-1 {
			return errors.New("replica: a mode or owner is out of range")
		}
		vals[i] = v
	}
	e.Mode, e.UID, e.GID = uint32(vals[0]), uint32(vals[1]), uint32(vals[2])
	t, err := r.varint()
	e.MTime = t
	return err
}

// writeManifest is the sink's opening: what it already holds, so the snapshot sends only what differs.
func writeManifest(w *writer, held map[Key]Entry) error {
	for k, e := range held {
		if err := w.head(opManifest, k); err != nil {
			return err
		}
		if err := w.byte(e.Kind); err != nil {
			return err
		}
		if err := w.meta(e); err != nil {
			return err
		}
		if err := w.varint(e.Size); err != nil {
			return err
		}
		if err := w.str(e.Target); err != nil {
			return err
		}
		if e.Kind == kindFile {
			if err := w.chunks(e.Chunks); err != nil {
				return err
			}
		}
	}
	if err := w.byte(opManEnd); err != nil {
		return err
	}
	return w.flush()
}

// maxManifest bounds what a sink may claim to hold, so a sender's memory is bounded too.
const maxManifest = 5_000_000

func readManifest(r *reader) (map[Key]Entry, error) {
	held := map[Key]Entry{}
	for {
		op, err := r.byte()
		if err != nil {
			return nil, err
		}
		if op == opManEnd {
			return held, nil
		}
		if op != opManifest || len(held) >= maxManifest {
			return nil, errors.New("replica: the manifest is not one")
		}
		k, err := r.key(maxRoots)
		if err != nil {
			return nil, err
		}
		var e Entry
		if e.Kind, err = r.byte(); err != nil {
			return nil, err
		}
		if err := r.meta(&e); err != nil {
			return nil, err
		}
		if e.Size, err = r.varint(); err != nil {
			return nil, err
		}
		if e.Target, err = r.str(maxPath); err != nil {
			return nil, err
		}
		if e.Kind == kindFile {
			if e.Chunks, err = r.chunks(e.Size); err != nil {
				return nil, err
			}
		}
		held[k] = e
	}
}
