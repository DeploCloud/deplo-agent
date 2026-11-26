//go:build linux

package replica

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/pace"
	"github.com/DeploCloud/deplo-agent/internal/room"
)

// Receive keeps the copy under dir: one folder per root name. It sends what it holds, then applies the sender's
// stream until ctx ends or the stream breaks; the copy stays as it is between two streams. ix carries what the
// copy's files hold from one stream to the next, so a reconnect does not read them all again.
func Receive(ctx context.Context, conn io.ReadWriter, dir string, names []string, st *Status, ix *Index) error {
	return receive(ctx, conn, dir, names, st, ix, false)
}

// ReceiveFirst is Receive for a stopped App's last changes: never paced, and every paced copy here waits for it.
func ReceiveFirst(ctx context.Context, conn io.ReadWriter, dir string, names []string, st *Status, ix *Index) error {
	defer pace.First()()
	return receive(ctx, conn, dir, names, st, ix, true)
}

func receive(ctx context.Context, conn io.ReadWriter, dir string, names []string, st *Status, ix *Index, fast bool) error {
	if len(names) == 0 || len(names) > maxRoots {
		return errors.New("replica: no roots to receive")
	}
	rc := &receiving{st: st, held: map[Key]Entry{}, buf: make([]byte, maxChunk), next: -1, fast: fast}
	for _, n := range names {
		p := filepath.Join(dir, rootDir(n))
		if err := os.MkdirAll(p, 0o700); err != nil {
			return err
		}
		r, err := openRoot(p)
		if err != nil {
			return err
		}
		defer r.close()
		// One stream writes a copy at a time: what a cut stream or an earlier run left half rebuilt goes first.
		if err := r.dropTemps(""); err != nil {
			return err
		}
		rc.roots = append(rc.roots, r)
	}
	for i, r := range rc.roots {
		err := r.walk(func(rel string, e Entry) error {
			// A receiver stopped for its App's move must not read the whole copy first.
			if err := ctx.Err(); err != nil {
				return err
			}
			k := Key{uint8(i), rel}
			if e.Kind == kindFile {
				list, ok := ix.lookup(names, k, e)
				if !ok {
					var err error
					list, e.Size, err = r.chunksOf(rel, fast)
					if errors.Is(err, os.ErrNotExist) {
						return nil
					}
					if err != nil {
						return err
					}
				}
				e.Chunks = list
			}
			rc.held[k] = e
			return nil
		})
		if err != nil {
			return err
		}
	}
	defer func() {
		if rc.wrong {
			ix.reset()
		} else {
			ix.keep(names, rc.held)
		}
	}()
	w := newWriter(conn)
	if err := writeManifest(w, rc.held); err != nil {
		return err
	}
	rd := newReader(conn)
	rc.rd = rd
	op, err := rd.byte()
	if err != nil {
		return err
	}
	if op != opHello {
		return errors.New("replica: the sender did not say hello")
	}
	raw, err := rd.str(64 << 10)
	if err != nil {
		return err
	}
	var h hello
	if err := json.Unmarshal([]byte(raw), &h); err != nil || h.V != version {
		return errors.New("replica: the sender speaks another version")
	}
	// The sender's roots by index, to this copy's: the same names, never another App's.
	if len(h.Roots) != len(names) {
		return errors.New("replica: the sender holds other data than this copy")
	}
	for i := range names {
		if h.Roots[i] != names[i] {
			return errors.New("replica: the sender holds other data than this copy")
		}
	}
	// A copy stopped for room says so until a whole snapshot lands: the next stream would clear it before Deplo reads it.
	st.set(func(s *Status) {
		s.Connected = true
		if !strings.Contains(s.Err, room.Marker) {
			s.Err = ""
		}
	})
	defer st.set(func(s *Status) { s.Connected = false })
	// A relay stream's close only ends its writing half: a read waiting on it is woken by a deadline.
	go func() {
		<-ctx.Done()
		if c, ok := conn.(interface{ SetReadDeadline(time.Time) error }); ok {
			_ = c.SetReadDeadline(time.Now())
		}
		if c, ok := conn.(io.Closer); ok {
			c.Close()
		}
	}()
	var seen map[Key]bool
	for {
		op, err := rd.byte()
		if err != nil {
			return err
		}
		touch := func() { st.set(func(s *Status) { s.AppliedAt = time.Now() }) }
		switch op {
		case opSnapshot:
			seen = map[Key]bool{}
		case opEnd:
			if seen == nil {
				return errors.New("replica: a snapshot ended that never began")
			}
			if err := sweep(rc.roots, rc.held, seen); err != nil {
				return err
			}
			seen = nil
			if err := os.WriteFile(filepath.Join(dir, readyFile), []byte(time.Now().UTC().Format(time.RFC3339)), 0o600); err != nil {
				return err
			}
			st.set(func(s *Status) { s.Synced, s.Err = true, "" })
			touch()
		case opBeat:
			v, err := rd.varint()
			if err != nil {
				return err
			}
			if rc.next = -1; v < 0 {
				rc.next = -v - 1
			}
			touch()
		case opFile, opDir, opLink, opKeep, opDelete:
			k, err := rd.key(len(rc.roots))
			if err != nil {
				return err
			}
			if seen != nil {
				seen[k] = true
			}
			err = rc.apply(op, k)
			rc.next = -1
			if err != nil {
				return fmt.Errorf("replica: %s: %w", k.Path, err)
			}
			touch()
		default:
			return fmt.Errorf("replica: unknown record %q", op)
		}
	}
}

// receiving is one stream into a copy.
type receiving struct {
	rd    *reader
	roots []*root
	held  map[Key]Entry
	st    *Status
	buf   []byte
	// wrong is set when the copy turned out not to hold what this side said it did.
	wrong bool
	// next is the size the sender gave the next file, -1 when it gave none.
	next int64
	fast bool
}

func (rc *receiving) apply(op byte, k Key) error {
	r := rc.roots[k.Root]
	var e Entry
	switch op {
	case opKeep:
		return nil
	case opDelete:
		rc.dropBelow(k)
		delete(rc.held, k)
		return r.remove(k.Path)
	case opDir:
		e.Kind = kindDir
		if err := rc.rd.meta(&e); err != nil {
			return err
		}
		if prev, ok := rc.held[k]; ok && prev.Kind != kindDir {
			delete(rc.held, k)
		}
		if err := r.mkdir(k.Path, e); err != nil {
			return err
		}
		rc.held[k] = e
		return nil
	case opLink:
		e.Kind = kindLink
		if err := rc.rd.meta(&e); err != nil {
			return err
		}
		t, err := rc.rd.str(maxPath)
		if err != nil {
			return err
		}
		e.Target = t
		rc.dropBelow(k)
		delete(rc.held, k)
		if err := r.symlink(k.Path, e); err != nil {
			return err
		}
		rc.held[k] = e
		return nil
	}
	e.Kind = kindFile
	if err := rc.rd.meta(&e); err != nil {
		return err
	}
	return rc.file(k, e)
}

// dropBelow forgets what the copy held under a path that is about to hold something else.
func (rc *receiving) dropBelow(k Key) {
	if prev, ok := rc.held[k]; !ok || prev.Kind != kindDir {
		return
	}
	for h := range rc.held {
		if h.Root == k.Root && len(h.Path) > len(k.Path) && h.Path[:len(k.Path)+1] == k.Path+"/" {
			delete(rc.held, h)
		}
	}
}

// maxSources bounds the files one record reads from at once.
const maxSources = 64

// file rebuilds one file from its pieces beside its place: ranges of what the copy holds, and the bytes it lacks.
// It goes in place only when its size and SHA-256 are the sender's.
func (rc *receiving) file(k Key, e Entry) error {
	t, err := rc.roots[k.Root].temp(k.Path)
	if err != nil {
		return err
	}
	done := false
	defer func() {
		if !done {
			t.abort()
		}
	}()
	srcs := map[Key]*os.File{}
	defer func() {
		for _, f := range srcs {
			f.Close()
		}
	}()
	sum := sha256.New()
	var list listOf
	gw := &gentle{f: t.f, need: max(rc.next, 0), fast: rc.fast}
	if prev, ok := rc.held[k]; ok && prev.Kind == kindFile && rc.next < 0 {
		gw.need = prev.Size
	}
	out := bufio.NewWriterSize(io.MultiWriter(gw, sum, &list), 256<<10)
	var total int64
	for {
		p, err := rc.rd.byte()
		if err != nil {
			return err
		}
		switch p {
		case pieceNew:
			n, err := rc.rd.uvarint()
			if err != nil {
				return err
			}
			if n == 0 || n > maxChunk {
				return errors.New("a piece of a size no chunk has")
			}
			if _, err := io.ReadFull(rc.rd.r, rc.buf[:n]); err != nil {
				return err
			}
			if _, err := out.Write(rc.buf[:n]); err != nil {
				return err
			}
			total += int64(n)
			rc.st.set(func(s *Status) { s.Bytes += int64(n) })
		case pieceCopy:
			src, err := rc.rd.key(len(rc.roots))
			if err != nil {
				return err
			}
			off, err := rc.rd.uvarint()
			if err != nil {
				return err
			}
			n, err := rc.rd.uvarint()
			if err != nil {
				return err
			}
			if n == 0 || off > 1<<62 || n > 1<<62 {
				return errors.New("a range no file holds")
			}
			f := srcs[src]
			if f == nil {
				if len(srcs) >= maxSources {
					for k, f := range srcs {
						f.Close()
						delete(srcs, k)
					}
				}
				if f, _, err = rc.roots[src.Root].open(src.Path); err != nil {
					rc.wrong = true
					return fmt.Errorf("the copy no longer holds %s: %w", src.Path, err)
				}
				srcs[src] = f
			}
			got, err := io.Copy(out, io.NewSectionReader(f, int64(off), int64(n)))
			dropPages(f, int64(off), int64(n))
			if err != nil {
				return err
			}
			if got != int64(n) {
				rc.wrong = true
				return fmt.Errorf("the copy holds less of %s than the sender knew", src.Path)
			}
			total += int64(n)
		case pieceEnd:
			size, err := rc.rd.varint()
			if err != nil {
				return err
			}
			var want [sha256.Size]byte
			if _, err := io.ReadFull(rc.rd.r, want[:]); err != nil {
				return err
			}
			if err := out.Flush(); err != nil {
				return err
			}
			gw.finish()
			if size != total || !bytes.Equal(want[:], sum.Sum(nil)) {
				rc.wrong = true
				return errors.New("the file rebuilt here is not the sender's: the copy is read again")
			}
			e.Size, e.Chunks = total, list.done()
			if e.Chunks == nil {
				e.Chunks = []chunk{}
			}
			rc.dropBelow(k)
			delete(rc.held, k)
			done = true
			if err := t.commit(e); err != nil {
				return err
			}
			rc.held[k] = e
			return nil
		default:
			return fmt.Errorf("unknown piece %q", p)
		}
	}
}

// sweep removes what the copy held and the snapshot did not name: it is gone at the sender.
func sweep(roots []*root, held map[Key]Entry, seen map[Key]bool) error {
	for k := range held {
		if seen[k] {
			continue
		}
		if err := roots[k.Root].remove(k.Path); err != nil {
			return err
		}
		delete(held, k)
	}
	return nil
}

// Promote replaces live data with the copy under dir: each root's folder into the matching live directory,
// which is emptied first. live maps a root name to the directory the App reads it from.
func Promote(dir string, names []string, live map[string]string) error {
	if !Ready(dir) {
		return errors.New("this server has no whole copy of the App's data yet")
	}
	// A copy moved half into place is never promoted again.
	used := sync.OnceFunc(func() { _ = os.Remove(filepath.Join(dir, readyFile)) })
	for _, n := range names {
		to, ok := live[n]
		if !ok {
			return fmt.Errorf("no place to put %s", n)
		}
		from, err := openRoot(filepath.Join(dir, rootDir(n)))
		if err != nil {
			return err
		}
		dst, err := openRoot(to)
		if err != nil {
			from.close()
			return err
		}
		err = moveTree(from, dst, used)
		from.close()
		dst.close()
		if err != nil {
			return fmt.Errorf("put %s in place: %w", n, err)
		}
	}
	return nil
}
