//go:build linux

package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/pace"
)

// Timing of a sender; vars so tests can hurry them.
var (
	// settleFor is how long a changed directory stays quiet before it is read, at most settleMax after its first change.
	settleFor = 100 * time.Millisecond
	settleMax = 500 * time.Millisecond
	// BeatEvery is how often a sender says it is still there.
	BeatEvery = 2 * time.Second
	// rescanEvery reads everything again when the kernel would not watch it all.
	rescanEvery = 5 * time.Second
)

type hello struct {
	V     int      `json:"v"`
	Roots []string `json:"roots"`
}

// sender is one stream's view of the data: what the receiver holds, per directory, so a change is found by comparing,
// and which chunks it holds and where, so a changed file travels as the bytes the receiver lacks.
type sender struct {
	roots  []*root
	w      *writer
	watch  *watcher
	held   map[Key]Entry
	kids   map[Key]map[string]bool
	chunks map[Key][]chunk
	loc    map[[sumLen]byte]place
	bytes  int64
	fast   bool
}

// place is where a receiver holds one chunk: a file, and the offset in it.
type place struct {
	k   Key
	off int64
}

// Send serves one receiver: it reads its manifest, sends a snapshot of what differs, then every change as it
// happens, until ctx ends or the stream breaks. names and dirs are the roots, in the order both sides know them.
func Send(ctx context.Context, conn io.ReadWriter, names, dirs []string) error {
	return send(ctx, conn, names, dirs, true)
}

// SendOnce sends one snapshot of what differs and ends: the last changes of an App stopped for its move. It is
// never paced, and every paced copy here waits for it: the App is down until they land (c46).
func SendOnce(ctx context.Context, conn io.ReadWriter, names, dirs []string) error {
	defer pace.First()()
	return send(ctx, conn, names, dirs, false)
}

func send(ctx context.Context, conn io.ReadWriter, names, dirs []string, follow bool) error {
	if len(names) != len(dirs) || len(names) == 0 || len(names) > maxRoots {
		return errors.New("replica: no roots to send")
	}
	rd := newReader(conn)
	held, err := readManifest(rd)
	if err != nil {
		return err
	}
	// What the receiver sends after its manifest is its probe, and its stop: read, so its window never fills.
	stopped := make(chan struct{})
	go func() {
		b := make([]byte, 512)
		for {
			n, err := rd.r.Read(b)
			// A relay stream's close only ends the receiver's writing half: a write blocked on it is woken.
			if bytes.IndexByte(b[:n], opStop) >= 0 {
				close(stopped)
				if c, ok := conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
					_ = c.SetWriteDeadline(time.Now())
				}
				return
			}
			if err != nil {
				return
			}
		}
	}()
	if c, ok := conn.(interface{ SetReadDeadline(time.Time) error }); ok {
		defer c.SetReadDeadline(time.Now())
	}
	s := &sender{w: newWriter(conn), held: map[Key]Entry{}, kids: map[Key]map[string]bool{},
		chunks: map[Key][]chunk{}, loc: map[[sumLen]byte]place{}, fast: !follow}
	for _, d := range dirs {
		r, err := openRoot(d)
		if err != nil {
			s.closeRoots()
			return err
		}
		s.roots = append(s.roots, r)
	}
	defer s.closeRoots()
	if s.watch, err = newWatcher(); err != nil {
		return err
	}
	defer s.watch.close()
	raw, _ := json.Marshal(hello{V: version, Roots: names})
	if err := s.w.byte(opHello); err != nil {
		return err
	}
	if err := s.w.str(string(raw)); err != nil {
		return err
	}
	err = s.snapshot(held)
	if err == nil && follow {
		err = s.follow(ctx)
	}
	return byReceiver(ctx, err, stopped)
}

// stopWait is how long a stream that broke waits for its receiver's stop, which may come just after the break.
var stopWait = 500 * time.Millisecond

// byReceiver drops the error of a stream its receiver stopped: Deplo held the copy, or it ended, and nothing failed
// here (c50: every held copy read "endpoint is closed for send" on its sender).
func byReceiver(ctx context.Context, err error, stopped <-chan struct{}) error {
	if err == nil || ctx.Err() != nil {
		return err
	}
	select {
	case <-stopped:
		return nil
	case <-time.After(stopWait):
		return err
	}
}

func (s *sender) closeRoots() {
	for _, r := range s.roots {
		r.close()
	}
}

func parentOf(k Key) Key {
	if i := strings.LastIndexByte(k.Path, '/'); i >= 0 {
		return Key{k.Root, k.Path[:i]}
	}
	return Key{k.Root, ""}
}

func base(p string) string { return p[strings.LastIndexByte(p, '/')+1:] }

func (s *sender) remember(k Key, e Entry) {
	s.held[k] = e
	p := parentOf(k)
	if s.kids[p] == nil {
		s.kids[p] = map[string]bool{}
	}
	s.kids[p][base(k.Path)] = true
}

// setChunks records what the receiver now holds of one file, and where each of its chunks can be copied from.
func (s *sender) setChunks(k Key, list []chunk) {
	s.dropChunks(k)
	s.chunks[k] = list
	var off int64
	for _, c := range list {
		if _, ok := s.loc[c.sum]; !ok {
			s.loc[c.sum] = place{k, off}
		}
		off += int64(c.size)
	}
}

// dropChunks forgets a file the receiver no longer holds as it was; a chunk also held elsewhere is simply not reused.
func (s *sender) dropChunks(k Key) {
	for _, c := range s.chunks[k] {
		if p, ok := s.loc[c.sum]; ok && p.k == k {
			delete(s.loc, c.sum)
		}
	}
	delete(s.chunks, k)
}

func (s *sender) dropBelow(k Key) {
	for h := range s.chunks {
		if h.Root == k.Root && strings.HasPrefix(h.Path, k.Path+"/") {
			s.dropChunks(h)
		}
	}
}

func (s *sender) forget(k Key) {
	if e, ok := s.held[k]; ok && e.Kind == kindDir {
		for name := range s.kids[k] {
			s.forget(Key{k.Root, k.Path + "/" + name})
		}
		delete(s.kids, k)
		s.watch.drop(k)
	}
	s.dropChunks(k)
	delete(s.held, k)
	if kids := s.kids[parentOf(k)]; kids != nil {
		delete(kids, base(k.Path))
	}
}

// snapshot sends every entry the receiver lacks or holds differently, keeps what it holds the same, and ends: the
// receiver then removes whatever was not named.
func (s *sender) snapshot(theirs map[Key]Entry) error {
	if err := s.w.byte(opSnapshot); err != nil {
		return err
	}
	// Everything the receiver holds can be copied from until the snapshot ends, kept or not.
	for k, e := range theirs {
		if e.Kind == kindFile {
			s.setChunks(k, e.Chunks)
		}
	}
	for i, r := range s.roots {
		k := Key{uint8(i), ""}
		if err := s.watch.add(r, k); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		err := r.walk(func(rel string, e Entry) error {
			k := Key{uint8(i), rel}
			if e.Kind == kindDir {
				if err := s.watch.add(r, k); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			prev, ok := theirs[k]
			if ok && prev.Same(e) {
				s.remember(k, e)
				return s.w.head(opKeep, k)
			}
			// What the receiver held below a directory that is now something else goes with it.
			if ok && prev.Kind == kindDir && e.Kind != kindDir {
				s.dropBelow(k)
			}
			return s.send(k, e)
		})
		if err != nil {
			return err
		}
	}
	// What the snapshot did not name leaves the receiver at its end.
	for k := range theirs {
		if _, ok := s.held[k]; !ok {
			s.dropChunks(k)
		}
	}
	if err := s.w.byte(opEnd); err != nil {
		return err
	}
	return s.w.flush()
}

// send writes one entry as it is now; a file that changed under it is sent whole again with the next look.
func (s *sender) send(k Key, e Entry) error {
	r := s.roots[k.Root]
	if e.Kind != kindFile {
		// The receiver no longer holds a file here to copy from.
		s.dropChunks(k)
	}
	switch e.Kind {
	case kindDir:
		if err := s.w.head(opDir, k); err != nil {
			return err
		}
		if err := s.w.meta(e); err != nil {
			return err
		}
	case kindLink:
		if err := s.w.head(opLink, k); err != nil {
			return err
		}
		if err := s.w.meta(e); err != nil {
			return err
		}
		if err := s.w.str(e.Target); err != nil {
			return err
		}
	case kindFile:
		f, cur, err := r.open(k.Path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		defer f.Close()
		if e, err = s.sendFile(k, cur, f); err != nil {
			return err
		}
	}
	s.remember(k, e)
	return nil
}

// sendFile writes a file as pieces: each chunk the receiver holds somewhere is a range it copies, the rest its bytes.
// The end says how big the file is and what its SHA-256 is, so the receiver never keeps one rebuilt wrong.
func (s *sender) sendFile(k Key, e Entry, f *os.File) (Entry, error) {
	// Its size first: a receiver without room refuses it before writing any of it (c46: 1.3 GB written, then refused).
	if err := s.w.byte(opBeat); err != nil {
		return e, err
	}
	if err := s.w.varint(-(e.Size + 1)); err != nil {
		return e, err
	}
	if err := s.w.head(opFile, k); err != nil {
		return e, err
	}
	if err := s.w.meta(e); err != nil {
		return e, err
	}
	whole := sha256.New()
	var list []chunk
	var total, run int64
	var from place
	flush := func() error {
		if run == 0 {
			return nil
		}
		defer func() { run = 0 }()
		if err := s.w.byte(pieceCopy); err != nil {
			return err
		}
		if err := s.w.byte(from.k.Root); err != nil {
			return err
		}
		if err := s.w.str(from.k.Path); err != nil {
			return err
		}
		if err := s.w.uvarint(uint64(from.off)); err != nil {
			return err
		}
		return s.w.uvarint(uint64(run))
	}
	emit := func(b []byte) error {
		c := chunk{sumOf(b), uint32(len(b))}
		list = append(list, c)
		total += int64(len(b))
		whole.Write(b)
		if p, ok := s.loc[c.sum]; ok {
			if run > 0 && p.k == from.k && from.off+run == p.off {
				run += int64(len(b))
				return nil
			}
			if err := flush(); err != nil {
				return err
			}
			from, run = p, int64(len(b))
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		if err := s.w.byte(pieceNew); err != nil {
			return err
		}
		if err := s.w.uvarint(uint64(len(b))); err != nil {
			return err
		}
		_, err := s.w.w.Write(b)
		s.bytes += int64(len(b))
		return err
	}
	var c chunker
	buf := make([]byte, 64<<10)
	in := &behind{f: f, fast: s.fast}
	for {
		n, err := in.Read(buf)
		if n > 0 {
			if werr := c.write(buf[:n], emit); werr != nil {
				return e, werr
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return e, err
		}
	}
	if err := c.flush(emit); err != nil {
		return e, err
	}
	if err := flush(); err != nil {
		return e, err
	}
	if err := s.w.byte(pieceEnd); err != nil {
		return e, err
	}
	if err := s.w.varint(total); err != nil {
		return e, err
	}
	if _, err := s.w.w.Write(whole.Sum(nil)); err != nil {
		return e, err
	}
	// It changed while being read: the receiver has what was read, and the next look sends it again.
	if total != e.Size {
		e.Size, e.MTime = total, -1
	}
	s.setChunks(k, list)
	return e, nil
}

// look reads one directory again and sends what changed in it since the receiver last heard.
func (s *sender) look(dir Key) error {
	r := s.roots[dir.Root]
	names, err := r.list(dir.Path)
	if errors.Is(err, os.ErrNotExist) {
		if dir.Path != "" {
			return s.removed(dir)
		}
		return nil
	}
	if err != nil {
		return err
	}
	now := map[string]bool{}
	sort.Strings(names)
	for _, n := range names {
		if strings.HasPrefix(n, ".deplo-replica-") {
			continue
		}
		k := Key{dir.Root, n}
		if dir.Path != "" {
			k.Path = dir.Path + "/" + n
		}
		e, ok, err := r.stat(k.Path)
		if errors.Is(err, os.ErrNotExist) || (err == nil && !ok) {
			continue
		}
		if err != nil {
			return err
		}
		now[n] = true
		prev, had := s.held[k]
		if had && prev.Same(e) {
			continue
		}
		if had && prev.Kind != e.Kind {
			if err := s.removed(k); err != nil {
				return err
			}
		}
		if err := s.send(k, e); err != nil {
			return err
		}
		if e.Kind == kindDir && (!had || prev.Kind != kindDir) {
			// A new directory may already hold files: everything in it travels now, and it is watched from here.
			if err := s.watch.add(r, k); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := s.look(k); err != nil {
				return err
			}
		}
	}
	for name := range s.kids[dir] {
		if !now[name] {
			k := Key{dir.Root, name}
			if dir.Path != "" {
				k.Path = dir.Path + "/" + name
			}
			if err := s.removed(k); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sender) removed(k Key) error {
	if _, ok := s.held[k]; !ok {
		return nil
	}
	s.forget(k)
	return s.w.head(opDelete, k)
}

// everything looks at every directory the receiver holds: after a lost event, or on a timer without watches.
func (s *sender) everything() error {
	dirs := []Key{}
	for i := range s.roots {
		dirs = append(dirs, Key{uint8(i), ""})
	}
	for k, e := range s.held {
		if e.Kind == kindDir {
			dirs = append(dirs, k)
		}
	}
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i].Path) < len(dirs[j].Path) })
	for _, d := range dirs {
		if err := s.look(d); err != nil {
			return err
		}
	}
	return nil
}

func (s *sender) follow(ctx context.Context) error {
	beat := time.NewTicker(BeatEvery)
	defer beat.Stop()
	rescan := time.NewTicker(rescanEvery)
	defer rescan.Stop()
	pending := map[Key]bool{}
	var first time.Time
	quiet := time.NewTimer(time.Hour)
	defer quiet.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k := <-s.watch.dirty:
			if len(pending) == 0 {
				first = time.Now()
			}
			pending[k] = true
			wait := settleFor
			if left := settleMax - time.Since(first); left < wait {
				wait = max(left, 0)
			}
			quiet.Reset(wait)
			continue
		case <-quiet.C:
			dirs := make([]Key, 0, len(pending))
			for k := range pending {
				dirs = append(dirs, k)
			}
			clear(pending)
			sort.Slice(dirs, func(i, j int) bool { return len(dirs[i].Path) < len(dirs[j].Path) })
			for _, d := range dirs {
				if _, ok := s.held[d]; !ok && d.Path != "" {
					continue
				}
				if err := s.look(d); err != nil {
					return err
				}
			}
		case <-s.watch.over:
			if err := s.everything(); err != nil {
				return err
			}
		case <-rescan.C:
			if !s.watch.degraded() {
				continue
			}
			if err := s.everything(); err != nil {
				return err
			}
		case t := <-beat.C:
			if err := s.w.byte(opBeat); err != nil {
				return err
			}
			if err := s.w.varint(t.UnixNano()); err != nil {
				return err
			}
		}
		if err := s.w.flush(); err != nil {
			return err
		}
	}
}
