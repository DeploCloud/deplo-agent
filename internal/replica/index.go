package replica

import (
	"slices"
	"sync"
)

// Index remembers the chunks of each file a copy holds between two streams, so a reconnect reads none of them
// again. A file whose size or time moved since is read once more; a nil Index reads every file.
type Index struct {
	mu    sync.Mutex
	names []string
	files map[Key]indexed
}

type indexed struct {
	size, mtime int64
	chunks      []chunk
}

// NewIndex is an empty one, for one App's copy.
func NewIndex() *Index { return &Index{files: map[Key]indexed{}} }

func (x *Index) lookup(names []string, k Key, e Entry) ([]chunk, bool) {
	if x == nil {
		return nil, false
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if !slices.Equal(x.names, names) {
		return nil, false
	}
	f, ok := x.files[k]
	if !ok || f.size != e.Size || f.mtime != e.MTime {
		return nil, false
	}
	return f.chunks, true
}

// keep replaces what the index holds with the files a stream left the copy holding.
func (x *Index) keep(names []string, held map[Key]Entry) {
	if x == nil {
		return
	}
	files := make(map[Key]indexed, len(held))
	for k, e := range held {
		if e.Kind == kindFile && e.Chunks != nil {
			files[k] = indexed{e.Size, e.MTime, e.Chunks}
		}
	}
	x.mu.Lock()
	x.names, x.files = slices.Clone(names), files
	x.mu.Unlock()
}

// reset forgets everything: a file rebuilt wrong means the copy is not what the index says.
func (x *Index) reset() {
	if x == nil {
		return
	}
	x.mu.Lock()
	x.names, x.files = nil, map[Key]indexed{}
	x.mu.Unlock()
}
