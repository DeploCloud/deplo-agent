package server

import (
	"fmt"
	"os"
	"sync"
)

// stateWritesInline has keepLater write at once, as before: the tests that read a state file back set it.
var stateWritesInline bool

// laterFile is one state file written off its caller's path, the newest content winning.
type laterFile struct {
	raw            []byte
	dirty, writing bool
}

var (
	laterMu    sync.Mutex
	laterFiles = map[string]*laterFile{}
	laterWork  sync.WaitGroup
)

// keepLater writes raw to path in the background, never holding its caller up: a sync on a busy disk took seconds
// under locks the cluster's answers, forwarders and restarts wait on (eu, 24 Nov 2026).
func keepLater(path string, raw []byte) {
	if stateWritesInline {
		writeLater(path, raw)
		return
	}
	laterMu.Lock()
	defer laterMu.Unlock()
	f := laterFiles[path]
	if f == nil {
		f = &laterFile{}
		laterFiles[path] = f
	}
	f.raw, f.dirty = raw, true
	if f.writing {
		return
	}
	f.writing = true
	laterWork.Add(1)
	go func() {
		defer laterWork.Done()
		for {
			laterMu.Lock()
			if !f.dirty {
				f.writing = false
				laterMu.Unlock()
				return
			}
			raw := f.raw
			f.dirty = false
			laterMu.Unlock()
			writeLater(path, raw)
		}
	}()
}

func writeLater(path string, raw []byte) {
	if err := writeFileAtomic(path, raw, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not keep %s: %v\n", path, err)
	}
}
