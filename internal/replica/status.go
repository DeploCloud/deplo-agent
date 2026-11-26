package replica

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Status is how one receiver stands, for the control plane to show.
type Status struct {
	mu sync.Mutex
	// Connected is a stream open to the sender now.
	Connected bool
	// Synced is set once this stream's whole snapshot landed: the copy is current. Ready says it can be promoted.
	Synced bool
	// AppliedAt is when the last change or beat arrived.
	AppliedAt time.Time
	// Bytes is the file data that crossed the network; what the copy already held is copied here, not counted.
	Bytes int64
	Err   string
}

// Snapshot copies the status out for reading.
func (s *Status) Snapshot() (connected, synced bool, appliedAt time.Time, bytes int64, err string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Connected, s.Synced, s.AppliedAt, s.Bytes, s.Err
}

// Fail records why the last stream ended, and that none is open.
func (s *Status) Fail(msg string) {
	s.set(func(s *Status) { s.Connected, s.Err = false, msg })
}

func (s *Status) set(f func(*Status)) {
	s.mu.Lock()
	f(s)
	s.mu.Unlock()
}

// readyFile marks a copy that once held a whole snapshot; a copy without it is never promoted.
const readyFile = ".deplo-replica-ready"

// Ready says whether the copy under dir has held a whole snapshot.
func Ready(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, readyFile))
	return err == nil
}

// rootDir names a root's folder in a copy: a volume's name, or "files".
func rootDir(name string) string {
	if name == FilesRoot {
		return "files"
	}
	return "volume-" + strings.TrimPrefix(name, "volume:")
}

// FilesRoot is the root name of an App's Files.
const FilesRoot = "files"

// VolumeRoot is the root name of one of its volumes.
func VolumeRoot(volume string) string { return "volume:" + volume }
