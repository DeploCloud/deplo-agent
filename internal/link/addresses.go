package link

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Addresses are the panel addresses this agent links to: at most a primary and the panel's fallback.
type Addresses struct {
	Primary  string `json:"primary"`
	Fallback string `json:"fallback,omitempty"`
}

// AddressesPath is the file under the agent dir that holds the saved pair.
func AddressesPath(agentDir string) string {
	return filepath.Join(agentDir, "link-addresses.json")
}

// LoadAddresses reads the saved pair; an agent that has none yet gets the zero pair.
func LoadAddresses(agentDir string) (Addresses, error) {
	raw, err := os.ReadFile(AddressesPath(agentDir))
	if errors.Is(err, fs.ErrNotExist) {
		return Addresses{}, nil
	}
	if err != nil {
		return Addresses{}, err
	}
	var a Addresses
	if err := json.Unmarshal(raw, &a); err != nil {
		return Addresses{}, fmt.Errorf("read %s: %w", filepath.Base(AddressesPath(agentDir)), err)
	}
	return a, nil
}

// SaveAddresses replaces the saved pair as a whole, atomically, so nothing else survives it.
func SaveAddresses(agentDir string, a Addresses) error {
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	path := AddressesPath(agentDir)
	tmp, err := os.CreateTemp(agentDir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// Store is the saved pair, shared by the dialer and the RPC that replaces it.
type Store struct {
	dir     string
	mu      sync.Mutex
	a       Addresses
	changed chan struct{}
}

// NewStore holds saved, the pair already on disk under agentDir.
func NewStore(agentDir string, saved Addresses) *Store {
	return &Store{dir: agentDir, a: saved, changed: make(chan struct{})}
}

// Current is the pair the next dial uses.
func (s *Store) Current() Addresses {
	a, _ := s.watch()
	return a
}

// watch is the pair and a channel closed once it is replaced, read together so no replacement is missed.
func (s *Store) watch() (Addresses, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.a, s.changed
}

// ErrInvalidAddresses is a pair the agent could not dial; it keeps the one it has.
var ErrInvalidAddresses = errors.New("not a pair of panel addresses")

// Replace saves a as the whole pair, the old one forgotten, and returns what it kept.
func (s *Store) Replace(a Addresses) (Addresses, error) {
	a = Addresses{Primary: strings.TrimSpace(a.Primary), Fallback: strings.TrimSpace(a.Fallback)}
	if !Dialable(a.Primary) || a.Fallback != "" && !Dialable(a.Fallback) {
		return s.Current(), fmt.Errorf("%w: %+v", ErrInvalidAddresses, a)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := SaveAddresses(s.dir, a); err != nil {
		return s.a, err
	}
	s.a = a
	close(s.changed)
	s.changed = make(chan struct{})
	return a, nil
}

// Dialable reports whether address is one the agent can link to: an http(s) URL with a host.
func Dialable(address string) bool {
	u, err := url.Parse(strings.TrimSpace(address))
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != ""
}
