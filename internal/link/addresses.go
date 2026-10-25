package link

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
