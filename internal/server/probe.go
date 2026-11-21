package server

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

// The health check of a container the proxy must wait for: this binary, mounted read-only from the stack
// directory, so an image with no shell passes it like any other (deplo#281). The panel renders the same paths.
const (
	probeDir  = "/.deplo-probe"
	probeFile = "deplo-agent"
)

// probeProcNet is the sockets of the namespace the probe runs in; a var so tests can point it at a fixture.
var probeProcNet = "/proc/net"

// Probe is `deplo-agent probe <port>`: 0 once a TCP socket of this network namespace listens on port, as the
// shell loop it replaces did, 1 while none does, 2 for a malformed call.
func Probe(args []string) int {
	if len(args) != 1 {
		return 2
	}
	port, err := strconv.ParseUint(args[0], 10, 16)
	if err != nil || port == 0 {
		return 2
	}
	for _, f := range []string{"tcp", "tcp6"} {
		b, err := os.ReadFile(filepath.Join(probeProcNet, f))
		if err == nil && slices.ContainsFunc(parseListeners(string(b)), func(l listener) bool { return l.port == int32(port) }) {
			return 0
		}
	}
	return 1
}

// PlaceProbe puts this binary at <stack dir>/.deplo-probe/deplo-agent, the folder a container's probe mounts.
// Renamed into place, so a check that runs meanwhile reads the old binary or the new one, never half of one.
func (s *Service) PlaceProbe() error {
	exe, err := captureExe()
	if err != nil {
		return err
	}
	dir := filepath.Join(s.stackDir, filepath.Base(probeDir))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Any user of any image runs it; Docker made the folder 0755 too when a stack mounted it first.
	if err := os.Chmod(dir, 0o755); err != nil {
		return err
	}
	dst := filepath.Join(dir, probeFile)
	src, err := os.Stat(exe)
	if err != nil {
		return err
	}
	if cur, err := os.Stat(dst); err == nil && os.SameFile(src, cur) {
		return nil
	}
	tmp := dst + ".new"
	_ = os.Remove(tmp)
	// A link shares the binary's mode: only one every user may read and run is linked, any other is copied.
	if src.Mode().Perm()&0o555 != 0o555 || os.Link(exe, tmp) != nil {
		if err := copyExecutable(exe, tmp); err != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("copy the probe: %w", err)
		}
	}
	return os.Rename(tmp, dst)
}

func copyExecutable(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
