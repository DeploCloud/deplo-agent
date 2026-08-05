package hostinfo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

// SetTimezone points the host's clock at an IANA zone.
//
// The name arrives off the wire, and what it ultimately does is relink
// /etc/localtime — so it is validated HERE, not only by the control plane, and
// validated by resolution rather than by pattern: the candidate is joined onto
// the zoneinfo dir and confirmed by realpath to still be inside it. A
// "../../etc/shadow" or an absolute path is rejected by construction, and so is
// a symlink inside the zoneinfo tree that points out of it. (safepath.Inside is
// the same guard the file RPCs use to contain an off-the-wire path.)
//
// timedatectl is preferred where it exists: on a systemd host it is what owns
// /etc/localtime, and relinking behind its back leaves systemd-timedated
// reporting the old zone. The manual relink is the fallback for the hosts
// without it (Alpine, minimal containers).
func SetTimezone(ctx context.Context, tz string) error {
	zonePath, err := resolveZone(tz)
	if err != nil {
		return err
	}

	if path, lookErr := exec.LookPath("timedatectl"); lookErr == nil {
		cmd := exec.CommandContext(ctx, path, "set-timezone", tz)
		out, runErr := cmd.CombinedOutput()
		if runErr == nil {
			return nil
		}
		// Not fatal on its own: timedatectl fails in a container with no dbus
		// even where the binary exists. Fall through to the relink, and keep its
		// message so a failure of BOTH paths explains the first attempt too.
		if relinkErr := relink(zonePath, tz); relinkErr != nil {
			return fmt.Errorf(
				"could not set the timezone: timedatectl said %q, and relinking /etc/localtime failed: %w",
				strings.TrimSpace(string(out)), relinkErr,
			)
		}
		return nil
	}
	return relink(zonePath, tz)
}

// resolveZone maps an IANA name onto its file under ZoneinfoDir, refusing
// anything that escapes that directory or does not exist.
func resolveZone(tz string) (string, error) {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return "", fmt.Errorf("no timezone given")
	}
	// Lexical guard first (absolute paths and ".." never reach the filesystem),
	// then the realpath guard, which is what actually defeats a symlink escape.
	joined, ok := safepath.Join(ZoneinfoDir, tz)
	if !ok {
		return "", fmt.Errorf("%q is not a valid timezone name", tz)
	}
	resolved, err := safepath.Inside(ZoneinfoDir, joined)
	if err != nil {
		return "", fmt.Errorf("%q is not a known timezone on this host", tz)
	}
	// Inside falls back to the canonical base on any escape or missing target,
	// so a result equal to the base means "did not resolve to a zone file".
	base, err := safepath.Inside(ZoneinfoDir, ".")
	if err != nil || resolved == base {
		return "", fmt.Errorf("%q is not a known timezone on this host", tz)
	}
	st, err := os.Stat(resolved)
	if err != nil || st.IsDir() {
		// A directory IS a valid path under zoneinfo ("Europe") but not a zone.
		return "", fmt.Errorf("%q is not a known timezone on this host", tz)
	}
	return resolved, nil
}

// relink replaces /etc/localtime with a symlink to the zone file and writes
// /etc/timezone, the two places readTimezone looks. The symlink is created
// alongside and renamed over the target so a reader never sees a host with no
// /etc/localtime at all.
func relink(zonePath, tz string) error {
	tmp := filepath.Join(filepath.Dir("/etc/localtime"), ".deplo-localtime.tmp")
	_ = os.Remove(tmp)
	if err := os.Symlink(zonePath, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, "/etc/localtime"); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// Best-effort: Debian reads this, systemd hosts ignore it, and a read-only
	// /etc must not turn a successful relink into a failure.
	_ = os.WriteFile("/etc/timezone", []byte(tz+"\n"), 0o644)
	return nil
}

// KnownTimezone reports whether the host has a zone file for this name, without
// changing anything. Used to reject a bad name before any side effect.
func KnownTimezone(tz string) bool {
	_, err := resolveZone(tz)
	return err == nil
}
