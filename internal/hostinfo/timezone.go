package hostinfo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

var setMu sync.Mutex

// SetTimezone points the host's clock at an IANA zone.
func SetTimezone(ctx context.Context, tz string) error {
	zonePath, err := resolveZone(tz)
	if err != nil {
		return err
	}

	setMu.Lock()
	defer setMu.Unlock()

	if path, lookErr := exec.LookPath("timedatectl"); lookErr == nil {
		cmd := exec.CommandContext(ctx, path, "set-timezone", tz)
		out, runErr := cmd.CombinedOutput()
		if runErr == nil {
			return nil
		}
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

func resolveZone(tz string) (string, error) {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return "", fmt.Errorf("no timezone given")
	}
	joined, ok := safepath.Join(ZoneinfoDir, tz)
	if !ok {
		return "", fmt.Errorf("%q is not a valid timezone name", tz)
	}
	resolved, err := safepath.Inside(ZoneinfoDir, joined)
	if err != nil {
		return "", fmt.Errorf("%q is not a known timezone on this host", tz)
	}
	base, err := safepath.Inside(ZoneinfoDir, ".")
	if err != nil || resolved == base {
		return "", fmt.Errorf("%q is not a known timezone on this host", tz)
	}
	st, err := os.Stat(resolved)
	if err != nil || st.IsDir() {
		return "", fmt.Errorf("%q is not a known timezone on this host", tz)
	}
	return resolved, nil
}

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
	_ = os.WriteFile("/etc/timezone", []byte(tz+"\n"), 0o644)
	return nil
}

// KnownTimezone reports whether the host has a zone file for this name, without changing anything.
func KnownTimezone(tz string) bool {
	_, err := resolveZone(tz)
	return err == nil
}
