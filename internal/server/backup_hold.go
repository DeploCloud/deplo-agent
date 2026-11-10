package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A volume copy pauses the containers that mount the volume, so the archive never sees a file mid-write.
// The ids go to a journal first, so a restart of this agent can still unpause them.
const holdStepTimeout = 30 * time.Second

type volumeHold struct {
	s       *Service
	ids     []string
	journal string
	start   time.Time
	once    sync.Once
	held    time.Duration
}

func (s *Service) holdJournalDir() string { return filepath.Join(s.dataBase, "backup-holds") }

// holdVolumes pauses every running container that mounts one of vols. It returns nil, nil when none does.
func (s *Service) holdVolumes(ctx context.Context, vols []string) (*volumeHold, error) {
	seen := map[string]bool{}
	var ids []string
	for _, v := range vols {
		if v == "" {
			continue
		}
		res, err := dockercli.Run(ctx, holdStepTimeout, "ps", "-q", "--no-trunc", "--filter", "status=running", "--filter", "volume="+v)
		if err != nil {
			return nil, err
		}
		for _, id := range strings.Fields(res.Stdout) {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	h := &volumeHold{s: s, ids: ids, start: time.Now()}
	if err := os.MkdirAll(s.holdJournalDir(), 0o700); err != nil {
		return nil, err
	}
	h.journal = filepath.Join(s.holdJournalDir(), time.Now().UTC().Format("20060102T150405.000000000")+".json")
	b, _ := json.Marshal(ids)
	if err := os.WriteFile(h.journal, b, 0o600); err != nil {
		return nil, err
	}
	for _, id := range ids {
		res, err := dockercli.Run(ctx, holdStepTimeout, "pause", id)
		if err == nil && res.Code == 0 {
			s.markHeld(id, true)
			continue
		}
		msg := res.Stderr
		if err != nil {
			msg = err.Error()
		}
		if strings.Contains(msg, "is not running") || strings.Contains(msg, "No such container") {
			continue
		}
		h.release()
		return nil, &holdError{msg: strings.TrimSpace(msg)}
	}
	h.start = time.Now()
	return h, nil
}

type holdError struct{ msg string }

func (e *holdError) Error() string {
	return "could not pause the app to copy its data consistently: " + e.msg
}

// release unpauses what the hold paused, retrying, and is safe to call twice. It returns how long the hold lasted.
func (h *volumeHold) release() time.Duration {
	if h == nil {
		return 0
	}
	h.once.Do(func() {
		h.held = time.Since(h.start)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 2*time.Minute)
		defer cancel()
		if unpauseAll(ctx, h.ids) {
			for _, id := range h.ids {
				h.s.markHeld(id, false)
			}
			_ = os.Remove(h.journal)
		}
	})
	return h.held
}

// unpauseAll reports whether every container is running again (one that is gone counts).
func unpauseAll(ctx context.Context, ids []string) bool {
	ok := true
	for _, id := range ids {
		var done bool
		for try := 0; try < 3 && !done; try++ {
			res, err := dockercli.Run(ctx, holdStepTimeout, "unpause", id)
			done = err == nil && res.Code == 0 ||
				strings.Contains(res.Stderr, "is not paused") || strings.Contains(res.Stderr, "No such container")
			if !done {
				time.Sleep(time.Duration(try+1) * 500 * time.Millisecond)
			}
		}
		ok = ok && done
	}
	return ok
}

// sweepBackupHolds unpauses what an earlier run of this agent paused for a copy and never released.
func sweepBackupHolds(dir string) {
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, f := range files {
		var ids []string
		if b, err := os.ReadFile(f); err == nil {
			_ = json.Unmarshal(b, &ids)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		ok := unpauseAll(ctx, ids)
		cancel()
		if ok {
			_ = os.Remove(f)
		}
	}
}

func (s *Service) markHeld(id string, on bool) {
	s.heldMu.Lock()
	defer s.heldMu.Unlock()
	if s.held == nil {
		s.held = map[string]bool{}
	}
	if on {
		s.held[id] = true
	} else {
		delete(s.held, id)
	}
}

// HeldForBackup reports whether the container is paused for a volume copy, so a health check does not count it as down.
func (s *Service) HeldForBackup(id string) bool {
	if id == "" {
		return false
	}
	s.heldMu.Lock()
	defer s.heldMu.Unlock()
	for h := range s.held {
		if strings.HasPrefix(h, id) {
			return true
		}
	}
	return false
}
