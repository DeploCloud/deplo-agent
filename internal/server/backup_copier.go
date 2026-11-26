package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// The proxy drops a paused App's routes whenever it reads containers, and reads them when one starts or stops.
// So a backup's copier starts before the pause and stops after the App runs again: nothing starts in between.
const copierLabel = "deplo.copier=1"

// copierSettle is how long the App runs on after the copier starts: the proxy's read of that start must end first.
var copierSettle = 3 * time.Second

type volumeCopier struct {
	id   string
	dirs map[string]string
	// stage is the folder on this server's disk the volumes are copied to while the App is paused, mounted at
	// /stage; staged says they were, and the archive is then read from there with the App running.
	stage  string
	staged bool
	// told is a pass started before the pause, waiting in the copier; cgroup is the copier's, for its CPU weight.
	told   bool
	cgroup string
}

// startCopier runs one helper that mounts every volume read-only and waits; nil when it cannot, and the copy
// then runs a helper per volume as before. stage, when set, is mounted for a copy on the same disk.
func startCopier(ctx context.Context, vols []string, stage string) *volumeCopier {
	c := &volumeCopier{dirs: map[string]string{}, stage: stage}
	args := []string{"-d", "--network", "none", "--label", copierLabel}
	for i, v := range vols {
		if v == "" || validateVolumeName(v) != nil || c.dirs[v] != "" {
			continue
		}
		c.dirs[v] = fmt.Sprintf("/v/%d", i)
		args = append(args, "-v", v+":"+c.dirs[v]+":ro")
	}
	if len(c.dirs) == 0 {
		removeStage(stage)
		return nil
	}
	if exe, err := captureExe(); stage != "" && err == nil {
		args = append(args, "-v", stage+":/stage", "-v", exe+":/deplo-agent:ro")
	} else {
		removeStage(stage)
		c.stage = ""
	}
	args = append(args, volumeHelperImage, "sleep", strconv.Itoa(int(backupStepTimeout.Seconds())))
	res, err := dockercli.Run(ctx, holdStepTimeout, volumeHelperRun(ctx, args...)...)
	if c.id = strings.TrimSpace(res.Stdout); err != nil || res.Code != 0 || c.id == "" {
		removeStage(stage)
		return nil
	}
	return c
}

// roomToStage says why the volumes cannot be copied on this server's disk first, "" when they can: what they
// hold, a fifth more for what is written meanwhile, and the archive too when it is written here, must leave a
// tenth of the disk free.
func (c *volumeCopier) roomToStage(ctx context.Context, archiveHere bool) string {
	if c == nil || c.stage == "" {
		return "the server could not set up a copy on its own disk first"
	}
	res, err := dockercli.Run(ctx, holdStepTimeout, "exec", c.id, "du", "-sk", "/v")
	kb, perr := strconv.ParseInt(strings.Fields(res.Stdout + " x")[0], 10, 64)
	if err != nil || res.Code != 0 || perr != nil {
		return "the server could not measure the data to copy it on its own disk first"
	}
	need := kb*1024 + kb*1024/5
	if archiveHere {
		need += kb * 1024
	}
	if free, total := stageSpace(c.stage); free-need < total/10 {
		return fmt.Sprintf("the server has %s free, too little to copy %s of data on its own disk first and keep a tenth of it free", humanBytes(free), humanBytes(kb*1024))
	}
	return ""
}

// copyToStage brings the copy on this server's disk up to date with every volume: all of it the first time, then
// only what changed since. It says how many bytes it copied.
func (c *volumeCopier) copyToStage(ctx context.Context, since time.Time) (int64, error) {
	var copied int64
	for _, dir := range c.dirs {
		argv := []string{"exec", c.id, "/deplo-agent", "stage-sync"}
		if !since.IsZero() {
			argv = append(argv, "--since", strconv.FormatInt(since.UnixNano(), 10))
		}
		res, err := dockercli.Run(ctx, backupStepTimeout, append(argv, dir, "/stage/"+strings.TrimPrefix(dir, "/v/"))...)
		if err == nil && res.Code != 0 {
			err = fmt.Errorf("%s", strings.TrimSpace(res.Stderr))
		}
		var files, n int64
		if err == nil {
			if _, serr := fmt.Sscan(res.Stdout, &files, &n); serr != nil {
				err = fmt.Errorf("the copy said %q", strings.TrimSpace(res.Stdout))
			}
		}
		if err != nil {
			c.clearStage(ctx)
			return 0, err
		}
		copied += n
	}
	return copied, nil
}

// readyPass starts, before the pause, the pass the App is paused for: no process starts inside the pause then
// (c49: past the 1 s on a busy one-core member). False when it could not; the pass then runs as before.
func (c *volumeCopier) readyPass(ctx context.Context) bool {
	if c == nil || c.stage == "" {
		return false
	}
	argv := []string{"exec", "-d", c.id, "/deplo-agent", "stage-sync", "--wait", "/stage"}
	for _, dir := range c.dirs {
		argv = append(argv, dir, "/stage/"+strings.TrimPrefix(dir, "/v/"))
	}
	if res, err := dockercli.Run(ctx, holdStepTimeout, argv...); err != nil || res.Code != 0 {
		return false
	}
	for end := time.Now().Add(holdStepTimeout); time.Now().Before(end) && ctx.Err() == nil; time.Sleep(5 * time.Millisecond) {
		if _, err := os.Stat(filepath.Join(c.stage, ".ready")); err == nil {
			c.told = true
			c.cgroup, _ = containerCgroup(ctx, c.id)
			return true
		}
	}
	return false
}

// tellPass runs the pass readyPass started, at the agent's weight on the CPU: what changed since since. It says
// the files and bytes it copied and how long the copy itself took.
func (c *volumeCopier) tellPass(ctx context.Context, since time.Time) (files, bytes int64, took time.Duration, err error) {
	defer copierFirst(c.cgroup)()
	if err := renameInto(filepath.Join(c.stage, ".go"), strconv.FormatInt(since.UnixNano(), 10)); err != nil {
		return 0, 0, 0, err
	}
	for ; ctx.Err() == nil; time.Sleep(2 * time.Millisecond) {
		raw, rerr := os.ReadFile(filepath.Join(c.stage, ".done"))
		if rerr != nil {
			continue
		}
		var ns int64
		if _, serr := fmt.Sscan(string(raw), &files, &bytes, &ns); serr != nil {
			c.clearStage(ctx)
			return 0, 0, 0, fmt.Errorf("the copy said %q", strings.TrimSpace(string(raw)))
		}
		return files, bytes, time.Duration(ns), nil
	}
	return 0, 0, 0, ctx.Err()
}

// clearStage stops whatever still copies inside the copier, which wrote the copy as root, and removes the copy
// from there, so the disk is free at once.
func (c *volumeCopier) clearStage(ctx context.Context) {
	if c.stage == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), holdStepTimeout)
	defer cancel()
	argv := []string{"exec", c.id, "sh", "-c", `kill -9 -1 2>/dev/null; rm -rf "$@"`, "sh"}
	for _, dir := range c.dirs {
		argv = append(argv, "/stage/"+strings.TrimPrefix(dir, "/v/"))
	}
	_, _ = dockercli.Run(ctx, holdStepTimeout, argv...)
}

// tarArgs is the command that writes vol as a tar stream to stdout.
func (c *volumeCopier) tarArgs(ctx context.Context, vol string) []string {
	if c != nil && c.dirs[vol] != "" {
		dir := c.dirs[vol]
		if c.staged {
			dir = "/stage/" + strings.TrimPrefix(dir, "/v/")
		}
		return []string{"exec", c.id, "tar", "-C", dir, "-cf", "-", "."}
	}
	return volumeHelperRun(ctx, "-v", vol+":/v:ro", volumeHelperImage, "tar", "-C", "/v", "-cf", "-", ".")
}

func (c *volumeCopier) remove() {
	if c == nil || c.id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), holdStepTimeout)
	defer cancel()
	c.clearStage(ctx)
	_, _ = dockercli.Run(ctx, holdStepTimeout, "rm", "-f", c.id)
	c.id = ""
	removeStage(c.stage)
}

// backupStageDir holds the copies a backup makes on this server's disk while its App is paused.
func (s *Service) backupStageDir() string { return filepath.Join(s.dataBase, "backup-copies") }

// newStage is an empty folder for one backup's copy; "" when none can be made.
func (s *Service) newStage() string {
	if err := os.MkdirAll(s.backupStageDir(), 0o700); err != nil {
		return ""
	}
	dir, err := os.MkdirTemp(s.backupStageDir(), "copy-")
	if err != nil {
		return ""
	}
	return dir
}

func removeStage(dir string) {
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}

// stageSpace is the free and whole space where a stage goes; a var so tests can fill the disk.
var stageSpace = storeFreeBytes

// removeCopiers removes what an earlier run of this agent left; its stop makes the proxy read containers again.
// The copies those left on disk go with them.
func (s *Service) removeCopiers() {
	left, _ := filepath.Glob(filepath.Join(s.backupStageDir(), "copy-*"))
	defer func() {
		for _, dir := range left {
			removeStage(dir)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res, err := dockercli.Run(ctx, holdStepTimeout, "ps", "-aq", "--filter", "label="+copierLabel)
	if err != nil {
		return
	}
	for _, id := range strings.Fields(res.Stdout) {
		_, _ = dockercli.Run(ctx, holdStepTimeout, "rm", "-f", id)
	}
}
