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
	if stage != "" {
		args = append(args, "-v", stage+":/stage")
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
// hold, a fifth more for what is written meanwhile, and 512 MiB must stay free.
func (c *volumeCopier) roomToStage(ctx context.Context) string {
	if c == nil || c.stage == "" {
		return "the server could not set up a copy on its own disk first"
	}
	res, err := dockercli.Run(ctx, holdStepTimeout, "exec", c.id, "du", "-sk", "/v")
	kb, perr := strconv.ParseInt(strings.Fields(res.Stdout + " x")[0], 10, 64)
	if err != nil || res.Code != 0 || perr != nil {
		return "the server could not measure the data to copy it on its own disk first"
	}
	need := kb*1024 + kb*1024/5 + 512<<20
	if free := stageFree(c.stage); free < need {
		return fmt.Sprintf("the server has %s free, too little to copy %s of data on its own disk first", humanBytes(free), humanBytes(kb*1024))
	}
	return ""
}

// copyToStage copies every volume to the stage, for a caller that holds the App paused.
func (c *volumeCopier) copyToStage(ctx context.Context) error {
	for _, dir := range c.dirs {
		to := "/stage/" + strings.TrimPrefix(dir, "/v/")
		res, err := dockercli.Run(ctx, backupStepTimeout, "exec", c.id, "sh", "-c", `mkdir -p "$2" && cp -a "$1/." "$2/"`, "sh", dir, to)
		if err == nil && res.Code != 0 {
			err = fmt.Errorf("%s", strings.TrimSpace(res.Stderr))
		}
		if err != nil {
			c.clearStage(ctx)
			return err
		}
	}
	c.staged = true
	return nil
}

// clearStage removes the copy from inside the copier, which wrote it as root, so it frees the disk at once.
func (c *volumeCopier) clearStage(ctx context.Context) {
	if c.stage == "" {
		return
	}
	argv := []string{"exec", c.id, "rm", "-rf"}
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

// stageFree is the free space where a stage goes; a var so tests can fill the disk.
var stageFree = filesystemFree

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
