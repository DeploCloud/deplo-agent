package server

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// holdDocker lists one container; run (the tar helper) and pause exit with the given codes.
func holdDocker(runCode, pauseCode string) string {
	return `case "$1" in
ps) echo abc123def456 ;;
run) exit ` + runCode + ` ;;
pause) echo "cannot pause" >&2; exit ` + pauseCode + ` ;;
esac`
}

func archiveWith(t *testing.T, s *Service) (error, []string) {
	t.Helper()
	var logs []string
	e := &bkEmitter{send: func(ev *pb.BackupEvent) error {
		if l := ev.GetLog(); l != nil {
			logs = append(logs, l.GetText())
		}
		return nil
	}}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := s.writeProjectArchive(context.Background(), &pb.ProjectDescriptor{Slug: "web", VolumeNames: []string{"deplo-web-data"}}, tw, e)
	if e.held > 0 {
		logs = append(logs, "held")
	}
	return err, logs
}

func argvVerbs(lines []string) string {
	var v []string
	for _, l := range lines {
		if f := strings.Fields(l); len(f) > 0 {
			v = append(v, f[0])
		}
	}
	return strings.Join(v, " ")
}

func TestVolumeBackup_pausesWhileCopyingThenResumes(t *testing.T) {
	argv := dockerOnPath(t, holdDocker("0", "0"))
	s := &Service{dataBase: t.TempDir()}
	err, logs := archiveWith(t, s)
	if err != nil {
		t.Fatal(err)
	}
	got := argvVerbs(argv())
	i, j, k := strings.Index(got, "pause"), strings.Index(got, "run"), strings.LastIndex(got, "unpause")
	if i < 0 || !(i < j && j < k) {
		t.Fatalf("want pause, then the copy, then unpause; got %q", got)
	}
	if !strings.Contains(strings.Join(logs, "|"), "held") {
		t.Fatalf("the result must carry how long the app was held: %v", logs)
	}
	if left, _ := filepath.Glob(filepath.Join(s.holdJournalDir(), "*")); len(left) != 0 {
		t.Fatalf("journal must be gone after a clean release: %v", left)
	}
	if s.HeldForBackup("abc123def456") {
		t.Fatal("no container is held once the copy ended")
	}
}

func TestVolumeBackup_resumesWhenTheCopyFails(t *testing.T) {
	argv := dockerOnPath(t, holdDocker("2", "0"))
	s := &Service{dataBase: t.TempDir()}
	err, _ := archiveWith(t, s)
	if err == nil {
		t.Fatal("a failed copy of a paused app is an error, not a warning")
	}
	if !strings.Contains(argvVerbs(argv()), "unpause") {
		t.Fatalf("the app must come back after a failed copy: %q", argvVerbs(argv()))
	}
}

func TestVolumeBackup_pauseRefusedFailsTheBackup(t *testing.T) {
	argv := dockerOnPath(t, holdDocker("0", "1"))
	s := &Service{dataBase: t.TempDir()}
	err, _ := archiveWith(t, s)
	if err == nil || !strings.Contains(err.Error(), "pause") {
		t.Fatalf("want a clear pause error, got %v", err)
	}
	if strings.Contains(argvVerbs(argv()), "run") {
		t.Fatal("never copy a volume that could not be held")
	}
}

func TestVolumeBackup_noRunningContainerNoHold(t *testing.T) {
	argv := dockerOnPath(t, "case \"$1\" in run) exit 0;; esac")
	s := &Service{dataBase: t.TempDir()}
	err, logs := archiveWith(t, s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(argvVerbs(argv()), "pause") || strings.Contains(strings.Join(logs, "|"), "held") {
		t.Fatal("nothing to hold, so nothing is paused or reported")
	}
}

func TestSweepBackupHolds_resumesWhatARestartLeftPaused(t *testing.T) {
	argv := dockerOnPath(t, "exit 0")
	dir := filepath.Join(t.TempDir(), "backup-holds")
	mustWrite(t, filepath.Join(dir, "a.json"), `["abc123def456","fff"]`)
	sweepBackupHolds(dir)
	if got := strings.Join(argv(), "\n"); !strings.Contains(got, "unpause abc123def456") || !strings.Contains(got, "unpause fff") {
		t.Fatalf("both containers must be unpaused: %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.json")); !os.IsNotExist(err) {
		t.Fatal("journal must be removed once resumed")
	}
}

func TestHeldForBackup_whileHeld(t *testing.T) {
	s := &Service{}
	s.markHeld("abc123def456", true)
	if !s.HeldForBackup("abc123") || s.HeldForBackup("") || s.HeldForBackup("zzz") {
		t.Fatal("a held container is matched by id prefix, and nothing else")
	}
}

// A writer that never stops must not be copied mid-write, and must be writing again afterwards.
func TestE2E_VolumeBackupPausesAWritingApp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	vol, ctr := "agentlink-181-vol", "agentlink-181-writer"
	_, _ = dockercli.Run(ctx, 10*time.Second, "rm", "-f", ctr)
	_, _ = dockercli.Run(ctx, 10*time.Second, "volume", "rm", "-f", vol)
	if res, err := dockercli.Run(ctx, 20*time.Second, "volume", "create", vol); err != nil || res.Code != 0 {
		t.Skipf("cannot create volume (%v / %s)", err, res.Stderr)
	}
	defer dockercli.Run(context.Background(), 15*time.Second, "volume", "rm", "-f", vol)
	defer dockercli.Run(context.Background(), 15*time.Second, "rm", "-f", ctr)
	ensureVolumeHelperImage(ctx)
	if res, err := dockercli.Run(ctx, 30*time.Second, "run", "-d", "--name", ctr, "-v", vol+":/v", volumeHelperImage,
		"sh", "-c", "head -c 3000000 /dev/zero > /v/big; while true; do echo tick >> /v/big; done"); err != nil || res.Code != 0 {
		t.Fatalf("start writer: %v / %s", err, res.Stderr)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	e := &bkEmitter{send: func(*pb.BackupEvent) error { return nil }}
	if err := svc.writeProjectArchive(ctx, &pb.ProjectDescriptor{Slug: "agentlink-181", VolumeNames: []string{vol}}, tw, e); err != nil {
		t.Fatalf("a held copy of a busy writer must succeed: %v", err)
	}
	if e.held <= 0 {
		t.Fatal("the app was written to, so it must have been held")
	}
	res, _ := dockercli.Run(ctx, 10*time.Second, "inspect", "-f", "{{.State.Paused}} {{.State.Running}}", ctr)
	if strings.TrimSpace(res.Stdout) != "false true" {
		t.Fatalf("the app must be running again, got %q", res.Stdout)
	}
	tr := tar.NewReader(&buf)
	for {
		h, err := tr.Next()
		if err != nil {
			t.Fatalf("archive has no big file: %v", err)
		}
		if strings.HasSuffix(h.Name, "/big") {
			n, err := io.Copy(io.Discard, tr)
			if err != nil || n != h.Size {
				t.Fatalf("file copied while changing: header %d, read %d, %v", h.Size, n, err)
			}
			return
		}
	}
}
