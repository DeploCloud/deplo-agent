package server

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
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
	err := s.writeProjectArchive(context.Background(), &pb.ProjectDescriptor{Slug: "agent-test-web", VolumeNames: []string{"deplo-agent-test-web-data"}}, tw, e)
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
	i, k := strings.Index(got, "pause"), strings.LastIndex(got, "unpause")
	j := strings.Index(got[max(i, 0):], "run") + max(i, 0)
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
	if got := argvVerbs(argv()); strings.Contains(got[strings.Index(got, "pause"):], "run") {
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
	needsRealDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	// Names no other run and nothing real can hold: nothing is removed before they are created.
	tag := fmt.Sprintf("deplo-agent-test-hold-%d-%d", os.Getpid(), time.Now().UnixNano())
	vol, ctr := tag+"-vol", tag+"-writer"
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
	if err := svc.writeProjectArchive(ctx, &pb.ProjectDescriptor{Slug: tag, VolumeNames: []string{vol}}, tw, e); err != nil {
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

// The proxy drops a paused App's routes whenever it reads containers, which it does when one starts or stops.
// So the copy starts before the pause and stops after the App runs again, and the copier itself is never paused.
func TestVolumeBackup_copierStartsBeforeThePauseAndStopsAfter(t *testing.T) {
	argv := dockerOnPath(t, `case "$1" in
ps) echo abc123def456; echo cop999 ;;
run) case "$*" in *" -d "*) echo cop999 ;; esac ;;
esac`)
	s := &Service{dataBase: t.TempDir()}
	if err, _ := archiveWith(t, s); err != nil {
		t.Fatal(err)
	}
	lines := argv()
	at := func(pred func(string) bool) int {
		for i, l := range lines {
			if pred(l) {
				return i
			}
		}
		return -1
	}
	start := at(func(l string) bool { return strings.HasPrefix(l, "run ") && strings.Contains(l, " -d ") })
	pause := at(func(l string) bool { return l == "pause abc123def456" })
	copy := at(func(l string) bool { return strings.HasPrefix(l, "exec ") && strings.Contains(l, " cop999 tar ") })
	unpause := at(func(l string) bool { return l == "unpause abc123def456" })
	stop := at(func(l string) bool { return l == "rm -f cop999" })
	if start < 0 || !(start < pause && pause < copy && copy < unpause && unpause < stop) {
		t.Fatalf("want the copier started, the App paused, the copy, the App unpaused, the copier gone; got %q", lines)
	}
	if at(func(l string) bool { return l == "pause cop999" }) >= 0 {
		t.Fatalf("the copier mounts the volume too, but is never paused: %q", lines)
	}
	for _, l := range lines[pause:unpause] {
		if strings.HasPrefix(l, "run ") {
			t.Fatalf("no helper starts or stops while the App is paused: %q", lines)
		}
	}
}

func TestVolumeBackup_copierGoesWhenTheCopyFails(t *testing.T) {
	argv := dockerOnPath(t, `case "$1" in
ps) echo abc123def456 ;;
run) case "$*" in *" -d "*) echo cop999 ;; esac ;;
exec) exit 2 ;;
esac`)
	s := &Service{dataBase: t.TempDir()}
	if err, _ := archiveWith(t, s); err == nil {
		t.Fatal("a failed copy of a paused app is an error")
	}
	got := strings.Join(argv(), "\n")
	if !strings.Contains(got, "unpause abc123def456") || !strings.Contains(got, "rm -f cop999") {
		t.Fatalf("the App runs again and the copier goes: %q", got)
	}
}

// eu, 23 Nov 2026: a relayed backup's stream broke and lab-web stayed paused 4 minutes, its tar blocked. The copy
// a reader gave up on is stopped, and the App runs again at once, not at the step's 30 minutes.
func TestVolumeBackup_aCopyItsReaderLeftEndsAndTheAppRunsAgain(t *testing.T) {
	argv := dockerOnPath(t, `case "$1" in
ps) echo abc123def456 ;;
run) case "$*" in *" -d "*) ;; *) exec yes ;; esac ;;
esac`)
	s := &Service{dataBase: t.TempDir()}
	done := make(chan error, 1)
	go func() {
		err, _ := archiveWith(t, s)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a copy that is no tar stream is an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the copy still holds the App paused after its reader gave up")
	}
	got := strings.Join(argv(), "\n")
	if !strings.Contains(got, "unpause abc123def456") || !strings.Contains(got, "rm -f deplo-helper-") {
		t.Fatalf("the App must run again and its copy stop: %q", argv())
	}
}

// A stream that stops taking the artifact frees what produces it, so a hold behind it ends.
func TestWriteArtifact_aStalledStreamFreesTheCopy(t *testing.T) {
	was := streamStallAfter
	streamStallAfter = 100 * time.Millisecond
	t.Cleanup(func() { streamStallAfter = was })
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	dest := &artifactDestination{stream: func([]byte) error { <-block; return nil }}
	produced := make(chan error, 1)
	go func() {
		_, _ = (&Service{}).writeArtifact(context.Background(), dest, func(w io.Writer) error {
			for {
				if _, err := w.Write(make([]byte, 64<<10)); err != nil {
					produced <- err
					return err
				}
			}
		})
	}()
	select {
	case <-produced:
	case <-time.After(5 * time.Second):
		t.Fatal("the copy is still held by a stream that stopped taking it")
	}
}
