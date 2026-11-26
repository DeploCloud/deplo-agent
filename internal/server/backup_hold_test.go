package server

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	err := s.writeProjectArchive(context.Background(), &pb.ProjectDescriptor{Slug: "agent-test-web", VolumeNames: []string{"deplo-agent-test-web-data"}}, tw, e, false)
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

// A frozen App keeps its routes: Docker, and so the proxy, still read it as running (memos, 24 Nov 2026).
func TestVolumeBackup_freezesTheAppWhereTheProxyCannotSeeIt(t *testing.T) {
	argv := dockerOnPath(t, holdDocker("0", "0"))
	cg := t.TempDir()
	freeze, events := filepath.Join(cg, "cgroup.freeze"), filepath.Join(cg, "cgroup.events")
	if os.WriteFile(freeze, []byte("0\n"), 0o644) != nil || os.WriteFile(events, []byte("populated 1\nfrozen 0\n"), 0o644) != nil {
		t.Fatal("write")
	}
	containerCgroup = func(context.Context, string) (string, error) { return cg, nil }
	// The kernel's side: cgroup.events follows cgroup.freeze, and the test notes every state it saw.
	var mu sync.Mutex
	var seen []string
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			b, _ := os.ReadFile(freeze)
			v := strings.TrimSpace(string(b))
			// A plain file read while the agent rewrites it is empty for a moment; the kernel's never is (CI, c41).
			if v == "" {
				continue
			}
			_ = os.WriteFile(events, []byte("populated 1\nfrozen "+v+"\n"), 0o644)
			mu.Lock()
			if len(seen) == 0 || seen[len(seen)-1] != v {
				seen = append(seen, v)
			}
			mu.Unlock()
		}
	}()
	s := &Service{dataBase: t.TempDir()}
	if err, _ := archiveWith(t, s); err != nil {
		t.Fatal(err)
	}
	if got := argvVerbs(argv()); strings.Contains(got, "pause") {
		t.Fatalf("docker paused the app: %q", got)
	}
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(seen, " ") != "0 1 0" {
		t.Fatalf("freeze states %v, want 0 1 0", seen)
	}
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
	if err := svc.writeProjectArchive(ctx, &pb.ProjectDescriptor{Slug: tag, VolumeNames: []string{vol}}, tw, e, false); err != nil {
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

// eu, 24 Nov 2026: the copy on lab-pause2's disk took a minute before its first byte, and the backup failed as a
// stalled stream. A stream that never had to wait is not stalled, however long the copy takes to start.
func TestWriteArtifact_aSlowStartIsNoStall(t *testing.T) {
	was := streamStallAfter
	streamStallAfter = 100 * time.Millisecond
	t.Cleanup(func() { streamStallAfter = was })
	dest := &artifactDestination{stream: func([]byte) error { return nil }}
	out, err := (&Service{}).writeArtifact(context.Background(), dest, func(w io.Writer) error {
		time.Sleep(500 * time.Millisecond)
		_, err := w.Write(make([]byte, 64<<10))
		return err
	})
	if err != nil || out.size == 0 {
		t.Fatalf("a copy that starts late must still be sent: %v (%d bytes)", err, out.size)
	}
}

// The proxy reads every container when the copier starts; a pause that lands while it reads drops the App's routes
// (eu, 24 Nov 2026: memos answered 404 for 2 s). The App is paused only once that read is over.
func TestVolumeBackup_theAppIsPausedOnceTheProxyReadTheCopier(t *testing.T) {
	was := copierSettle
	copierSettle = 300 * time.Millisecond
	t.Cleanup(func() { copierSettle = was })
	dir := t.TempDir()
	dockerOnPath(t, `case "$1" in
ps) echo abc123def456 ;;
run) case "$*" in *" -d "*) date +%s%N > `+dir+`/start; echo cop999 ;; esac ;;
pause) date +%s%N > `+dir+`/pause ;;
esac`)
	if err, _ := archiveWith(t, &Service{dataBase: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	read := func(n string) int64 {
		b, _ := os.ReadFile(filepath.Join(dir, n))
		v, _ := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		return v
	}
	if gap := time.Duration(read("pause") - read("start")); gap < copierSettle {
		t.Fatalf("the App was paused %v after the copier started, before the proxy could have read it", gap)
	}
}

// stagingDocker is a copier whose volumes hold 200 MB: du, both copies and the archive all run inside it. The
// copy made while the App is paused takes 5 MB.
var stagingDocker = stagingDockerSecond("5242880")

func stagingDockerSecond(bytes string) string {
	return `case "$1" in
ps) echo abc123def456 ;;
run) case "$*" in *" -d "*) echo cop999 ;; esac ;;
exec) case "$*" in
  *" du -sk "*) printf '204800\t/v\n' ;;
  *" stage-sync --since "*) echo "3 ` + bytes + `" ;;
  *" stage-sync "*) echo "40 209715200" ;;
  esac ;;
esac`
}

func stagedFree(t *testing.T, free, total int64) {
	t.Helper()
	was := stageSpace
	stageSpace = func(string) (int64, int64) { return free, total }
	t.Cleanup(func() { stageSpace = was })
}

// eu, 24 Nov 2026: a 2 GB App was paused 20 to 44 s, its whole copy on its own disk. Now that copy is made while
// it runs, and the pause lasts only for a second pass of what changed meanwhile; the archive is read from the copy.
func TestVolumeBackup_pausedOnlyForWhatChanged(t *testing.T) {
	argv := dockerOnPath(t, stagingDocker)
	stagedFree(t, 10<<30, 20<<30)
	s := &Service{dataBase: t.TempDir()}
	e := &bkEmitter{send: func(ev *pb.BackupEvent) error { return nil }}
	if err := s.writeProjectArchive(context.Background(), &pb.ProjectDescriptor{Slug: "agent-test-web", VolumeNames: []string{"deplo-agent-test-web-data"}}, tar.NewWriter(io.Discard), e, false); err != nil {
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
	whole := at(func(l string) bool {
		return strings.HasPrefix(l, "exec cop999 /deplo-agent stage-sync /v/0 /stage/0")
	})
	pause := at(func(l string) bool { return l == "pause abc123def456" })
	changed := at(func(l string) bool {
		return strings.HasPrefix(l, "exec cop999 /deplo-agent stage-sync --since ") && strings.HasSuffix(l, " /v/0 /stage/0")
	})
	unpause := at(func(l string) bool { return l == "unpause abc123def456" })
	archive := at(func(l string) bool {
		return strings.HasPrefix(l, "exec ") && strings.Contains(l, " cop999 tar -C /stage/0 ")
	})
	gone := at(func(l string) bool { return l == "rm -f cop999" })
	if whole < 0 || !(whole < pause && pause < changed && changed < unpause && unpause < archive && archive < gone) {
		t.Fatalf("want the data copied with the App running, the App paused, what changed copied, the App unpaused, the archive read from the copy, the copier gone; got %q", lines)
	}
	if !strings.Contains(strings.Join(lines, "\n"), ":/deplo-agent:ro") {
		t.Fatalf("the copier must carry this binary to copy with: %q", lines)
	}
	if e.heldWhy != "" {
		t.Fatalf("the second copy was smaller than the first, yet the backup says %q", e.heldWhy)
	}
	if left, _ := filepath.Glob(filepath.Join(s.backupStageDir(), "*")); len(left) != 0 {
		t.Fatalf("the copy on disk must be gone once the backup is written: %v", left)
	}
}

// When the data changes as fast as it is copied, the pause is no shorter than one copy: the backup says so.
func TestVolumeBackup_aSecondCopyNoSmallerSaysSo(t *testing.T) {
	dockerOnPath(t, stagingDockerSecond("209715200"))
	stagedFree(t, 10<<30, 20<<30)
	s := &Service{dataBase: t.TempDir()}
	e := &bkEmitter{send: func(ev *pb.BackupEvent) error { return nil }}
	if err := s.writeProjectArchive(context.Background(), &pb.ProjectDescriptor{Slug: "agent-test-web", VolumeNames: []string{"deplo-agent-test-web-data"}}, tar.NewWriter(io.Discard), e, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(e.heldWhy, "no smaller than the first") {
		t.Fatalf("the backup must say its second copy was as large as the first: %q", e.heldWhy)
	}
}

// Without room for the copy and a tenth of the disk free, the App is paused for the whole copy, at most holdBudget,
// and the backup says why; an archive written on the same disk counts too.
func TestVolumeBackup_tooLittleDiskPausesForTheWholeCopyAndSaysWhy(t *testing.T) {
	for _, c := range []struct {
		name        string
		free, total int64
		archiveHere bool
	}{
		{"too little free", 100 << 20, 20 << 30, false},
		{"a tenth must stay free", 1 << 30, 8 << 30, false},
		{"the archive is written here too", 500 << 20, 1 << 30, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			argv := dockerOnPath(t, stagingDocker)
			stagedFree(t, c.free, c.total)
			s := &Service{dataBase: t.TempDir()}
			var logs []string
			e := &bkEmitter{send: func(ev *pb.BackupEvent) error {
				if l := ev.GetLog(); l != nil {
					logs = append(logs, l.GetText())
				}
				return nil
			}}
			if err := s.writeProjectArchive(context.Background(), &pb.ProjectDescriptor{Slug: "agent-test-web", VolumeNames: []string{"deplo-agent-test-web-data"}}, tar.NewWriter(io.Discard), e, c.archiveHere); err != nil {
				t.Fatal(err)
			}
			got := strings.Join(argv(), "\n")
			if strings.Contains(got, "stage-sync") || !strings.Contains(got, " cop999 tar -C /v/0 ") {
				t.Fatalf("with too little disk the archive is read from the volume itself: %q", got)
			}
			if !strings.Contains(e.heldWhy, "too little") || !strings.Contains(strings.Join(logs, "|"), "stays paused for the whole copy") {
				t.Fatalf("the backup must say why the App stayed paused: %q %v", e.heldWhy, logs)
			}
		})
	}
}

func shortHold(t *testing.T) {
	t.Helper()
	was, settle := holdBudget, copierSettle
	holdBudget, copierSettle = 300*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { holdBudget, copierSettle = was, settle })
}

// deplo c44: with eu-1 at 90%, fise was paused 4.1 s and lab-kit-a 21.5 s, each for its whole copy. Now a copy that
// outlasts holdBudget is no backup, and the App runs again once the budget is spent, not once the copy ends.
func TestVolumeBackup_aWholeCopyLongerThanTheBudgetRunsTheAppAgainAndIsNoBackup(t *testing.T) {
	shortHold(t)
	dir := t.TempDir()
	argv := dockerOnPath(t, `case "$1" in
ps) echo abc123def456 ;;
run) case "$*" in *" -d "*) echo cop999 ;; esac ;;
unpause) date +%s%N > `+dir+`/unpause ;;
pause) date +%s%N > `+dir+`/pause ;;
exec) case "$*" in
  *" du -sk "*) printf '204800\t/v\n' ;;
  *" tar "*) exec sleep 3 ;;
  esac ;;
esac`)
	stagedFree(t, 100<<20, 20<<30)
	e := &bkEmitter{send: func(ev *pb.BackupEvent) error { return nil }}
	err := (&Service{dataBase: t.TempDir()}).writeProjectArchive(context.Background(), &pb.ProjectDescriptor{Slug: "agent-test-web", VolumeNames: []string{"deplo-agent-test-web-data"}}, tar.NewWriter(io.Discard), e, false)
	if err == nil || !strings.Contains(err.Error(), "could not be copied within") || !strings.Contains(err.Error(), "too little") {
		t.Fatalf("want a backup refused for its pause, with why: %v", err)
	}
	read := func(n string) int64 {
		b, _ := os.ReadFile(filepath.Join(dir, n))
		v, _ := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		return v
	}
	if held := time.Duration(read("unpause") - read("pause")); held <= 0 || held > holdBudget+500*time.Millisecond {
		t.Fatalf("the App was paused %v, its budget is %v: %q", held, holdBudget, argv())
	}
}

// What changed while the App was paused is copied within holdBudget too, or the backup is not kept.
func TestVolumeBackup_aSecondCopyLongerThanTheBudgetIsNoBackup(t *testing.T) {
	shortHold(t)
	argv := dockerOnPath(t, strings.Replace(stagingDocker, `*" stage-sync --since "*) echo "3 5242880" ;;`, `*" stage-sync --since "*) exec sleep 3 ;;`, 1))
	stagedFree(t, 10<<30, 20<<30)
	e := &bkEmitter{send: func(ev *pb.BackupEvent) error { return nil }}
	start := time.Now()
	err := (&Service{dataBase: t.TempDir()}).writeProjectArchive(context.Background(), &pb.ProjectDescriptor{Slug: "agent-test-web", VolumeNames: []string{"deplo-agent-test-web-data"}}, tar.NewWriter(io.Discard), e, false)
	if err == nil || !strings.Contains(err.Error(), "could not be copied within") {
		t.Fatalf("want a backup refused for its pause: %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the backup waited %v for a copy past its budget", took)
	}
	got := strings.Join(argv(), "\n")
	if !strings.Contains(got, "unpause abc123def456") || strings.Contains(got, "tar -C /stage/0") {
		t.Fatalf("the App runs again and the unfinished copy is never archived: %q", got)
	}
}
