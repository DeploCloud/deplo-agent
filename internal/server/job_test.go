package server

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	dir := t.TempDir()
	return New(dir+"/stacks", dir+"/tmp", dir, dir)
}

func TestTailBufKeepsTheEnd(t *testing.T) {
	b := newTailBuf(10)
	b.Write([]byte("0123456789"))
	if got := b.String(); got != "0123456789" {
		t.Fatalf("exact fit = %q, want the whole thing", got)
	}
	if b.over {
		t.Fatal("an exact fit must not report truncation")
	}

	b.Write([]byte("ABCDE"))
	got := b.String()
	if !strings.HasSuffix(got, "56789ABCDE") {
		t.Fatalf("tail = %q, want it to end with the LAST 10 bytes", got)
	}
	if !strings.Contains(got, "earlier output trimmed") {
		t.Fatalf("truncated output must say so, got %q", got)
	}
}

func TestTailBufSingleOversizedWrite(t *testing.T) {
	// One write bigger than the whole ceiling must not allocate the write:
	// only its tail survives. This is the `RUN yes` case.
	b := newTailBuf(4)
	b.Write([]byte(strings.Repeat("x", 1<<20) + "END!"))
	if !strings.HasSuffix(b.String(), "END!") {
		t.Fatalf("want the end of the giant write, got %q", b.String())
	}
	if len(b.buf) != 4 {
		t.Fatalf("retained %d bytes, want the ceiling of 4", len(b.buf))
	}
}

func TestTailBufNeverCutsARuneInHalf(t *testing.T) {
	// "ééé" is six bytes; keeping the last three lands mid-rune. The leading
	// continuation byte must be dropped rather than rendered as U+FFFD in the
	// middle of every truncated log.
	b := newTailBuf(3)
	b.Write([]byte("ééé"))
	got := b.String()
	if !utf8.ValidString(got) {
		t.Fatalf("tail is not valid UTF-8: %q", got)
	}
	if !strings.HasSuffix(got, "é") {
		t.Fatalf("tail = %q, want it to end on a whole rune", got)
	}
}

func TestStartJobRejectsBadInput(t *testing.T) {
	s := newTestService(t)
	cases := []struct {
		name string
		req  *pb.StartJobRequest
	}{
		{"no container", &pb.StartJobRequest{ProjectId: "prj_1", Command: "date"}},
		{"no project", &pb.StartJobRequest{Container: "c", Command: "date"}},
		{"no command", &pb.StartJobRequest{ProjectId: "prj_1", Container: "c", Command: "  "}},
		{"unknown shell", &pb.StartJobRequest{ProjectId: "prj_1", Container: "c", Command: "date", Shell: "zsh"}},
	}
	for _, c := range cases {
		_, err := s.StartJob(context.Background(), c.req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: err = %v, want InvalidArgument", c.name, err)
		}
	}
	if len(s.jobs) != 0 {
		t.Fatalf("a rejected request must not register a job, got %d", len(s.jobs))
	}
}

func TestStartJobRefusesBeyondTheLiveCap(t *testing.T) {
	s := newTestService(t)
	for i := 0; i < maxLiveJobs; i++ {
		s.jobs[newJobID()] = &job{startedAt: time.Now(), stdout: newTailBuf(16), stderr: newTailBuf(16)}
	}
	_, err := s.StartJob(context.Background(),
		&pb.StartJobRequest{ProjectId: "prj_1", Container: "c", Command: "date"})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("err = %v, want ResourceExhausted at the cap", err)
	}

	// A FINISHED job does not count against the cap - only live ones do.
	for _, j := range s.jobs {
		j.done = true
		break
	}
	if _, err := s.StartJob(context.Background(),
		&pb.StartJobRequest{ProjectId: "prj_1", Container: "no-such-container-deplo-test", Command: "date"}); err != nil {
		t.Fatalf("one finished job should free a slot, got %v", err)
	}
}

func TestPollJobUnknownIsNotAnError(t *testing.T) {
	s := newTestService(t)
	// "I have no record of this" must be a normal answer: the control plane
	// turns it into a `lost` run, whereas an RPC error would be indistinguishable
	// from an unreachable host, which means the opposite (keep waiting).
	resp, err := s.PollJob(context.Background(), &pb.PollJobRequest{JobId: "nope"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetFound() {
		t.Fatal("found = true for an id this agent never saw")
	}
}

func TestPollJobWithholdsOutputUntilTerminal(t *testing.T) {
	s := newTestService(t)
	j := &job{startedAt: time.Now(), stdout: newTailBuf(1 << 10), stderr: newTailBuf(1 << 10)}
	j.stdout.Write([]byte("half a log"))
	j.stderr.Write([]byte("a warning"))
	s.jobs["j1"] = j

	running, err := s.PollJob(context.Background(), &pb.PollJobRequest{JobId: "j1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !running.GetFound() || !running.GetRunning() {
		t.Fatalf("want found+running, got %+v", running)
	}
	if running.GetStdout() != "" || running.GetStderr() != "" {
		t.Fatalf("a running poll must carry no output, got %+v", running)
	}
	if running.GetFinishedAtUnix() != 0 {
		t.Fatal("finished_at must stay 0 while running")
	}

	s.finishJob(j, 3, false, "", "it failed")
	done, err := s.PollJob(context.Background(), &pb.PollJobRequest{JobId: "j1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if done.GetRunning() || done.GetExitCode() != 3 {
		t.Fatalf("want a terminal exit 3, got %+v", done)
	}
	if done.GetStdout() != "half a log" {
		t.Fatalf("stdout = %q, want the collected output", done.GetStdout())
	}
	if !strings.Contains(done.GetStderr(), "it failed") {
		t.Fatalf("stderr = %q, want the explanation appended", done.GetStderr())
	}
	if done.GetFinishedAtUnix() == 0 {
		t.Fatal("finished_at must be stamped once terminal")
	}
}

func TestFinishJobIsWriteOnce(t *testing.T) {
	s := newTestService(t)
	j := &job{startedAt: time.Now(), stdout: newTailBuf(64), stderr: newTailBuf(64)}
	// The timeout path and the natural-exit path can race; the first outcome
	// must win rather than the last one overwriting it.
	s.finishJob(j, 0, false, "", "")
	s.finishJob(j, -1, true, "", "timed out")
	if j.exitCode != 0 || j.timedOut {
		t.Fatalf("second finish overwrote the first: code=%d timedOut=%v", j.exitCode, j.timedOut)
	}
}

func TestKillJobIsIdempotent(t *testing.T) {
	s := newTestService(t)
	_, cancel := context.WithCancel(context.Background())
	j := &job{startedAt: time.Now(), stdout: newTailBuf(64), stderr: newTailBuf(64), cancel: cancel}
	s.jobs["j1"] = j

	first, err := s.KillJob(context.Background(), &pb.KillJobRequest{JobId: "j1"})
	if err != nil || !first.GetFound() {
		t.Fatalf("first kill: found=%v err=%v", first.GetFound(), err)
	}
	// Killing an unknown id, or one that already finished, answers found:false
	// rather than erroring - the Stop button races the job's natural exit.
	s.finishJob(j, -1, false, "", "stopped")
	second, err := s.KillJob(context.Background(), &pb.KillJobRequest{JobId: "j1"})
	if err != nil || second.GetFound() {
		t.Fatalf("second kill: found=%v err=%v, want found=false and no error", second.GetFound(), err)
	}
	missing, err := s.KillJob(context.Background(), &pb.KillJobRequest{JobId: "nope"})
	if err != nil || missing.GetFound() {
		t.Fatalf("unknown kill: found=%v err=%v", missing.GetFound(), err)
	}
}

func TestStartJobReturnsBeforeTouchingDocker(t *testing.T) {
	if !dockercli.Available(context.Background()) {
		t.Skip("docker not available")
	}
	s := newTestService(t)
	// The container does not exist, so the goroutine's assertOwned fails - but
	// that is the POINT: the RPC must have returned long before it found out.
	// With the checks in front of the spawn this call would take seconds.
	start := time.Now()
	resp, err := s.StartJob(context.Background(), &pb.StartJobRequest{
		ProjectId: "prj_cron_test",
		Container: "deplo-no-such-container-cron-test",
		Command:   "date",
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("StartJob took %s - the container check must be inside the goroutine", elapsed)
	}
	if resp.GetJobId() == "" {
		t.Fatal("no job id")
	}

	// It settles as a pre-spawn failure: exit -1 with a reason, never an RPC error.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		p, err := s.PollJob(context.Background(), &pb.PollJobRequest{JobId: resp.GetJobId()})
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if !p.GetRunning() {
			if p.GetExitCode() != -1 {
				t.Fatalf("exit = %d, want -1 for a command that never spawned", p.GetExitCode())
			}
			if p.GetStderr() == "" {
				t.Fatal("a pre-spawn failure must say why")
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("job never settled")
}

func TestTailBufKeepsTextAroundAStrayByte(t *testing.T) {
	b := newTailBuf(64)
	b.Write([]byte("ok\xff\xfe\xfdend\n"))
	got := b.String()
	if !utf8.ValidString(got) || !strings.HasPrefix(got, "ok") || !strings.HasSuffix(got, "end\n") {
		t.Fatalf("tail = %q, want ok...end with the stray bytes replaced", got)
	}
}
