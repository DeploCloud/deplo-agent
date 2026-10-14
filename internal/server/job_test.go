package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
		{"bad env name", &pb.StartJobRequest{ProjectId: "prj_1", Container: "c", Command: "date",
			Env: []*pb.CronEnvVar{{Name: "BAD-NAME", Value: "x"}}}},
		{"marker env name", &pb.StartJobRequest{ProjectId: "prj_1", Container: "c", Command: "date",
			Env: []*pb.CronEnvVar{{Name: jobMarkerEnv, Value: "x"}}}},
		{"multi-line value", &pb.StartJobRequest{ProjectId: "prj_1", Container: "c", Command: "date",
			Env: []*pb.CronEnvVar{{Name: "KEY", Value: "a\nb"}}}},
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
		s.jobs[newJobID()] = &job{startedAt: time.Now(), stdout: dockercli.NewTailBuf(16), stderr: dockercli.NewTailBuf(16)}
	}
	_, err := s.StartJob(context.Background(),
		&pb.StartJobRequest{ProjectId: "prj_1", Container: "c", Command: "date"})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("err = %v, want ResourceExhausted at the cap", err)
	}

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
	j := &job{startedAt: time.Now(), stdout: dockercli.NewTailBuf(1 << 10), stderr: dockercli.NewTailBuf(1 << 10)}
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
	j := &job{startedAt: time.Now(), stdout: dockercli.NewTailBuf(64), stderr: dockercli.NewTailBuf(64)}
	s.finishJob(j, 0, false, "", "")
	s.finishJob(j, -1, true, "", "timed out")
	if j.exitCode != 0 || j.timedOut {
		t.Fatalf("second finish overwrote the first: code=%d timedOut=%v", j.exitCode, j.timedOut)
	}
}

func TestKillJobIsIdempotent(t *testing.T) {
	s := newTestService(t)
	_, cancel := context.WithCancel(context.Background())
	j := &job{startedAt: time.Now(), stdout: dockercli.NewTailBuf(64), stderr: dockercli.NewTailBuf(64), cancel: cancel}
	s.jobs["j1"] = j

	first, err := s.KillJob(context.Background(), &pb.KillJobRequest{JobId: "j1"})
	if err != nil || !first.GetFound() {
		t.Fatalf("first kill: found=%v err=%v", first.GetFound(), err)
	}
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

// A job's variables reach the container through a 0600 env-file, never the root docker client's
// own environment, where LD_PRELOAD or DOCKER_HOST would configure the client itself.
func TestJobEnvNeverReachesTheDockerClient(t *testing.T) {
	out := t.TempDir()
	t.Setenv("TMPDIR", t.TempDir())
	bin := t.TempDir()
	script := "#!/bin/sh\n[ \"$1\" = inspect ] && { echo prj_deplo-agent-test; exit 0; }\n" +
		"case \" $* \" in *\" -e " + jobMarkerEnv + " \"*) ;; *) exit 0;; esac\n" +
		"env > " + out + "/env; printf '%s\\n' \"$@\" > " + out + "/args\n" +
		"while [ $# -gt 0 ]; do [ \"$1\" = --env-file ] && cp \"$2\" " + out + "/envfile; shift; done\n"
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	s := newTestService(t)
	resp, err := s.StartJob(context.Background(), &pb.StartJobRequest{
		ProjectId: "prj_deplo-agent-test", Container: "deplo-agent-test-cron", Shell: "sh", Command: "date",
		Env: []*pb.CronEnvVar{{Name: "API_KEY", Value: "s3cret value"}, {Name: "LD_PRELOAD", Value: "/x/evil.so"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		p, _ := s.PollJob(context.Background(), &pb.PollJobRequest{JobId: resp.GetJobId()})
		if !p.GetRunning() {
			if p.GetExitCode() != 0 {
				t.Fatalf("job failed: %s", p.GetStderr())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job never settled")
		}
		time.Sleep(50 * time.Millisecond)
	}

	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if env := read("env"); strings.Contains(env, "API_KEY") || strings.Contains(env, "evil.so") {
		t.Fatalf("a job variable reached the docker client's environment:\n%s", env)
	}
	if args := read("args"); strings.Contains(args, "s3cret") {
		t.Fatalf("a job variable's value rode argv:\n%s", args)
	}
	if got, want := read("envfile"), "API_KEY=s3cret value\nLD_PRELOAD=/x/evil.so\n"; got != want {
		t.Fatalf("env-file = %q, want %q", got, want)
	}
	if left, _ := filepath.Glob(filepath.Join(os.TempDir(), jobEnvFilePrefix+"*")); len(left) != 0 {
		t.Fatalf("the env-file outlived the job: %v", left)
	}
}

func TestSweepTempSecretsRemovesCronEnvFiles(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	stale := filepath.Join(os.TempDir(), jobEnvFilePrefix+"stale")
	if err := os.WriteFile(stale, []byte("K=v\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sweepTempSecrets()
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("a stale cron env-file survived the boot sweep: %v", err)
	}
}
