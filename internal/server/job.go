package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const (
	cronOutputTailBytes = 16 << 10

	cronRetainFinished = 30 * time.Minute

	maxLiveJobs = 64

	cronDefaultTimeout = time.Hour

	jobKillGrace = 3 * time.Second
)

type tailBuf struct {
	buf  []byte
	max  int
	over bool
}

func newTailBuf(max int) *tailBuf { return &tailBuf{max: max} }

func (t *tailBuf) Write(p []byte) (int, error) {
	n := len(p)
	if len(t.buf)+n <= t.max {
		t.buf = append(t.buf, p...)
		return n, nil
	}
	t.over = true
	if n >= t.max {
		t.buf = append(t.buf[:0], p[n-t.max:]...)
		return n, nil
	}
	t.buf = append(t.buf, p...)
	t.buf = append(t.buf[:0], t.buf[len(t.buf)-t.max:]...)
	return n, nil
}

// String returns the retained tail, prefixed with a note when anything was dropped.
func (t *tailBuf) String() string {
	b := t.buf
	for len(b) > 0 && !utf8.RuneStart(b[0]) {
		b = b[1:]
	}
	text := strings.ToValidUTF8(string(b), "\uFFFD")
	if !t.over {
		return text
	}
	kept := fmt.Sprintf("%d KiB", t.max>>10)
	if t.max < 1<<10 {
		kept = fmt.Sprintf("%d bytes", t.max)
	}
	return fmt.Sprintf("[deplo] earlier output trimmed - showing the last %s\n%s", kept, text)
}

type job struct {
	startedAt  time.Time
	finishedAt time.Time
	done       bool
	exitCode   int32
	timedOut   bool
	stdout     *tailBuf
	stderr     *tailBuf
	cancel     context.CancelFunc
}

func newJobID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// StartJob spawns the command and returns its handle.
func (s *Service) StartJob(ctx context.Context, req *pb.StartJobRequest) (*pb.StartJobResponse, error) {
	if req.GetContainer() == "" {
		return nil, status.Error(codes.InvalidArgument, "container is required")
	}
	if req.GetProjectId() == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	if strings.TrimSpace(req.GetCommand()) == "" {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	switch req.GetShell() {
	case "", "sh", "bash":
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown shell %q", req.GetShell())
	}

	s.mu.Lock()
	live := 0
	for _, j := range s.jobs {
		if !j.done {
			live++
		}
	}
	if live >= maxLiveJobs {
		s.mu.Unlock()
		return nil, status.Errorf(codes.ResourceExhausted,
			"this server is already running %d cron jobs", maxLiveJobs)
	}
	id := newJobID()
	jobCtx, cancel := context.WithCancel(context.Background())
	j := &job{
		startedAt: time.Now(),
		stdout:    newTailBuf(cronOutputTailBytes),
		stderr:    newTailBuf(cronOutputTailBytes),
		cancel:    cancel,
	}
	if s.jobs == nil {
		s.jobs = map[string]*job{}
	}
	s.jobs[id] = j
	s.mu.Unlock()

	go s.driveJob(jobCtx, id, req, j)
	return &pb.StartJobResponse{JobId: id}, nil
}

func (s *Service) driveJob(ctx context.Context, id string, req *pb.StartJobRequest, j *job) {
	defer j.cancel()
	func() {
		defer func() {
			if r := recover(); r != nil {
				s.finishJob(j, -1, false, "", fmt.Sprintf("cron job panicked: %v", r))
			}
		}()
		s.runJob(ctx, id, req, j)
	}()
	time.AfterFunc(cronRetainFinished, func() {
		s.mu.Lock()
		if s.jobs[id] == j {
			delete(s.jobs, id)
		}
		s.mu.Unlock()
	})
}

func (s *Service) runJob(ctx context.Context, id string, req *pb.StartJobRequest, j *job) {
	container := req.GetContainer()
	if err := assertOwned(ctx, container, req.GetProjectId()); err != nil {
		s.finishJob(j, -1, false, "", status.Convert(err).Message())
		return
	}

	var prefix []string
	switch req.GetShell() {
	case "bash":
		if !hasShell(ctx, container, "bash") {
			s.finishJob(j, -1, false, "",
				"This container has no bash. Switch the job to sh.")
			return
		}
		prefix = []string{"bash", "-c"}
	case "sh":
		if !hasShell(ctx, container, "sh") {
			s.finishJob(j, -1, false, "",
				"This container has no shell, so it cannot run a cron job.")
			return
		}
		prefix = []string{"sh", "-c"}
	default:
		plan := resolveShellPlan(ctx, container, req.GetImage())
		if plan.raw() {
			s.finishJob(j, -1, false, "",
				"This container has no shell, so it cannot run a cron job.")
			return
		}
		prefix = plan.run
	}

	args := []string{"exec"}
	if w := strings.TrimSpace(req.GetWorkdir()); w != "" {
		args = append(args, "-w", w)
	}
	if u := strings.TrimSpace(req.GetUser()); u != "" {
		args = append(args, "-u", u)
	}
	extraEnv := make([]string, 0, len(req.GetEnv()))
	for _, e := range req.GetEnv() {
		name := strings.TrimSpace(e.GetName())
		if name == "" {
			continue
		}
		args = append(args, "-e", name)
		extraEnv = append(extraEnv, name+"="+e.GetValue())
	}
	args = append(args, "-e", jobMarkerEnv)
	extraEnv = append(extraEnv, jobMarkerEnv+"="+id)
	args = append(args, container)
	args = append(args, prefix...)
	args = append(args, req.GetCommand())

	timeout := time.Duration(req.GetTimeoutSeconds()) * time.Second
	if timeout <= 0 {
		timeout = cronDefaultTimeout
	}

	execStart := time.Now()
	code, err := dockercli.StreamPipes(ctx, timeout, j.stdout, j.stderr, extraEnv, args...)

	if err != nil {
		switch {
		case ctx.Err() != nil:
			killMarkedProcesses(id, jobKillGrace)
			s.finishJob(j, -1, false, "", "The job was stopped.")
		case time.Since(execStart) >= timeout:
			killMarkedProcesses(id, jobKillGrace)
			s.finishJob(j, -1, true, "",
				fmt.Sprintf("The command was still running after %s and was stopped.", timeout))
		default:
			s.finishJob(j, -1, false, "", err.Error())
		}
		return
	}
	s.finishJob(j, int32(code), false, "", "")
}

func (s *Service) finishJob(j *job, code int32, timedOut bool, extraOut, extraErr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j.done {
		return
	}
	if extraOut != "" {
		_, _ = j.stdout.Write([]byte(extraOut))
	}
	if extraErr != "" {
		_, _ = j.stderr.Write([]byte(extraErr))
	}
	j.exitCode = code
	j.timedOut = timedOut
	j.finishedAt = time.Now()
	j.done = true
}

func hasShell(ctx context.Context, container, shell string) bool {
	res, err := dockercli.Run(ctx, 5*time.Second, "exec", container, shell, "-c", ":")
	return err == nil && res.Code == 0
}

// PollJob reports a job's state.
func (s *Service) PollJob(_ context.Context, req *pb.PollJobRequest) (*pb.PollJobResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[req.GetJobId()]
	if j == nil {
		return &pb.PollJobResponse{Found: false}, nil
	}
	resp := &pb.PollJobResponse{
		Found:         true,
		Running:       !j.done,
		StartedAtUnix: j.startedAt.Unix(),
	}
	if !j.done {
		return resp, nil
	}
	resp.ExitCode = j.exitCode
	resp.TimedOut = j.timedOut
	resp.Stdout = j.stdout.String()
	resp.Stderr = j.stderr.String()
	resp.FinishedAtUnix = j.finishedAt.Unix()
	return resp, nil
}

// KillJob cancels a running job.
func (s *Service) KillJob(_ context.Context, req *pb.KillJobRequest) (*pb.KillJobResponse, error) {
	s.mu.Lock()
	j := s.jobs[req.GetJobId()]
	live := j != nil && !j.done
	s.mu.Unlock()
	if !live {
		return &pb.KillJobResponse{Found: false}, nil
	}
	j.cancel()
	return &pb.KillJobResponse{Found: true}, nil
}
