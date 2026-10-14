package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

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

type job struct {
	startedAt  time.Time
	finishedAt time.Time
	done       bool
	exitCode   int32
	timedOut   bool
	stdout     *dockercli.TailBuf
	stderr     *dockercli.TailBuf
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
	for _, e := range req.GetEnv() {
		name := strings.TrimSpace(e.GetName())
		switch {
		case name == "":
		case !validRailpackSecret.MatchString(name) || name == jobMarkerEnv:
			return nil, status.Errorf(codes.InvalidArgument, "%q cannot be the name of a variable", name)
		case strings.ContainsAny(e.GetValue(), "\r\n"):
			return nil, status.Errorf(codes.InvalidArgument, "the value of %s has a line break, which a cron job cannot pass", name)
		}
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
		stdout:    dockercli.NewTailBuf(cronOutputTailBytes),
		stderr:    dockercli.NewTailBuf(cronOutputTailBytes),
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
	envFile, err := writeJobEnvFile(req.GetEnv())
	if err != nil {
		s.finishJob(j, -1, false, "", "write the job's variables: "+err.Error())
		return
	}
	if envFile != "" {
		defer os.Remove(envFile)
		args = append(args, "--env-file", envFile)
	}
	args = append(args, "-e", jobMarkerEnv)
	extraEnv := []string{jobMarkerEnv + "=" + id}
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

const jobEnvFilePrefix = "deplo-cronenv-"

// writeJobEnvFile hands the job's variables to the container through a 0600 file, never through
// the root docker client's own environment, where a name like LD_PRELOAD would configure it.
func writeJobEnvFile(env []*pb.CronEnvVar) (string, error) {
	var b strings.Builder
	for _, e := range env {
		if name := strings.TrimSpace(e.GetName()); name != "" {
			b.WriteString(name + "=" + e.GetValue() + "\n")
		}
	}
	if b.Len() == 0 {
		return "", nil
	}
	f, err := os.CreateTemp("", jobEnvFilePrefix+"*")
	if err != nil {
		return "", err
	}
	_, err = f.WriteString(b.String())
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
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
