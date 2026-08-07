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

// job.go implements the cron-job half of the contract: StartJob / PollJob /
// KillJob. A cron job is a `docker exec` the AGENT owns for its whole lifetime,
// on a JOB-scoped context — never the RPC's — so a control-plane restart does
// not kill it, exactly like a Deploy (D5). The control plane holds no connection
// between the start and the terminal poll; it comes back a minute later and
// asks.
//
// This is deliberately not built on Exec. Exec is a console REPL's synchronous
// "type a command, wait for it", capped at 30 seconds; a cron job runs for
// minutes or hours. Sharing the code would mean giving the REPL an unbounded
// deadline, which is exactly the thing that ceiling is there to prevent.
//
// The one thing this file owes the control plane is honesty about its own
// memory: the job handle lives as long as this PROCESS. An agent restart loses
// every in-flight job, and PollJob says so with `found: false` instead of
// inventing an exit code.

const (
	// Retained output per stream, per job. Attacker-controlled (`yes | head -c
	// 2G` is a legal cron command) and this agent is a root process shared by
	// every app on the host, so the buffer is a fixed-size ring — it never grows
	// with the command's output. Same reasoning as inflight.go's log budget.
	// Kept in step with CRON_OUTPUT_TAIL_BYTES in the control plane's
	// lib/data/crons.ts; the number is declared on the contract (agent.proto).
	cronOutputTailBytes = 16 << 10

	// How long a FINISHED job is kept so the control plane can still collect its
	// result. The scheduler polls once a minute, so this is ~30 missed ticks of
	// slack — enough to survive a control-plane restart, a lease handover, or a
	// network partition, without holding results for a host's whole uptime.
	cronRetainFinished = 30 * time.Minute

	// Concurrent LIVE jobs one agent will accept. Each is a `docker exec` plus a
	// goroutine plus two 16 KiB rings, so the memory is trivial; the cap exists
	// because a control-plane bug that starts a job per tick would otherwise
	// fork-bomb a shared host. Beyond it, StartJob answers ResourceExhausted,
	// which the control plane records as a failed attempt (and retries).
	maxLiveJobs = 64

	// Default when the caller names no timeout. Matches the control plane's own
	// default so the two agree on what "unset" means.
	cronDefaultTimeout = time.Hour
)

// tailBuf keeps the LAST n bytes written to it and nothing else. The tail, not
// the head: a job's value is its ending — the error, the summary line — while
// the head is startup boilerplate. Writes past the ceiling are free (no
// allocation, no growth), which is what makes an unbounded producer harmless.
type tailBuf struct {
	buf  []byte
	max  int
	over bool // something was dropped
}

func newTailBuf(max int) *tailBuf { return &tailBuf{max: max} }

func (t *tailBuf) Write(p []byte) (int, error) {
	n := len(p)
	if len(t.buf)+n <= t.max {
		t.buf = append(t.buf, p...)
		return n, nil
	}
	// Something is being dropped. `over` is set here and nowhere else, so a
	// write that exactly fills the ceiling is not reported as truncated.
	t.over = true
	if n >= t.max {
		// This write alone overflows: keep only its tail and forget the rest.
		t.buf = append(t.buf[:0], p[n-t.max:]...)
		return n, nil
	}
	t.buf = append(t.buf, p...)
	t.buf = append(t.buf[:0], t.buf[len(t.buf)-t.max:]...)
	return n, nil
}

// String returns the retained tail, prefixed with a note when anything was
// dropped. Slicing to a byte offset can cut a multi-byte rune in half, which
// would render as a replacement character in the middle of every truncated log,
// so leading bytes are dropped until what remains is valid UTF-8.
func (t *tailBuf) String() string {
	b := t.buf
	for len(b) > 0 && !utf8.Valid(b) {
		b = b[1:]
	}
	if !t.over {
		return string(b)
	}
	kept := fmt.Sprintf("%d KiB", t.max>>10)
	if t.max < 1<<10 {
		kept = fmt.Sprintf("%d bytes", t.max)
	}
	return fmt.Sprintf("[deplo] earlier output trimmed — showing the last %s\n%s", kept, string(b))
}

// job is one cron execution the agent is running or has recently finished.
// Every field except the immutable ones is read under the Service mutex.
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
		// crypto/rand failing is not survivable in any meaningful way, and a
		// duplicate id would silently attach two runs to one process. Fall back
		// to a monotonic-ish value rather than returning a fixed string.
		return fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// StartJob spawns the command and returns its handle. It is a map insert and a
// `go` — nothing that can block. `assertOwned` (a 5s docker inspect) and
// `resolveShellPlan` (up to four 5s shell probes on a cold cache) run INSIDE the
// goroutine: in front of the spawn they would make this RPC take up to ~25
// seconds, and the control plane's scheduler fires every job in one tick.
//
// A pre-spawn failure is therefore not an RPC error. It surfaces on the next
// PollJob as {running: false, exit_code: -1, stderr: "<why>"}, which is the same
// shape the control plane already handles for "the command could not run".
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

// driveJob runs the command to completion, then schedules the record's eviction
// so a control plane that was down when it finished can still collect the
// result.
func (s *Service) driveJob(ctx context.Context, id string, req *pb.StartJobRequest, j *job) {
	defer j.cancel()
	// A panic in the exec path must cost ONE cron run, not the whole agent —
	// which on a shared host would take down every tenant's deploys, streams and
	// metrics. Same containment as driveDeploy.
	func() {
		defer func() {
			if r := recover(); r != nil {
				s.finishJob(j, -1, false, "", fmt.Sprintf("cron job panicked: %v", r))
			}
		}()
		s.runJob(ctx, req, j)
	}()
	time.AfterFunc(cronRetainFinished, func() {
		s.mu.Lock()
		if s.jobs[id] == j {
			delete(s.jobs, id)
		}
		s.mu.Unlock()
	})
}

func (s *Service) runJob(ctx context.Context, req *pb.StartJobRequest, j *job) {
	container := req.GetContainer()
	if err := assertOwned(ctx, container, req.GetProjectId()); err != nil {
		s.finishJob(j, -1, false, "", status.Convert(err).Message())
		return
	}

	// The shell prefix. An empty request shell means "whatever this image has",
	// which is what Exec does; a NAMED shell that the image lacks is a hard
	// failure rather than a silent substitution — `set -o pipefail`, `[[` and
	// arrays all change meaning between bash and sh, so quietly running a bash
	// script under sh produces a wrong result that looks like a bug in the user's
	// command.
	var prefix []string
	switch req.GetShell() {
	case "bash":
		if !hasShell(ctx, container, "bash") {
			s.finishJob(j, -1, false, "",
				"This container has no bash. Switch the job to sh.")
			return
		}
		prefix = []string{"bash", "-lc"}
	case "sh":
		if !hasShell(ctx, container, "sh") {
			s.finishJob(j, -1, false, "",
				"This container has no shell, so it cannot run a cron job.")
			return
		}
		prefix = []string{"sh", "-lc"}
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
	// The NAME rides argv; the VALUE rides the docker client's own environment,
	// so a secret is never visible in `ps` to every user on the host. Same
	// discipline as the REDISCLI_AUTH path in backup.go.
	extraEnv := make([]string, 0, len(req.GetEnv()))
	for _, e := range req.GetEnv() {
		name := strings.TrimSpace(e.GetName())
		if name == "" {
			continue
		}
		args = append(args, "-e", name)
		extraEnv = append(extraEnv, name+"="+e.GetValue())
	}
	args = append(args, container)
	args = append(args, prefix...)
	args = append(args, req.GetCommand())

	timeout := time.Duration(req.GetTimeoutSeconds()) * time.Second
	if timeout <= 0 {
		timeout = cronDefaultTimeout
	}

	// StreamOut writes stdout to the ring and hands stderr lines to the callback
	// (which writes to the other ring), so neither buffer ever holds more than
	// its ceiling — the output is trimmed as it arrives, not after.
	execStart := time.Now()
	code, err := dockercli.StreamOut(ctx, timeout, j.stdout, func(line string) {
		_, _ = j.stderr.Write([]byte(line + "\n"))
	}, extraEnv, args...)

	if err != nil {
		// docker never produced an exit status. Three different things, and the
		// user needs them apart: we killed it (Stop), it outlived its timeout, or
		// docker itself could not run. Classified on the CONTEXT and the elapsed
		// time rather than on the error text, which is a diagnostic string and
		// not a contract.
		switch {
		case ctx.Err() != nil:
			s.finishJob(j, -1, false, "", "The job was stopped.")
		case time.Since(execStart) >= timeout:
			s.finishJob(j, -1, true, "",
				fmt.Sprintf("The command was still running after %s and was stopped.", timeout))
		default:
			s.finishJob(j, -1, false, "", err.Error())
		}
		return
	}
	s.finishJob(j, int32(code), false, "", "")
}

// finishJob stamps the terminal state. `extraOut`/`extraErr` are appended to
// whatever the command already produced, so a failure explanation never
// destroys the output that led to it.
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

// hasShell probes for a specific interpreter with a zero-side-effect command,
// the same `-c :` probe resolveShellPlan uses.
func hasShell(ctx context.Context, container, shell string) bool {
	res, err := dockercli.Run(ctx, 5*time.Second, "exec", container, shell, "-c", ":")
	return err == nil && res.Code == 0
}

// PollJob reports a job's state. Output rides along ONLY on the terminal poll:
// a hundred in-flight jobs each returning 32 KiB every minute is megabytes of
// wire per tick for data the control plane throws away until the job ends.
func (s *Service) PollJob(_ context.Context, req *pb.PollJobRequest) (*pb.PollJobResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[req.GetJobId()]
	if j == nil {
		// Not an error: "I have no record of this" is a legitimate answer that
		// the control plane turns into a `lost` run. An RPC error here would be
		// indistinguishable from the host being unreachable, which means the
		// opposite (keep waiting).
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

// KillJob cancels a running job. Idempotent by design: the control plane calls
// it from a Stop button and from its own deadline check, and racing with the
// job's natural exit must not produce an error.
func (s *Service) KillJob(_ context.Context, req *pb.KillJobRequest) (*pb.KillJobResponse, error) {
	s.mu.Lock()
	j := s.jobs[req.GetJobId()]
	live := j != nil && !j.done
	s.mu.Unlock()
	if !live {
		return &pb.KillJobResponse{Found: false}, nil
	}
	// Cancelling the job context kills the `docker exec` client. The process
	// INSIDE the container gets its stdin/stdout closed and normally dies with
	// it, but docker has no "kill the exec'd process" API, so a command that
	// ignores the disconnect can outlive this.
	//
	// ponytail: a killed job's in-container process may survive the exec client.
	//   Reaping it needs the exec's in-container PID (docker inspect on the exec
	//   id) plus a second exec to `kill` it. Add if anyone reports a zombie.
	j.cancel()
	return &pb.KillJobResponse{Found: true}, nil
}
