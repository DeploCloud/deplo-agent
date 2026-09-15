package server

import (
	"io"
	"os/exec"
	"strconv"
	"sync"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const defaultLogTail = 500
const maxLogTail = 5000

func logArgs(tail int, req *pb.FollowLogsRequest) []string {
	args := []string{"logs", "-f", "--tail", strconv.Itoa(tail)}
	if req.GetTimestamps() {
		args = append(args, "--timestamps")
	}
	if since := req.GetSinceUnix(); since > 0 {
		args = append(args, "--since", strconv.FormatInt(since, 10))
	}
	if until := req.GetUntilUnix(); until > 0 {
		args = append(args, "--until", strconv.FormatInt(until, 10))
	}
	return append(args, req.GetContainer())
}

// FollowLogs streams `docker logs -f` for a project-owned container.
func (s *Service) FollowLogs(req *pb.FollowLogsRequest, stream pb.Agent_FollowLogsServer) error {
	ctx := stream.Context()
	if err := assertOwned(ctx, req.GetContainer(), req.GetProjectId()); err != nil {
		return err
	}

	tail := int(req.GetTail())
	if tail <= 0 {
		tail = defaultLogTail
	}
	if tail > maxLogTail {
		tail = maxLogTail
	}

	cmd := exec.CommandContext(ctx, "docker", logArgs(tail, req)...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	var sendMu sync.Mutex
	pump := func(r io.Reader) {
		buf := make([]byte, 32*1024)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				sendMu.Lock()
				sendErr := stream.Send(&pb.LogChunk{Data: chunk})
				sendMu.Unlock()
				if sendErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pump(stdout) }()
	go func() { defer wg.Done(); pump(stderr) }()
	wg.Wait()

	werr := cmd.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if werr != nil {
		if _, ok := werr.(*exec.ExitError); ok {
			return nil
		}
		return werr
	}
	return nil
}
