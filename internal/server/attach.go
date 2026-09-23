package server

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/creack/pty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

type attachClient interface {
	read(p []byte) (int, error)
	write(data []byte)
	resize(cols, rows int)
	exitCode() int
	closeInput()
	close()
}

var attachBin = "docker"

var attachArgs = func(name string) []string {
	return []string{"attach", "--sig-proxy=false", name}
}

// Attach is the bidi RPC.
func (s *Service) Attach(stream pb.Agent_AttachServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "first Attach frame must be an open frame")
	}
	ctx := stream.Context()
	if err := assertOwned(ctx, open.GetContainer(), open.GetProjectId()); err != nil {
		return err
	}
	if !dockercli.IsRunning(ctx, open.GetContainer()) {
		return status.Error(codes.FailedPrecondition, "container is not running")
	}

	var client attachClient
	if open.GetTty() {
		cols, rows := dimsOrDefault(int(open.GetCols()), int(open.GetRows()))
		client, err = newAttachPTY(open.GetContainer(), cols, rows)
	} else {
		client, err = newAttachPipes(open.GetContainer())
	}
	if err != nil {
		return status.Errorf(codes.Internal, "attach: %v", err)
	}
	defer client.close()

	outDone := make(chan struct{})
	go func() {
		defer close(outDone)
		buf := make([]byte, 32*1024)
		for {
			n, rerr := client.read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				if serr := stream.Send(&pb.AttachOutput{
					Frame: &pb.AttachOutput_Data{Data: chunk},
				}); serr != nil {
					return
				}
			}
			if rerr != nil {
				_ = stream.Send(&pb.AttachOutput{
					Frame: &pb.AttachOutput_Exit{Exit: &pb.AttachExit{Code: int32(client.exitCode())}},
				})
				return
			}
		}
	}()

	type frameOrErr struct {
		in  *pb.AttachInput
		err error
	}
	recvCh := make(chan frameOrErr, 1)
	recvDone := make(chan struct{})
	defer close(recvDone)
	go func() {
		for {
			in, err := stream.Recv()
			select {
			case recvCh <- frameOrErr{in, err}:
			case <-recvDone:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-outDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case fe := <-recvCh:
			if fe.err == io.EOF {
				client.closeInput()
				select {
				case <-outDone:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if fe.err != nil {
				return fe.err
			}
			switch f := fe.in.GetFrame().(type) {
			case *pb.AttachInput_Data:
				client.write(f.Data)
			case *pb.AttachInput_Resize:
				client.resize(int(f.Resize.GetCols()), int(f.Resize.GetRows()))
			case *pb.AttachInput_Open:
			}
		}
	}
}

func dimsOrDefault(cols, rows int) (int, int) {
	if cols <= 0 {
		cols = 80
	}
	if rows <= 0 {
		rows = 24
	}
	return cols, rows
}

type attachPipes struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	merged    chan []byte
	done      chan struct{}
	closeOnce sync.Once
	code      int
}

func newAttachPipes(name string) (*attachPipes, error) {
	cmd := exec.Command(attachBin, attachArgs(name)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	a := &attachPipes{
		cmd:    cmd,
		stdin:  stdin,
		merged: make(chan []byte, 16),
		done:   make(chan struct{}),
	}

	var wg sync.WaitGroup
	wg.Add(2)
	pump := func(r io.Reader) {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				select {
				case a.merged <- chunk:
				case <-a.done:
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
	go pump(stdout)
	go pump(stderr)
	go func() {
		wg.Wait()
		werr := a.cmd.Wait()
		if ee := (&exec.ExitError{}); errors.As(werr, &ee) {
			a.code = ee.ExitCode()
		}
		close(a.merged)
	}()
	return a, nil
}

func (a *attachPipes) read(p []byte) (int, error) {
	select {
	case chunk, ok := <-a.merged:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, chunk)
		return n, nil
	case <-a.done:
		return 0, io.EOF
	}
}

func (a *attachPipes) write(data []byte) {
	_, _ = a.stdin.Write(data)
}

func (a *attachPipes) resize(_, _ int) {}

func (a *attachPipes) closeInput() { _ = a.stdin.Close() }

func (a *attachPipes) exitCode() int { return a.code }

func (a *attachPipes) close() {
	a.closeOnce.Do(func() { close(a.done) })
	_ = a.stdin.Close()
	if a.cmd.Process != nil {
		_ = a.cmd.Process.Kill()
	}
}

type attachPTY struct {
	cmd      *exec.Cmd
	ptmx     *os.File
	code     int
	waitOnce sync.Once
}

func (a *attachPTY) reap() {
	a.waitOnce.Do(func() {
		werr := a.cmd.Wait()
		if ee := (&exec.ExitError{}); errors.As(werr, &ee) {
			a.code = ee.ExitCode()
		}
	})
}

func newAttachPTY(name string, cols, rows int) (*attachPTY, error) {
	cmd := exec.Command(attachBin, attachArgs(name)...)
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
	if err != nil {
		return nil, err
	}
	return &attachPTY{cmd: cmd, ptmx: ptmx}, nil
}

func (a *attachPTY) read(p []byte) (int, error) {
	n, err := a.ptmx.Read(p)
	if err != nil {
		a.reap()
		return n, io.EOF
	}
	return n, nil
}

func (a *attachPTY) write(data []byte) { _, _ = a.ptmx.Write(data) }

func (a *attachPTY) resize(cols, rows int) {
	if cols <= 0 || rows <= 0 {
		return
	}
	_ = pty.Setsize(a.ptmx, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
}

func (a *attachPTY) exitCode() int { return a.code }

// closeInput is a no-op: a terminal has no half-close, its input ends with the session.
func (a *attachPTY) closeInput() {}

func (a *attachPTY) close() {
	_ = a.ptmx.Close()
	if a.cmd.Process != nil {
		_ = a.cmd.Process.Kill()
	}
	a.reap()
}
