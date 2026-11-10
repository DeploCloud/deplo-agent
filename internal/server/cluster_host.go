package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The agent installs and supervises the deplo-cluster binary and forwards calls to the leader's
// local socket, so the control plane never dials it (ADR-0043).
const (
	clusterBinaryName  = "deplo-cluster"
	clusterSocketEnv   = "DEPLO_CLUSTER_SOCKET"
	clusterCallTimeout = 2 * time.Minute
	maxClusterBody     = 64 << 20
)

var clusterMethod = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

type clusterState struct {
	Version string `json:"version"`
	Relay   bool   `json:"relay"`
}

// clusterHost is what this agent has running: one supervised process per role.
type clusterHost struct {
	svc    *Service
	mu     sync.Mutex
	state  clusterState
	cancel context.CancelFunc
	procs  []*clusterProc
}

type clusterProc struct {
	role    string
	args    []string
	pidFile string
	mu      sync.Mutex
	up      bool
	done    chan struct{}
}

func (p *clusterProc) running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.up
}

func (p *clusterProc) setUp(v bool) {
	p.mu.Lock()
	p.up = v
	p.mu.Unlock()
}

func (s *Service) clusterDir() (string, error) {
	if s.agentDir == "" {
		return "", status.Error(codes.FailedPrecondition, "this agent has no data directory to keep the cluster leader in")
	}
	return filepath.Join(s.agentDir, "cluster"), nil
}

func (s *Service) clusterHost() *clusterHost {
	s.clusterOnce.Do(func() { s.cluster = &clusterHost{svc: s} })
	return s.cluster
}

// StartCluster resumes supervising an installed leader after the agent (re)starts.
func (s *Service) StartCluster() {
	dir, err := s.clusterDir()
	if err != nil {
		return
	}
	raw, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		return
	}
	var st clusterState
	if json.Unmarshal(raw, &st) != nil {
		return
	}
	if _, err := os.Stat(filepath.Join(dir, clusterBinaryName)); err != nil {
		return
	}
	h := s.clusterHost()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.start(dir, st)
}

// start runs the processes for st; the caller holds h.mu and has stopped any earlier ones.
func (h *clusterHost) start(dir string, st clusterState) {
	ctx, cancel := context.WithCancel(context.Background())
	h.state, h.cancel = st, cancel
	h.procs = nil
	for _, name := range []string{"leader", "relay"} {
		if name == "relay" && !st.Relay {
			continue
		}
		p := &clusterProc{role: name, pidFile: filepath.Join(dir, name+".pid"), done: make(chan struct{})}
		if name == "relay" {
			p.args = []string{"relay"}
		}
		h.procs = append(h.procs, p)
		go p.supervise(ctx, filepath.Join(dir, clusterBinaryName), func() []string { return h.svc.clusterEnv(dir, name) })
	}
	go h.svc.keepRelaysFile(ctx, dir, st)
}

// stop ends every supervised process and waits for them; the caller holds h.mu.
func (h *clusterHost) stop() {
	if h.cancel != nil {
		h.cancel()
	}
	for _, p := range h.procs {
		<-p.done
	}
	h.cancel, h.procs, h.state = nil, nil, clusterState{}
}

func clusterSocket(dir string) string { return filepath.Join(dir, "leader.sock") }

var clusterBackoff = time.Second

// supervise keeps one process running until ctx ends, restarting it with a growing pause.
func (p *clusterProc) supervise(ctx context.Context, bin string, env func() []string) {
	defer close(p.done)
	reapStale(p.pidFile, bin)
	pause := clusterBackoff
	for ctx.Err() == nil {
		cmd := exec.CommandContext(ctx, bin, p.args...)
		cmd.Env = env()
		cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
		cmd.WaitDelay = 5 * time.Second
		started := time.Now()
		if err := cmd.Start(); err != nil {
			log.Printf("deplo-agent: cluster %v: start: %v", p.args, err)
		} else {
			_ = os.WriteFile(p.pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
			p.setUp(true)
			err := cmd.Wait()
			p.setUp(false)
			_ = os.Remove(p.pidFile)
			if ctx.Err() == nil {
				log.Printf("deplo-agent: cluster %v exited: %v; restarting", p.args, err)
			}
		}
		if time.Since(started) > time.Minute {
			pause = clusterBackoff
		}
		select {
		case <-ctx.Done():
		case <-time.After(pause):
			pause = min(pause*2, 30*time.Second)
		}
	}
}

// reapStale ends a process an earlier agent left behind (it re-execs on SelfUpdate), so two never run at once.
func reapStale(pidFile, bin string) {
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	defer os.Remove(pidFile)
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 1 {
		return
	}
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil || strings.TrimSuffix(exe, " (deleted)") != bin {
		return
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	for i := 0; i < 50; i++ {
		if syscall.Kill(pid, 0) != nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
}

// InstallCluster puts the verified deplo-cluster binary on this host and runs it; see the RPC's contract in proto/agent.proto.
func (s *Service) InstallCluster(ctx context.Context, req *pb.InstallClusterRequest) (*pb.InstallClusterResponse, error) {
	dir, err := s.clusterDir()
	if err != nil {
		return nil, err
	}
	bin := req.GetBinaries()[runtime.GOARCH]
	if bin == nil || bin.GetUrl() == "" || bin.GetSha256() == "" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"the cluster release has no binary for this host's architecture (%s)", runtime.GOARCH)
	}
	h := s.clusterHost()
	h.mu.Lock()
	defer h.mu.Unlock()

	installed := readClusterState(dir)
	if installed.Version != "" && versionNewer(installed.Version, req.GetVersion()) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"refusing to downgrade the cluster leader from v%s to v%s", installed.Version, req.GetVersion())
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, status.Errorf(codes.Internal, "create %s: %v", dir, err)
	}
	want := clusterState{Version: req.GetVersion(), Relay: req.GetRelay()}
	exe := filepath.Join(dir, clusterBinaryName)
	_, statErr := os.Stat(exe)
	if installed == want && statErr == nil && h.cancel != nil && h.allUp() {
		return &pb.InstallClusterResponse{Version: want.Version, Running: true}, nil
	}
	staged, err := stageVerified(ctx, dir, ".deplo-cluster-update-*", "cluster", bin.GetUrl(), bin.GetSha256())
	if err != nil {
		return nil, err
	}
	h.stop()
	if err := os.Rename(staged, exe); err != nil {
		os.Remove(staged)
		return nil, status.Errorf(codes.FailedPrecondition, "cannot install the cluster binary at %s: %v", exe, err)
	}
	raw, _ := json.Marshal(want)
	if err := os.WriteFile(filepath.Join(dir, "state.json"), raw, 0o600); err != nil {
		return nil, status.Errorf(codes.Internal, "remember the cluster install: %v", err)
	}
	h.start(dir, want)
	log.Printf("deplo-agent: cluster v%s installed in %s (relay=%v)", want.Version, dir, want.Relay)
	return &pb.InstallClusterResponse{Version: want.Version, Running: true}, nil
}

func (h *clusterHost) allUp() bool {
	for _, p := range h.procs {
		if !p.running() {
			return false
		}
	}
	return len(h.procs) > 0
}

func readClusterState(dir string) clusterState {
	var st clusterState
	if raw, err := os.ReadFile(filepath.Join(dir, "state.json")); err == nil {
		_ = json.Unmarshal(raw, &st)
	}
	return st
}

// RemoveCluster stops the supervised processes and deletes the binary and its state.
func (s *Service) RemoveCluster(context.Context, *pb.RemoveClusterRequest) (*pb.RemoveClusterResponse, error) {
	dir, err := s.clusterDir()
	if err != nil {
		return nil, err
	}
	h := s.clusterHost()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stop()
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return &pb.RemoveClusterResponse{}, nil
	}
	if err := os.RemoveAll(dir); err != nil {
		return nil, status.Errorf(codes.Internal, "remove %s: %v", dir, err)
	}
	return &pb.RemoveClusterResponse{Removed: true}, nil
}

// ClusterStatus reports what this host has installed and whether it is running.
func (s *Service) ClusterStatus(context.Context, *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	dir, err := s.clusterDir()
	if err != nil {
		return nil, err
	}
	h := s.clusterHost()
	h.mu.Lock()
	defer h.mu.Unlock()
	st := readClusterState(dir)
	m := s.clusterMember()
	m.mu.Lock()
	resp := &pb.ClusterStatusResponse{Member: m.st.Member, RelayLinks: s.relayLinks(m)}
	m.mu.Unlock()
	if _, err := os.Stat(filepath.Join(dir, clusterBinaryName)); err != nil || st.Version == "" {
		return resp, nil
	}
	resp.Installed, resp.Running, resp.Version, resp.Relay = true, h.allUp(), st.Version, st.Relay
	return resp, nil
}

// ClusterCall forwards one call to the leader's local socket and returns its answer untouched.
func (s *Service) ClusterCall(ctx context.Context, req *pb.ClusterCallRequest) (*pb.ClusterCallResponse, error) {
	dir, err := s.clusterDir()
	if err != nil {
		return nil, err
	}
	if !clusterMethod.MatchString(req.GetMethod()) {
		return nil, status.Errorf(codes.InvalidArgument, "%q is not a cluster call", req.GetMethod())
	}
	sock := clusterSocket(dir)
	if strings.HasPrefix(req.GetMethod(), "relay.") {
		if !readClusterState(dir).Relay {
			return nil, status.Error(codes.FailedPrecondition, "this host runs no cluster relay")
		}
		sock = relaySocket(dir)
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
		DisableKeepAlives: true,
	}}
	ctx, cancel := context.WithTimeout(ctx, clusterCallTimeout)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://cluster/call/"+req.GetMethod(), bytes.NewReader(req.GetPayload()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build the cluster call: %v", err)
	}
	resp, err := client.Do(hreq)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "the cluster leader on this server is not answering: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxClusterBody+1))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "the cluster leader on this server stopped answering: %v", err)
	}
	if len(body) > maxClusterBody {
		return nil, status.Errorf(codes.ResourceExhausted, "the cluster leader's answer is larger than %d bytes", maxClusterBody)
	}
	if resp.StatusCode != http.StatusOK {
		code := codes.Internal
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			code = codes.FailedPrecondition
		}
		return nil, status.Errorf(code, "the cluster leader refused %s: %s", req.GetMethod(), strings.TrimSpace(string(body)))
	}
	return &pb.ClusterCallResponse{Payload: body}, nil
}

// watchPingTimeout is how long a watch may stay silent before the agent gives it up; the leader pings every 20 s.
var watchPingTimeout = time.Minute

// ClusterWatch forwards the leader's event stream, line by line, until the caller or the leader leaves.
func (s *Service) ClusterWatch(req *pb.ClusterWatchRequest, stream pb.Agent_ClusterWatchServer) error {
	dir, err := s.clusterDir()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	sock := clusterSocket(dir)
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
		DisableKeepAlives: true,
	}}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://cluster/watch?since=%d", req.GetSince()), nil)
	if err != nil {
		return status.Errorf(codes.Internal, "build the cluster watch: %v", err)
	}
	resp, err := client.Do(hreq)
	if err != nil {
		return status.Errorf(codes.Unavailable, "the cluster leader on this server is not answering: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return status.Errorf(codes.FailedPrecondition, "the cluster leader refused the watch (%d)", resp.StatusCode)
	}
	// A leader that stops talking, pings included, is as good as gone.
	idle := time.AfterFunc(watchPingTimeout, cancel)
	defer idle.Stop()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), maxWatchLine)
	for sc.Scan() {
		idle.Reset(watchPingTimeout)
		if err := stream.Send(&pb.ClusterWatchLine{Line: append([]byte(nil), sc.Bytes()...)}); err != nil {
			return err
		}
	}
	if ctx.Err() != nil && stream.Context().Err() == nil {
		return status.Error(codes.Unavailable, "the cluster leader on this server went quiet")
	}
	if err := sc.Err(); err != nil && stream.Context().Err() == nil {
		return status.Errorf(codes.Unavailable, "the cluster leader on this server stopped answering: %v", err)
	}
	return nil
}

const maxWatchLine = 1 << 20
