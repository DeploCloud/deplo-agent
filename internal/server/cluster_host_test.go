package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// A stand-in for the release binary: it records how it was started, then stays up.
// Everything is recorded before `starts`, which is what the tests wait on.
const fakeCluster = "#!/bin/sh\nd=\"$(dirname \"$0\")\"\necho \"$DEPLO_CLUSTER_SOCKET\" >> \"$d/sockets\"\nenv | grep -E '^DEPLO_(CLUSTER|RELAY)_' | sort > \"$d/env.${1:-leader}\"\necho \"$@\" >> \"$d/starts\"\nexec sleep 300\n"

func clusterHostFor(t *testing.T) (*Service, string) {
	t.Helper()
	agentDir, err := os.MkdirTemp("", "ag")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(agentDir) })
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(agentDir)
	t.Cleanup(func() { s.RemoveCluster(context.Background(), &pb.RemoveClusterRequest{}) })
	return s, filepath.Join(agentDir, "cluster")
}

func installReq(version, script string, relay bool) *pb.InstallClusterRequest {
	return &pb.InstallClusterRequest{
		Version:  version,
		Binaries: binariesFor("https://example/deplo-cluster-linux-"+runtime.GOARCH, sha256Hex([]byte(script))),
		Relay:    relay,
	}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if ok() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func fileLines(path string) []string {
	raw, _ := os.ReadFile(path)
	return strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
}

func startsOf(dir string) int {
	raw, _ := os.ReadFile(filepath.Join(dir, "starts"))
	return strings.Count(string(raw), "\n")
}

func TestCluster_advertisesHostCapability(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.host") {
		t.Fatal("Hello does not advertise cluster.host")
	}
}

func TestCluster_needsADataDirectory(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	_, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
}

func TestInstallCluster_installsVerifiesAndRuns(t *testing.T) {
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	resp, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false))
	if err != nil || resp.GetVersion() != "0.1.0" || !resp.GetRunning() {
		t.Fatalf("install: %+v, %v", resp, err)
	}
	eventually(t, "the leader to start", func() bool { return startsOf(dir) == 1 })
	if got := fileLines(filepath.Join(dir, "sockets")); len(got) != 1 || got[0] != clusterSocket(dir) {
		t.Fatalf("the leader was told its socket is %v, want %s", got, clusterSocket(dir))
	}
	st, err := s.ClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if err != nil || !st.GetInstalled() || !st.GetRunning() || st.GetVersion() != "0.1.0" || st.GetRelay() {
		t.Fatalf("status: %+v, %v", st, err)
	}
}

func TestInstallCluster_refusesAnUnverifiedBinary(t *testing.T) {
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte("#!/bin/sh\nexit 0\n"), nil)
	_, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false))
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "cluster binary checksum mismatch") {
		t.Fatalf("got %v, want a checksum refusal naming the cluster binary", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, clusterBinaryName)); statErr == nil {
		t.Fatal("an unverified binary was installed")
	}
	st, _ := s.ClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if st.GetInstalled() {
		t.Fatalf("status says installed: %+v", st)
	}
}

func TestInstallCluster_needsABinaryForThisArchitecture(t *testing.T) {
	s, _ := clusterHostFor(t)
	_, err := s.InstallCluster(context.Background(), &pb.InstallClusterRequest{Version: "0.1.0"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v", err)
	}
}

func TestInstallCluster_isForwardOnly(t *testing.T) {
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.2.0", fakeCluster, false)); err != nil {
		t.Fatal(err)
	}
	_, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false))
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "downgrade") {
		t.Fatalf("got %v, want a downgrade refusal", err)
	}
	if st := readClusterState(dir); st.Version != "0.2.0" {
		t.Fatalf("the installed version moved to %s", st.Version)
	}
}

func TestInstallCluster_sameRequestChangesNothing(t *testing.T) {
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the leader to start", func() bool { return startsOf(dir) == 1 })
	stubDownload(t, nil, io.ErrUnexpectedEOF) // a second download would fail
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false)); err != nil {
		t.Fatalf("an idempotent install failed: %v", err)
	}
	if n := startsOf(dir); n != 1 {
		t.Fatalf("the leader started %d times", n)
	}
}

func TestInstallCluster_updateRestartsTheLeader(t *testing.T) {
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the leader to start", func() bool { return startsOf(dir) == 1 })
	newer := fakeCluster + "# 0.2.0\n"
	stubDownload(t, []byte(newer), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.2.0", newer, false)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the new leader to start", func() bool { return startsOf(dir) == 2 })
	if st := readClusterState(dir); st.Version != "0.2.0" {
		t.Fatalf("state: %+v", st)
	}
}

func TestInstallCluster_relayRunsBesideTheLeader(t *testing.T) {
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, true)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "both to start", func() bool { return startsOf(dir) == 2 })
	raw, _ := os.ReadFile(filepath.Join(dir, "starts"))
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	slices.Sort(lines)
	if lines[0] != "" || lines[1] != "relay" {
		t.Fatalf("started with %q, want one leader and one `relay`", lines)
	}
	st, _ := s.ClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if !st.GetRelay() || !st.GetRunning() {
		t.Fatalf("status: %+v", st)
	}
}

func TestCluster_restartsAProcessThatExits(t *testing.T) {
	old := clusterBackoff
	clusterBackoff = 10 * time.Millisecond
	t.Cleanup(func() { clusterBackoff = old })
	crashing := "#!/bin/sh\necho x >> \"$(dirname \"$0\")/starts\"\nexit 1\n"
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(crashing), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", crashing, false)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a restart", func() bool { return startsOf(dir) >= 3 })
}

func TestRemoveCluster_stopsAndDeletesAndIsIdempotent(t *testing.T) {
	s, dir := clusterHostFor(t)
	if r, err := s.RemoveCluster(context.Background(), &pb.RemoveClusterRequest{}); err != nil || r.GetRemoved() {
		t.Fatalf("removing nothing: %+v, %v", r, err)
	}
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the leader to start", func() bool { return startsOf(dir) == 1 })
	if r, err := s.RemoveCluster(context.Background(), &pb.RemoveClusterRequest{}); err != nil || !r.GetRemoved() {
		t.Fatalf("remove: %+v, %v", r, err)
	}
	if _, err := os.Stat(dir); err == nil {
		t.Fatal("the cluster directory is still there")
	}
	st, _ := s.ClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if st.GetInstalled() || st.GetRunning() {
		t.Fatalf("status after remove: %+v", st)
	}
}

func TestStartCluster_resumesAfterTheAgentRestarts(t *testing.T) {
	s, dir := clusterHostFor(t)
	stubDownload(t, []byte(fakeCluster), nil)
	if _, err := s.InstallCluster(context.Background(), installReq("0.1.0", fakeCluster, false)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the leader to start", func() bool { return startsOf(dir) == 1 })
	h := s.clusterHost()
	h.mu.Lock()
	h.stop()
	h.mu.Unlock()

	again := New(t.TempDir(), t.TempDir(), "/", "")
	again.SetAgentDir(filepath.Dir(dir))
	t.Cleanup(func() { again.RemoveCluster(context.Background(), &pb.RemoveClusterRequest{}) })
	again.StartCluster()
	eventually(t, "the leader to start again", func() bool { return startsOf(dir) == 2 })
}

func TestReapStale_endsALeftBehindProcessOfOurBinary(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, clusterBinaryName)
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep")
	}
	raw, _ := os.ReadFile(sleep)
	if err := os.WriteFile(bin, raw, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "300")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot run a copy of sleep: %v", err)
	}
	gone := make(chan struct{})
	go func() { cmd.Wait(); close(gone) }()
	pidFile := filepath.Join(dir, "leader.pid")
	os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
	reapStale(pidFile, bin)
	select {
	case <-gone:
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		t.Fatal("the left-behind process is still running")
	}
}

func TestReapStale_leavesAnotherProgramAlone(t *testing.T) {
	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Skip("no sleep")
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	pidFile := filepath.Join(t.TempDir(), "leader.pid")
	os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
	reapStale(pidFile, "/opt/deplo/cluster/"+clusterBinaryName)
	if err := syscall.Kill(cmd.Process.Pid, 0); err != nil {
		t.Fatalf("a process that is not the cluster binary was ended: %v", err)
	}
}

// fakeLeader answers on the leader's socket the way the cluster binary will.
func fakeLeader(t *testing.T, dir string, h http.HandlerFunc) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("unix", clusterSocket(dir))
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Close() })
}

func TestClusterCall_forwardsThePayloadAndReturnsTheAnswer(t *testing.T) {
	s, dir := clusterHostFor(t)
	fakeLeader(t, dir, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.URL.Path != "/call/place" {
			http.Error(w, "wrong route "+r.Method+" "+r.URL.Path, 400)
			return
		}
		w.Write(append([]byte("placed:"), body...))
	})
	resp, err := s.ClusterCall(context.Background(), &pb.ClusterCallRequest{Method: "place", Payload: []byte{0, 1, 2}})
	if err != nil || string(resp.GetPayload()) != "placed:\x00\x01\x02" {
		t.Fatalf("got %q, %v", resp.GetPayload(), err)
	}
}

func TestClusterCall_mapsRefusalsAndSilence(t *testing.T) {
	s, dir := clusterHostFor(t)
	if _, err := s.ClusterCall(context.Background(), &pb.ClusterCallRequest{Method: "place"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("no leader running: got %v, want Unavailable", err)
	}
	fakeLeader(t, dir, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/call/full" {
			http.Error(w, "no member has room", http.StatusConflict)
			return
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	_, err := s.ClusterCall(context.Background(), &pb.ClusterCallRequest{Method: "full"})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "no member has room") {
		t.Fatalf("got %v", err)
	}
	if _, err := s.ClusterCall(context.Background(), &pb.ClusterCallRequest{Method: "other"}); status.Code(err) != codes.Internal {
		t.Fatalf("got %v, want Internal", err)
	}
}

func TestClusterCall_refusesAnUnsafeMethodName(t *testing.T) {
	s, _ := clusterHostFor(t)
	for _, m := range []string{"", "../x", "Place", "a b", "a/b", "place?x=1"} {
		if _, err := s.ClusterCall(context.Background(), &pb.ClusterCallRequest{Method: m}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("method %q: got %v, want InvalidArgument", m, err)
		}
	}
}
