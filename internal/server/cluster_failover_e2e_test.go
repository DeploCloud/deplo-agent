package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// clusterBinary builds the real deplo-cluster from the checkout beside this repo; without one the test is skipped.
func clusterBinary(t *testing.T) string {
	t.Helper()
	repo := os.Getenv("DEPLO_CLUSTER_REPO")
	if repo == "" {
		repo = filepath.Join("..", "..", "..", "deplo-cluster")
	}
	if _, err := os.Stat(filepath.Join(repo, "go.mod")); err != nil {
		t.Skip("no deplo-cluster checkout beside this repo (set DEPLO_CLUSTER_REPO)")
	}
	bin := filepath.Join(t.TempDir(), "deplo-cluster")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build deplo-cluster: %v\n%s", err, out)
	}
	return bin
}

// runCluster starts one deplo-cluster process and ends it with the test.
func runCluster(t *testing.T, bin string, env map[string]string, args ...string) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var log bytes.Buffer
	cmd.Stdout, cmd.Stderr = &log, &log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if t.Failed() {
			t.Logf("deplo-cluster %v:\n%s", args, log.String())
		}
	})
}

type unixCaller struct {
	sock string
	n    int
	http *http.Client
}

func callerOn(sock string) *unixCaller {
	return &unixCaller{sock: sock, http: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}}
}

// call is the control plane's side: one forwarded call, counted so the test can show there were none.
func (c *unixCaller) call(t *testing.T, method string, body, into any) {
	t.Helper()
	c.n++
	raw, _ := json.Marshal(body)
	res, err := c.http.Post("http://cluster/call/"+method, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("%s: %d %s", method, res.StatusCode, out)
	}
	if into != nil {
		if err := json.Unmarshal(out, into); err != nil {
			t.Fatalf("%s: %v in %s", method, err, out)
		}
	}
}

func within(t *testing.T, limit time.Duration, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(limit); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The real leader and relay, two real members, and nobody playing Deplo once the cluster is set up: a member
// that dies has its App started on the other one, and removes its own copy when it returns. Takes about 2 min of real
// time, the 81 s floor being the point.
func TestE2E_aMembersDeathMovesItsAppWithNoControlPlaneInTheLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the real 81 s failover floor")
	}
	bin := clusterBinary(t)
	stubBridge(t, "")
	ca := testpki.New(t)
	root, err := os.MkdirTemp("", "fo")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	dir := func(name string) string {
		d := filepath.Join(root, name)
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
		return d
	}
	certs := func(d string) map[string]string {
		return map[string]string{"DEPLO_CLUSTER_CERT": filepath.Join(d, "agent.crt"), "DEPLO_CLUSTER_KEY": filepath.Join(d, "agent.key"), "DEPLO_CLUSTER_CA": filepath.Join(d, "ca.crt")}
	}
	with := func(base map[string]string, more map[string]string) map[string]string {
		for k, v := range more {
			base[k] = v
		}
		return base
	}

	var mu sync.Mutex
	started := map[*Service][]*pb.DeployRequest{}
	old := runFailoverDeploy
	runFailoverDeploy = func(s *Service, _ context.Context, req *pb.DeployRequest, e *emitter) {
		mu.Lock()
		started[s] = append(started[s], req)
		mu.Unlock()
		e.result(true, "", "")
	}
	t.Cleanup(func() { runFailoverDeploy = old })
	startsOn := func(s *Service) []*pb.DeployRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]*pb.DeployRequest(nil), started[s]...)
	}

	// The relay, on a loopback port nothing else holds.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	relayAddr := probe.Addr().String()
	probe.Close()
	relayDir, leaderDir := dir("relay"), dir("leader")
	ca.Provision(t, relayDir, "relay")
	leaderID := ca.Provision(t, leaderDir, "leader")
	relaySock := filepath.Join(relayDir, "relay.sock")
	runCluster(t, bin, with(certs(relayDir), map[string]string{"DEPLO_RELAY_LISTEN": relayAddr, "DEPLO_RELAY_SOCKET": relaySock, "DEPLO_RELAY_STATE": dir("relay/state")}), "relay")

	member := func(name string) (*Service, *wire.Identity) {
		d := dir(name)
		id := ca.Provision(t, d, name)
		s := New(t.TempDir(), t.TempDir(), "/", "")
		s.SetAgentDir(d)
		t.Cleanup(func() { s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{}) })
		return s, id
	}
	a, aID := member("srv_a")
	b, bID := member("srv_b")

	within(t, 10*time.Second, "the relay's socket", func() bool { _, err := os.Stat(relaySock); return err == nil })
	relay := callerOn(relaySock)
	relay.call(t, "relay.roster", map[string]any{"cluster": "clu_1", "parties": []wire.Party{
		{Fingerprint: leaderID.Fingerprint, Role: wire.RoleLeader},
		{Fingerprint: aID.Fingerprint, Role: wire.RoleMember},
		{Fingerprint: bID.Fingerprint, Role: wire.RoleMember},
	}}, nil)

	relaysFile := filepath.Join(leaderDir, "relays")
	if err := os.WriteFile(relaysFile, []byte("http://"+relayAddr+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	leaderSock := filepath.Join(leaderDir, "leader.sock")
	runCluster(t, bin, with(certs(leaderDir), map[string]string{"DEPLO_CLUSTER_SOCKET": leaderSock, "DEPLO_CLUSTER_RELAYS_FILE": relaysFile, "DEPLO_CLUSTER_STATE": dir("leader/state")}))
	for _, s := range []*Service{a, b} {
		if _, err := s.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{"http://" + relayAddr}}); err != nil {
			t.Fatal(err)
		}
	}

	// Deplo's last words: who is in the cluster, where the App runs, and its sealed bundle.
	within(t, 10*time.Second, "the leader's socket", func() bool { _, err := os.Stat(leaderSock); return err == nil })
	deplo := callerOn(leaderSock)
	fact := func(id string, fp string) map[string]any {
		return map[string]any{"id": id, "fingerprint": fp, "arch": "amd64", "freeMb": 4096}
	}
	deplo.call(t, "leader.roster", map[string]any{"cluster": "clu_1", "epoch": 1,
		"members":   []any{fact("srv_a", aID.Fingerprint), fact("srv_b", bID.Fingerprint)},
		"placement": []lease.Entry{{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web"}}}, nil)
	var key struct {
		PublicKey string `json:"publicKey"`
	}
	deplo.call(t, "leader.key", map[string]any{}, &key)
	rec, err := age.ParseX25519Recipient(key.PublicKey)
	if err != nil {
		t.Fatalf("the leader's key %q: %v", key.PublicKey, err)
	}
	bundle := map[string]any{"v": 1, "kind": "app", "workload": "prj_1", "deploymentId": "dep_1", "stack": map[string]any{
		"slug": "web", "projectId": "prj_1", "imageRef": "deplo/web:abc", "composeYaml": "services: {web: {image: deplo/web:abc}}",
		"network": "deplo-team-t1", "env": map[string]string{"TOKEN": "s3cret"}, "registryAuth": []any{}, "composeUpArgs": []string{},
		"ready":       map[string]any{"port": 3000, "portRequired": true},
		"restartRule": map[string]any{"probe": "PROBE_HTTP", "port": 3000, "path": "/"},
	}}
	var sealed bytes.Buffer
	w, _ := age.Encrypt(&sealed, rec)
	raw, _ := json.Marshal(bundle)
	_, _ = w.Write(raw)
	_ = w.Close()
	deplo.call(t, "leader.bundle.put", map[string]any{"cluster": "clu_1", "kind": "app", "id": "prj_1", "deploymentId": "dep_1",
		"sealed": base64.StdEncoding.EncodeToString(sealed.Bytes()),
		"meta":   map[string]any{"name": "web", "hasDomain": true, "createdAt": "2026-01-01T00:00:00Z"}}, nil)

	held := func(s *Service) bool { _, ok := s.leaseGuard().Admit("clu_1", 1); return ok && leaseHeld(s, "clu_1") }
	within(t, 30*time.Second, "both members to hold a lease", func() bool { return held(a) && held(b) })

	// Deplo stops here: from now on nothing calls the leader. Then the member running the App dies.
	silentFrom := deplo.n
	m := a.clusterMember()
	m.mu.Lock()
	m.cancel()
	m.mu.Unlock()
	died := time.Now()

	time.Sleep(70 * time.Second)
	if got := startsOn(b); len(got) != 0 {
		t.Fatalf("the App moved %s after the member died, before the 81 s floor", time.Since(died).Round(time.Second))
	}
	within(t, 60*time.Second, "the App to start on the other member", func() bool { return len(startsOn(b)) == 1 })
	if took := time.Since(died); took < 80*time.Second {
		t.Fatalf("the App moved after %s, under the 81 s floor", took.Round(time.Second))
	}
	req := startsOn(b)[0]
	if req.GetSlug() != "web" || req.GetEnv()["TOKEN"] != "s3cret" || req.GetNetwork() != "deplo-team-t1" || req.GetImageRef() != "deplo/web:abc" ||
		req.GetReady().GetPort() != 3000 || !req.GetReady().GetPortRequired() || req.GetRestartRule().GetProbe() != pb.RestartRule_PROBE_HTTP {
		t.Fatalf("the member was asked to deploy %+v", req)
	}
	if len(startsOn(a)) != 0 {
		t.Fatal("the dead member was asked to start something")
	}
	if deplo.n != silentFrom {
		t.Fatalf("the control plane made %d calls during the failover", deplo.n-silentFrom)
	}

	// Deplo comes back and reads what happened.
	var snap struct {
		Clusters map[string]struct {
			Placement []lease.Entry `json:"placement"`
			Moves     []struct {
				ID, Member, From, Reason string
			} `json:"moves"`
		} `json:"clusters"`
	}
	deplo.call(t, "leader.snapshot", map[string]any{}, &snap)
	view := snap.Clusters["clu_1"]
	if len(view.Placement) != 1 || view.Placement[0].Member != "srv_b" {
		t.Fatalf("the leader's placement: %+v", view.Placement)
	}
	if len(view.Moves) != 1 || view.Moves[0].ID != "prj_1" || view.Moves[0].From != "srv_a" || view.Moves[0].Member != "srv_b" || view.Moves[0].Reason != "stopped_answering" {
		t.Fatalf("the leader's moves: %+v", view.Moves)
	}
	if types := watchTypes(t, deplo, 5); !strings.Contains(types, "placement,failed_over,failover") {
		t.Fatalf("the buffered events: %s", types)
	}

	// The member comes back: on the leader's word it removes its copy of the App, and the leader says so once.
	var removed []string
	g := a.leaseGuard()
	g.has = func(string) bool { return true }
	g.remove = func(_ context.Context, e lease.Entry) error {
		mu.Lock()
		removed = append(removed, e.Slug)
		mu.Unlock()
		return nil
	}
	if _, err := a.SetClusterMember(context.Background(), &pb.SetClusterMemberRequest{Member: true, Relays: []string{"http://" + relayAddr}}); err != nil {
		t.Fatal(err)
	}
	within(t, 60*time.Second, "the leader to report the return", func() bool {
		return strings.Contains(watchTypes(t, deplo, 5), "member_returned")
	})
	time.Sleep(12 * time.Second)
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(removed, ",") != "web" || len(started[a]) != 0 {
		t.Fatalf("the returning member removed %v and started %d", removed, len(started[a]))
	}
	if n := strings.Count(watchTypes(t, deplo, 5), "member_returned"); n != 1 {
		t.Fatalf("%d return events for one return", n)
	}
}

func leaseHeld(s *Service, cluster string) bool {
	g := s.leaseGuard()
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	return c != nil && !c.RenewedAt.IsZero()
}

// watchTypes replays the leader's buffer from the start and returns the event types in order.
func watchTypes(t *testing.T, c *unixCaller, seconds int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://cluster/watch?since=0", nil)
	res, err := c.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var types []string
	var head uint64
	sc := bufio.NewScanner(res.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		var line struct {
			Hello bool   `json:"hello"`
			Head  uint64 `json:"head"`
			Seq   uint64 `json:"seq"`
			Type  string `json:"type"`
		}
		if json.Unmarshal(sc.Bytes(), &line) != nil {
			continue
		}
		if line.Hello {
			head = line.Head
			continue
		}
		if line.Type != "" {
			types = append(types, line.Type)
		}
		if line.Seq >= head {
			break
		}
	}
	return strings.Join(types, ",")
}
