package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/link"
)

// The agent is a cluster party (ADR-0044): it dials every relay it knows and holds one connection to each.
// It never listens for the cluster; the only socket a cluster process opens here is the relay's, behind the proxy.
const maxMemberRelays = 8

// relaysRefresh is how often the leader's relay list is rewritten; a var so tests can hurry it.
var relaysRefresh = 5 * time.Second

type memberState struct {
	Member bool     `json:"member"`
	Relays []string `json:"relays,omitempty"`
	// Cluster is the one this server is in, and Leaders the certificates Deplo said may lead it.
	Cluster string   `json:"cluster,omitempty"`
	Leaders []string `json:"leaders,omitempty"`
}

type clusterMember struct {
	mu     sync.Mutex
	st     memberState
	cancel context.CancelFunc
	party  *clusterlink.Party
}

func (s *Service) clusterMember() *clusterMember {
	s.memberOnce.Do(func() { s.member = &clusterMember{} })
	return s.member
}

func (s *Service) memberFile() string { return filepath.Join(s.agentDir, "cluster-member.json") }

// bridgeGateway is where the proxy's own network reaches this host: the one non-loopback address a relay may answer on.
var bridgeGateway = func(ctx context.Context) string {
	res, err := dockercli.Run(ctx, 10*time.Second, "network", "inspect", "bridge", "--format", "{{range .IPAM.Config}}{{.Gateway}} {{end}}")
	if err != nil || res.Code != 0 {
		return ""
	}
	for _, f := range strings.Fields(res.Stdout) {
		if ip, err := netip.ParseAddr(f); err == nil && ip.Is4() && ip.IsPrivate() {
			return ip.String()
		}
	}
	return ""
}

// relayListen is where the relay on this host answers: loopback, and the proxy's gateway when Docker has one.
func relayListen(ctx context.Context) []string {
	port := fmt.Sprint(wire.DefaultPort)
	out := []string{net.JoinHostPort("127.0.0.1", port)}
	if gw := bridgeGateway(ctx); gw != "" {
		out = append(out, net.JoinHostPort(gw, port))
	}
	return out
}

// clusterEnv is what a supervised cluster process is started with.
func (s *Service) clusterEnv(dir, role string) []string {
	env := os.Environ()
	if s.agentDir != "" {
		m := bootstrap.Paths(s.agentDir)
		env = append(env, "DEPLO_CLUSTER_CERT="+m.CertPath, "DEPLO_CLUSTER_KEY="+m.KeyPath, "DEPLO_CLUSTER_CA="+m.CAPath)
	}
	switch role {
	case "leader":
		state := filepath.Join(dir, "leader")
		_ = os.MkdirAll(state, 0o700)
		env = append(env, clusterSocketEnv+"="+clusterSocket(dir), "DEPLO_CLUSTER_RELAYS_FILE="+relaysFile(dir), "DEPLO_CLUSTER_STATE="+state)
	case "relay":
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		state := filepath.Join(dir, "relay")
		_ = os.MkdirAll(state, 0o700)
		env = append(env, "DEPLO_RELAY_SOCKET="+relaySocket(dir), "DEPLO_RELAY_STATE="+state,
			"DEPLO_RELAY_LISTEN="+strings.Join(relayListen(ctx), ","))
	}
	return env
}

func relaySocket(dir string) string { return filepath.Join(dir, "relay.sock") }
func relaysFile(dir string) string  { return filepath.Join(dir, "relays") }

// relayBase accepts an http(s) origin and nothing else: no credentials, path or query.
func relayBase(raw string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", false
	}
	return u.Scheme + "://" + u.Host, true
}

func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}

// relayURLs are the relays this host dials: the one beside it, the panel's own (read each time, so a moved
// panel address is followed), and any the control plane named. A loopback link address is this machine's own door, not a relay.
func (s *Service) relayURLs(local bool, extra []string) []string {
	var out []string
	add := func(raw string) {
		if base, ok := relayBase(raw); ok && !slices.Contains(out, base) {
			out = append(out, base)
		}
	}
	if local {
		add(fmt.Sprintf("http://127.0.0.1:%d", wire.DefaultPort))
	}
	if s.agentDir != "" {
		a, _ := link.LoadAddresses(s.agentDir)
		for _, raw := range []string{a.Primary, a.Fallback} {
			if u, err := url.Parse(raw); err == nil && !loopbackHost(u.Hostname()) {
				add(raw)
			}
		}
	}
	for _, raw := range extra {
		add(raw)
	}
	return out
}

// keepRelaysFile writes the leader's relay list and rewrites it when the panel's address moves.
func (s *Service) keepRelaysFile(ctx context.Context, dir string, st clusterState) {
	var last string
	for {
		m := s.clusterMember()
		m.mu.Lock()
		extra := m.st.Relays
		m.mu.Unlock()
		body := strings.Join(s.relayURLs(st.Relay, extra), "\n") + "\n"
		if body != last {
			tmp := relaysFile(dir) + ".tmp"
			if os.WriteFile(tmp, []byte(body), 0o600) == nil && os.Rename(tmp, relaysFile(dir)) == nil {
				last = body
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(relaysRefresh):
		}
	}
}

func (s *Service) clusterIdentity() (*wire.Identity, error) {
	m := bootstrap.Paths(s.agentDir)
	return wire.LoadIdentity(m.CertPath, m.KeyPath, m.CAPath)
}

func (s *Service) loadMember() memberState {
	var st memberState
	if raw, err := os.ReadFile(s.memberFile()); err == nil {
		_ = json.Unmarshal(raw, &st)
	}
	return st
}

// StartClusterMember resumes dialing the relays after the agent (re)starts.
func (s *Service) StartClusterMember() {
	if s.agentDir == "" {
		return
	}
	if st := s.loadMember(); st.Member {
		m := s.clusterMember()
		m.mu.Lock()
		defer m.mu.Unlock()
		s.startMember(m, st)
	}
}

// startMember (re)starts the party for st; the caller holds m.mu.
func (s *Service) startMember(m *clusterMember, st memberState) {
	if m.cancel != nil {
		m.cancel()
		m.cancel, m.party = nil, nil
	}
	m.st = st
	if !st.Member {
		s.leaseGuard().Forget()
		go s.forwarders().Clear()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := clusterlink.New(clusterlink.Config{
		Role:     wire.RoleMember,
		Identity: s.clusterIdentity,
		Relays: func() []string {
			m.mu.Lock()
			extra := m.st.Relays
			m.mu.Unlock()
			dir, _ := s.clusterDir()
			return s.relayURLs(readClusterState(dir).Relay, extra)
		},
		// The leader's lease order, or another member's image copy; any other stream is closed unanswered.
		Accept: s.acceptClusterStream,
		Logf:   func(f string, a ...any) { fmt.Fprintf(os.Stderr, "deplo-agent: "+f+"\n", a...) },
	})
	m.cancel, m.party = cancel, p
	go p.Run(ctx)
	go s.leaseGuard().Run(ctx)
	go s.forwarders().Resume()
}

// SetClusterMember makes this agent a cluster party, or stops it being one; see the RPC's contract in proto/agent.proto.
func (s *Service) SetClusterMember(_ context.Context, req *pb.SetClusterMemberRequest) (*pb.SetClusterMemberResponse, error) {
	if s.agentDir == "" || !bootstrap.Provisioned(s.agentDir) {
		return nil, status.Error(codes.FailedPrecondition, "this agent has no certificate to join a cluster with")
	}
	st := memberState{Member: req.GetMember()}
	if st.Member {
		if len(req.GetRelays()) > maxMemberRelays {
			return nil, status.Errorf(codes.InvalidArgument, "at most %d relays", maxMemberRelays)
		}
		for _, raw := range req.GetRelays() {
			base, ok := relayBase(raw)
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "%q is not a relay address (an http or https origin)", raw)
			}
			if !slices.Contains(st.Relays, base) {
				st.Relays = append(st.Relays, base)
			}
		}
		if len(req.GetLeaderFingerprints()) > maxMemberRelays || len(req.GetClusterId()) > 64 {
			return nil, status.Errorf(codes.InvalidArgument, "at most %d leaders, and a cluster id of 64 characters", maxMemberRelays)
		}
		st.Cluster = req.GetClusterId()
		for _, fp := range req.GetLeaderFingerprints() {
			if !validFingerprint(fp) {
				return nil, status.Errorf(codes.InvalidArgument, "%q is not a certificate fingerprint", fp)
			}
			st.Leaders = append(st.Leaders, fp)
		}
	}
	raw, _ := json.Marshal(st)
	tmp := s.memberFile() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return nil, status.Errorf(codes.Internal, "remember the cluster membership: %v", err)
	}
	if err := os.Rename(tmp, s.memberFile()); err != nil {
		return nil, status.Errorf(codes.Internal, "remember the cluster membership: %v", err)
	}
	m := s.clusterMember()
	m.mu.Lock()
	defer m.mu.Unlock()
	s.startMember(m, st)
	return &pb.SetClusterMemberResponse{Member: st.Member, RelayLinks: s.relayLinks(m)}, nil
}

// relayLinks is how many relays hold this member's connection; the caller holds m.mu.
func (s *Service) relayLinks(m *clusterMember) int32 {
	if m.party == nil {
		return 0
	}
	return int32(m.party.Connected())
}

// leaderPins is the cluster this server is in and the certificates Deplo handed it as that cluster's leaders.
func (s *Service) leaderPins() (cluster string, fingerprints []string) {
	m := s.clusterMember()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st.Cluster, m.st.Leaders
}

// acceptClusterStream sends a stream to the handler for who opened it.
func (s *Service) acceptClusterStream(from wire.Party, c net.Conn) {
	if from.Role == wire.RoleLeader {
		s.leaseGuard().Serve(from, c)
		return
	}
	s.acceptMemberStream(from, c)
}
