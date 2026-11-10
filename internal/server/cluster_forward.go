package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/forwarder"
)

// Name forwarders (#200, ADR-0028 amendment). A workload keeps one public name in its Environment wherever it
// runs: this member holds the name on the Environment's network with a small container, which relays to this agent;
// the agent connects to the workload's container when it runs here and over the relay to its member otherwise.
// The control plane renders which names go where; this file only runs them. The workload is addressed by id, and the
// leader's placement (which rides each lease renewal) says where it is, so a failover needs no new entry.
// Stream, member to member: "FWD <network> <kind>:<id>", answered OK or ERR, then raw bytes.
const (
	fwdVerb       = "FWD"
	fwdImage      = "deplo/forwarder:1"
	fwdLabel      = "deplo.forwarder=1"
	maxFwdEntries = 512
)

var (
	fwdName   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
	fwdOrigin = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,126}$`)
	fwdID     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

type heldName struct {
	Network string `json:"network"`
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Name    string `json:"name"`
	Port    uint32 `json:"port"`
	Origin  string `json:"origin"`
}

func (h heldName) key() string { return h.Network + "|" + h.Name }

type servedName struct {
	Network string   `json:"network"`
	Kind    string   `json:"kind"`
	ID      string   `json:"id"`
	Port    uint32   `json:"port"`
	Origin  string   `json:"origin"`
	Sources []string `json:"sources"`
}

type forwardState struct {
	Cluster string            `json:"cluster"`
	Members map[string]string `json:"members"` // server id -> certificate fingerprint
	Hold    []heldName        `json:"hold"`
	Serve   []servedName      `json:"serve"`
	Ports   map[string]int    `json:"ports"` // key -> the port this agent answers that name's container on
}

// forwardSpec is one forwarder container.
type forwardSpec struct {
	Container string
	Network   string
	Alias     string
	Port      uint32
	Target    string // host:port of this agent's listener on the network's gateway
}

// forwardOps is what name forwarders ask of docker and the network; tests replace it.
type forwardOps struct {
	network func(ctx context.Context, network string) (gateway string, subnet *net.IPNet, err error)
	ensure  func(ctx context.Context, s forwardSpec) error
	remove  func(ctx context.Context, container string) error
	list    func(ctx context.Context) ([]string, error)
	ip      func(ctx context.Context, container, network string) (string, error)
	listen  func(addr string) (net.Listener, error)
}

func containerFor(h heldName) string {
	sum := sha256.Sum256([]byte(h.key()))
	return "deplo-fwd-" + hex.EncodeToString(sum[:8])
}

type heldListener struct {
	ln     net.Listener
	subnet *net.IPNet
	port   int
}

type forwarders struct {
	s         *Service
	mu        sync.Mutex
	st        forwardState
	listeners map[string]*heldListener
}

func (s *Service) forwarders() *forwarders {
	s.fwdOnce.Do(func() {
		f := &forwarders{s: s, listeners: map[string]*heldListener{}}
		if s.agentDir != "" {
			if raw, err := os.ReadFile(f.file()); err == nil {
				_ = json.Unmarshal(raw, &f.st)
			}
		}
		s.fwd = f
	})
	return s.fwd
}

func (f *forwarders) file() string { return filepath.Join(f.s.agentDir, "cluster-forwarders.json") }

func (f *forwarders) ops() forwardOps {
	if f.s.fwdOps.ensure != nil {
		return f.s.fwdOps
	}
	return dockerForwardOps
}

func (f *forwarders) save() {
	if f.s.agentDir == "" {
		return
	}
	raw, _ := json.Marshal(f.st)
	tmp := f.file() + ".tmp"
	if os.WriteFile(tmp, raw, 0o600) == nil {
		_ = os.Rename(tmp, f.file())
	}
}

func validFingerprint(fp string) bool {
	return wire.Party{Fingerprint: fp, Role: wire.RoleMember}.Valid()
}

// validate refuses the whole request on one bad entry: a half-applied table is worse than the last good one.
func validateForwarders(req *pb.SetClusterForwardersRequest) (forwardState, error) {
	st := forwardState{Cluster: req.GetClusterId(), Members: map[string]string{}, Ports: map[string]int{}}
	if st.Cluster == "" {
		return st, fmt.Errorf("a cluster id is required")
	}
	if len(req.GetHold())+len(req.GetServe())+len(req.GetMembers()) > maxFwdEntries {
		return st, fmt.Errorf("at most %d entries", maxFwdEntries)
	}
	for _, m := range req.GetMembers() {
		if !fwdID.MatchString(m.GetServerId()) || !validFingerprint(m.GetFingerprint()) {
			return st, fmt.Errorf("member %q is not a server id with a certificate fingerprint", m.GetServerId())
		}
		st.Members[m.GetServerId()] = m.GetFingerprint()
	}
	kind := func(k string) bool { return k == "app" || k == "database" }
	seen := map[string]bool{}
	for _, h := range req.GetHold() {
		e := heldName{h.GetNetwork(), h.GetKind(), h.GetWorkloadId(), h.GetName(), h.GetPort(), h.GetOrigin()}
		if !dockercli.IsTenantNetwork(e.Network) || !kind(e.Kind) || !fwdID.MatchString(e.ID) ||
			!fwdName.MatchString(e.Name) || e.Port == 0 || e.Port > 65535 || !fwdOrigin.MatchString(e.Origin) {
			return st, fmt.Errorf("%q on %q is not a name this member can hold", e.Name, e.Network)
		}
		if seen[e.key()] {
			return st, fmt.Errorf("%q is held twice on %q", e.Name, e.Network)
		}
		seen[e.key()] = true
		st.Hold = append(st.Hold, e)
	}
	for _, v := range req.GetServe() {
		e := servedName{v.GetNetwork(), v.GetKind(), v.GetWorkloadId(), v.GetPort(), v.GetOrigin(), nil}
		if !dockercli.IsTenantNetwork(e.Network) || !kind(e.Kind) || !fwdID.MatchString(e.ID) ||
			e.Port == 0 || e.Port > 65535 || !fwdOrigin.MatchString(e.Origin) {
			return st, fmt.Errorf("%s:%s on %q is not a workload this member can serve", e.Kind, e.ID, e.Network)
		}
		for _, fp := range v.GetSourceFingerprints() {
			if !validFingerprint(fp) {
				return st, fmt.Errorf("%q is not a certificate fingerprint", fp)
			}
			e.Sources = append(e.Sources, fp)
		}
		st.Serve = append(st.Serve, e)
	}
	return st, nil
}

// SetClusterForwarders applies the full table; see the RPC's contract in proto/agent.proto.
func (s *Service) SetClusterForwarders(ctx context.Context, req *pb.SetClusterForwardersRequest) (*pb.SetClusterForwardersResponse, error) {
	m := s.clusterMember()
	m.mu.Lock()
	member := m.st.Member
	m.mu.Unlock()
	if !member {
		return nil, status.Error(codes.FailedPrecondition, "this agent is not a cluster member")
	}
	st, err := validateForwarders(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	f := s.forwarders()
	f.mu.Lock()
	defer f.mu.Unlock()
	st.Ports = f.st.Ports
	f.st = st
	if err := f.reconcile(ctx); err != nil {
		f.save()
		return nil, status.Errorf(codes.Unavailable, "run the name forwarders: %v", err)
	}
	f.save()
	return &pb.SetClusterForwardersResponse{Held: int32(len(st.Hold)), Served: int32(len(st.Serve))}, nil
}

// Resume runs the saved table again after the agent (re)starts.
func (f *forwarders) Resume() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.st.Hold) == 0 {
		return
	}
	if err := f.reconcile(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: name forwarders: %v\n", err)
	}
	f.save()
}

// Clear stops every forwarder and forgets the table: this agent left its cluster.
func (f *forwarders) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	idle := len(f.st.Hold) == 0 && len(f.listeners) == 0
	f.st = forwardState{}
	if idle {
		if f.s.agentDir != "" {
			_ = os.Remove(f.file())
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_ = f.reconcile(ctx)
	if f.s.agentDir != "" {
		_ = os.Remove(f.file())
	}
}

// reconcile makes the listeners and containers match the table; the caller holds f.mu.
func (f *forwarders) reconcile(ctx context.Context) error {
	ops := f.ops()
	var first error
	note := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	if f.st.Ports == nil {
		f.st.Ports = map[string]int{}
	}
	wanted := map[string]bool{}
	for _, h := range f.st.Hold {
		wanted[containerFor(h)] = true
		key := h.key()
		l := f.listeners[key]
		if l == nil {
			gw, subnet, err := ops.network(ctx, h.Network)
			if err != nil {
				note(fmt.Errorf("%s: %w", h.Network, err))
				continue
			}
			ln, err := ops.listen(net.JoinHostPort(gw, fmt.Sprint(f.st.Ports[key])))
			if err != nil {
				ln, err = ops.listen(net.JoinHostPort(gw, "0"))
			}
			if err != nil {
				note(fmt.Errorf("%s: %w", h.Name, err))
				continue
			}
			l = &heldListener{ln: ln, subnet: subnet, port: ln.Addr().(*net.TCPAddr).Port}
			f.listeners[key] = l
			f.st.Ports[key] = l.port
			go f.acceptHeld(key, l)
		}
		note(ops.ensure(ctx, forwardSpec{
			Container: containerFor(h), Network: h.Network, Alias: h.Name, Port: h.Port,
			Target: net.JoinHostPort(l.ln.Addr().(*net.TCPAddr).IP.String(), fmt.Sprint(l.port)),
		}))
	}
	live := map[string]bool{}
	for _, h := range f.st.Hold {
		live[h.key()] = true
	}
	for key, l := range f.listeners {
		if !live[key] {
			l.ln.Close()
			delete(f.listeners, key)
			delete(f.st.Ports, key)
		}
	}
	if have, err := ops.list(ctx); err == nil {
		for _, name := range have {
			if !wanted[name] {
				note(ops.remove(ctx, name))
			}
		}
	} else {
		note(err)
	}
	return first
}

func (f *forwarders) acceptHeld(key string, l *heldListener) {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		go f.serveHeld(key, l, c)
	}
}

// serveHeld carries one connection that reached a held name: to the workload's container here, or to its member.
func (f *forwarders) serveHeld(key string, l *heldListener, c net.Conn) {
	defer c.Close()
	// Only a container of this Environment's network may use its names; another network's reaches the gateway too.
	if tcp, ok := c.RemoteAddr().(*net.TCPAddr); !ok || !l.subnet.Contains(tcp.IP) {
		return
	}
	f.mu.Lock()
	var h heldName
	found := false
	for _, e := range f.st.Hold {
		if e.key() == key {
			h, found = e, true
		}
	}
	cluster, members := f.st.Cluster, f.st.Members
	f.mu.Unlock()
	if !found {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	memberID, here, ok := f.s.leaseGuard().locate(cluster, h.Kind, h.ID)
	if !ok {
		return
	}
	var up net.Conn
	var err error
	if here {
		up, err = f.dialLocal(ctx, h.Origin, h.Network, h.Port)
	} else {
		up, err = f.dialMember(ctx, members[memberID], h)
	}
	if err != nil {
		return
	}
	forwarder.Join(c, up)
}

func (f *forwarders) dialLocal(ctx context.Context, origin, network string, port uint32) (net.Conn, error) {
	ip, err := f.ops().ip(ctx, origin, network)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	return d.DialContext(ctx, "tcp", net.JoinHostPort(ip, fmt.Sprint(port)))
}

func (f *forwarders) dialMember(ctx context.Context, fingerprint string, h heldName) (net.Conn, error) {
	m := f.s.clusterMember()
	m.mu.Lock()
	party := m.party
	m.mu.Unlock()
	if party == nil || !validFingerprint(fingerprint) {
		return nil, fmt.Errorf("no way to reach the workload's server")
	}
	conn, err := party.Dial(ctx, wire.Party{Fingerprint: fingerprint, Role: wire.RoleMember})
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(wire.HeaderTimeout))
	if err := wire.WriteHeader(conn, fwdVerb, fmt.Sprintf("%s %s:%s", h.Network, h.Kind, h.ID)); err != nil {
		conn.Close()
		return nil, err
	}
	verb, _, err := wire.ReadHeader(conn)
	if err != nil || verb != wire.VerbOK {
		conn.Close()
		return nil, fmt.Errorf("the workload's server refused the stream")
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

// authorize is the receiving side's whole decision: a stream is served only when it matches an entry this agent was
// handed for a workload the leader placed here, from a member of this agent's own cluster.
func (f *forwarders) authorize(from wire.Party, network, kind, id string) (servedName, bool) {
	f.mu.Lock()
	st := f.st
	f.mu.Unlock()
	if from.Role != wire.RoleMember {
		return servedName{}, false
	}
	inCluster := false
	for _, fp := range st.Members {
		if fp == from.Fingerprint {
			inCluster = true
		}
	}
	if !inCluster {
		return servedName{}, false
	}
	if _, here, ok := f.s.leaseGuard().locate(st.Cluster, kind, id); !ok || !here {
		return servedName{}, false
	}
	for _, e := range st.Serve {
		if e.Network == network && e.Kind == kind && e.ID == id {
			for _, fp := range e.Sources {
				if fp == from.Fingerprint {
					return e, true
				}
			}
		}
	}
	return servedName{}, false
}

// serveForward answers another member's FWD stream.
func (s *Service) serveForward(from wire.Party, c net.Conn, arg string) {
	network, ref, _ := strings.Cut(arg, " ")
	kind, id, _ := strings.Cut(ref, ":")
	f := s.forwarders()
	e, ok := f.authorize(from, network, kind, id)
	if !ok {
		_ = wire.WriteHeader(c, wire.VerbErr, "not served")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	up, err := f.dialLocal(ctx, e.Origin, e.Network, e.Port)
	if err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, "not answering")
		return
	}
	if wire.WriteHeader(c, wire.VerbOK, "") != nil {
		up.Close()
		return
	}
	forwarder.Join(c, up)
}

// locate says which member runs a workload now, and whether that is this one, from the placement the last renewal carried.
// With replicas on several members this one is preferred, then the first in placement order.
func (g *leaseGuard) locate(cluster, kind, id string) (member string, here, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if c == nil {
		return "", false, false
	}
	for _, e := range c.Placement {
		if e.Kind != kind || e.ID != id || e.Member == "" {
			continue
		}
		if e.Member == c.Member {
			return e.Member, true, true
		}
		if !ok {
			member, ok = e.Member, true
		}
	}
	return member, false, ok
}

var dockerForwardOps = forwardOps{
	network: func(ctx context.Context, network string) (string, *net.IPNet, error) {
		if err := ensureTenantNetwork(ctx, network); err != nil {
			return "", nil, err
		}
		res, err := dockercli.Run(ctx, 10*time.Second, "network", "inspect", network,
			"--format", "{{range .IPAM.Config}}{{.Gateway}} {{.Subnet}}\n{{end}}")
		if err != nil || res.Code != 0 {
			return "", nil, fmt.Errorf("inspect network %s", network)
		}
		for _, line := range strings.Split(res.Stdout, "\n") {
			gw, sub, ok := strings.Cut(strings.TrimSpace(line), " ")
			if !ok {
				continue
			}
			if ip := net.ParseIP(gw); ip != nil && ip.To4() != nil {
				if _, n, err := net.ParseCIDR(sub); err == nil {
					return gw, n, nil
				}
			}
		}
		return "", nil, fmt.Errorf("network %s has no IPv4 gateway", network)
	},
	ensure: ensureForwarderContainer,
	remove: func(ctx context.Context, name string) error {
		_, err := dockercli.Run(ctx, 30*time.Second, "rm", "-f", name)
		return err
	},
	list: func(ctx context.Context) ([]string, error) {
		res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-a", "--filter", "label="+fwdLabel, "--format", "{{.Names}}")
		if err != nil || res.Code != 0 {
			return nil, fmt.Errorf("list the name forwarders")
		}
		return strings.Fields(res.Stdout), nil
	},
	ip: func(ctx context.Context, container, network string) (string, error) {
		res, err := dockercli.Run(ctx, 10*time.Second, "inspect", "--format",
			fmt.Sprintf(`{{(index .NetworkSettings.Networks %q).IPAddress}}`, network), container)
		ip := strings.TrimSpace(res.Stdout)
		if err != nil || res.Code != 0 || net.ParseIP(ip) == nil {
			return "", fmt.Errorf("%s is not running on %s", container, network)
		}
		return ip, nil
	},
	listen: func(addr string) (net.Listener, error) { return net.Listen("tcp", addr) },
}

// ensureForwarderContainer starts the container that holds one name: the agent's own binary, mounted read-only
// into an image with nothing else in it, dropped of every privilege.
func ensureForwarderContainer(ctx context.Context, s forwardSpec) error {
	spec := fmt.Sprintf("%s|%s|%d|%s", s.Network, s.Alias, s.Port, s.Target)
	sum := sha256.Sum256([]byte(spec))
	tag := hex.EncodeToString(sum[:6])
	if res, err := dockercli.Run(ctx, 10*time.Second, "inspect", "--format", `{{index .Config.Labels "deplo.forwarder.spec"}} {{.State.Running}}`, s.Container); err == nil && res.Code == 0 {
		if strings.TrimSpace(res.Stdout) == tag+" true" {
			return nil
		}
		_, _ = dockercli.Run(ctx, 30*time.Second, "rm", "-f", s.Container)
	}
	if res, err := dockercli.Run(ctx, 10*time.Second, "image", "inspect", fwdImage); err != nil || res.Code != 0 {
		empty := strings.NewReader(strings.Repeat("\x00", 1024))
		if code, err := dockercli.PipeIn(ctx, time.Minute, empty, nil, "import", "-", fwdImage); err != nil || code != 0 {
			return fmt.Errorf("prepare the forwarder image")
		}
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	res, err := dockercli.Run(ctx, time.Minute, "run", "-d", "--name", s.Container,
		"--network", s.Network, "--network-alias", s.Alias, "--restart", "unless-stopped",
		"--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--memory", "32m", "--pids-limit", "64",
		"--label", fwdLabel, "--label", "deplo.forwarder.spec="+tag,
		"-v", exe+":/deplo-agent:ro", fwdImage,
		"/deplo-agent", "forward", "--listen", fmt.Sprintf(":%d", s.Port), "--to", s.Target)
	if err != nil {
		return err
	}
	if res.Code != 0 {
		return fmt.Errorf("start the forwarder for %s: %s", s.Alias, oneLine(res.Stderr))
	}
	return nil
}
