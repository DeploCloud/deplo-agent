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
	"github.com/DeploCloud/deplo-agent/internal/forwarder"
)

// Name forwarders (#200, ADR-0028 amendment): this member holds the name of a workload that runs on another
// one, and carries each connection to it over the relay. The control plane renders the table; this only runs it.
// Stream, member to member: "FWD <network> <kind>:<id>", answered OK or ERR, then raw bytes.
const (
	fwdVerb       = "FWD"
	maxFwdEntries = 512
)

var (
	// A public name is db-<slug> or the App's deplo-<slug>, and nothing else.
	fwdName = regexp.MustCompile(`^(db|deplo)-[a-z0-9][a-z0-9-]{0,58}$`)
	fwdID   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	// What the proxy still resolves by name on every tenant network: never held, whatever the table says.
	fwdReserved = map[string]bool{"deplo-traefik": true, "deplo-socket-proxy": true}
)

// forwardableNetwork is an Environment's or a team's network; a preview's names stay on its one server.
func forwardableNetwork(name string) bool {
	return strings.HasPrefix(name, "deplo-env-") || strings.HasPrefix(name, "deplo-team-")
}

type heldName struct {
	Network string `json:"network"`
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Name    string `json:"name"`
	Port    uint32 `json:"port"`
}

func (h heldName) key() string { return h.Network + "|" + h.Name }

type servedName struct {
	Network string   `json:"network"`
	Kind    string   `json:"kind"`
	ID      string   `json:"id"`
	Port    uint32   `json:"port"`
	Sources []string `json:"sources"`
}

type forwardState struct {
	Cluster string            `json:"cluster"`
	Members map[string]string `json:"members"` // server id -> certificate fingerprint
	Hold    []heldName        `json:"hold"`
	Serve   []servedName      `json:"serve"`
}

// forwardSpec is one forwarder container.
type forwardSpec struct {
	Container string
	Network   string
	Alias     string
	Port      uint32
	// Dir holds the socket this agent answers that one container on; nothing else is handed it.
	Dir string
}

// forwardOps is what name forwarders ask of docker and the host; tests replace it.
type forwardOps struct {
	network func(ctx context.Context, network string) error
	ensure  func(ctx context.Context, s forwardSpec) error
	remove  func(ctx context.Context, container string) error
	list    func(ctx context.Context) ([]string, error)
	// target is the address, on network, of a running container of the workload on this host.
	target func(ctx context.Context, workloadID, network string) (string, error)
	listen func(dir string) (net.Listener, error)
}

// `_` is in no slug, so no stack's container (deplo-<slug>..., db-<slug>) can hold a forwarder's name.
const fwdContainerPrefix = "deplo_fwd_"

var fwdContainer = regexp.MustCompile(`^deplo_fwd_[0-9a-f]{16}$`)

func containerFor(h heldName) string {
	sum := sha256.Sum256([]byte(h.key()))
	return fwdContainerPrefix + hex.EncodeToString(sum[:8])
}

type heldListener struct {
	ln  net.Listener
	dir string
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

// dir is where one forwarder's socket lives; only that container is handed it.
func (f *forwarders) dir(container string) string {
	return filepath.Join(f.s.captureDir(), "forwarders", container)
}

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
	st := forwardState{Cluster: req.GetClusterId(), Members: map[string]string{}}
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
		e := heldName{h.GetNetwork(), h.GetKind(), h.GetWorkloadId(), h.GetName(), h.GetPort()}
		if !forwardableNetwork(e.Network) || !kind(e.Kind) || !fwdID.MatchString(e.ID) ||
			!fwdName.MatchString(e.Name) || fwdReserved[e.Name] || e.Port == 0 || e.Port > 65535 {
			return st, fmt.Errorf("%q on %q is not a name this member can hold", e.Name, e.Network)
		}
		if seen[e.key()] {
			return st, fmt.Errorf("%q is held twice on %q", e.Name, e.Network)
		}
		seen[e.key()] = true
		st.Hold = append(st.Hold, e)
	}
	for _, v := range req.GetServe() {
		e := servedName{v.GetNetwork(), v.GetKind(), v.GetWorkloadId(), v.GetPort(), nil}
		if !forwardableNetwork(e.Network) || !kind(e.Kind) || !fwdID.MatchString(e.ID) ||
			e.Port == 0 || e.Port > 65535 {
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
	wanted, networks := map[string]bool{}, map[string]error{}
	for _, h := range f.st.Hold {
		name := containerFor(h)
		wanted[name] = true
		if _, seen := networks[h.Network]; !seen {
			networks[h.Network] = ops.network(ctx, h.Network)
		}
		if err := networks[h.Network]; err != nil {
			note(fmt.Errorf("%s: %w", h.Network, err))
			continue
		}
		l := f.listeners[name]
		if l == nil {
			ln, err := ops.listen(f.dir(name))
			if err != nil {
				note(fmt.Errorf("%s: %w", h.Name, err))
				continue
			}
			l = &heldListener{ln: ln, dir: f.dir(name)}
			f.listeners[name] = l
			go f.acceptHeld(h.key(), l)
		}
		note(ops.ensure(ctx, forwardSpec{Container: name, Network: h.Network, Alias: h.Name, Port: h.Port, Dir: l.dir}))
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
	// The socket goes after its container, which is the only thing that can reach it.
	for name, l := range f.listeners {
		if !wanted[name] {
			l.ln.Close()
			_ = os.RemoveAll(l.dir)
			delete(f.listeners, name)
		}
	}
	return first
}

func (f *forwarders) acceptHeld(key string, l *heldListener) {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		go f.serveHeld(key, c)
	}
}

// serveHeld carries one connection that reached a held name: to the workload's container here, or to its member.
// Its caller is that name's own forwarder container, the only holder of the socket.
func (f *forwarders) serveHeld(key string, c net.Conn) {
	defer c.Close()
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
		up, err = f.dialLocal(ctx, h.ID, h.Network, h.Port)
	} else {
		up, err = f.dialMember(ctx, members[memberID], h)
	}
	if err != nil {
		return
	}
	forwarder.Join(c, up)
}

// dialLocal reaches the workload's own container: found by id on the entry's network, never by a name.
func (f *forwarders) dialLocal(ctx context.Context, workloadID, network string, port uint32) (net.Conn, error) {
	ip, err := f.ops().target(ctx, workloadID, network)
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
	up, err := f.dialLocal(ctx, e.ID, e.Network, e.Port)
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
