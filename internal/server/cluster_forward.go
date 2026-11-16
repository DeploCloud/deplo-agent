package server

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
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
// one, and carries each connection to it over the cluster's network. The control plane renders the table; this
// only runs it. Stream, member to member: "FWD <network> <kind>:<id>[ <port>[ <service>]]", answered OK or ERR,
// then raw bytes; the port is the one the caller dialed, sent only when it is not the entry's own.
const (
	fwdVerb = "FWD"
	// Every member of an Environment is handed an entry for each of its workloads, held or served.
	maxFwdEntries = 1024
	// fwdWatchEvery is how often a restarted forwarder container gets its redirect back.
	fwdWatchEvery = 15 * time.Second
)

var (
	// A whole workload's public name is db-<slug> or the App's deplo-<slug>; a compose service's is its own
	// name or hostname, which the entry then names the service of.
	fwdName        = regexp.MustCompile(`^(db|deplo)-[a-z0-9][a-z0-9-]{0,58}$`)
	fwdServiceName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	fwdID          = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	// A compose service, as its stack names it.
	fwdService = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)
	// What the proxy still resolves by name on every tenant network: never held, whatever the table says.
	fwdReserved = map[string]bool{"deplo": true, "postgres": true, "traefik": true, "deplo-traefik": true,
		"deplo-socket-proxy": true, "docker-socket-proxy": true}
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
	// Service is the compose service the name belongs to; empty for a whole App or a database.
	Service string `json:"service,omitempty"`
}

func (h heldName) key() string { return h.Network + "|" + h.Name }

type servedName struct {
	Network string   `json:"network"`
	Kind    string   `json:"kind"`
	ID      string   `json:"id"`
	Port    uint32   `json:"port"`
	Sources []string `json:"sources"`
	Service string   `json:"service,omitempty"`
}

type forwardState struct {
	Cluster string            `json:"cluster"`
	Members map[string]string `json:"members"` // server id -> certificate fingerprint
	Hold    []heldName        `json:"hold"`
	Serve   []servedName      `json:"serve"`
	// Answers are the Apps whose domains answer on every server, and ServeAnswers those this one answers for
	// while it runs them (ADR-0048).
	Answers      []answer `json:"answers,omitempty"`
	ServeAnswers []string `json:"serveAnswers,omitempty"`
	AnswerPort   uint32   `json:"answerPort,omitempty"`
	// The caps Deplo set for this cluster; zero is the default.
	AnswerMaxConns   uint32 `json:"answerMaxConns,omitempty"`
	AnswerMaxWaiting uint32 `json:"answerMaxWaiting,omitempty"`
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
	// target is the address, on network, of a running container of the workload (or of its service) on this host.
	target func(ctx context.Context, workloadID, network, service string) (string, error)
	listen func(dir string) (net.Listener, error)
	// pids is each forwarder container's process, and redirect sends every TCP port of its network to one.
	pids     func(ctx context.Context, containers []string) (map[string]int, error)
	redirect func(pid int, port uint32) error
	// proxyDial connects to a port on this server's own proxy, inside its network: where a passed-on domain goes.
	proxyDial func(ctx context.Context, port uint32) (net.Conn, error)
	// ready says whether every container of an App here answers, and since when.
	ready func(ctx context.Context, appID string) appReadiness
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
	// redirected is the process each container's redirect was set up in: a restarted container needs it again.
	redirected map[string]int
	// open counts the connections this agent carries to each member, so the least busy is tried first.
	open map[string]int
	// readiness is each App's last readiness answer here; readySeen is when it was first seen healthy.
	readiness map[string]*answerReadyCheck
	readySeen map[string]time.Time
	// gates are the connections waiting for each App; waiting counts them all, conns every passed-on connection.
	gates          map[string]*answerGate
	waiting, conns int
	// refused is every connection turned away since the last table, by App ("" past this server's own cap).
	refused map[refusalKey]*answerRefusal
}

func (s *Service) forwarders() *forwarders {
	s.fwdOnce.Do(func() {
		f := &forwarders{s: s, listeners: map[string]*heldListener{}, redirected: map[string]int{}, open: map[string]int{},
			readiness: map[string]*answerReadyCheck{}, readySeen: map[string]time.Time{}, gates: map[string]*answerGate{},
			refused: map[refusalKey]*answerRefusal{}}
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
	raw, err := json.Marshal(f.st)
	if err == nil {
		err = writeFileAtomic(f.file(), raw, 0o600)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not keep the name forwarders in %s: %v\n", f.file(), err)
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
	if len(req.GetHold())+len(req.GetServe())+len(req.GetMembers())+len(req.GetAnswers())+len(req.GetServeAnswers()) > maxFwdEntries {
		return st, fmt.Errorf("at most %d entries", maxFwdEntries)
	}
	for _, m := range req.GetMembers() {
		if !fwdID.MatchString(m.GetServerId()) || !validFingerprint(m.GetFingerprint()) {
			return st, fmt.Errorf("member %q is not a server id with a certificate fingerprint", m.GetServerId())
		}
		st.Members[m.GetServerId()] = m.GetFingerprint()
	}
	kind := func(k string) bool { return k == "app" || k == "database" }
	// A service belongs to a compose stack, which is an App.
	service := func(k, s string) bool { return s == "" || (k == "app" && fwdService.MatchString(s)) }
	seen := map[string]bool{}
	for _, h := range req.GetHold() {
		e := heldName{h.GetNetwork(), h.GetKind(), h.GetWorkloadId(), h.GetName(), h.GetPort(), h.GetService()}
		named := fwdName.MatchString(e.Name) || (e.Service != "" && fwdServiceName.MatchString(e.Name))
		if !forwardableNetwork(e.Network) || !kind(e.Kind) || !fwdID.MatchString(e.ID) || !service(e.Kind, e.Service) ||
			!named || fwdReserved[e.Name] || e.Port == 0 || e.Port > 65535 {
			return st, fmt.Errorf("%q on %q is not a name this member can hold", e.Name, e.Network)
		}
		if seen[e.key()] {
			return st, fmt.Errorf("%q is held twice on %q", e.Name, e.Network)
		}
		seen[e.key()] = true
		st.Hold = append(st.Hold, e)
	}
	for _, v := range req.GetServe() {
		e := servedName{v.GetNetwork(), v.GetKind(), v.GetWorkloadId(), v.GetPort(), nil, v.GetService()}
		if !forwardableNetwork(e.Network) || !kind(e.Kind) || !fwdID.MatchString(e.ID) || !service(e.Kind, e.Service) ||
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
	return st, validateAnswers(req, &st)
}

// SetClusterForwarders applies the full table; see the RPC's contract in proto/agent.proto.
func (s *Service) SetClusterForwarders(ctx context.Context, req *pb.SetClusterForwardersRequest) (*pb.SetClusterForwardersResponse, error) {
	m := s.clusterMember()
	m.mu.Lock()
	member := m.st.Member
	m.mu.Unlock()
	if !member {
		return nil, status.Error(codes.FailedPrecondition, "this server agent is not a cluster member")
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
	return &pb.SetClusterForwardersResponse{Held: int32(len(st.Hold)), Served: int32(len(st.Serve)),
		AnswerRefusals: f.takeRefusals()}, nil
}

// Resume runs the saved table again after the agent (re)starts.
func (f *forwarders) Resume() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.st.Hold)+len(f.st.Answers) == 0 {
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
	idle := len(f.st.Hold)+len(f.st.Answers) == 0 && len(f.listeners) == 0
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
	f.reconcileAnswers(ctx, wanted, note)
	f.redirectLocked(ctx)
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
			delete(f.redirected, name)
		}
	}
	return first
}

// redirectLocked sends every TCP port of each forwarder container to its one listener, once per container
// process: a container that restarted lost it with its network. A name whose redirect fails still answers on
// its own port. The caller holds f.mu.
func (f *forwarders) redirectLocked(ctx context.Context) {
	ops := f.ops()
	if ops.pids == nil || ops.redirect == nil || len(f.st.Hold)+len(f.st.Answers) == 0 {
		return
	}
	ports := map[string]uint32{}
	names := make([]string, 0, len(f.st.Hold)+len(f.st.Answers))
	for _, h := range f.st.Hold {
		c := containerFor(h)
		ports[c] = h.Port
		names = append(names, c)
	}
	if len(f.st.Answers) > 0 {
		ports[answerContainer()] = answerListen
		names = append(names, answerContainer())
	}
	pids, err := ops.pids(ctx, names)
	if err != nil {
		return
	}
	for c, pid := range pids {
		if pid <= 0 || f.redirected[c] == pid {
			continue
		}
		if err := ops.redirect(pid, ports[c]); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: name forwarder %s answers on its own port only: %v\n", c, err)
			continue
		}
		f.redirected[c] = pid
	}
}

// Watch puts a redirect back after a forwarder container restarted, until ctx ends.
func (f *forwarders) Watch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(fwdWatchEvery):
		}
		f.mu.Lock()
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		f.redirectLocked(wctx)
		cancel()
		f.mu.Unlock()
	}
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
	in, port := readDialedPort(c, h.Port)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Every member that runs the workload can answer: this one first, then the least busy, past any that refuse.
	here, others := f.s.leaseGuard().holders(cluster, h.Kind, h.ID)
	var up net.Conn
	via := ""
	if here {
		up, _ = f.dialLocal(ctx, h.ID, h.Network, h.Service, port)
	}
	if up == nil {
		up, via = f.dialHolders(others, func(member string) (net.Conn, error) {
			return f.dialMember(ctx, members[member], h, port)
		})
	}
	if up == nil {
		return
	}
	forwarder.Join(in, up)
	f.release(via)
}

// bufferedConn is a connection whose first bytes were read ahead.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }

// CloseWrite lets a half-close reach the forwarder's side as it would without the read-ahead.
func (b *bufferedConn) CloseWrite() error {
	if cw, ok := b.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return b.Conn.Close()
}

// readDialedPort reads the port line a forwarder opens with: the port its caller dialed. A forwarder from before
// the line sends the caller's bytes at once, so whatever is not the line is kept and the name's own port used.
func readDialedPort(c net.Conn, own uint32) (net.Conn, uint32) {
	r := bufio.NewReaderSize(c, 64)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	in := &bufferedConn{Conn: c, r: r}
	// A caller's own bytes rarely start with the verb's letter: those pass without waiting for more.
	if first, err := r.Peek(1); err != nil || first[0] != forwarder.HeaderVerb[0] {
		return in, own
	}
	head, err := r.Peek(len(forwarder.HeaderVerb) + 1)
	if err != nil || string(head) != forwarder.HeaderVerb+" " {
		return in, own
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return in, own
	}
	n, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, forwarder.HeaderVerb+" ")), 10, 16)
	if err != nil || n == 0 {
		return in, own
	}
	return in, uint32(n)
}

// dialLocal reaches the workload's own container, or its service's: found by id on the entry's network, never by a name.
func (f *forwarders) dialLocal(ctx context.Context, workloadID, network, service string, port uint32) (net.Conn, error) {
	ip, err := f.ops().target(ctx, workloadID, network, service)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	return d.DialContext(ctx, "tcp", net.JoinHostPort(ip, fmt.Sprint(port)))
}

// fwdArg is a FWD stream's header: the old form for a name's own port with no service, so an older agent still takes it.
func fwdArg(h heldName, port uint32) string {
	arg := fmt.Sprintf("%s %s:%s", h.Network, h.Kind, h.ID)
	if port != h.Port || h.Service != "" {
		arg += fmt.Sprintf(" %d", port)
	}
	if h.Service != "" {
		arg += " " + h.Service
	}
	return arg
}

func (f *forwarders) dialMember(ctx context.Context, fingerprint string, h heldName, port uint32) (net.Conn, error) {
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
	if err := wire.WriteHeader(conn, fwdVerb, fwdArg(h, port)); err != nil {
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
func (f *forwarders) authorize(from wire.Party, network, kind, id, service string) (servedName, bool) {
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
		if e.Network == network && e.Kind == kind && e.ID == id && e.Service == service {
			for _, fp := range e.Sources {
				if fp == from.Fingerprint {
					return e, true
				}
			}
		}
	}
	return servedName{}, false
}

// parseFwdArg reads "<network> <kind>:<id>[ <port>[ <service>]]"; a missing port is the entry's own.
func parseFwdArg(arg string) (network, kind, id, service string, port uint32, ok bool) {
	f := strings.Fields(arg)
	if len(f) < 2 || len(f) > 4 {
		return "", "", "", "", 0, false
	}
	network = f[0]
	kind, id, ok = strings.Cut(f[1], ":")
	if !ok {
		return "", "", "", "", 0, false
	}
	if len(f) >= 3 {
		n, err := strconv.ParseUint(f[2], 10, 16)
		if err != nil || n == 0 {
			return "", "", "", "", 0, false
		}
		port = uint32(n)
	}
	if len(f) == 4 {
		service = f[3]
	}
	return network, kind, id, service, port, true
}

// serveForward answers another member's FWD stream: on any port of the workload, as on one server.
func (s *Service) serveForward(from wire.Party, c net.Conn, arg string) {
	network, kind, id, service, port, ok := parseFwdArg(arg)
	f := s.forwarders()
	if ok && kind == answerKind {
		f.serveAnswerStream(from, c, network, id, port)
		return
	}
	var e servedName
	if ok {
		e, ok = f.authorize(from, network, kind, id, service)
	}
	if !ok {
		_ = wire.WriteHeader(c, wire.VerbErr, "not served")
		return
	}
	if port == 0 {
		port = e.Port
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	up, err := f.dialLocal(ctx, e.ID, e.Network, e.Service, port)
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

// holders is every member the leader placed a workload on, from the last renewal: whether this one is among them,
// and the others in placement order. A standby copy holds the stack, not the name, and is never one. This member
// holds nothing on a lease that is not current: a copy back from a power cut answers for nobody.
func (g *leaseGuard) holders(cluster, kind, id string) (here bool, others []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clusters[cluster]
	if c == nil {
		return false, nil
	}
	current := c.current(g.now())
	for _, e := range c.Placement {
		if e.Kind != kind || e.ID != id || e.Member == "" || e.Standby {
			continue
		}
		if e.Member == c.Member {
			here = here || current
		} else if !slices.Contains(others, e.Member) {
			others = append(others, e.Member)
		}
	}
	return here, others
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
	current := c.current(g.now())
	for _, e := range c.Placement {
		// A standby copy holds the stack, not the name: connections still go to the workload itself.
		if e.Kind != kind || e.ID != id || e.Member == "" || e.Standby {
			continue
		}
		if e.Member == c.Member {
			// Not on a lease that is not current: the copy here may be one that moved away.
			if current {
				return e.Member, true, true
			}
			continue
		}
		if !ok {
			member, ok = e.Member, true
		}
	}
	return member, false, ok
}
