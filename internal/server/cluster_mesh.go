package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/forwarder"
	"github.com/DeploCloud/deplo-agent/internal/mesh"
)

// The cluster's private network (ADR-0047): a WireGuard tunnel between the servers of a cluster, run in this agent.
// Streams between them go through it; a relay carries its packets only between servers no direct path joins.
const (
	meshVerb = "MESH"
	// meshStreamPort is where streams between cluster parties arrive, inside the tunnel and nowhere else.
	meshStreamPort   = 7000
	meshDefaultPort  = 51820
	maxMeshPeers     = 64
	maxMeshEndpoints = 8
	// The leader beside this agent reaches the network through it: DIAL a party, or ask whether it is UP.
	meshDoorDial = "DIAL"
	meshDoorUp   = "UP"
)

type meshPeerEntry struct {
	Fingerprint string   `json:"fingerprint"`
	PublicKey   string   `json:"publicKey"`
	Address     string   `json:"address"`
	Endpoints   []string `json:"endpoints,omitempty"`
}

// meshFile is what the network survives a restart with, so it comes back without Deplo.
type meshFile struct {
	PrivateKey string                     `json:"privateKey"`
	Port       uint16                     `json:"port"`
	Address    string                     `json:"address"`
	Clusters   map[string][]meshPeerEntry `json:"clusters"`
}

type clusterNetwork struct {
	s    *Service
	mu   sync.Mutex
	st   meshFile
	m    *mesh.Mesh
	ln   net.Listener
	door net.Listener
	// on is read under the member lock, so it never takes mu.
	on atomic.Bool
	// closeAfterHost: no cluster is left, but a leader or relay still runs here; the network closes once they stop.
	closeAfterHost bool
}

func (s *Service) network() *clusterNetwork {
	s.netOnce.Do(func() {
		n := &clusterNetwork{s: s}
		if s.agentDir != "" {
			if raw, err := os.ReadFile(n.file()); err == nil {
				_ = json.Unmarshal(raw, &n.st)
			}
		}
		s.cnet = n
	})
	return s.cnet
}

func (n *clusterNetwork) file() string { return filepath.Join(n.s.agentDir, "cluster-mesh.json") }

func meshDoor(dir string) string { return filepath.Join(dir, "mesh.sock") }

func (n *clusterNetwork) save() {
	raw, err := json.Marshal(n.st)
	if err == nil {
		err = writeFileAtomic(n.file(), raw, 0o600)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not keep the cluster network in %s: %v\n", n.file(), err)
	}
}

// current is the running network, or nil.
func (n *clusterNetwork) current() *mesh.Mesh {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.m
}

// StartClusterNetwork brings the saved network back after the agent (re)starts.
func (s *Service) StartClusterNetwork() {
	if s.agentDir == "" {
		return
	}
	n := s.network()
	n.mu.Lock()
	if len(n.st.Clusters) > 0 {
		if err := n.applyLocked(); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: cluster network: %v\n", err)
		}
	}
	n.mu.Unlock()
	s.syncNetworkParty()
}

// meshAddress accepts a unique-local IPv6 address, the only kind Deplo hands out inside the tunnel.
func meshAddress(raw string) (netip.Addr, bool) {
	a, err := netip.ParseAddr(raw)
	return a, err == nil && a.Is6() && !a.Is4In6() && a.As16()[0] == 0xfd && a.Zone() == ""
}

func validMeshEndpoint(raw string) (netip.AddrPort, bool) {
	ap, err := netip.ParseAddrPort(raw)
	if err != nil || ap.Port() == 0 {
		return ap, false
	}
	a := ap.Addr().Unmap()
	if !a.IsValid() || a.IsUnspecified() || a.IsMulticast() || a.Zone() != "" {
		return ap, false
	}
	return netip.AddrPortFrom(a, ap.Port()), true
}

func validateMesh(req *pb.SetClusterNetworkRequest) ([]meshPeerEntry, netip.Addr, error) {
	if !fwdID.MatchString(req.GetClusterId()) {
		return nil, netip.Addr{}, errors.New("a cluster id is required")
	}
	if len(req.GetPeers()) == 0 {
		return nil, netip.Addr{}, nil
	}
	self, ok := meshAddress(req.GetAddress())
	if !ok {
		return nil, self, fmt.Errorf("%q is not an address inside the tunnel", req.GetAddress())
	}
	if len(req.GetPeers()) > maxMeshPeers {
		return nil, self, fmt.Errorf("at most %d servers", maxMeshPeers)
	}
	var out []meshPeerEntry
	seen := map[string]bool{}
	for _, p := range req.GetPeers() {
		if !validFingerprint(p.GetFingerprint()) || seen[p.GetFingerprint()] {
			return nil, self, fmt.Errorf("%q is not one certificate fingerprint", p.GetFingerprint())
		}
		seen[p.GetFingerprint()] = true
		if _, err := mesh.ParseKey(p.GetPublicKey()); err != nil {
			return nil, self, err
		}
		a, ok := meshAddress(p.GetAddress())
		if !ok || a == self {
			return nil, self, fmt.Errorf("%q is not another server's address inside the tunnel", p.GetAddress())
		}
		if len(p.GetEndpoints()) > maxMeshEndpoints {
			return nil, self, fmt.Errorf("at most %d addresses for one server", maxMeshEndpoints)
		}
		e := meshPeerEntry{Fingerprint: p.GetFingerprint(), PublicKey: p.GetPublicKey(), Address: a.String()}
		for _, raw := range p.GetEndpoints() {
			ap, ok := validMeshEndpoint(raw)
			if !ok {
				return nil, self, fmt.Errorf("%q is not a host:port a server answers on", raw)
			}
			e.Endpoints = append(e.Endpoints, ap.String())
		}
		out = append(out, e)
	}
	return out, self, nil
}

// SetClusterNetwork puts this server on a cluster's network, or takes it off; see the RPC's contract in proto/agent.proto.
func (s *Service) SetClusterNetwork(_ context.Context, req *pb.SetClusterNetworkRequest) (*pb.SetClusterNetworkResponse, error) {
	if s.agentDir == "" || !bootstrap.Provisioned(s.agentDir) {
		return nil, status.Error(codes.FailedPrecondition, "this server agent has no certificate to join a cluster's network with")
	}
	peers, self, err := validateMesh(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	n := s.network()
	n.mu.Lock()
	if n.st.Clusters == nil {
		n.st.Clusters = map[string][]meshPeerEntry{}
	}
	if len(peers) == 0 {
		delete(n.st.Clusters, req.GetClusterId())
	} else {
		n.st.Clusters[req.GetClusterId()] = peers
		n.st.Address = self.String()
	}
	if n.st.PrivateKey == "" {
		k, err := mesh.NewPrivateKey()
		if err != nil {
			n.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "make the network key: %v", err)
		}
		n.st.PrivateKey = k.String()
	}
	err = n.applyLocked()
	n.save()
	resp := n.responseLocked()
	n.mu.Unlock()
	s.syncNetworkParty()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "bring the cluster network up: %v", err)
	}
	return resp, nil
}

// peersLocked is every server of every cluster this one is in, once each; the caller holds mu.
func (n *clusterNetwork) peersLocked() []mesh.Peer {
	ids := make([]string, 0, len(n.st.Clusters))
	for id := range n.st.Clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	seen := map[string]bool{}
	var out []mesh.Peer
	for _, id := range ids {
		for _, e := range n.st.Clusters[id] {
			key, err := mesh.ParseKey(e.PublicKey)
			addr, ok := meshAddress(e.Address)
			if err != nil || !ok || seen[e.Fingerprint] {
				continue
			}
			seen[e.Fingerprint] = true
			p := mesh.Peer{Fingerprint: e.Fingerprint, PublicKey: key, Address: addr}
			for _, raw := range e.Endpoints {
				if ap, ok := validMeshEndpoint(raw); ok {
					p.Endpoints = append(p.Endpoints, ap)
				}
			}
			out = append(out, p)
		}
	}
	return out
}

// applyLocked makes the running network match the saved one; the caller holds mu.
func (n *clusterNetwork) applyLocked() error {
	if len(n.st.Clusters) == 0 {
		if n.m != nil && n.s.clusterHost().live.Load() {
			n.closeAfterHost = true
			return nil
		}
		n.stopLocked()
		return nil
	}
	n.closeAfterHost = false
	addr, ok := meshAddress(n.st.Address)
	if !ok {
		return errors.New("no address inside the tunnel for this server")
	}
	if n.m != nil && n.m.Address() != addr {
		n.stopLocked()
	}
	if n.m == nil {
		key, err := mesh.ParseKey(n.st.PrivateKey)
		if err != nil {
			return err
		}
		port := n.st.Port
		if port == 0 {
			port = meshDefaultPort
		}
		if !udpFree(port) {
			port = 0
		}
		cfg := mesh.Config{PrivateKey: key, Port: port, Address: addr, Relay: n.relayDial,
			Logf: func(f string, a ...any) { fmt.Fprintf(os.Stderr, "deplo-agent: "+f+"\n", a...) }}
		m, err := mesh.Start(cfg)
		if err != nil {
			// The port is taken: any other one works, since peers learn it from this server's own probes.
			cfg.Port = 0
			if m, err = mesh.Start(cfg); err != nil {
				return err
			}
		}
		ln, err := m.Listen(meshStreamPort)
		if err != nil {
			m.Close()
			return err
		}
		n.m, n.ln, n.st.Port = m, ln, m.Port()
		go n.accept(ln)
		n.openDoorLocked()
	}
	n.on.Store(true)
	return n.m.SetPeers(n.peersLocked())
}

func (n *clusterNetwork) stopLocked() {
	n.on.Store(false)
	if n.door != nil {
		n.door.Close()
		n.door = nil
	}
	if n.ln != nil {
		n.ln.Close()
		n.ln = nil
	}
	if n.m != nil {
		closeMesh(n.m)
		n.m = nil
	}
}

// afterHostStopped closes a network that waited for this server's leader and relay to stop.
func (n *clusterNetwork) afterHostStopped() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closeAfterHost && len(n.st.Clusters) == 0 {
		n.stopLocked()
	}
	n.closeAfterHost = false
}

// closeMesh outlives a panic raised while closing; one in a goroutine of WireGuard's own cannot be caught here,
// which is why Mesh.Close stops those first and the network closes after the leader and relay.
func closeMesh(m *mesh.Mesh) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("deplo-agent: cluster network: closing it failed: %v", r)
		}
	}()
	m.Close()
}

func (n *clusterNetwork) responseLocked() *pb.SetClusterNetworkResponse {
	resp := &pb.SetClusterNetworkResponse{Addresses: hostAddresses()}
	if key, err := mesh.ParseKey(n.st.PrivateKey); err == nil {
		if pub, err := key.Public(); err == nil {
			resp.PublicKey = pub.String()
		}
	}
	if n.m == nil {
		return resp
	}
	resp.Port = uint32(n.m.Port())
	for _, st := range n.m.Status() {
		ps := &pb.ClusterNetworkPeerState{Fingerprint: st.Fingerprint, Path: st.Path, Endpoint: st.Endpoint}
		if !st.LastHandshake.IsZero() {
			ps.LastHandshakeUnix = st.LastHandshake.Unix()
		}
		resp.Peers = append(resp.Peers, ps)
	}
	sort.Slice(resp.Peers, func(i, j int) bool { return resp.Peers[i].Fingerprint < resp.Peers[j].Fingerprint })
	return resp
}

// accept hands every stream that arrives through the tunnel to the party, which checks who sent it.
func (n *clusterNetwork) accept(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			p := n.s.clusterParty()
			if p == nil {
				c.Close()
				return
			}
			p.Answer(c)
		}()
	}
}

// relayDial opens the stream a peer's packets ride when no direct path answers: relay only, never the tunnel itself.
func (n *clusterNetwork) relayDial(ctx context.Context, fingerprint string) (net.Conn, error) {
	p := n.s.clusterParty()
	if p == nil {
		return nil, errors.New("no relay is connected")
	}
	c, err := p.DialRelay(ctx, wire.Party{Fingerprint: fingerprint, Role: wire.RoleMember})
	if err != nil {
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(wire.HeaderTimeout))
	if err := wire.WriteHeader(c, meshVerb, ""); err != nil {
		c.Close()
		return nil, err
	}
	if verb, _, err := wire.ReadHeader(c); err != nil || verb != wire.VerbOK {
		c.Close()
		return nil, errors.New("the server refused the network's packets")
	}
	_ = c.SetDeadline(time.Time{})
	return c, nil
}

// serveMesh carries a peer's packets that came through a relay, for a server on this one's network only.
func (s *Service) serveMesh(from wire.Party, c net.Conn) {
	m := s.network().current()
	name := ""
	if m != nil {
		for _, fp := range append([]string{from.Fingerprint}, from.Previous...) {
			if m.Has(fp) {
				name = fp
				break
			}
		}
	}
	if name == "" {
		_ = wire.WriteHeader(c, wire.VerbErr, "not on this network")
		return
	}
	if wire.WriteHeader(c, wire.VerbOK, "") != nil {
		return
	}
	m.ServeRelay(name, c)
}

// networkDial is the party's way through the tunnel; it fails at once for a server that is not on it.
func (s *Service) networkDial(ctx context.Context, to wire.Party) (net.Conn, error) {
	m := s.network().current()
	if m == nil {
		return nil, errors.New("not on this cluster's network")
	}
	name, ok := s.meshName(m.Has, to.Fingerprint)
	if !ok {
		return nil, errors.New("not on this cluster's network")
	}
	return m.Dial(ctx, name, meshStreamPort)
}

// openDoorLocked lets the leader this agent supervises reach the network; only root can open the socket.
func (n *clusterNetwork) openDoorLocked() {
	dir, err := n.s.clusterDir()
	if err != nil || os.MkdirAll(dir, 0o700) != nil {
		return
	}
	path := meshDoor(dir)
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: the leader's way onto the cluster network: %v\n", err)
		return
	}
	_ = os.Chmod(path, 0o600)
	n.door = ln
	go n.serveDoor(ln)
}

func (n *clusterNetwork) serveDoor(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go n.doorConn(c)
	}
}

func (n *clusterNetwork) doorConn(c net.Conn) {
	_ = c.SetDeadline(time.Now().Add(wire.HeaderTimeout))
	verb, arg, err := wire.ReadHeader(c)
	m := n.current()
	if err != nil || m == nil {
		c.Close()
		return
	}
	switch verb {
	case meshDoorUp:
		for _, st := range m.Status() {
			if st.Path != mesh.PathNone {
				_ = wire.WriteHeader(c, wire.VerbOK, "")
				c.Close()
				return
			}
		}
		_ = wire.WriteHeader(c, wire.VerbErr, "no server answers")
		c.Close()
	case meshDoorDial:
		to, err := wire.ParseParty(arg)
		name, ok := "", false
		if err == nil {
			name, ok = n.s.meshName(m.Has, to.Fingerprint)
		}
		if !ok {
			_ = wire.WriteHeader(c, wire.VerbErr, "not on this network")
			c.Close()
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), wire.HeaderTimeout)
		up, err := m.Dial(ctx, name, meshStreamPort)
		cancel()
		if err != nil {
			_ = wire.WriteHeader(c, wire.VerbErr, "not answering")
			c.Close()
			return
		}
		if wire.WriteHeader(c, wire.VerbOK, "") != nil {
			up.Close()
			c.Close()
			return
		}
		_ = c.SetDeadline(time.Time{})
		forwarder.Join(c, up)
	default:
		c.Close()
	}
}

// udpFree says whether a UDP port can be taken now.
func udpFree(port uint16) bool {
	c, err := net.ListenUDP("udp", &net.UDPAddr{Port: int(port)})
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// hostAddresses is where this host may answer its peers: its own unicast addresses, never a container bridge's.
func hostAddresses() []string {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []string
	for _, ifc := range ifs {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		name := ifc.Name
		if strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "br-") || strings.HasPrefix(name, "veth") ||
			strings.HasPrefix(name, "virbr") || strings.HasPrefix(name, "cni") || strings.HasPrefix(name, "flannel") {
			continue
		}
		addrs, _ := ifc.Addrs()
		for _, a := range addrs {
			pfx, err := netip.ParsePrefix(a.String())
			if err != nil {
				continue
			}
			ip := pfx.Addr()
			if !ip.IsGlobalUnicast() || ip.IsLinkLocalUnicast() {
				continue
			}
			out = append(out, ip.String())
			if len(out) == maxMeshEndpoints {
				return out
			}
		}
	}
	return out
}
