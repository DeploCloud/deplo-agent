package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/forwarder"
)

// Entrances (ADR-0048): a cluster App's domain answers on every server. Each server's proxy passes the domain's
// TLS, untouched, and its certificate checks to this server's one entrance forwarder; this agent carries each
// connection to a server that runs the App and is ready, where that server's agent hands it to its own proxy.

const (
	entranceKind = "entrance"
	// entranceNetwork is the proxy's own network: the forwarder sits where the proxy reaches it.
	entranceNetwork = "deplo"
	// entranceAlias is the forwarder's name there, which the routes' services name.
	entranceAlias = "deplo-entrance"
	// The two ports a passed-on domain is reached on: its TLS, untouched, and port 80 for certificate checks.
	entranceTLS uint32 = 443
	entranceWeb uint32 = 80
	// entranceListen is the forwarder's one listener; every other port of its container is redirected to it.
	entranceListen     uint32 = 443
	maxEntrances              = 200
	entranceRoutesFile        = "entrances.yml"
)

var (
	// entranceWait is how long a connection waits for a server that runs the App to be ready, and entranceRetry
	// how often it looks again; vars so tests can shorten them.
	entranceWait  = 30 * time.Second
	entranceRetry = 500 * time.Millisecond
	// entranceSettle is how long an App has been ready before its proxy is sure to route it: Traefik applies a
	// container change up to 2 s after it happens.
	entranceSettle = 3 * time.Second
	// entranceReadyFor is how long a ready answer is reused before docker is asked again.
	entranceReadyFor = 5 * time.Second
)

type entrance struct {
	AppID   string          `json:"appId"`
	Routes  *entranceRoutes `json:"routes"`
	TLSPort uint32          `json:"tlsPort"`
	WebPort uint32          `json:"webPort"`
}

func entranceContainer() string {
	return containerFor(heldName{Network: entranceNetwork, Name: entranceAlias})
}

// validateEntrances reads the entrances of a forwarder table into st: each App once, on ports of its own, with
// routes of the forms the proxy passes a domain on by and nothing else.
func validateEntrances(req *pb.SetClusterForwardersRequest, st *forwardState) error {
	if len(req.GetEntrances()) > maxEntrances {
		return fmt.Errorf("at most %d Apps answer on every server", maxEntrances)
	}
	seen, ports, hosts := map[string]bool{}, map[uint32]bool{}, map[string]string{}
	for _, e := range req.GetEntrances() {
		en := entrance{AppID: e.GetAppId(), TLSPort: e.GetTlsPort(), WebPort: e.GetWebPort()}
		if !fwdID.MatchString(en.AppID) {
			return fmt.Errorf("%q is not an App id", en.AppID)
		}
		if seen[en.AppID] {
			return fmt.Errorf("%q is passed on twice", en.AppID)
		}
		seen[en.AppID] = true
		for _, p := range []uint32{en.TLSPort, en.WebPort} {
			if p < 1024 || p > 65535 || ports[p] {
				return fmt.Errorf("the entrance of %s needs two ports of its own, from 1024", en.AppID)
			}
			ports[p] = true
		}
		r, err := parseEntranceRoutes(en, e.GetRoutes())
		if err != nil {
			return fmt.Errorf("the entrance of %s: %w", en.AppID, err)
		}
		for _, h := range r.hosts() {
			if other, taken := hosts[h]; taken {
				return fmt.Errorf("%s is passed on for both %s and %s", h, other, en.AppID)
			}
			hosts[h] = en.AppID
		}
		en.Routes = r
		st.Entrances = append(st.Entrances, en)
	}
	for _, id := range req.GetServeEntrances() {
		if !fwdID.MatchString(id) {
			return fmt.Errorf("%q is not an App id", id)
		}
		st.ServeEntrances = append(st.ServeEntrances, id)
	}
	port := req.GetEntrancePort()
	if port > 65535 || (port == 0 && len(st.Entrances)+len(st.ServeEntrances) > 0) {
		return fmt.Errorf("passing domains on needs the port of the proxy's internal entrypoint")
	}
	st.EntrancePort = port
	return nil
}

// reconcileEntrances runs the one entrance forwarder and writes the proxy's entrances file; the caller holds f.mu.
func (f *forwarders) reconcileEntrances(ctx context.Context, wanted map[string]bool, note func(error)) {
	note(f.writeEntranceRoutes())
	if len(f.st.Entrances) == 0 {
		return
	}
	ops := f.ops()
	name := entranceContainer()
	wanted[name] = true
	l := f.listeners[name]
	if l == nil {
		ln, err := ops.listen(f.dir(name))
		if err != nil {
			note(fmt.Errorf("the entrance: %w", err))
			return
		}
		l = &heldListener{ln: ln, dir: f.dir(name)}
		f.listeners[name] = l
		go f.acceptEntrance(l)
	}
	note(ops.ensure(ctx, forwardSpec{Container: name, Network: entranceNetwork, Alias: entranceAlias,
		Port: entranceListen, Dir: l.dir}))
}

// writeEntranceRoutes rewrites the proxy's entrances file in place, which the proxy watches as one of its own
// files: a new file under the name would be a new inode the proxy never sees. An empty table writes no routes.
func (f *forwarders) writeEntranceRoutes() error {
	dir := f.s.traefikDir()
	if dir == "" {
		return nil
	}
	path := filepath.Join(dir, entranceRoutesFile)
	want := renderEntranceRoutes(f.st.Entrances)
	if have, err := os.ReadFile(path); err == nil && string(have) == string(want) {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, want, 0o644)
}

func (f *forwarders) acceptEntrance(l *heldListener) {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		go f.serveEntrance(c)
	}
}

// entranceAt is the App and the visitor's port behind one of the forwarder's ports; the caller holds f.mu.
func (f *forwarders) entranceAt(dialed uint32) (string, uint32, bool) {
	for _, e := range f.st.Entrances {
		switch dialed {
		case e.TLSPort:
			return e.AppID, entranceTLS, true
		case e.WebPort:
			return e.AppID, entranceWeb, true
		}
	}
	return "", 0, false
}

// serveEntrance carries one connection the proxy passed on, to a server that runs the App and is ready. While
// none is (a failover under way, an App still starting) the visitor's connection waits, unanswered, up to
// entranceWait, then closes: never a certificate the domain does not have, or a proxy without its route.
func (f *forwarders) serveEntrance(c net.Conn) {
	defer c.Close()
	in, dialed := readDialedPort(c, 0)
	f.mu.Lock()
	appID, port, found := f.entranceAt(dialed)
	cluster, members := f.st.Cluster, f.st.Members
	f.mu.Unlock()
	if !found {
		return
	}
	deadline := time.Now().Add(entranceWait)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		up, via := f.reachEntrance(ctx, cluster, members, appID, port)
		if up != nil {
			forwarder.Join(in, up)
			f.release(via)
			cancel()
			return
		}
		cancel()
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(entranceRetry)
	}
}

// reachEntrance tries every server the leader placed the App on: this one first when it is ready, then the
// others, the one this agent has the fewest connections open to first. A server that refuses is passed over.
func (f *forwarders) reachEntrance(ctx context.Context, cluster string, members map[string]string, appID string,
	port uint32) (net.Conn, string) {
	here, others := f.s.leaseGuard().holders(cluster, "app", appID)
	if here && f.ready(ctx, appID) {
		if up, err := f.dialProxy(ctx, port); err == nil {
			return up, ""
		}
	}
	h := heldName{Network: entranceNetwork, Kind: entranceKind, ID: appID, Port: entranceTLS}
	return f.dialHolders(others, func(member string) (net.Conn, error) {
		return f.dialMember(ctx, members[member], h, port)
	})
}

// dialProxy reaches this server's own proxy on its internal entrypoint, for a domain's TLS (behind the PROXY
// header the forwarding proxy sent) and for a certificate check alike: Deplo routes the check path there too.
func (f *forwarders) dialProxy(ctx context.Context, port uint32) (net.Conn, error) {
	if port != entranceTLS && port != entranceWeb {
		return nil, fmt.Errorf("a domain is passed on on 443 and 80 only")
	}
	f.mu.Lock()
	to := f.st.EntrancePort
	f.mu.Unlock()
	if to == 0 {
		return nil, fmt.Errorf("this server's proxy has no internal entrypoint")
	}
	ops := f.ops()
	if ops.proxyDial == nil {
		return nil, fmt.Errorf("this server's proxy cannot be reached")
	}
	return ops.proxyDial(ctx, to)
}

// ready says whether the App answers on this server now, long enough for the proxy to route it. A yes is kept
// a few seconds; a no is not, so a connection that waits sees the App the moment it is up.
func (f *forwarders) ready(ctx context.Context, appID string) bool {
	now := time.Now()
	f.mu.Lock()
	until := f.readyUntil[appID]
	f.mu.Unlock()
	if now.Before(until) {
		return true
	}
	ops := f.ops()
	if ops.ready == nil {
		return false
	}
	since, ok := ops.ready(ctx, appID)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !ok {
		delete(f.readySeen, appID)
		return false
	}
	// A health check says when the App became healthy only by being looked at: from the first yes seen here.
	if since.IsZero() {
		if f.readySeen[appID].IsZero() {
			f.readySeen[appID] = now
		}
		since = f.readySeen[appID]
	}
	if now.Sub(since) < entranceSettle {
		return false
	}
	f.readyUntil[appID] = now.Add(entranceReadyFor)
	return true
}

// dialHolders tries each member, the one with the fewest connections this agent opened to it first, and counts
// the connection against the member that took it until the caller releases it.
func (f *forwarders) dialHolders(members []string, dial func(member string) (net.Conn, error)) (net.Conn, string) {
	f.mu.Lock()
	order := slices.Clone(members)
	slices.SortStableFunc(order, func(a, b string) int { return f.open[a] - f.open[b] })
	f.mu.Unlock()
	for _, m := range order {
		if up, err := dial(m); err == nil {
			f.mu.Lock()
			f.open[m]++
			f.mu.Unlock()
			return up, m
		}
	}
	return nil, ""
}

func (f *forwarders) release(member string) {
	if member == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.open[member]--; f.open[member] <= 0 {
		delete(f.open, member)
	}
}

// serveEntranceStream answers another member's passed-on connection: only for an App Deplo said this server
// answers for, that the leader placed here and that is ready, from a member of this cluster, on 443 or 80.
func (f *forwarders) serveEntranceStream(from wire.Party, c net.Conn, network, appID string, port uint32) {
	if port == 0 {
		port = entranceTLS
	}
	if !f.authorizeEntrance(from, network, appID) || (port != entranceTLS && port != entranceWeb) {
		_ = wire.WriteHeader(c, wire.VerbErr, "not served")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if !f.ready(ctx, appID) {
		_ = wire.WriteHeader(c, wire.VerbErr, "not ready")
		return
	}
	up, err := f.dialProxy(ctx, port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: a domain of %s was passed on, but %v\n", appID, err)
		_ = wire.WriteHeader(c, wire.VerbErr, "not answering")
		return
	}
	if wire.WriteHeader(c, wire.VerbOK, "") != nil {
		up.Close()
		return
	}
	forwarder.Join(c, up)
}

func (f *forwarders) authorizeEntrance(from wire.Party, network, appID string) bool {
	f.mu.Lock()
	st := f.st
	f.mu.Unlock()
	if from.Role != wire.RoleMember || network != entranceNetwork || !slices.Contains(st.ServeEntrances, appID) {
		return false
	}
	inCluster := false
	for _, fp := range st.Members {
		inCluster = inCluster || fp == from.Fingerprint
	}
	here, _ := f.s.leaseGuard().holders(st.Cluster, "app", appID)
	return inCluster && here
}
