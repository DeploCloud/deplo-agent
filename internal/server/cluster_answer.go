package server

import (
	"bytes"
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

// Answers (ADR-0048): a cluster App's domain answers on every server. Each server's proxy passes the domain's TLS,
// untouched, and its certificate checks to this server's one answer forwarder; this agent carries each connection
// to a server that runs the App and is ready, where that server's agent hands it to its own proxy.

const (
	answerKind = "answer"
	// answerNetwork is the proxy's own network: the forwarder sits where the proxy reaches it.
	answerNetwork = "deplo"
	// answerAlias is the forwarder's name there, which the routes' services name.
	answerAlias = "deplo-answer"
	// The two ports a passed-on domain is reached on: its TLS, untouched, and port 80 for certificate checks.
	answerTLS uint32 = 443
	answerWeb uint32 = 80
	// answerProbe asks a member only whether it would answer for the App now; nothing is connected.
	answerProbe uint32 = 1
	// answerListen is the forwarder's one listener; every other port of its container is redirected to it.
	answerListen     uint32 = 443
	maxAnswers              = 200
	answerRoutesFile        = "answers.yml"
)

var (
	// answerWait is how long a connection waits for a server that runs the App to be ready, and answerRetry how
	// often one look, shared by every connection waiting for that App, is taken; vars so tests can shorten them.
	answerWait  = 30 * time.Second
	answerRetry = 500 * time.Millisecond
	// answerSettle is how long an App has been ready before its proxy is sure to route it: Traefik applies a
	// container change up to 2 s after it happens.
	answerSettle = 3 * time.Second
	// answerReadyFor is how long a ready answer is kept before docker is asked again.
	answerReadyFor = 5 * time.Second
	// At most this many connections wait for one App, and for all of this server's together, and this many pass
	// through this agent at once, waiting or carried: past either, a connection is closed at once.
	answerWaitPerApp    = 128
	answerWaitPerServer = 512
	answerMaxConns      = 2048
)

type answer struct {
	AppID   string        `json:"appId"`
	Routes  *answerRoutes `json:"routes"`
	TLSPort uint32        `json:"tlsPort"`
	WebPort uint32        `json:"webPort"`
}

// answerGate is one App's waiting connections: they wait on wake, which the one look for a route closes.
type answerGate struct {
	waiting int
	wake    chan struct{}
	// tried is when a connection last found no route: until a look finds one, the next ones go straight to wait.
	tried   time.Time
	looking bool
}

// answerReadyCheck is one App's last readiness answer, and the check under way that every caller waits for.
type answerReadyCheck struct {
	ok      bool
	until   time.Time
	pending chan struct{}
}

// appReadiness is what docker says of an App's containers here.
type appReadiness struct {
	OK bool
	// Since is when the last container without a health check started. Health says a container has one: when it
	// turned healthy is known only from the moment it is seen so.
	Since  time.Time
	Health bool
}

func answerContainer() string {
	return containerFor(heldName{Network: answerNetwork, Name: answerAlias})
}

// validateAnswers reads the answers of a forwarder table into st: each App once, on ports of its own, with routes
// of the forms the proxy passes a domain on by and nothing else.
func validateAnswers(req *pb.SetClusterForwardersRequest, st *forwardState) error {
	if len(req.GetAnswers()) > maxAnswers {
		return fmt.Errorf("at most %d Apps answer on every server", maxAnswers)
	}
	seen, ports, hosts := map[string]bool{}, map[uint32]bool{}, map[string]string{}
	for _, e := range req.GetAnswers() {
		a := answer{AppID: e.GetAppId(), TLSPort: e.GetTlsPort(), WebPort: e.GetWebPort()}
		if !fwdID.MatchString(a.AppID) {
			return fmt.Errorf("%q is not an App id", a.AppID)
		}
		if seen[a.AppID] {
			return fmt.Errorf("%q is passed on twice", a.AppID)
		}
		seen[a.AppID] = true
		for _, p := range []uint32{a.TLSPort, a.WebPort} {
			if p < 1024 || p > 65535 || ports[p] {
				return fmt.Errorf("the answer of %s needs two ports of its own, from 1024", a.AppID)
			}
			ports[p] = true
		}
		r, err := parseAnswerRoutes(a, e.GetRoutes())
		if err != nil {
			return fmt.Errorf("the answer of %s: %w", a.AppID, err)
		}
		for _, h := range r.hosts() {
			if other, taken := hosts[h]; taken {
				return fmt.Errorf("%s is passed on for both %s and %s", h, other, a.AppID)
			}
			hosts[h] = a.AppID
		}
		a.Routes = r
		st.Answers = append(st.Answers, a)
	}
	for _, id := range req.GetServeAnswers() {
		if !fwdID.MatchString(id) {
			return fmt.Errorf("%q is not an App id", id)
		}
		st.ServeAnswers = append(st.ServeAnswers, id)
	}
	port := req.GetAnswerPort()
	if port > 65535 || (port == 0 && len(st.Answers)+len(st.ServeAnswers) > 0) {
		return fmt.Errorf("passing domains on needs the port of the proxy's internal entrypoint")
	}
	st.AnswerPort = port
	return nil
}

// reconcileAnswers runs the one answer forwarder and writes the proxy's routes file; the caller holds f.mu.
func (f *forwarders) reconcileAnswers(ctx context.Context, wanted map[string]bool, note func(error)) {
	note(f.writeAnswerRoutes())
	if len(f.st.Answers) == 0 {
		return
	}
	ops := f.ops()
	name := answerContainer()
	wanted[name] = true
	l := f.listeners[name]
	if l == nil {
		ln, err := ops.listen(f.dir(name))
		if err != nil {
			note(fmt.Errorf("the answer forwarder: %w", err))
			return
		}
		l = &heldListener{ln: ln, dir: f.dir(name)}
		f.listeners[name] = l
		go f.acceptAnswers(l)
	}
	note(ops.ensure(ctx, forwardSpec{Container: name, Network: answerNetwork, Alias: answerAlias,
		Port: answerListen, Dir: l.dir}))
}

// writeAnswerRoutes rewrites the proxy's routes file, which the proxy watches by inode, so in place. It never
// empties it on the way: the new routes, padded to the old length, go over the old ones before the end is cut.
// A proxy reading a moment between sees a file it cannot parse, and keeps the routes it had.
func (f *forwarders) writeAnswerRoutes() error {
	dir := f.s.traefikDir()
	if dir == "" {
		return nil
	}
	path := filepath.Join(dir, answerRoutesFile)
	want := renderAnswerRoutes(f.st.Answers)
	if have, err := os.ReadFile(path); err == nil && bytes.Equal(have, want) {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return rewriteInPlace(path, want)
}

func rewriteInPlace(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return err
	}
	body := data
	if pad := st.Size() - int64(len(data)); pad > 0 {
		body = append(slices.Clone(data), bytes.Repeat([]byte(" "), int(pad))...)
	}
	if _, err := file.WriteAt(body, 0); err != nil {
		return err
	}
	return file.Truncate(int64(len(data)))
}

func (f *forwarders) acceptAnswers(l *heldListener) {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		if !f.admit() {
			c.Close()
			continue
		}
		go func() {
			defer f.leave()
			f.serveAnswer(c)
		}()
	}
}

// admit counts a connection through this agent, or refuses one past answerMaxConns.
func (f *forwarders) admit() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conns >= answerMaxConns {
		return false
	}
	f.conns++
	return true
}

func (f *forwarders) leave() {
	f.mu.Lock()
	f.conns--
	f.mu.Unlock()
}

// answerAt is the App and the visitor's port behind one of the forwarder's ports; the caller holds f.mu.
func (f *forwarders) answerAt(dialed uint32) (string, uint32, bool) {
	for _, a := range f.st.Answers {
		switch dialed {
		case a.TLSPort:
			return a.AppID, answerTLS, true
		case a.WebPort:
			return a.AppID, answerWeb, true
		}
	}
	return "", 0, false
}

// serveAnswer carries one connection the proxy passed on, to a server that runs the App and is ready. While none
// is (a failover under way, an App still starting) the connection waits, unanswered, up to answerWait, then
// closes: never a certificate the domain does not have, or a proxy without its route. Waiting costs nothing per
// connection: one look per App finds the route for all of them.
func (f *forwarders) serveAnswer(c net.Conn) {
	defer c.Close()
	in, dialed := readDialedPort(c, 0)
	f.mu.Lock()
	appID, port, found := f.answerAt(dialed)
	cluster, members := f.st.Cluster, f.st.Members
	f.mu.Unlock()
	if !found {
		return
	}
	deadline := time.Now().Add(answerWait)
	for {
		if f.mayTry(appID) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			up, via := f.reachAnswer(ctx, cluster, members, appID, port)
			if up != nil {
				forwarder.Join(in, up)
				f.release(via)
				cancel()
				return
			}
			cancel()
			f.noRoute(appID)
		}
		wake, ok := f.await(appID)
		if !ok {
			return
		}
		t := time.NewTimer(time.Until(deadline))
		select {
		case <-wake:
		case <-t.C:
		}
		t.Stop()
		f.unawait(appID)
		if !time.Now().Before(deadline) {
			return
		}
	}
}

// mayTry says whether a connection should try for a route itself: not while a look has just found none.
func (f *forwarders) mayTry(appID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	g := f.gates[appID]
	return g == nil || time.Since(g.tried) >= answerRetry
}

func (f *forwarders) noRoute(appID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gate(appID).tried = time.Now()
}

// gate is one App's waiting room; the caller holds f.mu.
func (f *forwarders) gate(appID string) *answerGate {
	g := f.gates[appID]
	if g == nil {
		g = &answerGate{wake: make(chan struct{})}
		f.gates[appID] = g
	}
	return g
}

// await joins the App's waiting connections, under both caps, and starts the look for a route if none runs.
func (f *forwarders) await(appID string) (<-chan struct{}, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g := f.gate(appID)
	if g.waiting >= answerWaitPerApp || f.waiting >= answerWaitPerServer {
		return nil, false
	}
	g.waiting++
	f.waiting++
	if !g.looking {
		g.looking = true
		go f.look(appID)
	}
	return g.wake, true
}

func (f *forwarders) unawait(appID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if g := f.gates[appID]; g != nil {
		g.waiting--
		f.waiting--
	}
}

// look is the one search for a route an App's waiting connections share: every answerRetry, while any waits, it
// asks whether this server or another one would answer, and wakes them all when one would.
func (f *forwarders) look(appID string) {
	for {
		time.Sleep(answerRetry)
		f.mu.Lock()
		g := f.gates[appID]
		if g == nil || g.waiting == 0 {
			if g != nil {
				g.looking = false
				delete(f.gates, appID)
			}
			f.mu.Unlock()
			return
		}
		cluster, members := f.st.Cluster, f.st.Members
		f.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		open := f.routeOpen(ctx, cluster, members, appID)
		cancel()
		if open {
			f.mu.Lock()
			g.tried = time.Time{}
			close(g.wake)
			g.wake = make(chan struct{})
			f.mu.Unlock()
		}
	}
}

// routeOpen asks, without connecting anyone, whether this server or another that runs the App would answer now.
func (f *forwarders) routeOpen(ctx context.Context, cluster string, members map[string]string, appID string) bool {
	here, others := f.s.leaseGuard().holders(cluster, "app", appID)
	if here && f.ready(ctx, appID) {
		return true
	}
	h := heldName{Network: answerNetwork, Kind: answerKind, ID: appID, Port: answerTLS}
	for _, m := range others {
		if c, err := f.dialMember(ctx, members[m], h, answerProbe); err == nil {
			c.Close()
			return true
		}
	}
	return false
}

// reachAnswer tries every server the leader placed the App on: this one first when it is ready, then the others,
// the one this agent has the fewest connections open to first. A server that refuses is passed over.
func (f *forwarders) reachAnswer(ctx context.Context, cluster string, members map[string]string, appID string,
	port uint32) (net.Conn, string) {
	here, others := f.s.leaseGuard().holders(cluster, "app", appID)
	if here && f.ready(ctx, appID) {
		if up, err := f.dialProxy(ctx, port); err == nil {
			return up, ""
		}
	}
	h := heldName{Network: answerNetwork, Kind: answerKind, ID: appID, Port: answerTLS}
	return f.dialHolders(others, func(member string) (net.Conn, error) {
		return f.dialMember(ctx, members[member], h, port)
	})
}

// dialProxy reaches this server's own proxy on its internal entrypoint, for a domain's TLS (behind the PROXY
// header the forwarding proxy sent) and for a certificate check alike: Deplo routes the check path there too.
func (f *forwarders) dialProxy(ctx context.Context, port uint32) (net.Conn, error) {
	if port != answerTLS && port != answerWeb {
		return nil, fmt.Errorf("a domain is passed on on 443 and 80 only")
	}
	f.mu.Lock()
	to := f.st.AnswerPort
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

// ready says whether the App answers on this server now, long enough for the proxy to route it. One check runs at
// a time per App and every caller shares it; a yes is kept answerReadyFor, a no until the next look.
func (f *forwarders) ready(ctx context.Context, appID string) bool {
	for {
		f.mu.Lock()
		rc := f.readiness[appID]
		if rc == nil {
			rc = &answerReadyCheck{}
			f.readiness[appID] = rc
		}
		if time.Now().Before(rc.until) {
			ok := rc.ok
			f.mu.Unlock()
			return ok
		}
		if p := rc.pending; p != nil {
			f.mu.Unlock()
			select {
			case <-p:
				continue
			case <-ctx.Done():
				return false
			}
		}
		done := make(chan struct{})
		rc.pending = done
		f.mu.Unlock()

		var r appReadiness
		if ops := f.ops(); ops.ready != nil {
			r = ops.ready(ctx, appID)
		}
		now := time.Now()
		f.mu.Lock()
		since := r.Since
		if !r.OK {
			delete(f.readySeen, appID)
		} else if r.Health {
			if f.readySeen[appID].IsZero() {
				f.readySeen[appID] = now
			}
			if seen := f.readySeen[appID]; seen.After(since) {
				since = seen
			}
		}
		rc.ok = r.OK && now.Sub(since) >= answerSettle
		rc.until = now.Add(answerRetry)
		if rc.ok {
			rc.until = now.Add(answerReadyFor)
		}
		rc.pending = nil
		close(done)
		ok := rc.ok
		f.mu.Unlock()
		return ok
	}
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

// serveAnswerStream answers another member's passed-on connection: only for an App Deplo said this server answers
// for, that the leader placed here and that is ready, from a member of this cluster, on 443 or 80. A probe gets
// that answer alone.
func (f *forwarders) serveAnswerStream(from wire.Party, c net.Conn, network, appID string, port uint32) {
	if port == 0 {
		port = answerTLS
	}
	if !f.authorizeAnswer(from, network, appID) || (port != answerTLS && port != answerWeb && port != answerProbe) {
		_ = wire.WriteHeader(c, wire.VerbErr, "not served")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if !f.ready(ctx, appID) {
		_ = wire.WriteHeader(c, wire.VerbErr, "not ready")
		return
	}
	if port == answerProbe {
		_ = wire.WriteHeader(c, wire.VerbOK, "")
		return
	}
	if !f.admit() {
		_ = wire.WriteHeader(c, wire.VerbErr, "busy")
		return
	}
	defer f.leave()
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

func (f *forwarders) authorizeAnswer(from wire.Party, network, appID string) bool {
	f.mu.Lock()
	st := f.st
	f.mu.Unlock()
	if from.Role != wire.RoleMember || network != answerNetwork || !slices.Contains(st.ServeAnswers, appID) {
		return false
	}
	inCluster := false
	for _, fp := range st.Members {
		inCluster = inCluster || fp == from.Fingerprint
	}
	here, _ := f.s.leaseGuard().holders(st.Cluster, "app", appID)
	return inCluster && here
}
