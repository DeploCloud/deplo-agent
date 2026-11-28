package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/forwarder"
	"github.com/DeploCloud/deplo-agent/internal/hostinfo"
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
	answerWait = 30 * time.Second
	// answerStuck is how long a container may sit created and never started before its App counts as broken.
	answerStuck = 30 * time.Second
	answerRetry = 500 * time.Millisecond
	// answerSettle is how long an App has been ready before its proxy is sure to route it: Traefik applies a
	// container change up to 2 s after it happens.
	answerSettle = 3 * time.Second
	// answerSettleAtOnce is answerSettle for a proxy whose providers are not throttled: it routes a change the
	// moment it happens (c41), so a failover's App answers 2 s sooner (c43).
	answerSettleAtOnce = time.Second
	// proxySettle is how long a restarted proxy gets to read its routes: it reads them at its start (c41).
	proxySettle = 500 * time.Millisecond
	// answerReadyFor is how long a ready answer is kept before docker is asked again.
	answerReadyFor = 5 * time.Second
	// proxyDownFor is how long a refused dial to this server's own proxy holds: while its proxy is recreated, the
	// members passing it visitors hear "not answering" at once instead of each connection dialing again (c38).
	proxyDownFor = time.Second
	// localCheck is how often the visitors carried to this server's own proxy are checked against the placement.
	localCheck = time.Second
	// By default at most this many connections pass through this agent at once, waiting or carried (a quarter of
	// them may wait), and this many wait for one App: past either, a connection is closed at once. Deplo can set
	// both per cluster.
	answerMaxConns   = 2048
	answerWaitPerApp = 128
)

// answerMaxCap bounds what a table may set.
const answerMaxCap = 65536

// answerRefusal counts the connections turned away for one reason, and when.
type answerRefusal struct {
	count       uint32
	first, last time.Time
	cap         int
	memory      bool
}

// refusalKey is what a refusal is counted by: the App, or none for this server's caps, and which of them.
type refusalKey struct {
	app     string
	waiting bool
}

// answerHostMemory is this host's memory, read once; tests replace it.
var answerHostMemory = sync.OnceValue(hostinfo.MemTotalBytes)

// answerConnBytes is what one carried connection may cost this agent: two copy buffers of 32 KB.
const answerConnBytes = 64 << 10

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
	broken  bool
	until   time.Time
	pending chan struct{}
}

// appReadiness is what docker says of an App's containers here.
type appReadiness struct {
	OK bool
	// Since is when the last container started, or passed the oldest health check docker keeps. Health says one
	// is healthy with no such check to tell since when: then it counts from the moment it is seen so.
	Since  time.Time
	Health bool
	// Broken: it runs here but waiting will not make it ready, a health check failing or a container that never
	// started. Its visitors are passed on at once, to the App's own error, rather than held.
	Broken bool
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
	conns, waiting := req.GetAnswerMaxConns(), req.GetAnswerMaxWaiting()
	if conns > answerMaxCap || (conns > 0 && conns < 4) || waiting > answerMaxCap {
		return fmt.Errorf("the answers' connection caps are out of range")
	}
	st.AnswerMaxConns, st.AnswerMaxWaiting = conns, waiting
	return nil
}

// answerCaps is this cluster's caps: connections through this agent, waiting for all of its Apps, waiting for one.
// The caller holds f.mu.
// A cluster's cap never takes more than a quarter of this server's memory: one that joined later, or is smaller
// than the one the cap was checked against, holds it to what fits (memory says so).
func (f *forwarders) answerCaps() (conns, waiting, perApp int, memory bool) {
	conns, perApp = answerMaxConns, answerWaitPerApp
	if f.st.AnswerMaxConns > 0 {
		conns = int(f.st.AnswerMaxConns)
	}
	if f.st.AnswerMaxWaiting > 0 {
		perApp = int(f.st.AnswerMaxWaiting)
	}
	if fit := int(answerHostMemory() / 4 / answerConnBytes); fit > 0 && fit < conns {
		conns, memory = max(fit, 4), true
	}
	return conns, max(conns/4, 1), perApp, memory
}

// refuse counts one connection turned away, by App or, empty, past one of this server's caps; the caller holds f.mu.
func (f *forwarders) refuse(k refusalKey, limit int, memory bool) {
	r := f.refused[k]
	now := time.Now()
	if r == nil {
		r = &answerRefusal{first: now}
		f.refused[k] = r
	}
	r.count++
	r.last, r.cap, r.memory = now, limit, memory
}

// takeRefusals hands Deplo what was turned away since the last table, and forgets it; the caller holds f.mu.
func (f *forwarders) takeRefusals() []*pb.ClusterAnswerRefusal {
	out := make([]*pb.ClusterAnswerRefusal, 0, len(f.refused))
	for k, r := range f.refused {
		out = append(out, &pb.ClusterAnswerRefusal{AppId: k.app, Count: r.count, FirstUnixMs: r.first.UnixMilli(),
			LastUnixMs: r.last.UnixMilli(), Cap: uint32(r.cap), Waiting: k.waiting, Memory: r.memory})
	}
	slices.SortFunc(out, func(a, b *pb.ClusterAnswerRefusal) int {
		if c := strings.Compare(a.AppId, b.AppId); c != 0 {
			return c
		}
		return strings.Compare(fmt.Sprint(a.Waiting), fmt.Sprint(b.Waiting))
	})
	clear(f.refused)
	return out
}

// reconcileAnswers runs the one answer forwarder and writes the proxy's routes file; the caller holds f.dock.
func (f *forwarders) reconcileAnswers(ctx context.Context, st forwardState, wanted map[string]bool, note func(error)) {
	atOnce := f.s.proxyAppliesAtOnce()
	f.mu.Lock()
	f.atOnce = atOnce
	note(f.writeAnswerRoutes())
	f.mu.Unlock()
	if len(st.Answers) == 0 {
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
	f.loadFiles()
	want := renderAnswerRoutes(f.st.Answers, f.files, f.zoneRoutersLocked())
	if have, err := os.ReadFile(path); err == nil && bytes.Equal(have, want) {
		return os.Chmod(path, 0o600)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return rewriteInPlace(path, want)
}

// loadFiles reads back, once, the certificates an earlier run of this agent installed in the routes file.
// The caller holds f.mu.
func (f *forwarders) loadFiles() {
	if f.filesLoaded {
		return
	}
	f.filesLoaded = true
	dir := f.s.traefikDir()
	if dir == "" {
		return
	}
	raw, err := os.ReadFile(filepath.Join(dir, answerRoutesFile))
	if err != nil {
		return
	}
	var r answerRoutes
	if json.Unmarshal(raw, &r) != nil || r.TLS == nil {
		return
	}
	for _, c := range r.TLS.Certificates {
		if pc, err := parseProxyCert(c.CertFile, c.KeyFile); err == nil {
			pc.installed = time.Now()
			f.files = append(f.files, pc)
		}
	}
}

// The routes file can hold a key, so only its owner reads it; the proxy reads it as root.
func rewriteInPlace(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Chmod(0o600); err != nil {
		return err
	}
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

// admit counts a connection through this agent, or refuses one past this server's cap.
func (f *forwarders) admit() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if conns, _, _, memory := f.answerCaps(); f.conns >= conns {
		f.refuse(refusalKey{}, conns, memory)
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
	f.answerFor(in, cluster, members, appID, port, "")
}

// answerFor waits for a server that runs the App and carries the visitor there, its PROXY header first when the
// connection does not start with one (a visitor held at this server's front door).
func (f *forwarders) answerFor(in net.Conn, cluster string, members map[string]string, appID string, port uint32, header string) {
	deadline := time.Now().Add(answerWait)
	for {
		if f.mayTry(appID) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			up, via := f.reachAnswer(ctx, cluster, members, appID, port)
			if up != nil && header != "" {
				if _, err := io.WriteString(up, header); err != nil {
					up.Close()
					up = nil
					f.release(via)
				}
			}
			if up != nil {
				if via == "" {
					f.carry(appID, in, up)
				} else {
					forwarder.Join(moving{in}, moving{up})
				}
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
	_, waiting, perApp, memory := f.answerCaps()
	if g.waiting >= perApp {
		f.refuse(refusalKey{app: appID, waiting: true}, perApp, false)
		return nil, false
	}
	if f.waiting >= waiting {
		f.refuse(refusalKey{waiting: true}, waiting, memory)
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
	here, others := f.s.leaseGuard().visitorHolders(cluster, appID)
	if here && f.answers(ctx, appID) {
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
	here, others := f.s.leaseGuard().visitorHolders(cluster, appID)
	if here && f.ready(ctx, appID) {
		if up, err := f.dialProxy(ctx, port); err == nil {
			return up, ""
		}
	}
	h := heldName{Network: answerNetwork, Kind: answerKind, ID: appID, Port: answerTLS}
	up, via := f.dialHolders(others, func(member string) (net.Conn, error) {
		return f.dialMember(ctx, members[member], h, port)
	})
	if up == nil && here && f.broken(appID) {
		if up, err := f.dialProxy(ctx, port); err == nil {
			return up, ""
		}
	}
	return up, via
}

// dialProxy reaches this server's own proxy on its internal entrypoint, for a domain's TLS (behind the PROXY
// header the forwarding proxy sent) and for a certificate check alike: Deplo routes the check path there too.
func (f *forwarders) dialProxy(ctx context.Context, port uint32) (net.Conn, error) {
	if port != answerTLS && port != answerWeb {
		return nil, fmt.Errorf("a domain is passed on on 443 and 80 only")
	}
	f.mu.Lock()
	to := f.st.AnswerPort
	settling, standby := f.settling()
	f.mu.Unlock()
	if to == 0 {
		return nil, fmt.Errorf("this server's proxy has no internal entrypoint")
	}
	if settling && standby != nil {
		return standby(ctx, to)
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
		// A proxy restart changes no container: what was found ready stays, behind the restart and its settle,
		// and answers through the standby proxy when one runs.
		settling, standby := f.settling()
		if f.replacing[appID] > 0 || (settling && standby == nil) {
			f.mu.Unlock()
			return false
		}
		rc := f.readiness[appID]
		if rc == nil {
			rc = &answerReadyCheck{}
			f.readiness[appID] = rc
		}
		// Nor does it ask docker again meanwhile, busy recreating the proxy (c43: a look then took 3 s).
		if settling && rc.ok {
			f.mu.Unlock()
			return true
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
		f.mu.Lock()
		f.noteReadiness(appID, rc, r, time.Now())
		rc.pending = nil
		close(done)
		ok := rc.ok
		f.mu.Unlock()
		return ok
	}
}

// noteReadiness keeps what one look found of an App here; the caller holds f.mu.
func (f *forwarders) noteReadiness(appID string, rc *answerReadyCheck, r appReadiness, now time.Time) {
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
	rc.ok = r.OK && now.Sub(since) >= f.settleFor()
	rc.broken = r.Broken
	rc.until = now.Add(answerRetry)
	if rc.ok {
		rc.until = now.Add(answerReadyFor)
		f.keepHandover()
	}
}

// settleFor is how long an App runs before this server's proxy surely routes it; the caller holds f.mu.
func (f *forwarders) settleFor() time.Duration {
	if f.atOnce {
		return min(answerSettle, answerSettleAtOnce)
	}
	return answerSettle
}

// proxyAppliesAtOnce says this server's proxy is not throttled, so it routes a container change at once.
func (s *Service) proxyAppliesAtOnce() bool {
	raw, err := os.ReadFile(s.traefikCompose())
	return err == nil && bytes.Contains(raw, []byte("--providers.providersthrottleduration=0"))
}

// broken says whether the App's last readiness check here found it running but broken.
func (f *forwarders) broken(appID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	rc := f.readiness[appID]
	return rc != nil && rc.broken
}

// answers says whether this server takes the App's visitors now: ready, or broken, so waiting would not help.
func (f *forwarders) answers(ctx context.Context, appID string) bool {
	return f.ready(ctx, appID) || f.broken(appID)
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
	if restarting.Load() || !f.answers(ctx, appID) || f.proxyIsDown() {
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
	f.proxyAnswered(appID, err)
	if err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, "not answering")
		return
	}
	if wire.WriteHeader(c, wire.VerbOK, "") != nil {
		up.Close()
		return
	}
	f.passedOn.Do(func() {
		stderrf("deplo-agent: cluster: first visitor passed on here %d ms after the agent started", time.Since(agentStarted).Milliseconds())
	})
	f.carry(appID, c, up)
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
		inCluster = inCluster || from.Is(fp)
	}
	here, _ := f.s.leaseGuard().visitorHolders(st.Cluster, appID)
	return inCluster && here
}
