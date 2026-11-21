package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/hostmetrics"
)

// failoverCert is one pair of a failover's bundle, as Deplo seals it.
type failoverCert struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
}

// certSettle is how long the proxy takes to load a routes file it watches; a var so tests can shorten it.
var certSettle = answerSettle

// installCerts puts a failover's certificates in the routes file, for the proxy to serve before the App's router
// asks it to order one. Only a pair for hostnames this server passes on goes in, and only one newer than what the
// proxy has for the same names.
func (f *forwarders) installCerts(raw json.RawMessage, now time.Time) (int, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	var list []failoverCert
	if json.Unmarshal(raw, &list) != nil {
		return 0, errors.New("the bundle's certificates are unreadable")
	}
	stored := f.storeCerts()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadFiles()
	answered := map[string]bool{}
	for _, a := range f.st.Answers {
		if a.Routes != nil {
			for _, h := range a.Routes.hosts() {
				answered[h] = true
			}
		}
	}
	n := 0
	for _, c := range list {
		pc, err := parseProxyCert(c.Cert, c.Key)
		if err != nil || !pc.NotAfter.After(now) || len(pc.Names) == 0 {
			continue
		}
		if slices.ContainsFunc(pc.Names, func(h string) bool { return !answered[h] }) {
			continue
		}
		newer := func(o proxyCert) bool { return sameNames(o.Names, pc.Names) && !o.NotAfter.Before(pc.NotAfter) }
		if slices.ContainsFunc(stored, newer) || slices.ContainsFunc(f.files, newer) {
			continue
		}
		f.files = slices.DeleteFunc(f.files, func(o proxyCert) bool { return sameNames(o.Names, pc.Names) })
		pc.installed = now
		f.files = append(f.files, pc)
		n++
	}
	if n == 0 {
		return 0, nil
	}
	return n, f.writeAnswerRoutes()
}

// installedCerts is the certificates installed as files.
func (f *forwarders) installedCerts() []proxyCert {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadFiles()
	return slices.Clone(f.files)
}

// storeCerts is the pairs of the proxy's ACME store, or none when it cannot be read.
func (f *forwarders) storeCerts() []proxyCert {
	acme, err := readProxyACME(f.s.traefikCompose())
	if err != nil {
		return nil
	}
	st, err := readACMEStore(acme.Store)
	if err != nil {
		return nil
	}
	return st.certs()
}

// storePlan is what the proxy's next start puts in its store: every installed pair of an App that runs here, and
// no entry for the hostnames of an App passed on here that does not run here.
func (f *forwarders) storePlan(ctx context.Context) storeMerge {
	f.mu.Lock()
	f.loadFiles()
	files, answers, cluster := slices.Clone(f.files), slices.Clone(f.st.Answers), f.st.Cluster
	f.mu.Unlock()
	var m storeMerge
	var running []string
	if runs := f.ops().runs; runs != nil {
		for _, a := range answers {
			if a.Routes == nil {
				continue
			}
			here, err := runs(ctx, a.AppID)
			if err != nil {
				continue
			}
			if here {
				// Only once a lease heard since this agent started places it here: a copy left running after a move
				// would renew a certificate the cluster no longer asks of this server (#266).
				if f.placedHere(cluster, a.AppID) {
					running = append(running, a.Routes.hosts()...)
				}
				continue
			}
			// Only an App the leader runs elsewhere: one stopped on its own server keeps its certificate.
			if _, others := f.s.leaseGuard().answerHolders(cluster, a.AppID); len(others) > 0 {
				m.drop = append(m.drop, a.Routes.hosts()...)
			}
		}
	}
	zv := f.s.currentZoneView()
	for _, c := range files {
		if zone := zv.zoneOf(c); zone != "" {
			// A zone's certificate goes into the store only where it is renewed; elsewhere it stays a file.
			if zv.renew[zone] {
				m.keep = append(m.keep, c)
			}
			continue
		}
		if !slices.ContainsFunc(c.Names, func(h string) bool { return !slices.Contains(running, h) }) {
			m.keep = append(m.keep, c)
		}
	}
	for key, names := range zv.sets {
		if zv.renew[key] || zv.unsure[key] {
			m.protect = append(m.protect, names)
		} else {
			m.dropSets = append(m.dropSets, names)
		}
	}
	if len(zv.covered) > 0 {
		m.covered = zv.covered
	}
	m.dns = zv.dns
	if acme, err := readProxyACME(f.s.traefikCompose()); err == nil {
		if st, err := readACMEStore(acme.Store); err == nil {
			for _, name := range st.order {
				for _, e := range st.entries(name) {
					if c, err := parseProxyCert(string(e.Certificate), string(e.Key)); err == nil && !m.drops(c) && m.misfiledUnder(name, c, acme) {
						m.misfiled = append(m.misfiled, c)
					}
				}
			}
		}
	}
	return m
}

// staleAfter is how long an installed pair of an App that does not run here is kept: a failover installs it a
// moment before the App starts.
var staleAfter = 15 * time.Minute

// pruneFiles takes out of the routes file, live, a pair that expired and one of an App that has not run here
// since it was installed. Neither needs the proxy stopped: only its store is erased by a running proxy.
func (f *forwarders) pruneFiles(ctx context.Context, now time.Time) {
	f.mu.Lock()
	f.loadFiles()
	files, answers := slices.Clone(f.files), slices.Clone(f.st.Answers)
	f.mu.Unlock()
	runs := f.ops().runs
	var gone []proxyCert
	for _, c := range files {
		zone := f.s.currentZoneView().zoneOf(c)
		if !c.NotAfter.After(now) {
			// Kept while a copy of its App left running here may still ask for it: the file is what stops the order.
			if zone != "" || !f.staleCopyRuns(ctx, answers, c) {
				gone = append(gone, c)
			}
			continue
		}
		if zone != "" {
			// A zone's certificate stays while the zone has one, whichever App runs here.
			if !f.s.currentZoneView().held[zone] {
				gone = append(gone, c)
			}
			continue
		}
		if runs == nil || now.Sub(c.installed) < staleAfter {
			continue
		}
		for _, a := range answers {
			if a.Routes == nil || !slices.ContainsFunc(c.Names, func(h string) bool { return slices.Contains(a.Routes.hosts(), h) }) {
				continue
			}
			if here, err := runs(ctx, a.AppID); err == nil && !here {
				gone = append(gone, c)
			}
			break
		}
	}
	if len(gone) > 0 {
		if err := f.forgetFiles(gone); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: proxy certificates: could not rewrite the routes file: %v\n", err)
		}
	}
}

// placedHere says whether a lease heard since this agent started places the App here, with no return under way.
func (f *forwarders) placedHere(cluster, appID string) bool {
	here, _ := f.s.leaseGuard().answerHolders(cluster, appID)
	return here
}

// staleCopyRuns says whether the App a pair serves runs here although no current lease places it here.
func (f *forwarders) staleCopyRuns(ctx context.Context, answers []answer, c proxyCert) bool {
	runs := f.ops().runs
	if runs == nil {
		return false
	}
	f.mu.Lock()
	cluster := f.st.Cluster
	f.mu.Unlock()
	for _, a := range answers {
		if a.Routes == nil || !slices.ContainsFunc(c.Names, func(h string) bool { return slices.Contains(a.Routes.hosts(), h) }) {
			continue
		}
		here, err := runs(ctx, a.AppID)
		return err == nil && here && !f.placedHere(cluster, a.AppID)
	}
	return false
}

// keepAsFiles puts pairs in the routes file, each unless a file as new already covers the same names.
func (f *forwarders) keepAsFiles(list []proxyCert) error {
	if len(list) == 0 {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadFiles()
	for _, c := range list {
		if slices.ContainsFunc(f.files, func(o proxyCert) bool { return sameNames(o.Names, c.Names) && !o.NotAfter.Before(c.NotAfter) }) {
			continue
		}
		f.files = slices.DeleteFunc(f.files, func(o proxyCert) bool { return sameNames(o.Names, c.Names) })
		c.installed = time.Now()
		f.files = append(f.files, c)
	}
	return f.writeAnswerRoutes()
}

// forgetFiles takes pairs out of the routes file: ones now in the store (the proxy is stopped then), or pruned.
func (f *forwarders) forgetFiles(gone []proxyCert) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = slices.DeleteFunc(f.files, func(c proxyCert) bool {
		return slices.ContainsFunc(gone, func(g proxyCert) bool { return g.Sum == c.Sum })
	})
	return f.writeAnswerRoutes()
}

// stopProxy stops this server's proxy; a var so tests can answer without Docker.
var stopProxy = func(ctx context.Context) error {
	res, err := dockercli.Run(ctx, 60*time.Second, "stop", "-t", "15", traefikContainer)
	if err != nil {
		return err
	}
	if res.Code != 0 {
		return fmt.Errorf("could not stop the proxy: %s", firstLine(res.Stderr))
	}
	return nil
}

// proxyStartMu keeps two starts of the proxy, and their store changes, from crossing.
var proxyStartMu sync.Mutex

// startProxy (re)starts the proxy as applyTraefik does, and when its store must change it stops the proxy first
// and changes the store in between: a running proxy erases what it did not write. It returns the hostnames whose
// pair went into the store.
func (s *Service) startProxy(ctx context.Context, path string, restartOnly bool) ([]string, error) {
	proxyStartMu.Lock()
	defer proxyStartMu.Unlock()
	f := s.forwarders()
	plan := f.storePlan(ctx)
	var merged []string
	if !plan.empty() {
		merged = s.mergeStore(ctx, path, plan)
	}
	return merged, s.applyTraefik(ctx, path, restartOnly)
}

func (s *Service) mergeStore(ctx context.Context, path string, plan storeMerge) []string {
	acme, err := readProxyACME(path)
	if err != nil {
		return nil
	}
	st, err := readACMEStore(acme.Store)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: proxy certificates: %v\n", err)
		return nil
	}
	if err := stopProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: proxy certificates: %v\n", err)
		return nil
	}
	changed, zones := plan.apply(st, acme)
	if changed {
		if err := st.write(acme.Store); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: proxy certificates: could not write the store: %v\n", err)
			return nil
		}
	}
	if err := s.forwarders().forgetFiles(plan.keep); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: proxy certificates: could not rewrite the routes file: %v\n", err)
	}
	// A zone's certificate this server no longer renews keeps being served, from the routes file.
	if err := s.forwarders().keepAsFiles(zones); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: proxy certificates: could not rewrite the routes file: %v\n", err)
	}
	var hosts []string
	for _, c := range append(slices.Clone(plan.keep), plan.misfiled...) {
		hosts = append(hosts, c.Names...)
	}
	return hosts
}

// restartAt is when the proxy restarts so that its store holds an installed pair before deadline, the renewal
// window: the last quiet hour that leaves a margin, or at once (urgent) when none is left.
func restartAt(now, deadline time.Time, quiet int) (time.Time, bool) {
	margin := time.Hour
	if deadline.Sub(now) > 48*time.Hour {
		margin = 24 * time.Hour
	}
	latest := deadline.Add(-margin)
	c := time.Date(latest.Year(), latest.Month(), latest.Day(), quiet, 0, 0, 0, latest.Location())
	if c.After(latest) {
		c = c.AddDate(0, 0, -1)
	}
	if !c.After(now) {
		return now, true
	}
	return c, false
}

// keepProxyCerts restarts the proxy once, when no other start came, before an installed pair's renewal window,
// and keeps the hourly traffic profile the quiet hour is read from.
func (s *Service) keepProxyCerts(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.traffic().sample(now)
			s.forwarders().pruneFiles(ctx, now)
			s.restartForRenewal(ctx, now)
			s.dropStaleEntries(ctx, now)
		}
	}
}

// staleFor is how long the store must have held the certificate of an App that does not run here before the
// proxy restarts to drop it: a move back within it needs no restart.
var staleFor = 2 * time.Minute

// dropStaleEntries restarts the proxy once its store holds the certificate of a passed-on App that runs elsewhere.
// Kept, its renewal would go through on the ACME account's authorization from when the App ran here, with no
// check at all, and the certificate would be issued twice (found on the lab, ADR-0048).
func (s *Service) dropStaleEntries(ctx context.Context, now time.Time) {
	f := s.forwarders()
	plan := f.storePlan(ctx)
	stale := false
	var zoneHosts []string
	for _, c := range f.storeCerts() {
		if plan.drops(c) {
			stale = true
			if !slices.ContainsFunc(plan.drop, c.covers) {
				zoneHosts = append(zoneHosts, c.Names...)
			}
		}
	}
	f.mu.Lock()
	if !stale {
		f.staleSince = time.Time{}
		f.mu.Unlock()
		return
	}
	if f.staleSince.IsZero() {
		f.staleSince = now
	}
	since, tried := f.staleSince, f.renewTried
	f.mu.Unlock()
	if now.Sub(since) < staleFor || now.Sub(tried) < renewRetry {
		return
	}
	if _, err := s.startProxy(ctx, s.traefikCompose(), true); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not restart the proxy to drop a moved App's certificate: %v\n", err)
	}
	left := false
	for _, c := range f.storeCerts() {
		left = left || plan.drops(c)
	}
	f.mu.Lock()
	f.staleSince = time.Time{}
	if left {
		f.renewTried = now
	}
	f.mu.Unlock()
	if !left {
		if len(zoneHosts) > 0 {
			s.proxyRestarts().add(&pb.ProxyRestart{AtUnixMs: now.UnixMilli(), Hosts: zoneHosts, Dropped: true, Zone: true})
		} else {
			s.proxyRestarts().add(&pb.ProxyRestart{AtUnixMs: now.UnixMilli(), Hosts: plan.drop, Dropped: true})
		}
	}
}

// renewRetry is how long a restart for a renewal waits after one that put nothing in the store.
var renewRetry = time.Hour

func (s *Service) restartForRenewal(ctx context.Context, now time.Time) {
	f := s.forwarders()
	// Only a pair of an App that runs here goes into the store; one that does not is pruned instead. A stored pair
	// whose names are now checked another way moves before its window too.
	plan := f.storePlan(ctx)
	files := append(slices.Clone(plan.keep), plan.misfiled...)
	if len(files) == 0 {
		return
	}
	f.mu.Lock()
	tried := f.renewTried
	f.mu.Unlock()
	if now.Sub(tried) < renewRetry {
		return
	}
	path := s.traefikCompose()
	acme, err := readProxyACME(path)
	if err != nil {
		return
	}
	deadline := files[0].NotAfter
	for _, c := range files[1:] {
		if c.NotAfter.Before(deadline) {
			deadline = c.NotAfter
		}
	}
	at, urgent := restartAt(now, deadline.Add(-renewBefore(acme.Duration)), s.traffic().quietHour(now.Location()))
	if now.Before(at) {
		return
	}
	merged, err := s.startProxy(ctx, path, true)
	if len(merged) == 0 {
		f.mu.Lock()
		f.renewTried = now
		f.mu.Unlock()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not restart the proxy for a certificate's renewal: %v\n", err)
		return
	}
	if len(merged) > 0 {
		s.proxyRestarts().add(&pb.ProxyRestart{AtUnixMs: now.UnixMilli(), Hosts: merged, Urgent: urgent})
	}
}

// restartLog is the proxy restarts made for a renewal that Deplo has not read yet, kept on disk.
type restartLog struct {
	mu   sync.Mutex
	path string
}

var (
	restartsOnce sync.Map
	maxRestarts  = 100
)

func (s *Service) proxyRestarts() *restartLog {
	l, _ := restartsOnce.LoadOrStore(s, &restartLog{path: filepath.Join(s.agentDir, "proxy-restarts.json")})
	return l.(*restartLog)
}

type savedRestart struct {
	At      int64    `json:"at"`
	Hosts   []string `json:"hosts"`
	Urgent  bool     `json:"urgent,omitempty"`
	Dropped bool     `json:"dropped,omitempty"`
}

func (l *restartLog) read() []savedRestart {
	var out []savedRestart
	if raw, err := os.ReadFile(l.path); err == nil {
		_ = json.Unmarshal(raw, &out)
	}
	return out
}

func (l *restartLog) write(list []savedRestart) {
	if filepath.Dir(l.path) == "." {
		return
	}
	raw, _ := json.Marshal(list)
	_ = os.WriteFile(l.path, raw, 0o600)
}

func (l *restartLog) add(r *pb.ProxyRestart) {
	l.mu.Lock()
	defer l.mu.Unlock()
	list := append(l.read(), savedRestart{At: r.GetAtUnixMs(), Hosts: r.GetHosts(), Urgent: r.GetUrgent(), Dropped: r.GetDropped()})
	if len(list) > maxRestarts {
		list = list[len(list)-maxRestarts:]
	}
	l.write(list)
}

// take forgets what Deplo says it has recorded and hands it the rest.
func (l *restartLog) take(seen int64) []*pb.ProxyRestart {
	l.mu.Lock()
	defer l.mu.Unlock()
	list := l.read()
	kept := slices.DeleteFunc(slices.Clone(list), func(r savedRestart) bool { return r.At <= seen })
	if len(kept) != len(list) {
		l.write(kept)
	}
	out := make([]*pb.ProxyRestart, 0, len(kept))
	for _, r := range kept {
		out = append(out, &pb.ProxyRestart{AtUnixMs: r.At, Hosts: r.Hosts, Urgent: r.Urgent, Dropped: r.Dropped})
	}
	return out
}

// trafficProfile is this server's network traffic per hour over the last seven days, which the quiet hour is
// read from.
type trafficProfile struct {
	mu      sync.Mutex
	path    string
	Hours   map[int64]uint64 `json:"hours"`
	last    uint64
	hasLast bool
	loaded  bool
}

var trafficOnce sync.Map

// netCounters reads the host's traffic counters; a var so tests can feed it.
var netCounters = hostmetrics.NetCounters

func (s *Service) traffic() *trafficProfile {
	p, _ := trafficOnce.LoadOrStore(s, &trafficProfile{path: filepath.Join(s.agentDir, "traffic-hours.json"), Hours: map[int64]uint64{}})
	return p.(*trafficProfile)
}

func (p *trafficProfile) load() {
	if p.loaded {
		return
	}
	p.loaded = true
	if raw, err := os.ReadFile(p.path); err == nil {
		_ = json.Unmarshal(raw, p)
	}
	if p.Hours == nil {
		p.Hours = map[int64]uint64{}
	}
}

func (p *trafficProfile) sample(now time.Time) {
	rx, tx, ok := netCounters()
	if !ok {
		return
	}
	p.add(now, uint64(rx)+uint64(tx))
}

func (p *trafficProfile) add(now time.Time, total uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.load()
	if p.hasLast && total >= p.last {
		p.Hours[now.Unix()/3600] += total - p.last
	}
	p.last, p.hasLast = total, true
	oldest := now.Add(-7*24*time.Hour).Unix() / 3600
	for h := range p.Hours {
		if h < oldest {
			delete(p.Hours, h)
		}
	}
	if filepath.Dir(p.path) != "." {
		raw, _ := json.Marshal(p)
		_ = os.WriteFile(p.path, raw, 0o600)
	}
}

// quietHour is the hour of the day with the least traffic on average, in loc; 4 until every hour has been seen.
func (p *trafficProfile) quietHour(loc *time.Location) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.load()
	var sum [24]uint64
	var n [24]int
	for h, b := range p.Hours {
		hod := time.Unix(h*3600, 0).In(loc).Hour()
		sum[hod] += b
		n[hod]++
	}
	best, bestAvg := -1, uint64(0)
	for h := 0; h < 24; h++ {
		if n[h] == 0 {
			return 4
		}
		if avg := sum[h] / uint64(n[h]); best < 0 || avg < bestAvg {
			best, bestAvg = h, avg
		}
	}
	return best
}
