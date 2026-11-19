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
	files, answers := slices.Clone(f.files), slices.Clone(f.st.Answers)
	f.mu.Unlock()
	var m storeMerge
	if runs := f.ops().runs; runs != nil {
		for _, a := range answers {
			if a.Routes == nil {
				continue
			}
			if here, err := runs(ctx, a.AppID); err == nil && !here {
				m.drop = append(m.drop, a.Routes.hosts()...)
			}
		}
	}
	for _, c := range files {
		if !slices.ContainsFunc(c.Names, func(h string) bool { return slices.Contains(m.drop, h) }) {
			m.keep = append(m.keep, c)
		}
	}
	return m
}

// forgetFiles takes the pairs now in the store out of the routes file; the proxy is stopped meanwhile.
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
	if plan.apply(st, acme.Resolver) {
		if err := st.write(acme.Store); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: proxy certificates: could not write the store: %v\n", err)
			return nil
		}
	}
	if err := s.forwarders().forgetFiles(plan.keep); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: proxy certificates: could not rewrite the routes file: %v\n", err)
	}
	var hosts []string
	for _, c := range plan.keep {
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
			s.restartForRenewal(ctx, now)
		}
	}
}

func (s *Service) restartForRenewal(ctx context.Context, now time.Time) {
	files := s.forwarders().installedCerts()
	if len(files) == 0 {
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
	At     int64    `json:"at"`
	Hosts  []string `json:"hosts"`
	Urgent bool     `json:"urgent,omitempty"`
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
	list := append(l.read(), savedRestart{At: r.GetAtUnixMs(), Hosts: r.GetHosts(), Urgent: r.GetUrgent()})
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
		out = append(out, &pb.ProxyRestart{AtUnixMs: r.At, Hosts: r.Hosts, Urgent: r.Urgent})
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
