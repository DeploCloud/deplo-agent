package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/dnszone"
)

// A cluster's DNS zone (ADR-0049): this agent decides who is healthy and the majority, and hands the answers to an
// unprivileged name server process it starts (internal/dnszone). https://deplo.build/docs/operations/clusters/dns-zone
const (
	zoneDefaultTTL   = 30
	zoneDefaultDrop  = 10
	maxZoneServers   = 64
	maxZoneAddresses = 8
	zoneTick         = time.Second
	zoneRebindEvery  = 10 * time.Second
)

type zoneMember struct {
	Fingerprint string   `json:"fingerprint"`
	V4          []string `json:"v4,omitempty"`
	V6          []string `json:"v6,omitempty"`
	Emptying    bool     `json:"emptying,omitempty"`
	// Where it is (phase 2), when Deplo knows.
	Located bool    `json:"located,omitempty"`
	Lat     float64 `json:"lat,omitempty"`
	Lon     float64 `json:"lon,omitempty"`
}

type zoneEntry struct {
	Name  string   `json:"name"`
	Names []string `json:"names"`
	// Homes: the member that runs each name's App, by certificate, when it runs on one server at a time.
	Homes map[string]string `json:"homes,omitempty"`
}

// zoneCluster is what Deplo said for one cluster, and the last set this server agreed on for it.
type zoneCluster struct {
	Self        string          `json:"self"`
	Zones       []zoneEntry     `json:"zones"`
	Members     []zoneMember    `json:"members"`
	NameServers []string        `json:"nameServers"`
	TTL         uint32          `json:"ttl"`
	DropAfter   uint32          `json:"dropAfter"`
	Serial      uint32          `json:"serial"`
	Last        *dnszone.Agreed `json:"last,omitempty"`
	// Phase 3: the certificate checks the cluster's proxies may answer, and this server's part in each zone's
	// certificate. The pairs Deplo sent with them are never kept here.
	Challenges []zoneChallenge `json:"challenges,omitempty"`
	Certs      []zoneCertRole  `json:"certs,omitempty"`
	pairs      map[string]proxyCert
	// heard says Deplo sent it since this agent started: a role read back from disk may have moved meanwhile.
	heard bool
}

type zoneFile struct {
	Clusters map[string]*zoneCluster `json:"clusters"`
}

type clusterZones struct {
	s  *Service
	mu sync.Mutex
	st zoneFile
	// txt is every certificate check record this server answers, per cluster and per member that set it.
	txt map[string]map[string]txtState
	// certs is how each zone's certificate went on this server's proxy, by zone.
	certs map[string]*zoneCertRun
	authz map[string]*zoneAuthzRun
	// seq numbers this server's own record sets, seqs the last one taken from each member; tended is the last
	// look at the zones' certificates.
	seq    int64
	seqs   map[string]int64
	tended time.Time
	// geo is the digest of the location table on disk, "" for none; geoRead says it was looked at.
	geo      string
	geoRead  bool
	sup      *dnszone.Supervisor
	verdicts map[string]dnszone.Verdict
	// lastHeard and back say when a server was heard again after a silence.
	lastHeard, back map[string]time.Time
	sent            []byte
	since           time.Time
	stop            chan struct{}
	done            chan struct{}
}

func (s *Service) zones() *clusterZones {
	s.zoneOnce.Do(func() {
		z := &clusterZones{s: s, verdicts: map[string]dnszone.Verdict{}, txt: map[string]map[string]txtState{},
			certs: map[string]*zoneCertRun{}}
		if s.agentDir != "" {
			if raw, err := os.ReadFile(z.file()); err == nil {
				_ = json.Unmarshal(raw, &z.st)
			}
		}
		if z.st.Clusters == nil {
			z.st.Clusters = map[string]*zoneCluster{}
		}
		s.zone = z
		z.publishViewLocked()
	})
	return s.zone
}

func (z *clusterZones) file() string { return filepath.Join(z.s.agentDir, "cluster-zone.json") }

func (z *clusterZones) save() {
	raw, err := json.Marshal(z.st)
	if err == nil {
		err = writeFileAtomic(z.file(), raw, 0o600)
	}
	if err != nil {
		log.Printf("deplo-agent: could not keep the cluster's DNS zone in %s: %v", z.file(), err)
	}
}

// StartClusterZone answers again for the zones kept on disk, with Deplo down or not.
func (s *Service) StartClusterZone() {
	if s.agentDir == "" {
		return
	}
	z := s.zones()
	z.mu.Lock()
	defer z.mu.Unlock()
	z.ensureLocked()
}

func zoneAddr(raw string) (netip.Addr, bool) {
	a, err := netip.ParseAddr(raw)
	if err != nil {
		return a, false
	}
	a = a.Unmap()
	return a, a.Zone() == "" && a.IsGlobalUnicast()
}

func validateZone(req *pb.SetClusterZoneRequest) (*zoneCluster, error) {
	if !fwdID.MatchString(req.GetClusterId()) {
		return nil, errors.New("a cluster id is required")
	}
	if len(req.GetZones()) == 0 {
		return nil, nil
	}
	c := &zoneCluster{Self: req.GetSelfFingerprint(), TTL: req.GetAnswerTtlSeconds(), DropAfter: req.GetDropAfterSeconds()}
	switch {
	case c.TTL == 0:
		c.TTL = zoneDefaultTTL
	case c.TTL < 10 || c.TTL > 300:
		return nil, fmt.Errorf("answers live 10 to 300 s, not %d", c.TTL)
	}
	switch {
	case c.DropAfter == 0:
		c.DropAfter = zoneDefaultDrop
	case c.DropAfter < 5 || c.DropAfter > 60:
		return nil, fmt.Errorf("a silent server leaves the answers after 5 to 60 s, not %d", c.DropAfter)
	}
	if n := len(req.GetServers()); n == 0 || n > maxZoneServers {
		return nil, fmt.Errorf("1 to %d servers, not %d", maxZoneServers, n)
	}
	seen := map[string]bool{}
	for _, srv := range req.GetServers() {
		fp := srv.GetFingerprint()
		if !validFingerprint(fp) || seen[fp] {
			return nil, fmt.Errorf("%q is not one certificate fingerprint", fp)
		}
		seen[fp] = true
		if len(srv.GetAddresses()) > maxZoneAddresses {
			return nil, fmt.Errorf("at most %d addresses for one server", maxZoneAddresses)
		}
		m := zoneMember{Fingerprint: fp, Emptying: srv.GetEmptying()}
		if srv.GetLocated() {
			lat, lon := srv.GetLatitude(), srv.GetLongitude()
			if math.IsNaN(lat) || math.IsNaN(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
				return nil, fmt.Errorf("server %s is placed off the globe", fp)
			}
			m.Located, m.Lat, m.Lon = true, lat, lon
		}
		for _, raw := range srv.GetAddresses() {
			a, ok := zoneAddr(raw)
			if !ok {
				return nil, fmt.Errorf("%q is not an address a server answers on", raw)
			}
			if a.Is4() {
				m.V4 = append(m.V4, a.String())
			} else {
				m.V6 = append(m.V6, a.String())
			}
		}
		c.Members = append(c.Members, m)
	}
	if !seen[c.Self] {
		return nil, errors.New("this server is not one of the cluster's servers")
	}
	if len(req.GetNameServers()) > 4 {
		return nil, errors.New("at most 4 name servers")
	}
	named := map[string]bool{}
	for _, fp := range req.GetNameServers() {
		if !seen[fp] || named[fp] {
			return nil, fmt.Errorf("name server %q is not one of the cluster's servers", fp)
		}
		named[fp] = true
		c.NameServers = append(c.NameServers, fp)
	}
	for _, raw := range req.GetZones() {
		name, err := dnszone.Name(raw.GetName())
		if err != nil {
			return nil, err
		}
		e := zoneEntry{Name: name}
		for _, n := range raw.GetNames() {
			cn, err := dnszone.Name(n)
			if err != nil {
				return nil, err
			}
			e.Names = append(e.Names, cn)
		}
		for _, h := range raw.GetHomes() {
			cn, err := dnszone.Name(h.GetName())
			if err != nil {
				return nil, err
			}
			if !slices.Contains(e.Names, cn) || !seen[h.GetFingerprint()] || e.Homes[cn] != "" {
				return nil, fmt.Errorf("the home of %s is not one member for one of the zone's names", cn)
			}
			if e.Homes == nil {
				e.Homes = map[string]string{}
			}
			e.Homes[cn] = h.GetFingerprint()
		}
		c.Zones = append(c.Zones, e)
	}
	if err := validateZoneCerts(req, c, seen); err != nil {
		return nil, err
	}
	// The process refuses a table that does not hold together: refuse it here, in the RPC's reply.
	if err := dnszone.CheckTable(&dnszone.Table{Zones: c.tableZones(dnszone.Verdict{State: dnszone.StateFail}, nil)}); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *zoneCluster) addrsOf(fp string) dnszone.Addrs {
	for _, m := range c.Members {
		if m.Fingerprint == fp {
			return dnszone.Addrs{V4: m.V4, V6: m.V6}
		}
	}
	return dnszone.Addrs{}
}

// tableZones is what the process answers for this cluster's zones under a verdict, with the check records set.
func (c *zoneCluster) tableZones(v dnszone.Verdict, txt map[string]txtState) []dnszone.Zone {
	ns := make([]dnszone.Addrs, 0, len(c.NameServers))
	for _, fp := range c.NameServers {
		ns = append(ns, c.addrsOf(fp))
	}
	near := c.near(v)
	out := make([]dnszone.Zone, 0, len(c.Zones))
	for _, e := range c.Zones {
		out = append(out, dnszone.Zone{Name: e.Name, Names: e.Names, NameServers: ns, Fail: v.State == dnszone.StateFail,
			Servers: v.Addrs, Near: near, Homes: c.homes(e, v), TXT: c.records(e.Name, txt), TTL: c.TTL, Serial: c.Serial})
	}
	return out
}

// homes are a zone's names answered with their App's server first, while that server is one the verdict names.
func (c *zoneCluster) homes(e zoneEntry, v dnszone.Verdict) []dnszone.Home {
	if v.State == dnszone.StateFail || len(e.Homes) == 0 {
		return nil
	}
	want := map[string]bool{}
	for _, a := range append(append([]string{}, v.Addrs.V4...), v.Addrs.V6...) {
		want[a] = true
	}
	var out []dnszone.Home
	for _, n := range e.Names {
		fp := e.Homes[n]
		if fp == "" || !slices.Contains(v.Servers, fp) {
			continue
		}
		h := dnszone.Home{Name: n}
		a := c.addrsOf(fp)
		for _, ip := range a.V4 {
			if want[ip] {
				h.V4 = append(h.V4, ip)
			}
		}
		for _, ip := range a.V6 {
			if want[ip] {
				h.V6 = append(h.V6, ip)
			}
		}
		if len(h.V4)+len(h.V6) > 0 {
			out = append(out, h)
		}
	}
	return out
}

// near is the servers of a verdict one by one, where each is; none unless every one of them has a place, since a
// server with none cannot be ranked and leaving it out would be a guess.
func (c *zoneCluster) near(v dnszone.Verdict) []dnszone.Place {
	if v.State == dnszone.StateFail || len(v.Servers) == 0 {
		return nil
	}
	want := map[string]bool{}
	for _, a := range append(append([]string{}, v.Addrs.V4...), v.Addrs.V6...) {
		want[a] = true
	}
	byFP := map[string]zoneMember{}
	for _, m := range c.Members {
		byFP[m.Fingerprint] = m
	}
	var out []dnszone.Place
	named := 0
	for _, fp := range v.Servers {
		m, ok := byFP[fp]
		if !ok || !m.Located {
			return nil
		}
		p := dnszone.Place{Lat: m.Lat, Lon: m.Lon}
		for _, a := range m.V4 {
			if want[a] {
				p.V4 = append(p.V4, a)
			}
		}
		for _, a := range m.V6 {
			if want[a] {
				p.V6 = append(p.V6, a)
			}
		}
		if len(p.V4)+len(p.V6) == 0 {
			return nil
		}
		named += len(p.V4) + len(p.V6)
		out = append(out, p)
	}
	// The last agreed set may name addresses a server has since changed: then it is answered whole, as agreed.
	if named != len(want) {
		return nil
	}
	return out
}

// sameShape is whether two configurations answer the same records; the SOA serial moves only when they do not.
func sameShape(a, b *zoneCluster) bool {
	return reflect.DeepEqual(a.Zones, b.Zones) && reflect.DeepEqual(a.Members, b.Members) &&
		reflect.DeepEqual(a.NameServers, b.NameServers) && a.TTL == b.TTL
}

// SetClusterZone hands this server its cluster's DNS zones; see the RPC's contract in proto/agent.proto.
func (s *Service) SetClusterZone(_ context.Context, req *pb.SetClusterZoneRequest) (*pb.SetClusterZoneResponse, error) {
	if s.agentDir == "" || !bootstrap.Provisioned(s.agentDir) {
		return nil, status.Error(codes.FailedPrecondition, "this server agent has no certificate to answer for a cluster with")
	}
	c, err := validateZone(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	z := s.zones()
	z.mu.Lock()
	id := req.GetClusterId()
	old := z.st.Clusters[id]
	if c == nil {
		delete(z.st.Clusters, id)
		delete(z.verdicts, id)
		delete(z.txt, id)
	} else {
		if old != nil {
			c.Last, c.Serial = old.Last, old.Serial
		}
		if old == nil || !sameShape(old, c) {
			c.Serial = max(uint32(time.Now().Unix()), c.Serial+1)
		}
		c.heard = true
		z.st.Clusters[id] = c
	}
	z.save()
	z.publishViewLocked()
	z.ensureLocked()
	sup := z.sup
	z.mu.Unlock()
	if sup != nil {
		sup.Rebind()
	}
	if c != nil {
		s.installZonePairs(c)
		c.pairs = nil
	}
	s.refreshZoneRoutes()
	z.mu.Lock()
	defer z.mu.Unlock()
	z.pushLocked(time.Now())
	resp := &pb.SetClusterZoneResponse{}
	if c != nil {
		resp.Certificates = z.certStatesLocked(c, time.Now())
	}
	if z.sup != nil {
		resp.Listening, resp.Problem = z.sup.Listening()
	}
	if v, ok := z.verdicts[id]; ok {
		resp.State, resp.Answering, resp.Heard = v.State, v.Servers, int32(v.Heard)
	}
	resp.GeoSha256 = z.geoLocked()
	return resp, nil
}

func (z *clusterZones) geoFile() string { return filepath.Join(z.s.agentDir, "dns-geo.bin") }

// geoLocked is the digest of the location table on disk, read once; "" when there is none or it does not hold together.
func (z *clusterZones) geoLocked() string {
	if z.geoRead {
		return z.geo
	}
	z.geoRead = true
	raw, err := os.ReadFile(z.geoFile())
	if err != nil {
		return ""
	}
	g, err := dnszone.ParseGeo(raw)
	if err != nil {
		log.Printf("deplo-agent: the location table in %s is refused: %v", z.geoFile(), err)
		return ""
	}
	z.geo = g.Digest()
	return z.geo
}

// SetGeoTable keeps the location table Deplo's release carries and has the name server read it; see proto/agent.proto.
func (s *Service) SetGeoTable(_ context.Context, req *pb.SetGeoTableRequest) (*pb.SetGeoTableResponse, error) {
	if s.agentDir == "" {
		return nil, status.Error(codes.FailedPrecondition, "this server agent has no directory to keep a location table in")
	}
	if len(req.GetTableGz()) > dnszone.MaxGeoBytes {
		return nil, status.Error(codes.InvalidArgument, "the location table is too large")
	}
	zr, err := gzip.NewReader(bytes.NewReader(req.GetTableGz()))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "the location table is not gzip: %v", err)
	}
	raw, err := io.ReadAll(io.LimitReader(zr, dnszone.MaxGeoBytes+1))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "the location table does not unpack: %v", err)
	}
	g, err := dnszone.ParseGeo(raw)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !strings.EqualFold(g.Digest(), req.GetSha256()) {
		return nil, status.Errorf(codes.InvalidArgument, "the location table is %s, not %s as sent", g.Digest(), req.GetSha256())
	}
	z := s.zones()
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.geoLocked() != g.Digest() {
		if err := writeFileAtomic(z.geoFile(), raw, 0o600); err != nil {
			return nil, status.Errorf(codes.Internal, "keeping the location table: %v", err)
		}
		z.geo = g.Digest()
		log.Printf("deplo-agent: the cluster's name server places visitors with location table %d (%s)", g.Release(), g.Digest()[:12])
		if z.sup != nil {
			z.sup.Reload()
		}
	}
	return &pb.SetGeoTableResponse{Sha256: g.Digest(), Release: g.Release()}, nil
}

// ensureLocked runs the name server and its loop while any cluster has a zone here, and stops them when none has.
func (z *clusterZones) ensureLocked() {
	if len(z.st.Clusters) == 0 {
		if z.sup != nil {
			close(z.stop)
			sup, done := z.sup, z.done
			z.sup, z.sent = nil, nil
			z.mu.Unlock()
			<-done
			sup.Close()
			z.mu.Lock()
		}
		return
	}
	if z.sup != nil {
		return
	}
	exe, err := os.Executable()
	if err != nil {
		log.Printf("deplo-agent: the cluster's name server cannot start: %v", err)
		return
	}
	cfg := dnszone.Config{
		Path: exe, Args: []string{"dns"}, Port: 53,
		Root:    filepath.Join(z.s.agentDir, "dns-root"),
		PIDFile: filepath.Join(z.s.agentDir, "dns.pid"),
		GeoPath: z.geoFile(),
		Logf:    func(f string, a ...any) { log.Printf("deplo-agent: "+f, a...) },
	}
	if z.s.zoneConfig != nil {
		z.s.zoneConfig(&cfg)
	}
	z.since = time.Now()
	z.sup = dnszone.Start(cfg)
	z.stop, z.done = make(chan struct{}), make(chan struct{})
	go z.loop(z.stop, z.done, z.sup)
}

func (z *clusterZones) loop(stop, done chan struct{}, sup *dnszone.Supervisor) {
	defer close(done)
	tick := time.NewTicker(zoneTick)
	defer tick.Stop()
	rebind := time.Now()
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			if now.Sub(rebind) >= zoneRebindEvery {
				rebind = now
				sup.Rebind()
			}
			z.mu.Lock()
			z.pushLocked(now)
			z.mu.Unlock()
			z.s.tendZoneCerts(now)
		}
	}
}

// holdLocked is who leaves the answers while still counting: a server heard again after a silence, until it has been
// heard for the removal time, and this one for the removal time after it starts. A proxy just started answers with
// its default certificate, then a 502, for a moment (ADR-0049 lab). The caller holds z.mu.
func (z *clusterZones) holdLocked(now time.Time, heard map[string]time.Time, drop time.Duration, c *zoneCluster) map[string]bool {
	if z.lastHeard == nil {
		z.lastHeard, z.back = map[string]time.Time{}, map[string]time.Time{}
	}
	hold := map[string]bool{}
	for _, m := range c.Members {
		fp := m.Fingerprint
		if fp == c.Self {
			continue
		}
		if at, ok := heard[fp]; ok {
			if prev := z.lastHeard[fp]; !prev.IsZero() && at.Sub(prev) >= drop {
				z.back[fp] = at
			}
			z.lastHeard[fp] = at
		}
		if b, ok := z.back[fp]; ok && now.Sub(b) < drop {
			hold[fp] = true
		}
	}
	// As the others hold it: for the removal time from its start.
	if now.Sub(z.since) < drop {
		hold[c.Self] = true
	}
	return hold
}

// heard is when each server of the private network was last heard, and when this server's network started.
func (s *Service) heard() (map[string]time.Time, time.Time) {
	if s.zoneHeard != nil {
		return s.zoneHeard()
	}
	m := s.network().current()
	if m == nil {
		return nil, time.Time{}
	}
	return m.Heard(), m.Started()
}

// pushLocked decides how each cluster is answered now and hands the process the table when it changed.
func (z *clusterZones) pushLocked(now time.Time) {
	if z.sup == nil {
		return
	}
	heard, netSince := z.s.heard()
	since := z.since
	if netSince.After(since) {
		since = netSince
	}
	ids := make([]string, 0, len(z.st.Clusters))
	for id := range z.st.Clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var table dnszone.Table
	taken := map[string]string{}
	agreed := false
	z.pruneTXTLocked(now)
	for _, id := range ids {
		c := z.st.Clusters[id]
		drop := time.Duration(c.DropAfter) * time.Second
		members := make([]dnszone.Member, 0, len(c.Members))
		for _, m := range c.Members {
			members = append(members, dnszone.Member{Fingerprint: m.Fingerprint, Addrs: dnszone.Addrs{V4: m.V4, V6: m.V6}, Emptying: m.Emptying})
		}
		v := dnszone.DecideHolding(members, c.Self, heard, now, drop, now.Before(since.Add(drop)), c.Last, z.holdLocked(now, heard, drop, c))
		if v.Agreed != nil {
			c.Last, agreed = v.Agreed, true
		}
		if was, ok := z.verdicts[id]; !ok || was.State != v.State {
			log.Printf("deplo-agent: cluster %s: the DNS zone answers %s, hearing %d of %d servers", id, v.State, v.Heard, v.Of)
		}
		z.verdicts[id] = v
		for _, zn := range c.tableZones(v, z.txt[id]) {
			if other, dup := taken[zn.Name]; dup {
				log.Printf("deplo-agent: zone %s is given to clusters %s and %s; %s answers it", zn.Name, other, id, other)
				continue
			}
			taken[zn.Name] = id
			table.Zones = append(table.Zones, zn)
		}
	}
	if agreed {
		z.save()
	}
	raw, _ := json.Marshal(table)
	if string(raw) == string(z.sent) {
		return
	}
	if err := z.sup.SetTable(&table); err != nil {
		log.Printf("deplo-agent: the cluster's DNS answers were refused: %v", err)
		return
	}
	z.sent = raw
}
