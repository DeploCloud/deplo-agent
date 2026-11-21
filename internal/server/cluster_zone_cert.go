package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/miekg/dns"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/dnszone"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// Certificate checks over DNS answered by the cluster's own zone, and each zone's own certificate (ADR-0049
// phase 3): https://deplo.build/docs/operations/clusters/dns-zone
const (
	txtVerb = "TXT"
	// txtKeep bounds how long a check record outlives its check when the proxy never cleans it up.
	txtKeep       = 10 * time.Minute
	maxTXTBody    = 64 << 10
	maxChallenges = 4096
	maxZoneCerts  = 16
	// zoneResolver is the proxy's certificate resolver whose checks go through this agent, over DNS.
	zoneResolver = "deplo-dns"
	// answerEntrypoint is the proxy's internal entrypoint (ADR-0048): the order router is reachable from nowhere.
	answerEntrypoint = "answer"
	zoneRouterRule   = "Host(`deplo-zone-certificate.invalid`)"
)

var (
	// txtWait is how long a check waits for the other members to take its record; a var so tests can shorten it.
	txtWait = 3 * time.Second
	// zoneLogEvery is how often a renewer reads its proxy's log for a failed order, and zoneRetry how long the order
	// router then stays out before the proxy tries again.
	zoneLogEvery = 30 * time.Second
	zoneRetry    = []time.Duration{2 * time.Minute, 6 * time.Minute, 20 * time.Minute, time.Hour}
	zoneTendEach = 10 * time.Second
)

// zoneChallenge is one name a check sets a record at, and the only member whose proxy may set it.
type zoneChallenge struct {
	Asked string `json:"asked"`
	At    string `json:"at"`
	Owner string `json:"owner"`
}

// zoneCertRole is this server's part in one zone's certificate. Zone is the canonical zone name.
type zoneCertRole struct {
	Zone    string   `json:"zone"`
	Renew   bool     `json:"renew,omitempty"`
	Order   bool     `json:"order,omitempty"`
	Sum     string   `json:"sum,omitempty"`
	Covered []string `json:"covered,omitempty"`
}

// txtState is the records one member set: name to value to when it was first set.
type txtState map[string]map[string]time.Time

// txtBody is what a member sends the others: its whole set for one cluster, newest seq wins.
type txtBody struct {
	Seq int64               `json:"seq"`
	TXT map[string][]string `json:"txt"`
}

// zoneCertRun is what this server's proxy did with one zone's certificate lately.
type zoneCertRun struct {
	err      string
	errAt    time.Time
	sum      string
	failures int
	retryAt  time.Time
	logSince time.Time
}

// zoneCertView is what the proxy's store and routes file need of the zones' certificates, kept whole and replaced.
type zoneCertView struct {
	zones   map[string]bool // every zone answered here, bare
	held    map[string]bool // zones with a certificate of their own
	renew   map[string]bool // zones this server renews
	unsure  map[string]bool // zones whose renewer Deplo has not named since this agent started
	order   map[string]bool // zones whose certificate this server's proxy may order now
	covered map[string]bool // hostnames a zone's certificate serves
	dns     map[string]bool // hostnames whose own certificate comes over DNS
}

func bare(n string) string { return strings.TrimSuffix(n, ".") }

// zoneCertNames is the name set of a zone's certificate.
func zoneCertNames(zone string) []string { z := bare(zone); return []string{z, "*." + z} }

// zoneOf is the zone a pair is the certificate of, or "".
func (v *zoneCertView) zoneOf(c proxyCert) string {
	if v == nil || len(c.Names) != 2 {
		return ""
	}
	for z := range v.zones {
		if sameNames(c.Names, zoneCertNames(z)) {
			return z
		}
	}
	return ""
}

func (s *Service) currentZoneView() *zoneCertView {
	if v := s.zoneView.Load(); v != nil {
		return v
	}
	return &zoneCertView{}
}

var sumPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// validateZoneCerts reads the checks and the zones' certificates of a zone request into c.
func validateZoneCerts(req *pb.SetClusterZoneRequest, c *zoneCluster, members map[string]bool) error {
	zones := map[string]bool{}
	for _, e := range c.Zones {
		zones[e.Name] = true
	}
	inZone := func(n string) string {
		for z := range zones {
			if dns.IsSubDomain(z, n) {
				return z
			}
		}
		return ""
	}
	if len(req.GetChallenges()) > maxChallenges {
		return fmt.Errorf("at most %d certificate checks", maxChallenges)
	}
	asked := map[string]bool{}
	for _, ch := range req.GetChallenges() {
		a, err := dnszone.Name(ch.GetAsked())
		if err != nil {
			return err
		}
		at, err := dnszone.Name(ch.GetAt())
		if err != nil {
			return err
		}
		if !strings.HasPrefix(a, dnszone.ChallengeLabel) || !strings.HasPrefix(at, dnszone.ChallengeLabel) || inZone(at) == "" {
			return fmt.Errorf("%s is not a certificate check answered inside the cluster's zones", a)
		}
		if asked[a] {
			return fmt.Errorf("the certificate check %s is listed twice", a)
		}
		asked[a] = true
		if !members[ch.GetOwnerFingerprint()] {
			return fmt.Errorf("the certificate check %s names a server that is not the cluster's", a)
		}
		c.Challenges = append(c.Challenges, zoneChallenge{Asked: a, At: at, Owner: ch.GetOwnerFingerprint()})
	}
	if len(req.GetCertificates()) > maxZoneCerts {
		return fmt.Errorf("at most %d zone certificates", maxZoneCerts)
	}
	for _, zc := range req.GetCertificates() {
		zone, err := dnszone.Name(zc.GetZone())
		if err != nil {
			return err
		}
		if !zones[zone] || slices.ContainsFunc(c.Certs, func(r zoneCertRole) bool { return r.Zone == zone }) {
			return fmt.Errorf("the certificate of %s is not one zone of the cluster's", bare(zone))
		}
		role := zoneCertRole{Zone: zone, Renew: zc.GetRenew(), Order: zc.GetOrder(), Sum: strings.ToLower(zc.GetSha256())}
		if role.Sum != "" && !sumPattern.MatchString(role.Sum) {
			return errors.New("a zone certificate's digest is not a SHA-256")
		}
		if len(zc.GetCovered()) > maxNamesPerZone {
			return fmt.Errorf("at most %d names under a zone certificate", maxNamesPerZone)
		}
		for _, h := range zc.GetCovered() {
			n, err := dnszone.Name(h)
			if err != nil || !dns.IsSubDomain(zone, n) {
				return fmt.Errorf("%q is not a name of zone %s", h, bare(zone))
			}
			role.Covered = append(role.Covered, bare(n))
		}
		if zc.GetCertPem() != "" || zc.GetKeyPem() != "" {
			pc, err := parseProxyCert(zc.GetCertPem(), zc.GetKeyPem())
			if err != nil {
				return err
			}
			if !sameNames(pc.Names, zoneCertNames(zone)) || pc.Sum != role.Sum {
				return fmt.Errorf("the certificate sent for %s is not that zone's", bare(zone))
			}
			if c.pairs == nil {
				c.pairs = map[string]proxyCert{}
			}
			c.pairs[zone] = pc
		}
		c.Certs = append(c.Certs, role)
	}
	return nil
}

const maxNamesPerZone = 4096

// records is the check records answered in one zone: only those set by the one member each name names.
func (c *zoneCluster) records(zone string, txt map[string]txtState) []dnszone.TXT {
	if len(txt) == 0 {
		return nil
	}
	owner := map[string]string{}
	for _, ch := range c.Challenges {
		owner[ch.At] = ch.Owner
	}
	vals := map[string]map[string]bool{}
	for sender, st := range txt {
		for name, values := range st {
			if owner[name] != sender || !dns.IsSubDomain(zone, name) {
				continue
			}
			if vals[name] == nil {
				vals[name] = map[string]bool{}
			}
			for v := range values {
				vals[name][v] = true
			}
		}
	}
	out := make([]dnszone.TXT, 0, len(vals))
	for name, set := range vals {
		list := make([]string, 0, len(set))
		for v := range set {
			list = append(list, v)
		}
		sort.Strings(list)
		if len(list) > 8 {
			list = list[:8]
		}
		out = append(out, dnszone.TXT{Name: name, Values: list})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (z *clusterZones) pruneTXTLocked(now time.Time) {
	for id, senders := range z.txt {
		for fp, st := range senders {
			for name, values := range st {
				for v, at := range values {
					if now.Sub(at) > txtKeep {
						delete(values, v)
					}
				}
				if len(values) == 0 {
					delete(st, name)
				}
			}
			if len(st) == 0 {
				delete(senders, fp)
			}
		}
		if len(senders) == 0 {
			delete(z.txt, id)
		}
	}
}

// checkForLocked finds the check this server's proxy asked for: its cluster, and its name inside the zone.
func (z *clusterZones) checkForLocked(fqdn string) (id, at string, ok bool) {
	ids := make([]string, 0, len(z.st.Clusters))
	for id := range z.st.Clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		c := z.st.Clusters[id]
		for _, ch := range c.Challenges {
			if ch.Owner == c.Self && (ch.Asked == fqdn || ch.At == fqdn) {
				return id, ch.At, true
			}
		}
	}
	return "", "", false
}

var errNotACheck = errors.New("not a certificate check this server's proxy may answer")

// txtRefused is a member saying no to a record, which fails the check before any certificate authority reads it.
type txtRefused struct{ why string }

func (e *txtRefused) Error() string {
	return "a server of the cluster refused the check record: " + e.why
}

// presentTXT sets (or clears) a check record this server's proxy asked for, here and on every other member. A set
// that a member refuses is taken back, so the proxy's order stops before any certificate authority looks.
func (s *Service) presentTXT(ctx context.Context, fqdn, value string, add bool) error {
	z := s.zones()
	z.mu.Lock()
	id, at, ok := z.checkForLocked(fqdn)
	if !ok {
		z.mu.Unlock()
		return errNotACheck
	}
	c := z.st.Clusters[id]
	now := time.Now()
	z.setOwnLocked(id, c.Self, at, value, add, now)
	body, members := z.ownBodyLocked(id, c), c.otherMembers()
	z.pushLocked(now)
	z.mu.Unlock()
	if !add {
		go func() {
			bg, cancel := context.WithTimeout(context.Background(), txtWait)
			defer cancel()
			_ = s.sendTXT(bg, id, members, body)
		}()
		return nil
	}
	err := s.sendTXT(ctx, id, members, body)
	if err == nil {
		return nil
	}
	z.mu.Lock()
	z.setOwnLocked(id, c.Self, at, value, false, time.Now())
	body = z.ownBodyLocked(id, c)
	z.pushLocked(time.Now())
	z.mu.Unlock()
	go func() {
		bg, cancel := context.WithTimeout(context.Background(), txtWait)
		defer cancel()
		_ = s.sendTXT(bg, id, members, body)
	}()
	return err
}

func (z *clusterZones) setOwnLocked(id, self, at, value string, add bool, now time.Time) {
	if z.txt[id] == nil {
		z.txt[id] = map[string]txtState{}
	}
	st := z.txt[id][self]
	if st == nil {
		st = txtState{}
		z.txt[id][self] = st
	}
	if add {
		if st[at] == nil {
			st[at] = map[string]time.Time{}
		}
		if _, had := st[at][value]; !had {
			st[at][value] = now
		}
		return
	}
	delete(st[at], value)
	if len(st[at]) == 0 {
		delete(st, at)
	}
}

// ownBodyLocked is this server's whole set for a cluster, numbered so a member never takes an older one after a newer.
func (z *clusterZones) ownBodyLocked(id string, c *zoneCluster) txtBody {
	seq := time.Now().UnixNano()
	if seq <= z.seq {
		seq = z.seq + 1
	}
	z.seq = seq
	b := txtBody{Seq: seq, TXT: map[string][]string{}}
	for name, values := range z.txt[id][c.Self] {
		for v := range values {
			b.TXT[name] = append(b.TXT[name], v)
		}
		sort.Strings(b.TXT[name])
	}
	return b
}

func (c *zoneCluster) otherMembers() []string {
	var out []string
	for _, m := range c.Members {
		if m.Fingerprint != c.Self {
			out = append(out, m.Fingerprint)
		}
	}
	return out
}

// sendTXT hands every other member this server's records for a cluster. A member that cannot be reached is passed
// over (stopped, or cut off: it answers SERVFAIL then); one that refuses fails the whole check.
func (s *Service) sendTXT(ctx context.Context, cluster string, members []string, body txtBody) error {
	m := s.clusterMember()
	m.mu.Lock()
	party := m.party
	m.mu.Unlock()
	if party == nil || len(members) == 0 {
		return nil
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, txtWait)
	defer cancel()
	errs := make(chan error, len(members))
	for _, fp := range members {
		go func() { errs <- sendTXTTo(ctx, party, fp, cluster, raw) }()
	}
	var refused error
	for range members {
		var r *txtRefused
		if err := <-errs; errors.As(err, &r) && refused == nil {
			refused = err
		}
	}
	return refused
}

func sendTXTTo(ctx context.Context, party *clusterlink.Party, fp, cluster string, raw []byte) error {
	conn, err := party.Dial(ctx, wire.Party{Fingerprint: fp, Role: wire.RoleMember})
	if err != nil {
		return err
	}
	defer conn.Close()
	if d, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(d)
	}
	if err := wire.WriteHeader(conn, txtVerb, cluster); err != nil {
		return err
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		return err
	}
	verb, why, err := wire.ReadHeader(conn)
	if err != nil {
		return err
	}
	if verb != wire.VerbOK {
		return &txtRefused{why: why}
	}
	return nil
}

// receiveTXT takes a member's records for a cluster this server answers for: only names whose check it owns.
func (s *Service) receiveTXT(from wire.Party, c net.Conn, cluster string) {
	_ = c.SetDeadline(time.Now().Add(wire.HeaderTimeout))
	var body txtBody
	if err := json.NewDecoder(io.LimitReader(c, maxTXTBody)).Decode(&body); err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, "the records are unreadable")
		return
	}
	z := s.zones()
	z.mu.Lock()
	now := time.Now()
	err := z.takeTXTLocked(cluster, from.Fingerprint, body, now)
	if err == nil {
		z.pushLocked(now)
	}
	z.mu.Unlock()
	if err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, oneLine(err.Error()))
		return
	}
	_ = wire.WriteHeader(c, wire.VerbOK, "")
}

func (z *clusterZones) takeTXTLocked(cluster, sender string, body txtBody, now time.Time) error {
	c := z.st.Clusters[cluster]
	if c == nil {
		return errors.New("this server answers for no zone of that cluster")
	}
	if sender == c.Self || !slices.ContainsFunc(c.Members, func(m zoneMember) bool { return m.Fingerprint == sender }) {
		return errors.New("the sender is not a server of the cluster")
	}
	owns := map[string]bool{}
	for _, ch := range c.Challenges {
		if ch.Owner == sender {
			owns[ch.At] = true
		}
	}
	if len(body.TXT) > maxChallenges {
		return errors.New("too many records")
	}
	next := txtState{}
	was := z.txt[cluster][sender]
	for raw, values := range body.TXT {
		name, err := dnszone.Name(raw)
		if err != nil || !owns[name] {
			return fmt.Errorf("%s is not a check that server may answer", raw)
		}
		if len(values) == 0 || len(values) > 8 {
			return fmt.Errorf("%s: 1 to 8 values", raw)
		}
		next[name] = map[string]time.Time{}
		for _, v := range values {
			if !dnszone.TXTValue(v) {
				return fmt.Errorf("%s: a value is not a check's", raw)
			}
			at := now
			if old, ok := was[name][v]; ok {
				at = old
			}
			next[name][v] = at
		}
	}
	if z.seqs == nil {
		z.seqs = map[string]int64{}
	}
	key := cluster + "|" + sender
	if body.Seq <= z.seqs[key] {
		return nil
	}
	z.seqs[key] = body.Seq
	if z.txt[cluster] == nil {
		z.txt[cluster] = map[string]txtState{}
	}
	z.txt[cluster][sender] = next
	return nil
}

// publishViewLocked replaces what the proxy's store and routes file read of the zones' certificates.
func (z *clusterZones) publishViewLocked() {
	v := &zoneCertView{zones: map[string]bool{}, held: map[string]bool{}, renew: map[string]bool{}, order: map[string]bool{},
		unsure: map[string]bool{}, covered: map[string]bool{}, dns: map[string]bool{}}
	now := time.Now()
	for _, c := range z.st.Clusters {
		for _, e := range c.Zones {
			v.zones[bare(e.Name)] = true
		}
		for _, ch := range c.Challenges {
			v.dns[bare(strings.TrimPrefix(ch.Asked, dnszone.ChallengeLabel))] = true
		}
		for _, r := range c.Certs {
			zn := bare(r.Zone)
			v.held[zn] = true
			// Its pair comes over DNS too, so one taken over goes in the store under the resolver that renews it.
			v.dns[zn], v.dns["*."+zn] = true, true
			for _, h := range r.Covered {
				v.covered[h] = true
			}
			// Back from a power cut, it may no longer renew it: nothing is renewed nor dropped until Deplo says (#266).
			if !c.heard {
				v.unsure[zn] = true
				continue
			}
			if !r.Renew {
				continue
			}
			v.renew[zn] = true
			run := z.certs[zn]
			v.order[zn] = r.Order && (run == nil || !now.Before(run.retryAt))
		}
	}
	z.s.zoneView.Store(v)
}

// installZonePairs puts each zone certificate Deplo sent in the routes file, unless the proxy holds it or a newer one.
func (s *Service) installZonePairs(c *zoneCluster) {
	if len(c.pairs) == 0 {
		return
	}
	f := s.forwarders()
	stored := f.storeCerts()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadFiles()
	n := 0
	for _, pc := range c.pairs {
		if !pc.NotAfter.After(time.Now()) {
			continue
		}
		newer := func(o proxyCert) bool { return sameNames(o.Names, pc.Names) && !o.NotAfter.Before(pc.NotAfter) }
		if slices.ContainsFunc(stored, newer) || slices.ContainsFunc(f.files, newer) {
			continue
		}
		f.files = slices.DeleteFunc(f.files, func(o proxyCert) bool { return sameNames(o.Names, pc.Names) })
		pc.installed = time.Now()
		f.files = append(f.files, pc)
		n++
	}
	if n > 0 {
		if err := f.writeAnswerRoutes(); err != nil {
			log.Printf("deplo-agent: could not install a zone's certificate: %v", err)
		}
	}
}

// refreshZoneRoutes rewrites the routes file when the zones' order routers changed.
func (s *Service) refreshZoneRoutes() {
	f := s.forwarders()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.writeAnswerRoutes(); err != nil {
		log.Printf("deplo-agent: could not write the proxy's routes: %v", err)
	}
}

func zoneRouterName(zone string) string {
	sum := sha256.Sum256([]byte(zone))
	return "deplo-zone-" + hex.EncodeToString(sum[:6])
}

// zoneRoutersLocked are the routers that make this server's proxy order and renew the zones it renews. One appears
// only while its store holds the certificate, or while none exists anywhere: never a new order beside a valid one.
func (f *forwarders) zoneRoutersLocked() map[string]answerHTTPRouter {
	v := f.s.currentZoneView()
	if len(v.renew) == 0 {
		return nil
	}
	now := time.Now()
	store := f.storeCerts()
	valid := func(list []proxyCert, zone string) bool {
		return slices.ContainsFunc(list, func(c proxyCert) bool { return c.NotAfter.After(now) && v.zoneOf(c) == zone })
	}
	out := map[string]answerHTTPRouter{}
	for zone := range v.renew {
		if !valid(store, zone) && (!v.order[zone] || valid(f.files, zone)) {
			continue
		}
		out[zoneRouterName(zone)] = answerHTTPRouter{Rule: zoneRouterRule, EntryPoints: []string{answerEntrypoint},
			Service: "noop@internal", Priority: 1,
			TLS: &answerRouterTLS{CertResolver: zoneResolver, Domains: []answerTLSDomain{{Main: zone, SANs: []string{"*." + zone}}}}}
	}
	return out
}

// bestZonePair is the newest valid certificate of a zone this server holds, in its store or as a file.
func (s *Service) bestZonePair(zone string, now time.Time) *proxyCert {
	v := s.currentZoneView()
	f := s.forwarders()
	var best *proxyCert
	for _, c := range append(f.storeCerts(), f.installedCerts()...) {
		if c.NotAfter.After(now) && v.zoneOf(c) == bare(zone) && (best == nil || c.NotAfter.After(best.NotAfter)) {
			best = &c
		}
	}
	return best
}

// certStatesLocked is how this server holds each zone certificate of a cluster: a pair Deplo lacks goes with its key.
func (z *clusterZones) certStatesLocked(c *zoneCluster, now time.Time) []*pb.ClusterZoneCertificateState {
	var out []*pb.ClusterZoneCertificateState
	for _, r := range c.Certs {
		st := &pb.ClusterZoneCertificateState{Zone: bare(r.Zone)}
		best := z.s.bestZonePair(r.Zone, now)
		if best != nil {
			st.Sha256, st.NotAfterUnixMs = best.Sum, best.NotAfter.UnixMilli()
			if best.Sum != r.Sum {
				st.CertPem, st.KeyPem = best.Cert, best.Key
			}
		}
		run := z.certs[bare(r.Zone)]
		if run != nil && run.err != "" && (best == nil || best.Sum == run.sum) {
			st.Error, st.ErrorAtUnixMs = run.err, run.errAt.UnixMilli()
		}
		st.Ordering = r.Renew && r.Order && best == nil && (run == nil || !now.Before(run.retryAt))
		out = append(out, st)
	}
	return out
}

// tendZoneCerts keeps the checks' endpoint where the proxy reaches it, and the order routers and their retries right.
func (s *Service) tendZoneCerts(now time.Time) {
	z := s.zones()
	z.mu.Lock()
	want, renews := false, map[string]bool{}
	for _, c := range z.st.Clusters {
		for _, ch := range c.Challenges {
			want = want || ch.Owner == c.Self
		}
		for _, r := range c.Certs {
			if r.Renew && c.heard {
				renews[bare(r.Zone)] = true
			}
		}
	}
	due := now.Sub(z.tended) >= zoneTendEach
	if due {
		z.tended = now
	}
	z.mu.Unlock()
	s.challenge().ensure(want, now)
	if !due {
		return
	}
	if len(renews) > 0 {
		s.readZoneOrders(now, renews)
		s.settleZoneAuthz(now, renews)
	}
	// An order whose wait is over may go again.
	z.mu.Lock()
	z.publishViewLocked()
	z.mu.Unlock()
	s.refreshZoneRoutes()
}

// readZoneOrders reads the proxy's log for a failed order of a zone this server renews; a failed first order takes
// the order router out for a while, after which the proxy tries again.
func (s *Service) readZoneOrders(now time.Time, renews map[string]bool) {
	z := s.zones()
	z.mu.Lock()
	since := now
	for zone := range renews {
		run := z.certs[zone]
		if run == nil {
			run = &zoneCertRun{logSince: now.Add(-zoneLogEvery)}
			z.certs[zone] = run
		}
		if run.logSince.Before(since) {
			since = run.logSince
		}
	}
	z.mu.Unlock()
	if now.Sub(since) < zoneLogEvery {
		return
	}
	logs, err := s.readProxyLog(since)
	if err != nil {
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	changed := false
	for zone := range renews {
		run := z.certs[zone]
		run.logSince = now
		best := z.s.bestZonePair(zone, now)
		sum := ""
		if best != nil {
			sum = best.Sum
		}
		if run.err != "" && sum != run.sum {
			run.err, run.failures, run.retryAt, changed = "", 0, time.Time{}, true
		}
		msg, ok := zoneOrderError(logs, zone)
		if !ok {
			continue
		}
		run.err, run.errAt, run.sum = msg, now, sum
		if best == nil {
			run.retryAt = now.Add(zoneRetry[min(run.failures, len(zoneRetry)-1)])
			run.failures++
			changed = true
		}
		log.Printf("deplo-agent: the proxy could not get the certificate of zone %s: %s", zone, msg)
	}
	if changed {
		z.publishViewLocked()
	}
}

func (s *Service) readProxyLog(since time.Time) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if s.proxyLogs != nil {
		return s.proxyLogs(ctx, since)
	}
	res, err := dockercli.Run(ctx, 15*time.Second, "logs", "--since", since.UTC().Format(time.RFC3339), traefikContainer)
	if err != nil {
		return "", err
	}
	if res.Code != 0 {
		return "", errors.New(firstLine(res.Stderr))
	}
	return res.Stdout + "\n" + res.Stderr, nil
}

var ansiCode = regexp.MustCompile("\x1b\\[[0-9;]*m")

// zoneOrderError is the last failed order or renewal of a zone's certificate in the proxy's log, as it said it.
func zoneOrderError(logs, zone string) (string, bool) {
	var found string
	for _, line := range strings.Split(ansiCode.ReplaceAllString(logs, ""), "\n") {
		if !strings.Contains(line, "providerName="+zoneResolver+".acme") || !strings.Contains(line, "*."+zone) ||
			!(strings.Contains(line, "Unable to obtain ACME certificate") || strings.Contains(line, "Error renewing ACME certificate")) {
			continue
		}
		i := strings.Index(line, " error=")
		if i < 0 {
			continue
		}
		rest := line[i+len(" error="):]
		msg := rest
		if q, err := strconv.QuotedPrefix(rest); err == nil {
			msg, _ = strconv.Unquote(q)
		} else if j := strings.IndexByte(rest, ' '); j >= 0 {
			msg = rest[:j]
		}
		if strings.Contains(msg, "-----BEGIN") {
			continue
		}
		found = oneLine(msg)
	}
	if found == "" {
		return "", false
	}
	if len(found) > 600 {
		found = found[:600]
	}
	return found, true
}
