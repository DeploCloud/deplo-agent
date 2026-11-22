package server

import (
	"encoding/json"
	"slices"
	"time"
)

// knownCertNames is every hostname whose certificate Deplo says exists: a zone's own covers it, or it has its own
// that one server renews and every other holds (ADR-0049).
func (s *Service) knownCertNames() map[string]bool {
	z := s.zones()
	z.mu.Lock()
	defer z.mu.Unlock()
	out := map[string]bool{}
	for _, c := range z.st.Clusters {
		for _, r := range c.Certs {
			if r.Sum == "" {
				continue
			}
			for _, n := range append(slices.Clone(r.Names), r.Covered...) {
				out[n] = true
			}
		}
	}
	return out
}

// certsMissing is the App's hostnames whose certificate exists but of which this server holds no valid pair, the
// bundle's own counted: such a server never takes the App, so the leader tries the next one.
func (s *Service) certsMissing(appID string, bundle json.RawMessage, now time.Time) []string {
	known := s.knownCertNames()
	if len(known) == 0 {
		return nil
	}
	f := s.forwarders()
	have := append(f.storeCerts(), f.installedCerts()...)
	var list []failoverCert
	if len(bundle) > 0 && json.Unmarshal(bundle, &list) == nil {
		for _, c := range list {
			if pc, err := parseProxyCert(c.Cert, c.Key); err == nil {
				have = append(have, pc)
			}
		}
	}
	f.mu.Lock()
	answers := slices.Clone(f.st.Answers)
	f.mu.Unlock()
	var missing []string
	for _, a := range answers {
		if a.AppID != appID || a.Routes == nil || a.Routes.TCP == nil {
			continue
		}
		for _, h := range a.Routes.hosts() {
			if known[h] && !slices.ContainsFunc(have, func(c proxyCert) bool { return c.NotAfter.After(now) && c.covers(h) }) {
				missing = append(missing, h)
			}
		}
	}
	slices.Sort(missing)
	return slices.Compact(missing)
}
