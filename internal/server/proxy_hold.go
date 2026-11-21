package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
)

// A server back from a power cut, or weeks away, orders no certificate the cluster did not ask it for: before Docker
// starts its proxy, every pair the cluster may have moved leaves the store for the routes file, where it is served
// and never renewed, and goes back only on word heard since (ADR-0049, #266).

const proxyHoldUnit = "deplo-proxy-hold.service"

var proxyHoldUnitPath = "/etc/systemd/system/" + proxyHoldUnit

// ProxyHold moves out of the proxy's store every pair renewed over DNS or serving a hostname this server passes on,
// and takes out the routers that would order one. It answers how many pairs it moved.
func ProxyHold(agentDir string) (int, error) {
	s := &Service{agentDir: agentDir}
	acme, err := readProxyACME(s.traefikCompose())
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	st, err := readACMEStore(acme.Store)
	if err != nil {
		return 0, err
	}
	path := filepath.Join(s.traefikDir(), answerRoutesFile)
	var routes answerRoutes
	if raw, err := os.ReadFile(path); err == nil && len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &routes); err != nil {
			return 0, errors.New("the proxy's routes file is unreadable")
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	var answered []string
	if routes.TCP != nil {
		answered = routes.hosts()
	}
	var held []proxyCert
	for _, name := range st.order {
		list := st.entries(name)
		kept := list[:0:0]
		for _, e := range list {
			c, err := parseProxyCert(string(e.Certificate), string(e.Key))
			if err == nil && (name == zoneResolver || slices.ContainsFunc(c.Names, func(h string) bool { return slices.Contains(answered, h) })) {
				held = append(held, c)
				continue
			}
			kept = append(kept, e)
		}
		if len(kept) != len(list) {
			st.setEntries(name, kept)
		}
	}
	if len(held) == 0 {
		return 0, nil
	}
	if routes.TLS == nil {
		routes.TLS = &answerTLSFiles{}
	}
	for _, c := range held {
		if !slices.ContainsFunc(routes.TLS.Certificates, func(f answerTLSFile) bool { return f.CertFile == c.Cert }) {
			routes.TLS.Certificates = append(routes.TLS.Certificates, answerTLSFile{CertFile: c.Cert, KeyFile: c.Key})
		}
	}
	if routes.HTTP != nil {
		for k, r := range routes.HTTP.Routers {
			if r.TLS != nil && r.TLS.CertResolver != "" {
				delete(routes.HTTP.Routers, k)
			}
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(routes); err != nil {
		return 0, err
	}
	// The routes file first: a pair is never in neither, which would let the proxy order it afresh.
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return 0, err
	}
	if err := rewriteInPlace(path, buf.Bytes()); err != nil {
		return 0, err
	}
	return len(held), st.write(acme.Store)
}

// proxyHoldUnitText runs the hold once per boot, before Docker starts the proxy; a hold that fails delays nothing.
func proxyHoldUnitText(exe, agentDir string) string {
	return fmt.Sprintf(`[Unit]
Description=Deplo: keep the proxy from renewing a certificate the cluster may have moved
Before=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=%s proxy-hold --agent-dir %s
TimeoutStartSec=30

[Install]
WantedBy=docker.service
`, exe, agentDir)
}

// EnsureProxyHold installs the hold's unit for the next boot. It never runs it now: a running agent keeps its
// proxy's store right itself.
func (s *Service) EnsureProxyHold() {
	if _, err := exec.LookPath("systemctl"); err != nil || s.agentDir == "" || os.Geteuid() != 0 {
		return
	}
	exe, err := os.Executable()
	if err != nil {
		return
	}
	if r, err := filepath.EvalSymlinks(exe); err == nil {
		exe = r
	}
	want := proxyHoldUnitText(exe, s.agentDir)
	if have, err := os.ReadFile(proxyHoldUnitPath); err == nil && string(have) == want {
		return
	}
	if err := writeFileAtomic(proxyHoldUnitPath, []byte(want), 0o644); err != nil {
		log.Printf("deplo-agent: could not install %s: %v", proxyHoldUnit, err)
		return
	}
	ctx := context.Background()
	for _, args := range [][]string{{"daemon-reload"}, {"enable", proxyHoldUnit}} {
		if err := runSystemctl(ctx, args...); err != nil {
			// Taken out again, so the next start tries the whole install once more.
			_ = os.Remove(proxyHoldUnitPath)
			log.Printf("deplo-agent: %s: %v", proxyHoldUnit, err)
			return
		}
	}
}
