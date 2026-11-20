package server

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dnszone"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// The endpoint this server's proxy calls to set a certificate check's TXT record (Traefik's HTTP-request DNS
// provider). It listens on the proxy's own loopback, so nothing outside the proxy reaches it, and asks a secret only
// the proxy reads, so a request the proxy makes on a tenant's behalf cannot pass either.
const (
	challengePort       = 8444
	challengeUser       = "deplo"
	challengeSecretFile = "dns-challenge.secret"
	maxChallengeBody    = 4096
)

// challengeRecheck is how often the endpoint checks that it still listens in the proxy's current network.
var challengeRecheck = 2 * time.Second

// A check's value is the base64url of a SHA-256, unpadded.
var challengeValue = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

type challengeEndpoint struct {
	s       *Service
	mu      sync.Mutex
	ln      net.Listener
	srv     *http.Server
	ns      uint64
	pid     int
	checked time.Time
	secret  []byte
	mtime   time.Time
}

func (s *Service) challenge() *challengeEndpoint {
	s.chalOnce.Do(func() { s.chal = &challengeEndpoint{s: s} })
	return s.chal
}

func (s *Service) challengeSecretPath() string {
	dir := s.traefikDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, challengeSecretFile)
}

// ensureChallengeSecret makes the secret the proxy's checks are signed with, once: docker would make a missing
// mounted file a directory.
func ensureChallengeSecret(path string) error {
	if path == "" || regularFile(path) {
		return nil
	}
	_ = os.Remove(path)
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	return writeFileAtomic(path, []byte(hex.EncodeToString(b)), 0o600)
}

// ensure keeps the endpoint listening in the proxy's current network while want, and closes it otherwise.
func (e *challengeEndpoint) ensure(want bool, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !want {
		e.closeLocked()
		return
	}
	if e.ln != nil && now.Sub(e.checked) < challengeRecheck {
		return
	}
	e.checked = now
	if e.s.chalListen != nil {
		if e.ln == nil {
			ln, ns, err := e.s.chalListen()
			if err == nil {
				e.serveLocked(ln, ns)
			}
		}
		return
	}
	pid, ns, err := e.proxyNetworkLocked()
	if err != nil {
		e.closeLocked()
		return
	}
	if e.ln != nil && ns == e.ns {
		return
	}
	e.closeLocked()
	ln, err := listenInNetwork(pid, net.JoinHostPort("127.0.0.1", strconv.Itoa(challengePort)))
	if err != nil {
		log.Printf("deplo-agent: the certificate checks' endpoint cannot listen in the proxy's network: %v", err)
		return
	}
	e.serveLocked(ln, ns)
}

// rebind makes the next look find the proxy's network again at once: after a start this agent made.
func (e *challengeEndpoint) rebind() {
	e.mu.Lock()
	e.checked = time.Time{}
	e.pid = 0
	e.mu.Unlock()
}

// proxyNetworkLocked is the proxy's process and the network namespace it runs in.
func (e *challengeEndpoint) proxyNetworkLocked() (int, uint64, error) {
	if e.pid > 0 {
		if ns, err := netnsOf(e.pid); err == nil {
			return e.pid, ns, nil
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := dockercli.Run(ctx, 10*time.Second, "inspect", "--format", "{{.State.Pid}}", traefikContainer)
	if err != nil || res.Code != 0 {
		return 0, 0, errors.New("this server's proxy is not running")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(res.Stdout))
	if err != nil || pid <= 0 {
		return 0, 0, errors.New("this server's proxy is not running")
	}
	ns, err := netnsOf(pid)
	if err != nil {
		return 0, 0, err
	}
	e.pid = pid
	return pid, ns, nil
}

func netnsOf(pid int) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(fmt.Sprintf("/proc/%d/ns/net", pid), &st); err != nil {
		return 0, err
	}
	return st.Ino, nil
}

func (e *challengeEndpoint) serveLocked(ln net.Listener, ns uint64) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /present", e.handle(true))
	mux.HandleFunc("POST /cleanup", e.handle(false))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 15 * time.Second, MaxHeaderBytes: 8 << 10, ErrorLog: log.New(io.Discard, "", 0)}
	e.ln, e.srv, e.ns = ln, srv, ns
	go func() { _ = srv.Serve(ln) }()
}

func (e *challengeEndpoint) closeLocked() {
	if e.srv != nil {
		_ = e.srv.Close()
	}
	e.ln, e.srv, e.ns = nil, nil, 0
}

// authorized checks the proxy's secret, read again whenever the file changes.
func (e *challengeEndpoint) authorized(r *http.Request) bool {
	user, pass, ok := r.BasicAuth()
	if !ok || user != challengeUser {
		return false
	}
	path := e.s.challengeSecretPath()
	st, err := os.Stat(path)
	if err != nil || !st.Mode().IsRegular() {
		return false
	}
	e.mu.Lock()
	if !st.ModTime().Equal(e.mtime) || e.secret == nil {
		raw, err := os.ReadFile(path)
		if err != nil {
			e.mu.Unlock()
			return false
		}
		e.secret, e.mtime = []byte(strings.TrimSpace(string(raw))), st.ModTime()
	}
	secret := e.secret
	e.mu.Unlock()
	return len(secret) >= 32 && subtle.ConstantTimeCompare([]byte(pass), secret) == 1
}

func (e *challengeEndpoint) handle(add bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !e.authorized(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="deplo"`)
			http.Error(w, "not this server's proxy", http.StatusUnauthorized)
			return
		}
		var msg struct {
			FQDN  string `json:"fqdn"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, maxChallengeBody)).Decode(&msg); err != nil {
			http.Error(w, "unreadable", http.StatusBadRequest)
			return
		}
		fqdn, err := dnszone.Name(msg.FQDN)
		if err != nil || !strings.HasPrefix(fqdn, dnszone.ChallengeLabel) {
			http.Error(w, errNotACheck.Error(), http.StatusForbidden)
			return
		}
		if !challengeValue.MatchString(msg.Value) {
			http.Error(w, "not a check's value", http.StatusBadRequest)
			return
		}
		err = e.s.presentTXT(r.Context(), fqdn, msg.Value, add)
		var refused *txtRefused
		switch {
		case errors.Is(err, errNotACheck):
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		case errors.As(err, &refused):
			log.Printf("deplo-agent: the certificate check at %s was refused: %s", fqdn, refused.why)
			http.Error(w, err.Error(), http.StatusConflict)
			return
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
	}
}
