package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/forwarder"
)

// Entrances (ADR-0048): a cluster App's domain answers on every server. Here, on a server that does not run the
// App, a forwarder on the proxy's network carries the labels the proxy passes the domain on by; each connection
// goes through this agent and the cluster's network to the App's server, whose agent hands it to its own proxy.

const (
	entranceKind = "entrance"
	// entranceNetwork is the proxy's own network: the forwarder sits where the proxy reaches it.
	entranceNetwork = "deplo"
	// The two ports a passed-on domain is reached on: its TLS, untouched, and port 80 for certificate checks.
	entranceTLS       uint32 = 443
	entranceWeb       uint32 = 80
	maxEntranceLabels        = 64
)

// proxyWebPort is where a server's own proxy answers port 80; a var so tests can point it elsewhere.
var proxyWebPort uint32 = 80

var entranceLabel = regexp.MustCompile(`^traefik\.[A-Za-z0-9_.-]{1,200}$`)

type entrance struct {
	AppID  string   `json:"appId"`
	Labels []string `json:"labels"`
}

func entranceContainer(appID string) string {
	return containerFor(heldName{Network: entranceNetwork, Name: entranceKind + ":" + appID})
}

func entranceAlias(appID string) string {
	sum := sha256.Sum256([]byte(appID))
	return "deplo-entrance-" + hex.EncodeToString(sum[:6])
}

// validateEntrances reads the entrances of a forwarder table into st: only proxy labels, each App once.
func validateEntrances(req *pb.SetClusterForwardersRequest, st *forwardState) error {
	seen := map[string]bool{}
	for _, e := range req.GetEntrances() {
		en := entrance{AppID: e.GetAppId()}
		if !fwdID.MatchString(en.AppID) || len(e.GetLabels()) == 0 || len(e.GetLabels()) > maxEntranceLabels {
			return fmt.Errorf("%q is not an App this member can pass domains on for", en.AppID)
		}
		if seen[en.AppID] {
			return fmt.Errorf("%q is passed on twice", en.AppID)
		}
		seen[en.AppID] = true
		for _, l := range e.GetLabels() {
			k, v, ok := strings.Cut(l, "=")
			if !ok || !entranceLabel.MatchString(k) || len(v) > 4096 || strings.ContainsAny(v, "\r\n\x00") {
				return fmt.Errorf("%q is not a proxy label", k)
			}
			en.Labels = append(en.Labels, l)
		}
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

// reconcileEntrances runs one forwarder per entrance, beside the name forwarders; the caller holds f.mu.
func (f *forwarders) reconcileEntrances(ctx context.Context, wanted map[string]bool, note func(error)) {
	ops := f.ops()
	for _, e := range f.st.Entrances {
		name := entranceContainer(e.AppID)
		wanted[name] = true
		l := f.listeners[name]
		if l == nil {
			ln, err := ops.listen(f.dir(name))
			if err != nil {
				note(fmt.Errorf("the entrance of %s: %w", e.AppID, err))
				continue
			}
			l = &heldListener{ln: ln, dir: f.dir(name)}
			f.listeners[name] = l
			go f.acceptEntrance(e.AppID, l)
		}
		note(ops.ensure(ctx, forwardSpec{Container: name, Network: entranceNetwork, Alias: entranceAlias(e.AppID),
			Port: entranceTLS, Dir: l.dir, Labels: e.Labels}))
	}
}

func (f *forwarders) acceptEntrance(appID string, l *heldListener) {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		go f.serveEntrance(appID, c)
	}
}

// serveEntrance carries one connection the proxy passed on: to this server's own proxy while the App runs here
// (a failover the table has not caught up with yet), else to the server that runs it.
func (f *forwarders) serveEntrance(appID string, c net.Conn) {
	defer c.Close()
	f.mu.Lock()
	found := slices.ContainsFunc(f.st.Entrances, func(e entrance) bool { return e.AppID == appID })
	cluster, members := f.st.Cluster, f.st.Members
	f.mu.Unlock()
	if !found {
		return
	}
	in, port := readDialedPort(c, entranceTLS)
	if port != entranceTLS && port != entranceWeb {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	memberID, here, ok := f.s.leaseGuard().locate(cluster, "app", appID)
	if !ok {
		return
	}
	var up net.Conn
	var err error
	if here {
		up, err = f.dialProxy(ctx, port)
	} else {
		up, err = f.dialMember(ctx, members[memberID], heldName{Network: entranceNetwork, Kind: entranceKind, ID: appID, Port: entranceTLS}, port)
	}
	if err != nil {
		return
	}
	forwarder.Join(in, up)
}

// dialProxy reaches this server's own proxy: its internal entrypoint for a domain's TLS, which reads the PROXY
// header the forwarding proxy sent, or its port 80 for a certificate check.
func (f *forwarders) dialProxy(ctx context.Context, port uint32) (net.Conn, error) {
	f.mu.Lock()
	to := f.st.EntrancePort
	f.mu.Unlock()
	if port == entranceWeb {
		to = proxyWebPort
	}
	if to == 0 {
		return nil, fmt.Errorf("this server's proxy has no internal entrypoint")
	}
	ops := f.ops()
	if ops.proxy == nil {
		return nil, fmt.Errorf("this server's proxy cannot be found")
	}
	ip, err := ops.proxy(ctx)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	return d.DialContext(ctx, "tcp", net.JoinHostPort(ip, fmt.Sprint(to)))
}

// serveEntranceStream answers another member's passed-on connection: only for an App Deplo said this server
// answers for, that the leader placed here, from a member of this cluster, on 443 or 80.
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
	_, here, ok := f.s.leaseGuard().locate(st.Cluster, "app", appID)
	return inCluster && ok && here
}
