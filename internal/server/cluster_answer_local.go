package server

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"slices"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/forwarder"
)

// The visitors this agent carries to its own server's proxy: held while the proxy is down or settling, closed once
// their App runs elsewhere (deplo ADR-0048).

// carry joins a visitor to this server's own proxy until either side ends, or the App runs elsewhere: a connection
// kept open across a move is closed, so it reconnects, never getting the 404 of a proxy the App left (c39).
func (f *forwarders) carry(appID string, in, up net.Conn) {
	f.mu.Lock()
	if f.local == nil {
		f.local = map[string]map[net.Conn]net.Conn{}
	}
	if f.local[appID] == nil {
		f.local[appID] = map[net.Conn]net.Conn{}
	}
	f.local[appID][in] = up
	if !f.watching {
		f.watching = true
		go f.watchLocal()
	}
	cluster := f.st.Cluster
	f.mu.Unlock()
	here := func() bool { h, _ := f.s.leaseGuard().answerHolders(cluster, appID); return h }
	forwarder.Join(leased{in, here}, up)
	f.mu.Lock()
	delete(f.local[appID], in)
	if len(f.local[appID]) == 0 {
		delete(f.local, appID)
	}
	f.mu.Unlock()
}

// leased is a visitor carried to this server's own proxy: each read it passes on asks the lease first, so a server
// thawed after its lease ran out passes nothing more to a copy that may be stale (c41).
type leased struct {
	net.Conn
	here func() bool
}

var errNotHere = errors.New("this server no longer runs the App")

func (c leased) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && !c.here() {
		c.Conn.Close()
		return 0, errNotHere
	}
	return n, err
}

func (c leased) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// watchLocal closes, every localCheck, the visitors carried here for an App the placement no longer puts here.
// ponytail: one goroutine for the agent's life, started by the first visitor carried.
func (f *forwarders) watchLocal() {
	for {
		time.Sleep(localCheck)
		f.mu.Lock()
		cluster := f.st.Cluster
		apps := slices.Collect(maps.Keys(f.local))
		f.mu.Unlock()
		for _, appID := range apps {
			if here, _ := f.s.leaseGuard().answerHolders(cluster, appID); here {
				continue
			}
			f.mu.Lock()
			for in, up := range f.local[appID] {
				in.Close()
				up.Close()
			}
			f.mu.Unlock()
		}
	}
}

// proxyRestarted notes that this server's proxy just started: every App waits proxySettle more before its
// visitors reach it, the time the proxy takes to read its routes.
func (f *forwarders) proxyRestarted() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proxyStarted, f.stopping = time.Now(), false
}

// passHeld takes a visitor held at this server's front door for an App of the cluster, behind the PROXY header its
// proxy would have sent: to another server that runs it at once, or here once the proxy is back and settled.
func (f *forwarders) passHeld(c net.Conn, host string) bool {
	f.mu.Lock()
	cluster, members := f.st.Cluster, f.st.Members
	var appID string
	for _, a := range f.st.Answers {
		if a.Routes != nil && slices.Contains(a.Routes.hosts(), host) {
			appID = a.AppID
		}
	}
	f.mu.Unlock()
	if appID == "" || !f.admit() {
		return false
	}
	go func() {
		defer f.leave()
		defer c.Close()
		f.answerFor(c, cluster, members, appID, answerTLS, proxyHeader(c))
	}()
	return true
}

// proxyStopping notes that this server's proxy is about to stop: no visitor is passed to it, from here or another
// server, until it is back and settled (c42: a held visitor met the new proxy before it read its routes, 404).
func (f *forwarders) proxyStopping(standby standbyDialer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopping = true
	if standby != nil {
		f.standby = standby
	}
}

// standbyGone is the standby proxy ending: this server's own proxy takes every visitor again.
func (f *forwarders) standbyGone() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.standby = nil
}

// settling says whether this server's proxy cannot take a visitor now, and the standby to reach instead; the
// caller holds f.mu.
func (f *forwarders) settling() (bool, standbyDialer) {
	return f.stopping || time.Since(f.proxyStarted) < proxySettle, f.standby
}

// recreating holds the App's visitors here while a reroute replaces its containers: a ready kept from before passed
// them to a proxy still routing the old container (c40: a 502 at each recreate of a domain added). Done looks again.
func (f *forwarders) recreating(appID string) (done func()) {
	if appID == "" {
		return func() {}
	}
	forget := func() {
		if rc := f.readiness[appID]; rc != nil {
			rc.until = time.Time{}
		}
	}
	f.mu.Lock()
	if f.replacing == nil {
		f.replacing = map[string]int{}
	}
	f.replacing[appID]++
	forget()
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.replacing[appID]--; f.replacing[appID] <= 0 {
			delete(f.replacing, appID)
		}
		forget()
	}
}

func (f *forwarders) proxyIsDown() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return time.Now().Before(f.proxyDown)
}

// proxyAnswered notes how this server's proxy took a passed-on visitor: a refusal holds for proxyDownFor.
func (f *forwarders) proxyAnswered(appID string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		if f.proxyDownSaid {
			fmt.Fprintf(os.Stderr, "deplo-agent: this server's proxy takes the domains passed on to it again\n")
		}
		f.proxyDown, f.proxyDownSaid = time.Time{}, false
		return
	}
	f.proxyDown = time.Now().Add(proxyDownFor)
	if !f.proxyDownSaid {
		f.proxyDownSaid = true
		fmt.Fprintf(os.Stderr, "deplo-agent: a domain of %s was passed on, but %v: the servers passing them wait until this server's proxy answers\n", appID, err)
	}
}
