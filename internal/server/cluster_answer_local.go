package server

import (
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
	f.mu.Unlock()
	forwarder.Join(in, up)
	f.mu.Lock()
	delete(f.local[appID], in)
	if len(f.local[appID]) == 0 {
		delete(f.local, appID)
	}
	f.mu.Unlock()
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

// proxyRestarted notes that this server's proxy just started: every App waits answerSettle more before its
// visitors reach it, the time the proxy takes to read its routes.
func (f *forwarders) proxyRestarted() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proxyStarted = time.Now()
	for _, rc := range f.readiness {
		rc.until = time.Time{}
	}
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
