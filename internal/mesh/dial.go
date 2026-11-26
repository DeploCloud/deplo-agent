package mesh

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"time"
)

// Dial opens a TCP connection inside the tunnel to a peer's port, filed under the run of the peer that answered it.
func (m *Mesh) Dial(ctx context.Context, fingerprint string, port uint16) (net.Conn, error) {
	m.mu.RLock()
	p := m.peers[fingerprint]
	m.mu.RUnlock()
	if p == nil {
		return nil, fmt.Errorf("that server is not on this cluster's network")
	}
	c, err := m.dialTCP(ctx, fingerprint, netip.AddrPortFrom(p.Address, port))
	if err != nil {
		return nil, err
	}
	return m.open(ctx, fingerprint, c)
}

// dialEvery is how often a dial that hears nothing sends another SYN beside the first.
var dialEvery = time.Second

// dialTCP sends a fresh SYN every dialEvery until one connects: TCP's own wait 1, 3, then 7 s, and visitors carried
// to a member whose agent had just come back got there 4 to 5 s late (c46). An answer that refuses ends it. The peer's
// next run heard sends one at once: a renewal waiting on a restarted agent reached it 2 s after its return (c48).
func (m *Mesh) dialTCP(ctx context.Context, fp string, to netip.AddrPort) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	type result struct {
		c   net.Conn
		err error
	}
	results := make(chan result, 64)
	tick := time.NewTicker(dialEvery)
	defer tick.Stop()
	pending := 0
	try := func() {
		pending++
		go func() {
			c, err := m.tnet.DialContextTCPAddrPort(ctx, to)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{c: c}
		}()
	}
	try()
	back := m.backOf(fp)
	for {
		select {
		case <-back:
			back = m.backOf(fp)
			if pending < 16 {
				try()
			}
		case r := <-results:
			pending--
			if r.err != nil && ctx.Err() == nil {
				return nil, r.err
			}
			if r.err == nil {
				// The slower ones end with ctx; one that connected anyway is closed.
				go func(n int) {
					for ; n > 0; n-- {
						if late := <-results; late.c != nil {
							late.c.Close()
						}
					}
				}(pending)
				return r.c, nil
			}
		case <-tick.C:
			if pending < 16 {
				try()
			}
		case <-ctx.Done():
			go func(n int) {
				for ; n > 0; n-- {
					if late := <-results; late.c != nil {
						late.c.Close()
					}
				}
			}(pending)
			return nil, ctx.Err()
		}
	}
}
