package mesh

import (
	"crypto/rand"
	"encoding/binary"
	"net/netip"
	"time"
)

// directLocked is the direct path to use now, if one answers; the caller holds m.mu.
func (p *peer) directLocked(now time.Time) (netip.AddrPort, bool) {
	if i, ok := p.live[p.current]; ok && now.Sub(i.at) < liveFor {
		return p.current, true
	}
	return netip.AddrPort{}, false
}

type outPing struct {
	to  netip.AddrPort
	pkt []byte
}

// tick forgets stale paths, picks each peer's best direct path, and sends the pings that are due.
func (m *Mesh) tick(now time.Time) {
	var out []outPing
	m.mu.Lock()
	changed := false
	for _, p := range m.peers {
		for a, i := range p.live {
			if now.Sub(i.at) >= liveFor {
				delete(p.live, a)
				changed = true
			}
		}
		for a, at := range p.learned {
			if now.Sub(at) >= learnedFor {
				delete(p.learned, a)
				changed = true
			}
		}
		for n, s := range p.waiting {
			if now.Sub(s.at) >= waitPong {
				delete(p.waiting, n)
			}
		}
		p.pick()
		targets := p.targetsLocked()
		for a := range targets {
			every := pingOther
			if _, ok := p.live[a]; ok {
				every = pingLive
			}
			if now.Sub(p.lastPing[a]) < every {
				continue
			}
			p.lastPing[a] = now
			nonce := randomNonce()
			p.waiting[nonce] = sentPing{addr: a, at: now}
			out = append(out, outPing{a, probe{kind: probePing, sender: m.pub, nonce: nonce}.encode(p.pair)})
		}
		for a := range p.lastPing {
			if !targets[a] {
				delete(p.lastPing, a)
			}
		}
		if l := p.relay; l != nil {
			if _, direct := p.directLocked(now); direct && now.Sub(l.lastUsed()) > relayIdle {
				l.c.Close()
				p.relay = nil
			}
		}
	}
	if changed {
		m.reindexLocked()
	}
	m.mu.Unlock()
	for _, o := range out {
		m.bind.sendUDP(o.pkt, o.to)
	}
}

// targetsLocked is every address p may answer on directly; the caller holds m.mu.
func (p *peer) targetsLocked() map[netip.AddrPort]bool {
	targets := map[netip.AddrPort]bool{}
	for _, a := range p.Endpoints {
		targets[norm(a)] = true
	}
	for a := range p.learned {
		targets[a] = true
	}
	for a := range p.live {
		targets[a] = true
	}
	return targets
}

// pick keeps the current direct path while it answers, unless another one is twice as fast.
func (p *peer) pick() {
	cur, curOK := p.live[p.current]
	best, bestRTT := netip.AddrPort{}, time.Duration(0)
	for a, i := range p.live {
		if !best.IsValid() || i.rtt < bestRTT {
			best, bestRTT = a, i.rtt
		}
	}
	if curOK && (bestRTT*2 >= cur.rtt || best == p.current) {
		return
	}
	p.current = best
}

func randomNonce() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint64(b[:])
}

// handleProbe answers a ping and records a pong; anything that does not prove its sender is ignored.
func (m *Mesh) handleProbe(b []byte, from netip.AddrPort) {
	pr, ok := parseProbe(b)
	if !ok {
		return
	}
	now := time.Now()
	m.mu.Lock()
	p := m.byKey[pr.sender]
	if p == nil || !probeValid(p.pair, b) {
		m.mu.Unlock()
		return
	}
	switch pr.kind {
	case probePing:
		if _, known := p.learned[from]; !known {
			p.learned[from] = now
			m.reindexLocked()
		}
		p.learned[from] = now
		if now.Sub(p.pongWindow) >= time.Second {
			p.pongWindow, p.pongs = now, 0
		}
		if p.pongs >= maxPongs {
			m.mu.Unlock()
			return
		}
		p.pongs++
		reply := probe{kind: probePong, sender: m.pub, nonce: pr.nonce}.encode(p.pair)
		m.mu.Unlock()
		m.bind.sendUDP(reply, from)
	case probePong:
		s, ok := p.waiting[pr.nonce]
		if !ok || s.addr != from {
			m.mu.Unlock()
			return
		}
		delete(p.waiting, pr.nonce)
		_, had := p.live[from]
		p.live[from] = pathInfo{at: now, rtt: now.Sub(s.at)}
		var flush [][]byte
		var to netip.AddrPort
		if !had {
			m.reindexLocked()
			if _, ok := p.directLocked(now); !ok {
				p.pick()
			}
			if a, ok := p.directLocked(now); ok {
				flush, to = p.takePending(now), a
			}
		}
		m.mu.Unlock()
		// Taken while it waited, one of p's initiations makes this end's own queued one stale.
		flush = p.withoutStaleInits(flush)
		p.noteSent(flush)
		for _, b := range flush {
			m.bind.sendUDP(b, to)
		}
	case probeMoved:
		if p.Next.IsZero() || m.byKey[p.Next] != nil {
			m.mu.Unlock()
			return
		}
		ipc, again := m.takeNextLocked(p)
		m.mu.Unlock()
		if ipc == "" || m.dev.IpcSet(ipc) != nil {
			return
		}
		for _, b := range again {
			m.bind.deliver(p, b)
		}
	default:
		m.mu.Unlock()
	}
}
