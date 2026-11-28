package mesh

import (
	"bytes"
	"time"
)

// isInitiation says whether a WireGuard message starts a handshake: type 1, little-endian.
func isInitiation(b []byte) bool {
	return len(b) >= 4 && b[0] == 1 && b[1] == 0 && b[2] == 0 && b[3] == 0
}

// isResponse says whether a WireGuard message answers a handshake initiation: type 2, little-endian.
func isResponse(b []byte) bool {
	return len(b) >= 4 && b[0] == 2 && b[1] == 0 && b[2] == 0 && b[3] == 0
}

// noteSent records the handshake initiations among what WireGuard hands over for p, held or sent.
func (p *peer) noteSent(bufs [][]byte) {
	for _, b := range bufs {
		if isInitiation(b) {
			p.sentInit.Store(time.Now().UnixNano())
		}
	}
}

// withoutStaleInits drops this end's initiations made before it took p's: WireGuard answers p's now, and one of
// its own sent after that would undo the answer.
func (p *peer) withoutStaleInits(bufs [][]byte) [][]byte {
	if time.Since(time.Unix(0, p.tookInit.Load())) >= crossWindow {
		return bufs
	}
	out := bufs[:0:0]
	for _, b := range bufs {
		if !isInitiation(b) {
			out = append(out, b)
		}
	}
	return out
}

// crossed says whether an initiation from p crossed this end's own, and must give way to it: two initiations in
// flight undo each other and WireGuard tries again only after 5 s. The lower public key's goes on.
func (m *Mesh) crossed(p *peer, pkt []byte) bool {
	if !isInitiation(pkt) || time.Since(time.Unix(0, p.sentInit.Load())) >= crossWindow {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return bytes.Compare(m.pub[:], p.PublicKey[:]) < 0
}

// takeInitiation lets a peer's handshake in, and drops this end's own queued one: sent after it, it would only
// undo the handshake WireGuard is answering now.
func (m *Mesh) takeInitiation(p *peer, pkt []byte) bool {
	// An answer ends this end's handshake: a restarted peer's next initiation crosses nothing, and is let in.
	if isResponse(pkt) {
		p.sentInit.Store(0)
		return true
	}
	if m.crossed(p, pkt) {
		return false
	}
	if isInitiation(pkt) {
		// Taking p's undoes this end's own: left noted, it dropped p's next one as crossed (c46, 5 s).
		p.sentInit.Store(0)
		p.tookInit.Store(time.Now().UnixNano())
		m.mu.Lock()
		if !p.Next.IsZero() {
			p.heldInits = append(p.heldInits[max(len(p.heldInits)-3, 0):], heldInit{append([]byte(nil), pkt...), time.Now()})
		}
		kept := p.pending[:0]
		for _, b := range p.pending {
			if !isInitiation(b) {
				kept = append(kept, b)
			}
		}
		p.pending = kept
		m.mu.Unlock()
	}
	return true
}
