package mesh

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// A probe finds and proves a direct path. WireGuard's own messages start with 1 to 4, so the magic byte never
// collides; the MAC is keyed by the two peers' static keys, so nobody else can make one.
const (
	probeMagic = 0xde
	probePing  = 1
	probePong  = 2
	probeLen   = 1 + 1 + 32 + 8 + 16
)

type probe struct {
	kind   byte
	sender Key
	nonce  uint64
}

func probeMAC(key [32]byte, head []byte) []byte {
	h := hmac.New(sha256.New, key[:])
	h.Write(head)
	return h.Sum(nil)[:16]
}

func (p probe) encode(key [32]byte) []byte {
	b := make([]byte, probeLen)
	b[0], b[1] = probeMagic, p.kind
	copy(b[2:34], p.sender[:])
	binary.BigEndian.PutUint64(b[34:42], p.nonce)
	copy(b[42:], probeMAC(key, b[:42]))
	return b
}

// isProbe says whether a datagram is a probe rather than WireGuard.
func isProbe(b []byte) bool { return len(b) > 0 && b[0] == probeMagic }

// parseProbe reads a probe's claims; the caller checks the MAC with the claimed sender's pair key.
func parseProbe(b []byte) (probe, bool) {
	if len(b) != probeLen || b[0] != probeMagic || (b[1] != probePing && b[1] != probePong) {
		return probe{}, false
	}
	var p probe
	p.kind = b[1]
	copy(p.sender[:], b[2:34])
	p.nonce = binary.BigEndian.Uint64(b[34:42])
	return p, true
}

func probeValid(key [32]byte, b []byte) bool {
	return len(b) == probeLen && hmac.Equal(probeMAC(key, b[:42]), b[42:])
}
