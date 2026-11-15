// Package mesh is a cluster's private network: a WireGuard tunnel between its servers, run inside the agent on a
// network stack of its own, so the host's interfaces, routes and firewall are never touched (ADR-0047).
package mesh

import (
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// Key is a WireGuard key: a Curve25519 private or public key.
type Key [32]byte

// NewPrivateKey makes a fresh private key; WireGuard clamps it itself.
func NewPrivateKey() (Key, error) {
	var k Key
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return k, err
	}
	copy(k[:], priv.Bytes())
	return k, nil
}

// Public is the public key of a private one.
func (k Key) Public() (Key, error) {
	var out Key
	priv, err := ecdh.X25519().NewPrivateKey(k[:])
	if err != nil {
		return out, err
	}
	copy(out[:], priv.PublicKey().Bytes())
	return out, nil
}

// ParseKey reads a key in WireGuard's base64 form.
func ParseKey(s string) (Key, error) {
	var k Key
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(raw) != len(k) {
		return k, fmt.Errorf("%q is not a WireGuard key", s)
	}
	copy(k[:], raw)
	return k, nil
}

func (k Key) String() string { return base64.StdEncoding.EncodeToString(k[:]) }

func (k Key) hex() string { return hex.EncodeToString(k[:]) }

// IsZero reports an unset key.
func (k Key) IsZero() bool { return k == Key{} }

// pairKey is what two peers share without sending anything: the probe MAC key from their static keys.
func pairKey(priv, peer Key) ([32]byte, error) {
	p, err := ecdh.X25519().NewPrivateKey(priv[:])
	if err != nil {
		return [32]byte{}, err
	}
	pub, err := ecdh.X25519().NewPublicKey(peer[:])
	if err != nil {
		return [32]byte{}, err
	}
	shared, err := p.ECDH(pub)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte("deplo mesh probe v1 "), shared...)), nil
}
