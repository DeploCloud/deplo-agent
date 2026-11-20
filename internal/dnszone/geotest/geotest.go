// Package geotest builds small location tables for tests, in the format internal/dnszone reads.
package geotest

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"net/netip"
)

// Block is where the addresses from Start on are, up to the next block's start; Place -1 is nowhere.
type Block struct {
	Start string
	Place int
}

// Place is a position in degrees.
type Place struct{ Lat, Lon float64 }

var (
	Milan   = Place{45.46, 9.19}
	NewYork = Place{40.71, -74.01}
	Tokyo   = Place{35.68, 139.69}
)

// Build is a table of release (YYYYMMDD), whose blocks rise from 0.0.0.0 and :: and name places by index.
func Build(release uint32, places []Place, v4, v6 []Block) []byte {
	le := binary.LittleEndian
	b := make([]byte, 32)
	copy(b, "DEPLOGEO")
	le.PutUint16(b[8:], 1)
	le.PutUint32(b[12:], release)
	le.PutUint32(b[16:], uint32(len(places)))
	le.PutUint32(b[20:], uint32(len(v4)))
	le.PutUint32(b[24:], uint32(len(v6)))
	for _, p := range places {
		b = le.AppendUint16(b, uint16(int16(math.Round(p.Lat*100))))
		b = le.AppendUint16(b, uint16(int16(math.Round(p.Lon*100))))
	}
	for _, k := range v4 {
		a := netip.MustParseAddr(k.Start).As4()
		b = le.AppendUint32(b, binary.BigEndian.Uint32(a[:]))
	}
	for _, k := range v4 {
		b = le.AppendUint16(b, at(k.Place))
	}
	for _, k := range v6 {
		a := netip.MustParseAddr(k.Start).As16()
		b = le.AppendUint64(b, binary.BigEndian.Uint64(a[:8]))
	}
	for _, k := range v6 {
		b = le.AppendUint16(b, at(k.Place))
	}
	sum := sha256.Sum256(b)
	return append(b, sum[:]...)
}

func at(i int) uint16 {
	if i < 0 {
		return 0xFFFF
	}
	return uint16(i)
}

// World places three subnets of each family in Milan, New York and Tokyo, and nothing else anywhere.
func World() []byte {
	return Build(20261101, []Place{Milan, NewYork, Tokyo},
		[]Block{{"0.0.0.0", -1}, {"192.0.2.0", 2}, {"192.0.3.0", -1}, {"198.51.100.0", 0}, {"198.51.101.0", -1},
			{"203.0.113.0", 1}, {"203.0.114.0", -1}},
		[]Block{{"::", -1}, {"2001:db8:1000::", 0}, {"2001:db8:2000::", 1}, {"2001:db8:3000::", 2}, {"2001:db8:4000::", -1}})
}

// Subnets of World: where a fake visitor of each place asks from.
const (
	MilanV4   = "198.51.100.0/24"
	NewYorkV4 = "203.0.113.0/24"
	TokyoV4   = "192.0.2.0/24"
	MilanV6   = "2001:db8:1000::/48"
	NowhereV4 = "100.64.0.0/24"
)
