package dnszone

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"sort"
)

// The location table (ADR-0049 phase 2): where an address is, roughly, built by Deplo's release from DB-IP's
// "IP to City Lite" and handed to the agent with the zone. Never downloaded by a server.
//
//	0   "DEPLOGEO"    8 bytes
//	8   version       u16 = 1, then u16 0
//	12  release       u32, YYYYMMDD
//	16  P, N4, N6     u32 each, then u32 0
//	32  P × (lat, lon) i16 each, hundredths of a degree
//	    N4 × u32 IPv4 block start, then N4 × u16 place (0xFFFF: none)
//	    N6 × u64 IPv6 block start (its first 64 bits), then N6 × u16 place
//	    SHA-256 of everything before it
//
// Integers are little-endian; block starts rise strictly from 0, so every address falls in exactly one block.
const (
	geoMagic   = "DEPLOGEO"
	geoVersion = 1
	geoHeader  = 32
	geoNone    = 0xFFFF
	// MaxGeoBytes bounds a table, compressed or not; DB-IP's whole world reduces to about 13 MB.
	MaxGeoBytes  = 64 << 20
	maxGeoPlaces = geoNone
	maxGeoBlocks = 8 << 20
)

// Point is a position on the globe, in degrees.
type Point struct{ Lat, Lon float64 }

// Geo is a location table made ready to look addresses up in.
type Geo struct {
	release uint32
	digest  string
	places  []Point
	v4      []uint32
	v4p     []uint16
	v6      []uint64
	v6p     []uint16
}

// ParseGeo checks a location table whole, its digest included, and refuses any that does not hold together.
func ParseGeo(raw []byte) (*Geo, error) {
	if len(raw) > MaxGeoBytes {
		return nil, fmt.Errorf("a location table of %d bytes", len(raw))
	}
	if len(raw) < geoHeader+sha256.Size || !bytes.Equal(raw[:8], []byte(geoMagic)) {
		return nil, errors.New("not a location table")
	}
	body, sum := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	if got := sha256.Sum256(body); !bytes.Equal(got[:], sum) {
		return nil, errors.New("the location table is damaged: its digest does not match")
	}
	le := binary.LittleEndian
	if v := le.Uint16(raw[8:]); v != geoVersion {
		return nil, fmt.Errorf("a location table of version %d", v)
	}
	g := &Geo{release: le.Uint32(raw[12:]), digest: hex.EncodeToString(sum)}
	p, n4, n6 := int(le.Uint32(raw[16:])), int(le.Uint32(raw[20:])), int(le.Uint32(raw[24:]))
	if p > maxGeoPlaces || n4 < 1 || n4 > maxGeoBlocks || n6 < 1 || n6 > maxGeoBlocks {
		return nil, fmt.Errorf("a location table of %d places, %d and %d blocks", p, n4, n6)
	}
	if want := geoHeader + 4*p + 6*n4 + 10*n6; len(body) != want {
		return nil, fmt.Errorf("a location table of %d bytes, %d expected", len(body), want)
	}
	off := geoHeader
	g.places = make([]Point, p)
	for i := range g.places {
		lat, lon := int16(le.Uint16(body[off:])), int16(le.Uint16(body[off+2:]))
		if lat < -9000 || lat > 9000 || lon < -18000 || lon > 18000 {
			return nil, fmt.Errorf("place %d is off the globe", i)
		}
		g.places[i] = Point{Lat: float64(lat) / 100, Lon: float64(lon) / 100}
		off += 4
	}
	g.v4, g.v4p = make([]uint32, n4), make([]uint16, n4)
	for i := range g.v4 {
		g.v4[i] = le.Uint32(body[off:])
		off += 4
	}
	for i := range g.v4p {
		g.v4p[i] = le.Uint16(body[off:])
		off += 2
	}
	g.v6, g.v6p = make([]uint64, n6), make([]uint16, n6)
	for i := range g.v6 {
		g.v6[i] = le.Uint64(body[off:])
		off += 8
	}
	for i := range g.v6p {
		g.v6p[i] = le.Uint16(body[off:])
		off += 2
	}
	if g.v4[0] != 0 || g.v6[0] != 0 {
		return nil, errors.New("the location table does not start at the first address")
	}
	for i := 1; i < n4; i++ {
		if g.v4[i] <= g.v4[i-1] {
			return nil, fmt.Errorf("IPv4 block %d is out of order", i)
		}
	}
	for i := 1; i < n6; i++ {
		if g.v6[i] <= g.v6[i-1] {
			return nil, fmt.Errorf("IPv6 block %d is out of order", i)
		}
	}
	for _, list := range [][]uint16{g.v4p, g.v6p} {
		for i, at := range list {
			if at != geoNone && int(at) >= p {
				return nil, fmt.Errorf("block %d names place %d of %d", i, at, p)
			}
		}
	}
	return g, nil
}

// Digest names the table: the hex SHA-256 it carries.
func (g *Geo) Digest() string { return g.digest }

// Release is DB-IP's release the table was built from, as YYYYMMDD.
func (g *Geo) Release() uint32 { return g.release }

// Locate is where an address is, or false when the table has no place for it.
func (g *Geo) Locate(a netip.Addr) (Point, bool) {
	if g == nil || !a.IsValid() {
		return Point{}, false
	}
	a = a.Unmap()
	var at uint16
	if a.Is4() {
		b := a.As4()
		k := binary.BigEndian.Uint32(b[:])
		at = g.v4p[sort.Search(len(g.v4), func(i int) bool { return g.v4[i] > k })-1]
	} else {
		b := a.As16()
		k := binary.BigEndian.Uint64(b[:8])
		at = g.v6p[sort.Search(len(g.v6), func(i int) bool { return g.v6[i] > k })-1]
	}
	if at == geoNone {
		return Point{}, false
	}
	return g.places[at], true
}
