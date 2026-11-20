package dnszone

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DeploCloud/deplo-agent/internal/dnszone/geotest"
)

func world(t testing.TB) *Geo {
	t.Helper()
	g, err := ParseGeo(geotest.World())
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestGeo_locatesAnAddress(t *testing.T) {
	g := world(t)
	if g.Release() != 20261101 || len(g.Digest()) != 64 {
		t.Fatalf("release %d digest %q", g.Release(), g.Digest())
	}
	for addr, want := range map[string]*geotest.Place{
		"198.51.100.0": &geotest.Milan, "198.51.100.255": &geotest.Milan, "198.51.101.0": nil,
		"203.0.113.77": &geotest.NewYork, "192.0.2.53": &geotest.Tokyo, "0.0.0.0": nil, "255.255.255.255": nil,
		"10.0.0.53": nil, "::ffff:198.51.100.9": &geotest.Milan,
		"2001:db8:1000:ab::1": &geotest.Milan, "2001:db8:2fff:ffff::": &geotest.NewYork, "2001:db8:3000::": &geotest.Tokyo,
		"2001:db8:4000::": nil, "::": nil, "ffff::1": nil,
	} {
		at, ok := g.Locate(netip.MustParseAddr(addr))
		switch {
		case want == nil && ok:
			t.Errorf("%s: placed at %v", addr, at)
		case want != nil && (!ok || at.Lat != want.Lat || at.Lon != want.Lon):
			t.Errorf("%s: %v %v, want %v", addr, at, ok, *want)
		}
	}
	var none *Geo
	if _, ok := none.Locate(netip.MustParseAddr("198.51.100.1")); ok {
		t.Fatal("no table placed somebody")
	}
}

// reseal recomputes a table's digest after a test changed it, so the structural checks are what refuse it.
func reseal(raw []byte) []byte {
	body := raw[:len(raw)-sha256.Size]
	sum := sha256.Sum256(body)
	return append(append([]byte{}, body...), sum[:]...)
}

func TestGeo_refusesACorruptTable(t *testing.T) {
	good := geotest.World()
	if _, err := ParseGeo(good); err != nil {
		t.Fatal(err)
	}
	// Any one byte changed anywhere is caught by the digest.
	for i := range good {
		bad := append([]byte{}, good...)
		bad[i] ^= 0x40
		if _, err := ParseGeo(bad); err == nil {
			t.Fatalf("byte %d changed: accepted", i)
		}
	}
	le := binary.LittleEndian
	places := 32
	v4 := places + 3*4
	structural := map[string]func([]byte) []byte{
		"cut short":      func(b []byte) []byte { return b[:len(b)-1] },
		"empty":          func([]byte) []byte { return nil },
		"another format": func(b []byte) []byte { copy(b, "NOTGEO!!"); return reseal(b) },
		"version 2":      func(b []byte) []byte { le.PutUint16(b[8:], 2); return reseal(b) },
		"a byte too many": func(b []byte) []byte {
			return reseal(append(append(b[:len(b)-sha256.Size], 0), make([]byte, sha256.Size)...))
		},
		"a place off":       func(b []byte) []byte { le.PutUint16(b[places:], uint16(9001)); return reseal(b) },
		"a longitude off":   func(b []byte) []byte { le.PutUint16(b[places+2:], uint16(18001)); return reseal(b) },
		"not from zero":     func(b []byte) []byte { le.PutUint32(b[v4:], 1); return reseal(b) },
		"out of order":      func(b []byte) []byte { le.PutUint32(b[v4+8:], 0xFFFFFFFF); return reseal(b) },
		"a place not there": func(b []byte) []byte { le.PutUint16(b[v4+7*4+2:], 3); return reseal(b) },
		"no IPv6 block":     func(b []byte) []byte { le.PutUint32(b[24:], 0); return reseal(b) },
		"too many places":   func(b []byte) []byte { le.PutUint32(b[16:], 0x10000); return reseal(b) },
	}
	for what, mutate := range structural {
		if _, err := ParseGeo(mutate(append([]byte{}, good...))); err == nil {
			t.Errorf("%s: accepted", what)
		}
	}
	if _, err := ParseGeo(make([]byte, MaxGeoBytes+1)); err == nil || !strings.Contains(err.Error(), "bytes") {
		t.Errorf("a huge table: %v", err)
	}
}

// The table Deplo's release builds (lib/geo in DeploCloud/deplo writes testdata/geo-table.golden.bin from a small CSV):
// this agent reads it and places each address of the reference where the panel does.
func TestGeo_thePanelsTableParses(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "geo-table.golden.bin"))
	if err != nil {
		t.Fatal(err)
	}
	g, err := ParseGeo(raw)
	if err != nil {
		t.Fatalf("the reference table is refused: %v", err)
	}
	var lookups []struct {
		Address string      `json:"address"`
		At      *[2]float64 `json:"at"`
	}
	ref, err := os.ReadFile(filepath.Join("testdata", "geo-lookups.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(ref, &lookups); err != nil {
		t.Fatal(err)
	}
	if len(lookups) == 0 {
		t.Fatal("no lookup in the reference")
	}
	for _, l := range lookups {
		at, ok := g.Locate(netip.MustParseAddr(l.Address))
		switch {
		case l.At == nil && ok:
			t.Errorf("%s: placed at %v, the panel places it nowhere", l.Address, at)
		case l.At != nil && (!ok || at.Lat != l.At[0] || at.Lon != l.At[1]):
			t.Errorf("%s: %v %v, the panel says %v", l.Address, at, ok, *l.At)
		}
	}
}
