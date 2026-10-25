package link

import (
	"errors"
	"os"
	"strings"
	"testing"
)

const generated = "https://deplo-cb00710a.deplo.site"

func TestStore_aReplacementKeepsOnlyWhatItReceived(t *testing.T) {
	dir := t.TempDir()
	st := NewStore(dir, Addresses{Primary: "https://old.example.com", Fallback: "https://deplo-old.deplo.site"})

	kept, err := st.Replace(Addresses{Primary: "https://new.example.com", Fallback: generated})
	if err != nil {
		t.Fatal(err)
	}
	want := Addresses{Primary: "https://new.example.com", Fallback: generated}
	if kept != want || st.Current() != want {
		t.Fatalf("kept %+v, holds %+v; want only %+v", kept, st.Current(), want)
	}
	// What the next start reads.
	if saved, err := LoadAddresses(dir); err != nil || saved != want {
		t.Fatalf("after a restart: %+v, %v; want %+v", saved, err, want)
	}
	if fi, err := os.Stat(AddressesPath(dir)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("saved file: %v, %v; want mode 0600", fi, err)
	}
}

func TestStore_anEmptyFallbackIsDropped(t *testing.T) {
	dir := t.TempDir()
	st := NewStore(dir, Addresses{Primary: "https://panel.example.com", Fallback: generated})

	kept, err := st.Replace(Addresses{Primary: "https://panel.example.com", Fallback: "  "})
	if err != nil {
		t.Fatal(err)
	}
	if kept.Fallback != "" || st.Current().Fallback != "" {
		t.Fatalf("kept %+v: the fallback should be gone", kept)
	}
	raw, _ := os.ReadFile(AddressesPath(dir))
	if strings.Contains(string(raw), "fallback") || strings.Contains(string(raw), "deplo.site") {
		t.Fatalf("the saved file still names a fallback: %s", raw)
	}
}

func TestStore_refusesAPairItCouldNotDial(t *testing.T) {
	was := Addresses{Primary: "https://panel.example.com", Fallback: generated}
	for name, a := range map[string]Addresses{
		"no primary":                    {Fallback: generated},
		"a primary that is no address":  {Primary: "panel.example.com"},
		"a fallback that is no address": {Primary: "https://panel.example.com", Fallback: "ftp://x"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := SaveAddresses(dir, was); err != nil {
				t.Fatal(err)
			}
			st := NewStore(dir, was)
			if _, err := st.Replace(a); !errors.Is(err, ErrInvalidAddresses) {
				t.Fatalf("Replace(%+v) = %v, want ErrInvalidAddresses", a, err)
			}
			if st.Current() != was {
				t.Fatalf("holds %+v after a refused pair, want %+v", st.Current(), was)
			}
			if saved, _ := LoadAddresses(dir); saved != was {
				t.Fatalf("saved %+v after a refused pair, want %+v", saved, was)
			}
		})
	}
}
