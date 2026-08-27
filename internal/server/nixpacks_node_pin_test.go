package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteNodeVersionPin(t *testing.T) {
	t.Run("writes the pin when the repository says nothing", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"name":"x"}`), 0o644); err != nil {
			t.Fatal(err)
		}
		wrote, err := writeNodeVersionPin(dir, "24")
		if err != nil || !wrote {
			t.Fatalf("wrote=%v err=%v", wrote, err)
		}
		b, err := os.ReadFile(filepath.Join(dir, ".nvmrc"))
		if err != nil || string(b) != "24\n" {
			t.Fatalf("got %q err=%v", b, err)
		}
	})

	for _, tc := range []struct{ name, file, body string }{
		{"nvmrc", ".nvmrc", "20"},
		{"node-version", ".node-version", "v20"},
		{"engines", "package.json", `{"engines":{"node":">=20"}}`},
	} {
		t.Run("leaves "+tc.name+" alone", func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, tc.file), []byte(tc.body), 0o644); err != nil {
				t.Fatal(err)
			}
			wrote, err := writeNodeVersionPin(dir, "24")
			if err != nil || wrote {
				t.Fatalf("wrote=%v err=%v", wrote, err)
			}
		})
	}
}
