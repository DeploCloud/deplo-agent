package server

import (
	"testing"
)

// The ceiling has to be a real bound on any disk, and never so small on a modest VPS that it defeats the point of caching at all.
func TestBuildCacheCeilingScalesAndClamps(t *testing.T) {
	const gib = int64(1) << 30
	cases := []struct {
		name string
		disk int64
		want int64
	}{
		{"242 GB host (this one)", 242 * gib, 24 * gib},
		{"100 GB host", 100 * gib, 10 * gib},
		{"20 GB VPS", 20 * gib, 2 * gib},
		{"8 GB VPS", 8 * gib, 2 * gib},
		{"2 TB host", 2048 * gib, 50 * gib},
	}
	for _, c := range cases {
		got := buildCacheCeilingFor(c.disk)
		if got/gib != c.want/gib {
			t.Errorf("%s: buildCacheCeilingFor(%d GiB) = %d GiB; want %d",
				c.name, c.disk/gib, got/gib, c.want/gib)
		}
		if got < buildCacheCapMin || got > buildCacheCapMax {
			t.Errorf("%s: ceiling %d escapes its clamp", c.name, got)
		}
		if c.disk >= buildCacheCapMin && got > c.disk {
			t.Errorf("%s: ceiling %d exceeds the whole disk %d", c.name, got, c.disk)
		}
	}
}

// An unmeasurable filesystem must yield NO ceiling - better to fall back to the age filter alone than to invent one out of a failed statfs and prune somebody's cache on the strength of it.
func TestBuildCacheCeilingZeroWhenDiskUnknown(t *testing.T) {
	if got := buildCacheCeiling("/definitely/not/a/real/mount/point"); got != 0 {
		t.Fatalf("unmeasurable path produced a ceiling of %d; want 0", got)
	}
}

// filesystemBytes reads the real filesystem behind a path, and treats "" as the root - the same default the metrics sampler uses.
func TestFilesystemBytes(t *testing.T) {
	if got := filesystemBytes(t.TempDir()); got <= 0 {
		t.Fatalf("filesystemBytes(tempdir) = %d; want a positive size", got)
	}
	if got := filesystemBytes(""); got <= 0 {
		t.Fatalf(`filesystemBytes("") = %d; want the root filesystem's size`, got)
	}
}
