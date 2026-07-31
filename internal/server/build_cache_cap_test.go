package server

import (
	"testing"
)

// The ceiling has to be a real bound on any disk, and never so small on a modest
// VPS that it defeats the point of caching at all.
func TestBuildCacheCapScalesAndClamps(t *testing.T) {
	const gib = int64(1) << 30
	cases := []struct {
		name              string
		disk              int64
		wantMax, wantFree int64
	}{
		// A tenth, straight through.
		{"242 GB host (this one)", 242 * gib, 24 * gib, 20 * gib},
		{"100 GB host", 100 * gib, 10 * gib, 10 * gib},
		// Small VPS: the floor keeps enough for a nix layer + a package cache.
		{"20 GB VPS", 20 * gib, 2 * gib, 2 * gib},
		{"8 GB VPS", 8 * gib, 2 * gib, 2 * gib},
		// Big disk: the cache must never become the biggest thing on it.
		{"2 TB host", 2048 * gib, 50 * gib, 20 * gib},
	}
	for _, c := range cases {
		gotMax, gotFree := buildCacheCap(c.disk)
		// A tenth of 242 GiB is 24.2 GiB; compare on whole GiB.
		if gotMax/gib != c.wantMax/gib || gotFree/gib != c.wantFree/gib {
			t.Errorf("%s: buildCacheCap(%d) = (%d, %d) GiB; want (%d, %d)",
				c.name, c.disk/gib, gotMax/gib, gotFree/gib, c.wantMax/gib, c.wantFree/gib)
		}
		if gotMax < buildCacheCapMin || gotMax > buildCacheCapMax {
			t.Errorf("%s: max %d escapes its clamp", c.name, gotMax)
		}
		// A ceiling that exceeds the disk would be no ceiling at all.
		if c.disk >= buildCacheCapMin && gotMax > c.disk {
			t.Errorf("%s: cap %d exceeds the whole disk %d", c.name, gotMax, c.disk)
		}
	}
}

// An unmeasurable filesystem must yield NO flags — better to fall back to the
// age filter alone than to invent a ceiling out of a failed statfs and prune
// somebody's cache on the strength of it.
func TestBuildCacheCapArgsSilentWhenDiskUnknown(t *testing.T) {
	if got := buildCacheCapArgs(t.Context(), "/definitely/not/a/real/mount/point"); got != nil {
		t.Fatalf("unmeasurable path produced flags: %v", got)
	}
}

// filesystemBytes reads the real filesystem behind a path, and treats "" as the
// root — the same default the metrics sampler uses.
func TestFilesystemBytes(t *testing.T) {
	if got := filesystemBytes(t.TempDir()); got <= 0 {
		t.Fatalf("filesystemBytes(tempdir) = %d; want a positive size", got)
	}
	if got := filesystemBytes(""); got <= 0 {
		t.Fatalf(`filesystemBytes("") = %d; want the root filesystem's size`, got)
	}
}
