package server

import (
	"testing"
	"time"
)

// The cache-record recovery: classic docker prints bare ids, buildx prints a table; headers, totals and warnings never look like ids.
func TestPrunedCacheRecordIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  string
		want int
	}{
		{"classic bare ids", "pu0aq3k0be2nxyf87qw0jbh08\nvhcz1lchp7f0nrnu29jj0oy1n\n\nTotal:\t1.5GB\n", 2},
		{"buildx table", "ID\t\tRECLAIMABLE\tSIZE\tLAST ACCESSED\nx1y2z3a4b5c6d7e8 \ttrue\t203.7MB\t2 hours ago\n\nTotal:\t203.7MB\n", 1},
		{"legacy hex id", "0123456789ab\nTotal reclaimed space: 12MB\n", 1},
		{"total only", "Total reclaimed space: 1.5GB\n", 0},
		{"warnings and noise", "WARNING! This will remove all dangling build cache.\nDeleted build cache objects:\nTotal:\t0B\n", 0},
		{"empty", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(prunedCacheRecordIDs(tc.out)); got != tc.want {
				t.Errorf("prunedCacheRecordIDs(%q) counted %d, want %d", tc.out, got, tc.want)
			}
		})
	}
}
func TestParseHumanSize(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"0B", 0},
		{"8.19kB", 8190},
		{"276kB", 276000},
		{"449.4MB", 449400000},
		{"3.89GB", 3890000000},
		{"1.5GiB", 1610612736},
		{"", 0},
		{"garbage", 0},
	} {
		if got := parseHumanSize(tc.in); got != tc.want {
			t.Errorf("parseHumanSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// Docker's own printed total is authoritative INCLUDING ZERO: "Total reclaimed space: 0B" means the prune freed nothing, and falling back to the pre-flight estimate there is how the history once recorded a gigabyte that was never freed.
func TestParsePrunedTotal_zeroTotalIsAuthoritative(t *testing.T) {
	for _, tc := range []struct {
		name   string
		out    string
		want   int64
		wantOK bool
	}{
		{"image prune says 0B", "Deleted Images:\nuntagged: x\nTotal reclaimed space: 0B\n", 0, true},
		{"buildx says 0B", "Total:\t0B\n", 0, true},
		{"real total", "Total reclaimed space: 449.4MB\n", 449400000, true},
		{"no total line", "nothing to see here\n", 0, false},
		{"unreadable total", "Total: garbage\n", 0, false},
		{"empty output", "", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parsePrunedTotal(tc.out)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("parsePrunedTotal(%q) = %d, %v, want %d, %v", tc.out, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// An object whose age we cannot read is never a candidate while an age filter is set - we would rather leave it behind than delete something we know nothing about.
func TestOlderThan_unparseableNeverQualifies(t *testing.T) {
	cutoff := time.Now().Add(-24 * time.Hour)
	if olderThan("not a timestamp", cutoff) {
		t.Error("an unreadable timestamp must not qualify under an age filter")
	}
	if !olderThan("not a timestamp", time.Time{}) {
		t.Error("with no age filter, everything qualifies")
	}
	old := time.Now().Add(-48 * time.Hour).Format(time.RFC3339Nano)
	if !olderThan(old, cutoff) {
		t.Errorf("%q is older than the cutoff", old)
	}
}
