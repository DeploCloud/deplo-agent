package server

import (
	"testing"
	"time"
)

func cacheRec(size string, age time.Duration, now time.Time) buildCacheRecord {
	return buildCacheRecord{
		ID:         "rec",
		Size:       size,
		LastUsedAt: now.Add(-age).Format(time.RFC3339Nano),
	}
}

// The shape that defeated the old ceiling: a cache well over the limit whose RECLAIMABLE part is under it, which is all docker's own space flags ever look at - they free 0 bytes and the cache keeps growing.
func TestCeilingEvictsOldestUntilUnderTheLimit(t *testing.T) {
	now := time.Now()
	const gb = int64(1e9)
	records := []buildCacheRecord{
		cacheRec("10GB", 100*time.Hour, now),
		cacheRec("10GB", 50*time.Hour, now),
		cacheRec("10GB", 10*time.Hour, now),
		cacheRec("7.14GB", time.Hour, now),
	}

	hours, over := ceilingEvictionHours(records, 25*gb, now)
	if !over {
		t.Fatal("a 37.14 GB cache under a 25 GB ceiling has to evict")
	}
	if hours != 50 {
		t.Fatalf("until=%dh; want 50h - the two oldest records cover the 12.14 GB overflow", hours)
	}
}

func TestCeilingLeavesACacheUnderTheLimitAlone(t *testing.T) {
	now := time.Now()
	records := []buildCacheRecord{cacheRec("5GB", 400*time.Hour, now)}
	if hours, over := ceilingEvictionHours(records, int64(10e9), now); over {
		t.Fatalf("pruned (until=%dh) a cache already under the ceiling", hours)
	}
}

// Never 0h: that would evict the cache of a build running right now, and a converging sweep beats a destructive one.
func TestCeilingFloorsTheAgeAtOneHour(t *testing.T) {
	now := time.Now()
	records := []buildCacheRecord{cacheRec("20GB", 20*time.Minute, now)}
	hours, over := ceilingEvictionHours(records, int64(1e9), now)
	if !over || hours != buildCacheCeilingMinHours {
		t.Fatalf("until=%dh over=%v; want %dh", hours, over, buildCacheCeilingMinHours)
	}
}

// An undatable record still counts toward the total, so the ceiling fires; it just cannot steer the age.
func TestCeilingFiresOnRecordsWithNoUsableDate(t *testing.T) {
	now := time.Now()
	records := []buildCacheRecord{{ID: "x", Size: "30GB", CreatedAt: "not a date"}}
	hours, over := ceilingEvictionHours(records, int64(1e9), now)
	if !over || hours != buildCacheCeilingMinHours {
		t.Fatalf("until=%dh over=%v; want %dh", hours, over, buildCacheCeilingMinHours)
	}
}

func TestCeilingIgnoresAnEmptyCache(t *testing.T) {
	if _, over := ceilingEvictionHours(nil, int64(1e9), time.Now()); over {
		t.Fatal("an empty cache has nothing to evict")
	}
}
