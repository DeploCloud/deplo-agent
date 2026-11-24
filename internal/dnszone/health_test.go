package dnszone

import (
	"slices"
	"testing"
	"time"
)

func members(n int) []Member {
	var out []Member
	for i := range n {
		out = append(out, Member{Fingerprint: "s" + itoa(i), Addrs: Addrs{V4: []string{"203.0.113." + itoa(i+1)}}})
	}
	return out
}

// heardFrom says every listed server was heard a second ago.
func heardFrom(now time.Time, fps ...string) map[string]time.Time {
	h := map[string]time.Time{}
	for _, fp := range fps {
		h[fp] = now.Add(-time.Second)
	}
	return h
}

func TestDecide_majorityNamesTheHealthyServers(t *testing.T) {
	now := time.Now()
	m := members(3)
	v := Decide(m, "s0", heardFrom(now, "s1", "s2"), now, 10*time.Second, false, nil)
	if v.State != StateAnswer || !slices.Equal(v.Servers, []string{"s0", "s1", "s2"}) || v.Agreed == nil {
		t.Fatalf("all heard: %+v", v)
	}
	// s2 stopped 11 s ago: it leaves the answer, two of three is still a majority.
	h := heardFrom(now, "s1")
	h["s2"] = now.Add(-11 * time.Second)
	v = Decide(m, "s0", h, now, 10*time.Second, false, v.Agreed)
	if v.State != StateAnswer || !slices.Equal(v.Servers, []string{"s0", "s1"}) || !slices.Equal(v.Addrs.V4, []string{"203.0.113.1", "203.0.113.2"}) {
		t.Fatalf("s2 silent: %+v", v)
	}
	// Heard again, it comes back.
	v = Decide(m, "s0", heardFrom(now, "s1", "s2"), now, 10*time.Second, false, v.Agreed)
	if !slices.Equal(v.Servers, []string{"s0", "s1", "s2"}) {
		t.Fatalf("s2 back: %+v", v)
	}
	// The removal time is the cluster's: at 30 s, 11 s of silence is not enough.
	v = Decide(m, "s0", h, now, 30*time.Second, false, nil)
	if !slices.Equal(v.Servers, []string{"s0", "s1", "s2"}) {
		t.Fatalf("removal time 30 s: %+v", v)
	}
}

func TestDecide_anEmptiedServerNeverAppearsButStillCounts(t *testing.T) {
	now := time.Now()
	m := members(3)
	m[2].Emptying = true
	v := Decide(m, "s0", heardFrom(now, "s2"), now, 10*time.Second, false, nil)
	if v.State != StateAnswer || v.Heard != 2 || !slices.Equal(v.Servers, []string{"s0"}) {
		t.Fatalf("%+v", v)
	}
	// Every server it hears is being emptied: the last agreed set, never an empty answer.
	m[0].Emptying = true
	last := &Agreed{Servers: []string{"s1"}, Addrs: Addrs{V4: []string{"203.0.113.2"}}}
	v = Decide(m, "s0", heardFrom(now, "s2"), now, 10*time.Second, false, last)
	if v.State != StateStale || !slices.Equal(v.Servers, []string{"s1"}) {
		t.Fatalf("%+v", v)
	}
}

func TestDecide_fewerThanHalfFailsNeverOnlyItself(t *testing.T) {
	now := time.Now()
	last := &Agreed{Servers: []string{"s0", "s1", "s2"}, Addrs: Addrs{V4: []string{"203.0.113.1"}}}
	for _, n := range []int{3, 4, 5, 7} {
		v := Decide(members(n), "s0", nil, now, 10*time.Second, false, last)
		if v.State != StateFail || len(v.Servers) != 0 || v.Agreed != nil {
			t.Fatalf("%d servers, alone: %+v", n, v)
		}
	}
}

func TestDecide_exactlyHalfRepeatsTheLastAgreedSet(t *testing.T) {
	now := time.Now()
	m := members(4)
	all := Decide(m, "s0", heardFrom(now, "s1", "s2", "s3"), now, 10*time.Second, false, nil)
	// An even split: each side hears two of four, and both answer what they last agreed on.
	left := Decide(m, "s0", heardFrom(now, "s1"), now, 10*time.Second, false, all.Agreed)
	right := Decide(m, "s2", heardFrom(now, "s3"), now, 10*time.Second, false, all.Agreed)
	for _, v := range []Verdict{left, right} {
		if v.State != StateStale || !slices.Equal(v.Servers, []string{"s0", "s1", "s2", "s3"}) || v.Agreed != nil {
			t.Fatalf("split: %+v", v)
		}
	}
	// Nothing agreed yet: an error, rather than each half naming itself.
	if v := Decide(m, "s0", heardFrom(now, "s1"), now, 10*time.Second, false, nil); v.State != StateFail {
		t.Fatalf("split with nothing agreed: %+v", v)
	}
}

func TestDecide_aPartitionOfFive(t *testing.T) {
	now := time.Now()
	m := members(5)
	big := []string{"s0", "s1", "s2"}
	for _, self := range big {
		v := Decide(m, self, heardFrom(now, big...), now, 10*time.Second, false, nil)
		if v.State != StateAnswer || !slices.Equal(v.Servers, big) {
			t.Fatalf("the bigger side, %s: %+v", self, v)
		}
	}
	for _, self := range []string{"s3", "s4"} {
		v := Decide(m, self, heardFrom(now, "s3", "s4"), now, 10*time.Second, false, &Agreed{Servers: []string{"s3"}})
		if v.State != StateFail {
			t.Fatalf("the smaller side, %s: %+v", self, v)
		}
	}
}

// ADR-0049: a restarted name server answers once it hears more than half of its cluster, never its old set.
func TestDecide_warmingAnswersOnceItHearsMoreThanHalf(t *testing.T) {
	now := time.Now()
	last := &Agreed{Servers: []string{"s0", "s1", "s2"}, Addrs: Addrs{V4: []string{"203.0.113.1", "203.0.113.2", "203.0.113.3"}}}
	// Back with its old set and hearing only itself: nothing from the old set, nothing at all.
	if v := Decide(members(3), "s0", nil, now, 10*time.Second, true, last); v.State != StateWarming || len(v.Servers) != 0 {
		t.Fatalf("alone: %+v", v)
	}
	// Two of three, one peer unreachable: it answers what it hears now, s2 left out.
	v := Decide(members(3), "s0", heardFrom(now, "s1"), now, 10*time.Second, true, last)
	if v.State != StateAnswer || !slices.Equal(v.Servers, []string{"s0", "s1"}) || !slices.Equal(v.Addrs.V4, []string{"203.0.113.1", "203.0.113.2"}) {
		t.Fatalf("two of three: %+v", v)
	}
	// Holding itself until its proxy serves, it names the peer it hears.
	v = DecideHolding(members(3), "s0", heardFrom(now, "s1"), now, 10*time.Second, true, last, map[string]bool{"s0": true})
	if v.State != StateAnswer || !slices.Equal(v.Servers, []string{"s1"}) {
		t.Fatalf("held itself: %+v", v)
	}
	// Exactly half is no majority: still nothing.
	if v := Decide(members(4), "s0", heardFrom(now, "s1"), now, 10*time.Second, true, last); v.State != StateWarming {
		t.Fatalf("two of four: %+v", v)
	}
	// Hearing every server, it answers what it hears now, not what it agreed before.
	v = Decide(members(3), "s0", heardFrom(now, "s1", "s2"), now, 10*time.Second, true, &Agreed{Servers: []string{"s9"}})
	if v.State != StateAnswer || !slices.Equal(v.Servers, []string{"s0", "s1", "s2"}) {
		t.Fatalf("heard all: %+v", v)
	}
	// A cluster of one is always a majority of itself.
	if v := Decide(members(1), "s0", nil, now, 10*time.Second, true, nil); v.State != StateAnswer || len(v.Servers) != 1 {
		t.Fatalf("one server: %+v", v)
	}
}

// A server just back, or this one before its proxy serves, counts for the majority but is named by no answer.
func TestDecideHolding_aHeldServerCountsButIsNamedNowhere(t *testing.T) {
	now := time.Now()
	m := members(3)
	hold := map[string]bool{"s2": true}
	v := DecideHolding(m, "s0", heardFrom(now, "s1", "s2"), now, 10*time.Second, false, nil, hold)
	if v.State != StateAnswer || v.Heard != 3 || !slices.Equal(v.Servers, []string{"s0", "s1"}) ||
		!slices.Equal(v.Addrs.V4, []string{"203.0.113.1", "203.0.113.2"}) {
		t.Fatalf("verdict %+v", v)
	}
	// Starting and hearing every server, it answers them without the held one.
	last := &Agreed{Servers: []string{"s0", "s1", "s2"}, Addrs: Addrs{V4: []string{"203.0.113.1", "203.0.113.2", "203.0.113.3"}}}
	v = DecideHolding(m, "s0", heardFrom(now, "s1", "s2"), now, 10*time.Second, true, last, map[string]bool{"s0": true})
	if v.State != StateAnswer || !slices.Equal(v.Servers, []string{"s1", "s2"}) ||
		!slices.Equal(v.Addrs.V4, []string{"203.0.113.2", "203.0.113.3"}) {
		t.Fatalf("warming %+v", v)
	}
	// Holding every server it hears would answer nothing: it answers the last agreed set.
	v = DecideHolding(m, "s0", heardFrom(now, "s1", "s2"), now, 10*time.Second, false, last, map[string]bool{"s0": true, "s1": true, "s2": true})
	if !slices.Equal(v.Addrs.V4, last.Addrs.V4) {
		t.Fatalf("all held %+v", v)
	}
}

// ADR-0049, members lost one after another: a member the leading leader no longer counts is out of the count and
// of every answer, and a led server that hears exactly half answers with what it hears, not the last agreed set.
func TestDecideLed_aGoneMemberLeavesTheCountAndHalfIsEnough(t *testing.T) {
	now := time.Now()
	m := members(3)
	stale := &Agreed{Servers: []string{"s0", "s1", "s2"}, Addrs: Addrs{V4: []string{"203.0.113.1", "203.0.113.2", "203.0.113.3"}}}
	// s2 gone past the grace, s1 lost since: alone, s0 would fail; led with s2 out, it is one of two.
	if v := Decide(m, "s0", heardFrom(now), now, 10*time.Second, false, stale); v.State != StateFail {
		t.Fatalf("unled: %+v", v)
	}
	v := DecideLed(m, map[string]bool{"s2": true}, "s0", heardFrom(now), now, 10*time.Second, false, stale, nil)
	if v.State != StateAnswer || v.Of != 2 || !slices.Equal(v.Servers, []string{"s0"}) {
		t.Fatalf("led with s2 gone: %+v", v)
	}
	// s2 is never named while gone, even when heard again: it catches up before it hosts.
	v = DecideLed(m, map[string]bool{"s2": true}, "s0", heardFrom(now, "s1", "s2"), now, 10*time.Second, false, stale, nil)
	if !slices.Equal(v.Servers, []string{"s0", "s1"}) {
		t.Fatalf("s2 heard while gone: %+v", v)
	}
	// Warming names nothing, the last agreed set included.
	if v := DecideLed(m, nil, "s0", heardFrom(now), now, 10*time.Second, true, stale, nil); v.State != StateWarming || len(v.Servers) != 0 {
		t.Fatalf("warming: %+v", v)
	}
}

// eu-4, new on 23 Nov 2026, heard every other server first as just back and named only itself for 10 s.
func TestDecideHolding_neverOnlyItselfWhileTheOthersAreHeldAsBack(t *testing.T) {
	now := time.Now()
	for _, n := range []int{2, 3, 4} {
		m := members(n)
		hold := map[string]bool{}
		var fps []string
		for _, x := range m[1:] {
			hold[x.Fingerprint], fps = true, append(fps, x.Fingerprint)
		}
		v := DecideHolding(m, "s0", heardFrom(now, fps...), now, 10*time.Second, false, nil, hold)
		if v.State != StateWarming || len(v.Servers) != 0 || v.Agreed != nil {
			t.Fatalf("%d servers, the others held: %+v", n, v)
		}
	}
	// One of three held still answers the two it can name.
	v := DecideHolding(members(3), "s0", heardFrom(now, "s1", "s2"), now, 10*time.Second, false, nil, map[string]bool{"s1": true})
	if v.State != StateAnswer || !slices.Equal(v.Servers, []string{"s0", "s2"}) {
		t.Fatalf("one held: %+v", v)
	}
	// The last one left of a led cluster still answers alone: nobody else counts.
	v = DecideLed(members(3), map[string]bool{"s1": true, "s2": true}, "s0", heardFrom(now), now, 10*time.Second, false, nil, nil)
	if v.State != StateAnswer || !slices.Equal(v.Servers, []string{"s0"}) {
		t.Fatalf("last one led: %+v", v)
	}
}
