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

func TestDecide_warmingAnswersTheLastAgreedSet(t *testing.T) {
	now := time.Now()
	last := &Agreed{Servers: []string{"s0", "s1", "s2"}, Addrs: Addrs{V4: []string{"203.0.113.1", "203.0.113.2", "203.0.113.3"}}}
	v := Decide(members(3), "s0", nil, now, 10*time.Second, true, last)
	if v.State != StateWarming || len(v.Servers) != 3 {
		t.Fatalf("%+v", v)
	}
	if v := Decide(members(3), "s0", nil, now, 10*time.Second, true, nil); v.State != StateFail {
		t.Fatalf("nothing agreed: %+v", v)
	}
	// A cluster of one is always a majority of itself.
	if v := Decide(members(1), "s0", nil, now, 10*time.Second, false, nil); v.State != StateAnswer || len(v.Servers) != 1 {
		t.Fatalf("one server: %+v", v)
	}
}
