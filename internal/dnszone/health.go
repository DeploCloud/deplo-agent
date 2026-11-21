package dnszone

import "time"

// How a name server answers for its cluster now.
const (
	// StateAnswer: it hears more than half of the cluster and names the healthy servers.
	StateAnswer = "answer"
	// StateStale: it hears exactly half, so no side can hold a majority: the last agreed set.
	StateStale = "stale"
	// StateFail: it hears fewer than half, or has nothing agreed to fall back on: SERVFAIL.
	StateFail = "fail"
	// StateWarming: just started, a server not heard yet may simply not have spoken: the last agreed set.
	StateWarming = "warming"
)

// Member is one server of the cluster as the zone knows it.
type Member struct {
	Fingerprint string
	Addrs       Addrs
	// Emptying: Deplo is moving everything off it, so it leaves the answers while it still counts.
	Emptying bool
}

// Agreed is the last set a name server named while it heard more than half of its cluster.
type Agreed struct {
	Servers []string `json:"servers"`
	Addrs   Addrs    `json:"addrs"`
}

// Verdict is how one name server answers now.
type Verdict struct {
	State   string
	Servers []string
	Addrs   Addrs
	Heard   int
	Of      int
	// Agreed is set when this verdict is a new last agreed set.
	Agreed *Agreed
}

// Decide is the majority rule of ADR-0049: heard is when each member was last heard, self is always heard.
func Decide(members []Member, self string, heard map[string]time.Time, now time.Time, dropAfter time.Duration,
	warming bool, last *Agreed) Verdict {
	return DecideHolding(members, self, heard, now, dropAfter, warming, last, nil)
}

// DecideHolding is Decide with servers held out of every answer, the last agreed set's included, while they still
// count: one just back, or this one until its proxy serves (a moment of its proxy's default certificate otherwise).
func DecideHolding(members []Member, self string, heard map[string]time.Time, now time.Time, dropAfter time.Duration,
	warming bool, last *Agreed, hold map[string]bool) Verdict {
	v := Verdict{Of: len(members)}
	var healthy []Member
	for _, m := range members {
		at, ok := heard[m.Fingerprint]
		if m.Fingerprint == self || ok && now.Sub(at) < dropAfter {
			v.Heard++
			if !m.Emptying && !hold[m.Fingerprint] {
				healthy = append(healthy, m)
			}
		}
	}
	fallBack := func(state string) Verdict {
		if last == nil || len(last.Servers) == 0 {
			v.State = StateFail
			return v
		}
		v.State, v.Servers, v.Addrs = state, last.Servers, last.Addrs
		if kept, addrs := withoutHeld(last, members, hold); len(kept) > 0 {
			v.Servers, v.Addrs = kept, addrs
		}
		return v
	}
	switch {
	case v.Of == 0:
		v.State = StateFail
		return v
	case warming:
		return fallBack(StateWarming)
	case 2*v.Heard < v.Of:
		v.State = StateFail
		return v
	case 2*v.Heard == v.Of:
		return fallBack(StateStale)
	}
	for _, m := range healthy {
		v.Servers = append(v.Servers, m.Fingerprint)
		v.Addrs.V4 = append(v.Addrs.V4, m.Addrs.V4...)
		v.Addrs.V6 = append(v.Addrs.V6, m.Addrs.V6...)
	}
	if len(v.Addrs.V4)+len(v.Addrs.V6) == 0 {
		// Every server it hears is being emptied: naming none would take every domain down.
		v.Servers, v.Addrs = nil, Addrs{}
		return fallBack(StateStale)
	}
	v.State = StateAnswer
	if last == nil || !sameStrings(last.Servers, v.Servers) || !sameStrings(last.Addrs.V4, v.Addrs.V4) ||
		!sameStrings(last.Addrs.V6, v.Addrs.V6) {
		v.Agreed = &Agreed{Servers: v.Servers, Addrs: v.Addrs}
	}
	return v
}

// withoutHeld is the last agreed set less the servers held out, by their addresses; nil when nothing is held.
func withoutHeld(last *Agreed, members []Member, hold map[string]bool) ([]string, Addrs) {
	if len(hold) == 0 {
		return nil, Addrs{}
	}
	gone := map[string]bool{}
	for _, m := range members {
		if hold[m.Fingerprint] {
			for _, a := range append(append([]string(nil), m.Addrs.V4...), m.Addrs.V6...) {
				gone[a] = true
			}
		}
	}
	var servers []string
	for _, fp := range last.Servers {
		if !hold[fp] {
			servers = append(servers, fp)
		}
	}
	var out Addrs
	for _, a := range last.Addrs.V4 {
		if !gone[a] {
			out.V4 = append(out.V4, a)
		}
	}
	for _, a := range last.Addrs.V6 {
		if !gone[a] {
			out.V6 = append(out.V6, a)
		}
	}
	if len(out.V4)+len(out.V6) == 0 {
		return nil, Addrs{}
	}
	return servers, out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
