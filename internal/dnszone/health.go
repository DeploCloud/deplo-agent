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
	v := Verdict{Of: len(members)}
	var healthy []Member
	for _, m := range members {
		at, ok := heard[m.Fingerprint]
		if m.Fingerprint == self || ok && now.Sub(at) < dropAfter {
			v.Heard++
			if !m.Emptying {
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
