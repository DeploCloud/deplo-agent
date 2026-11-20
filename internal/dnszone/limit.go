package dnszone

import (
	"net/netip"
	"sync"
	"time"
)

// A UDP answer's source can be forged, so each client network gets a budget: past it, every second answer goes out
// truncated (a real resolver then asks over TCP, which cannot be forged) and the rest are dropped.
const (
	limitRate    = 50
	limitBurst   = 100
	limitSlip    = 2
	limitTracked = 100_000
	limitIdle    = time.Minute
)

type verdict int

const (
	send verdict = iota
	slip
	drop
)

type bucket struct {
	tokens float64
	at     time.Time
	over   uint32
}

type limiter struct {
	mu        sync.Mutex
	rate      float64
	burst     float64
	buckets   map[netip.Prefix]*bucket
	swept     time.Time
	truncated uint64
	dropped   uint64
	networks  map[netip.Prefix]bool
}

func newLimiter(rate, burst float64) *limiter {
	return &limiter{rate: rate, burst: burst, buckets: map[netip.Prefix]*bucket{}, networks: map[netip.Prefix]bool{}}
}

// network is the client network a budget is kept for: a /24 for IPv4, a /56 for IPv6.
func network(a netip.Addr) netip.Prefix {
	a = a.Unmap()
	bits := 56
	if a.Is4() {
		bits = 24
	}
	p, _ := a.Prefix(bits)
	return p
}

func (l *limiter) allow(a netip.Addr, now time.Time) verdict {
	key := network(a)
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.swept) > 10*time.Second {
		l.sweep(now)
	}
	b := l.buckets[key]
	if b == nil {
		if len(l.buckets) >= limitTracked {
			// Too many networks at once is itself an attack: a new one gets no budget until some go quiet.
			l.dropped++
			return drop
		}
		b = &bucket{tokens: l.burst, at: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.at).Seconds()*l.rate)
	b.at = now
	if b.tokens >= 1 {
		b.tokens--
		b.over = 0
		return send
	}
	b.over++
	l.networks[key] = true
	if b.over%limitSlip == 0 {
		l.truncated++
		return slip
	}
	l.dropped++
	return drop
}

func (l *limiter) sweep(now time.Time) {
	l.swept = now
	for k, b := range l.buckets {
		if now.Sub(b.at) > limitIdle {
			delete(l.buckets, k)
		}
	}
}

// take returns what was limited since the last call, and forgets it.
func (l *limiter) take() (truncated, dropped uint64, networks int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	truncated, dropped, networks = l.truncated, l.dropped, len(l.networks)
	l.truncated, l.dropped = 0, 0
	l.networks = map[netip.Prefix]bool{}
	return
}
