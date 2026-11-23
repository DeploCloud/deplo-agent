package server

import (
	"strings"
	"testing"
	"time"
)

func TestRouteWatch_unpauseAfterAReadNeedsAnotherRead(t *testing.T) {
	now := time.Unix(1000, 0)
	w := newRouteWatch()
	w.event("pause", "app", true, now)
	w.event("start", "helper", false, now) // the proxy reads every container: app is paused, so it has no routes
	w.event("unpause", "app", true, now)
	if w.dueAt(now.Add(proxyResyncAfter)) != true {
		t.Fatal("an App unpaused after the proxy read it paused must make the proxy read again")
	}
	w.resynced()
	if w.dueAt(now.Add(time.Hour)) {
		t.Fatal("one read is enough")
	}
}

func TestRouteWatch_aReadAfterTheUnpauseIsEnough(t *testing.T) {
	now := time.Unix(1000, 0)
	w := newRouteWatch()
	w.event("pause", "app", true, now)
	w.event("start", "helper", false, now)
	w.event("unpause", "app", true, now)
	w.event("die", "helper", false, now.Add(50*time.Millisecond))
	if w.dueAt(now.Add(time.Hour)) {
		t.Fatal("the proxy read again once the App ran: nothing to do")
	}
}

func TestRouteWatch_aPauseNoReadSawChangesNothing(t *testing.T) {
	now := time.Unix(1000, 0)
	w := newRouteWatch()
	w.event("pause", "app", true, now)
	w.event("unpause", "app", true, now)
	if w.dueAt(now.Add(time.Hour)) {
		t.Fatal("the proxy never read while it was paused, so it still routes it")
	}
	w.event("pause", "other", false, now)
	w.event("die", "x", false, now)
	w.event("unpause", "other", false, now)
	if w.dueAt(now.Add(time.Hour)) {
		t.Fatal("a container without routes is none of the proxy's business")
	}
}

func TestRouteWatch_waitsBeforeReading(t *testing.T) {
	now := time.Unix(1000, 0)
	w := newRouteWatch()
	w.event("pause", "app", true, now)
	w.event("die", "helper", false, now)
	w.event("unpause", "app", true, now)
	if w.dueAt(now) {
		t.Fatal("give the event that usually follows a moment first")
	}
}

func TestProxyResync_startsAndStopsAContainer(t *testing.T) {
	argv := dockerOnPath(t, "exit 0")
	proxyResync(t.Context())
	got := strings.Join(argv(), "\n")
	if !strings.Contains(got, "--rm --log-driver=none --network none") || !strings.Contains(got, volumeHelperImage+" true") {
		t.Fatalf("the proxy reads containers again only when one starts or stops: %q", got)
	}
}
