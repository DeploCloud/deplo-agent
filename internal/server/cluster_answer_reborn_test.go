package server

import (
	"sort"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/mesh"
)

// c50: after a kill -9 of eu-1's agent, visitors carried to it through eu-2 were answered 0.6 to 3.1 s past its return.
func TestAnswers_carriedVisitorsReachAPeerWithinASecondOfItsReturn(t *testing.T) {
	a, b, fa, _, members := answerPair(t)
	id, _ := b.clusterIdentity()
	joinNetwork(t, a, b, sourcePrint[a], id.Fingerprint, true)
	eventually(t, "a direct path between the members", func() bool { return pathFrom(a, id.Fingerprint) == mesh.PathDirect })
	// The killed agent is on no relay either.
	relayOf[a].Refuse(true)
	bothAnswer(t, a, b, members)
	placeOn(t, a, "srvA", appEntry("srvB"))
	placeOn(t, b, "srvB", appEntry("srvB"))
	s := fa.spec(t, answerAlias)
	if got := askPort(t, s, shopTLS); got != "proxy-b:PING" {
		t.Fatalf("before: %q", got)
	}

	n := b.network()
	n.mu.Lock()
	n.stopLocked()
	n.mu.Unlock()
	type visit struct {
		at  time.Time
		got string
	}
	done := make(chan visit, 64)
	ask := func() { go func() { got := askPort(t, s, shopTLS); done <- visit{time.Now(), got} }() }
	for i := 0; i < 6; i++ {
		ask()
		time.Sleep(200 * time.Millisecond)
	}
	n.mu.Lock()
	if err := n.applyLocked(); err != nil {
		n.mu.Unlock()
		t.Fatal(err)
	}
	n.mu.Unlock()
	back := time.Now()
	for i := 0; i < 6; i++ {
		ask()
		time.Sleep(100 * time.Millisecond)
	}
	var late []time.Duration
	for i := 0; i < 12; i++ {
		v := <-done
		if v.got != "proxy-b:PING" {
			t.Fatalf("a visitor got %q", v.got)
		}
		late = append(late, v.at.Sub(back))
	}
	sort.Slice(late, func(i, j int) bool { return late[i] < late[j] })
	t.Logf("answered past the return: %v", late)
	if worst := late[len(late)-1]; worst > time.Second {
		t.Fatalf("a carried visitor was answered %v after its server was back", worst)
	}
}
