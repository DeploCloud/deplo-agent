package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

const appRestoreJSON = `{"kind":"BACKUP_KIND_PROJECT","fromReplica":true,"project":{"slug":"lab","volumeNames":["deplo-lab_data"]}}`

func labPlacedOn(member string, epoch uint64) lease.Order {
	return lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: epoch,
		Placement: []lease.Entry{{Kind: "app", ID: "prj_1", Member: member, Slug: "lab", SingleWriter: true}}}
}

func appStartOrder(epoch uint64) lease.Order {
	o := labPlacedOn("srv_a", epoch)
	o.Start = &lease.Start{DeploymentID: "rev_1", Slug: "lab", ProjectID: "prj_1", ComposeYAML: "services: {web: {image: busybox}}",
		Restore: &lease.Restore{Request: json.RawMessage(appRestoreJSON)}}
	return o
}

// eu, 24 Nov 2026: lab-pause2's start on eu-2 failed after its copy was promoted, it then ran on eu-3, and the 2 GB
// volume stayed on eu-2 until it was removed by hand.
func TestFailedStart_whatItLeftGoesOnceTheAppRunsOnAnotherMember(t *testing.T) {
	f := newGuard(t)
	var mu sync.Mutex
	var dropped []string
	f.g.start = func(context.Context, *lease.Start) error {
		return errors.New("this server has no whole copy of the App's data yet")
	}
	f.g.dropFailed = func(_ context.Context, slug string, vols []string, still func() bool) error {
		mu.Lock()
		defer mu.Unlock()
		dropped = append(dropped, slug+" "+strings.Join(vols, ",")+" "+map[bool]string{true: "elsewhere", false: "here"}[still()])
		return nil
	}
	gone := func() string {
		f.g.work.Wait()
		mu.Lock()
		defer mu.Unlock()
		return strings.Join(dropped, ";")
	}
	if r := serve(t, f.g, labPlacedOn("srv_dead", 2)); !r.OK {
		t.Fatalf("renewal %+v", r)
	}
	if r := serve(t, f.g, appStartOrder(2)); r.OK {
		t.Fatalf("the start succeeded: %+v", r)
	}
	// Still placed on the member it left: nothing runs it yet, so its data here stays.
	f.clk.Add(time.Second)
	serve(t, f.g, labPlacedOn("srv_dead", 2))
	serve(t, f.g, labPlacedOn("", 2))
	if got := gone(); got != "" {
		t.Fatalf("removed before the App ran elsewhere: %s", got)
	}
	serve(t, f.g, labPlacedOn("srv_c", 2))
	if got := gone(); got != "lab deplo-lab_data elsewhere" {
		t.Fatalf("removed %q", got)
	}
	serve(t, f.g, labPlacedOn("srv_c", 2))
	if got := gone(); got != "lab deplo-lab_data elsewhere" {
		t.Fatalf("removed twice: %q", got)
	}
}

func TestFailedStart_aStartThatLaterWorksHereKeepsItsData(t *testing.T) {
	f := newGuard(t)
	called := false
	f.g.start = func(context.Context, *lease.Start) error { return errors.New("not ready") }
	f.g.dropFailed = func(context.Context, string, []string, func() bool) error { called = true; return nil }
	serve(t, f.g, labPlacedOn("srv_dead", 2))
	serve(t, f.g, appStartOrder(2))
	f.g.start = func(context.Context, *lease.Start) error { return nil }
	serve(t, f.g, appStartOrder(2))
	serve(t, f.g, labPlacedOn("srv_a", 2))
	serve(t, f.g, labPlacedOn("srv_c", 3))
	f.g.work.Wait()
	if called {
		t.Fatal("the data of a start that worked here was removed when the App moved on later")
	}
}

// c39: the leader's own server restarted it with its agent and the next leader's first renewal came 2.4 s later,
// which nothing said: a new epoch skipped the check.
func TestRenew_aLateFirstRenewalFromANewLeaderIsSaid(t *testing.T) {
	f := newGuard(t)
	var said []string
	f.g.logf = func(format string, args ...any) { said = append(said, fmt.Sprintf(format, args...)) }
	f.g.Renew(labPlacedOn("srv_a", 2))
	f.clk.Add(1100 * time.Millisecond)
	f.g.Renew(labPlacedOn("srv_a", 3))
	if len(said) != 0 {
		t.Fatalf("a hand-over on time was said: %q", said)
	}
	f.clk.Add(2400 * time.Millisecond)
	f.g.Renew(labPlacedOn("srv_a", 4))
	if len(said) != 1 || !strings.Contains(said[0], "a renewal came 2.4s after the one before, from a new leader") {
		t.Fatalf("said %q", said)
	}
}

func TestRenew_aLateRenewalIsSaid(t *testing.T) {
	f := newGuard(t)
	var said []string
	f.g.logf = func(format string, args ...any) { said = append(said, fmt.Sprintf(format, args...)) }
	f.g.Renew(labPlacedOn("srv_a", 2))
	f.clk.Add(time.Second)
	f.g.Renew(labPlacedOn("srv_a", 2))
	if len(said) != 0 {
		t.Fatalf("a renewal on time was said: %q", said)
	}
	f.clk.Add(1100 * time.Millisecond)
	f.g.Renew(labPlacedOn("srv_a", 2))
	if len(said) != 0 {
		t.Fatalf("a renewal a tenth late was said: %q", said)
	}
	f.clk.Add(1300 * time.Millisecond)
	f.g.Renew(labPlacedOn("srv_a", 2))
	if len(said) != 1 || !strings.Contains(said[0], "a renewal came 1.3s after the one before") {
		t.Fatalf("said %q", said)
	}
}

// c50: every member's renewals came 1.7 and 2.0 s apart with nothing in a journal. A late one says whether the
// leader sent it late, its stream was slow to arrive, or the lease was busy here.
func TestRenew_aLateRenewalSaysWhereItsTimeWent(t *testing.T) {
	f := newGuard(t)
	var said []string
	f.g.logf = func(format string, args ...any) { said = append(said, fmt.Sprintf(format, args...)) }
	f.g.Renew(labPlacedOn("srv_a", 2))
	f.clk.Add(2 * time.Second)
	o := labPlacedOn("srv_a", 2)
	o.Heard = map[string]int64{o.Member: 1980}
	f.g.renewRead(o, 30*time.Millisecond)
	if len(said) != 1 || !strings.Contains(said[0], "came 2.0s after the one before: the leader sent it 1980 ms after this server's last answer, its stream took 30 ms to read and the lease 0 ms to be free") {
		t.Fatalf("said %q", said)
	}
}
