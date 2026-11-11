package server

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// keptBack is a member whose return is done and told, with what files held still on it: its backup failed.
func keptBack(t *testing.T) *keepFixture {
	t.Helper()
	f := back(t)
	f.keepErr = errors.New("the bucket did not answer")
	f.renew(askedBack(filesKeep()))
	if r := f.renew(askedBack(filesKeep())); r.Returned == nil || len(r.Returned.Kept) != 1 {
		t.Fatalf("report %+v", r.Returned)
	}
	f.kept = nil
	return f
}

// stillKept is the leader's order once it has the return: no question, and where what the member kept goes.
func stillKept(keeps ...lease.Keep) lease.Order {
	o := movedAway(1)
	o.Keep = keeps
	return o
}

// pass lets d go by with the leader renewing every 30 s, and gives the last reply.
func (f *keepFixture) pass(d time.Duration, o lease.Order) (last lease.Reply) {
	for ; d > 0; d -= 30 * time.Second {
		f.clk.Add(30 * time.Second)
		last = f.renew(o)
	}
	return last
}

func TestKeepRetry_whatStayedIsSavedLaterAndTheLeaderIsToldUntilItStopsAsking(t *testing.T) {
	f := keptBack(t)
	o := stillKept(filesKeep())
	// Not at once: the destination failed a moment ago.
	if r := f.renew(o); len(f.kept) != 0 || r.Later != nil || r.Returned != nil {
		t.Fatalf("tried %d times at once, reply %+v", len(f.kept), r)
	}
	f.keepErr = nil
	f.pass(time.Minute, o)
	if len(f.kept) != 1 || f.kept[0].Slug != "files" || strings.Join(f.purged, ",") != "files" {
		t.Fatalf("saved %+v, purged %v", f.kept, f.purged)
	}
	for range 2 {
		r := f.renew(o)
		if r.Returned != nil || r.Later == nil || strings.Join(r.Later.Gone, ",") != "files" || len(r.Later.Saved) != 1 {
			t.Fatalf("reply %+v", r.Later)
		}
		if sv := r.Later.Saved[0]; sv.Slug != "files" || sv.Ref != "dst_1" || sv.SizeBytes != 42 || sv.ObjectKey != "deplo/t/app/prj_2/left-x-srv_a.tar.gz" {
			t.Fatalf("saved %+v", sv)
		}
	}
	// The leader has it: nothing of it is remembered.
	if r := f.renew(movedAway(1)); r.Later != nil {
		t.Fatalf("reply %+v to an order that asks nothing", r.Later)
	}
	if raw, _ := os.ReadFile(f.file); strings.Contains(string(raw), "left-x-") || strings.Contains(string(raw), `"keeping"`) {
		t.Fatalf("the lease file still holds it: %s", raw)
	}
	if r := f.pass(10*time.Minute, o); len(f.kept) != 1 || r.Later == nil || len(r.Later.Saved) != 0 || strings.Join(r.Later.Gone, ",") != "files" {
		t.Fatalf("asked again: saved %d times, reply %+v", len(f.kept), r.Later)
	}
}

func TestKeepRetry_waitsLongerAfterEachFailureAndNeverMoreThanAnHour(t *testing.T) {
	f := keptBack(t)
	o := stillKept(filesKeep())
	for _, step := range []struct {
		wait  time.Duration
		tries int
	}{{time.Minute, 1}, {time.Minute, 1}, {time.Minute, 2}, {3 * time.Minute, 2}, {time.Minute, 3}} {
		f.pass(step.wait, o)
		if len(f.kept) != step.tries {
			t.Fatalf("after another %s: %d tries, want %d", step.wait, len(f.kept), step.tries)
		}
	}
	f.pass(6*time.Hour, o)
	before := len(f.kept)
	f.pass(2*time.Hour+time.Minute, o)
	if got := len(f.kept) - before; got != 2 {
		t.Fatalf("%d tries in two hours, want one an hour", got)
	}
	raw, _ := os.ReadFile(f.file)
	if !strings.Contains(string(raw), "the bucket did not answer") || strings.Contains(string(raw), "secret_test") {
		t.Fatalf("the lease file: %s", raw)
	}
}

func TestKeepRetry_anAgentThatRestartsStillKnowsWhatItKeepsAndNeverArchivesTwice(t *testing.T) {
	f := keptBack(t)
	o := stillKept(filesKeep())
	f.keepErr, f.purgeErrs = nil, 1
	if r := f.pass(time.Minute, o); len(f.kept) != 1 || len(f.purged) != 0 || r.Later != nil {
		t.Fatalf("saved %d, purged %v, reply %+v", len(f.kept), f.purged, r.Later)
	}
	f.restart()
	f.renew(o)
	if len(f.kept) != 1 || strings.Join(f.purged, ",") != "files" {
		t.Fatalf("archived %d times, purged %v", len(f.kept), f.purged)
	}
	if r := f.renew(o); r.Later == nil || len(r.Later.Saved) != 1 || r.Later.Saved[0].Slug != "files" {
		t.Fatalf("reply %+v", r.Later)
	}
}

func TestKeepRetry_aStackThatRunsHereAgainIsNotArchivedAndNoLongerKept(t *testing.T) {
	f := keptBack(t)
	f.keepErr = nil
	// The leader placed files on this member again: its volume is the live one.
	o := orderFor(1)
	o.Keep = []lease.Keep{{Slug: "files"}}
	f.pass(time.Minute, o)
	if len(f.kept) != 0 || len(f.purged) != 0 {
		t.Fatalf("saved %+v, purged %v", f.kept, f.purged)
	}
	if r := f.renew(o); r.Later == nil || strings.Join(r.Later.Gone, ",") != "files" || len(r.Later.Saved) != 0 {
		t.Fatalf("reply %+v", r.Later)
	}
}

func TestKeepRetry_waitsWhileThereIsNowhereToSaveItOrItsStackRunsNowhere(t *testing.T) {
	f := back(t)
	f.renew(askedBack())
	if r := f.renew(askedBack()); r.Returned == nil || len(r.Returned.Kept) != 1 || r.Returned.Kept[0].Reason != lease.KeptNoDestination {
		t.Fatalf("report %+v", r.Returned)
	}
	if r := f.pass(5*time.Minute, stillKept(lease.Keep{Slug: "files"})); len(f.kept) != 0 || r.Later != nil {
		t.Fatalf("saved %+v, reply %+v", f.kept, r.Later)
	}
	// A destination on a server the leader gave no certificate for is no better.
	far := filesKeep()
	far.StoreMember = "srv_z"
	if r := f.pass(5*time.Minute, stillKept(far)); len(f.kept) != 0 || r.Later != nil {
		t.Fatalf("saved %+v, reply %+v", f.kept, r.Later)
	}
	// Waiting for room: it runs on no server, so what is here may be all there is.
	waiting := stillKept(filesKeep())
	waiting.Placement[2].Member = ""
	if r := f.pass(5*time.Minute, waiting); len(f.kept) != 0 || r.Later != nil {
		t.Fatalf("saved %+v, reply %+v", f.kept, r.Later)
	}
	// The team gave it a destination: the next try saves it.
	if f.pass(2*time.Minute, stillKept(filesKeep())); len(f.kept) != 1 || strings.Join(f.purged, ",") != "files" {
		t.Fatalf("saved %+v, purged %v", f.kept, f.purged)
	}
}

func TestKeepRetry_aStartOrderSaysNothingAboutWhatIsKeptOrSaved(t *testing.T) {
	f := keptBack(t)
	o := stillKept(filesKeep())
	f.keepErr = nil
	f.pass(time.Minute, o)
	if strings.Join(f.purged, ",") != "files" {
		t.Fatalf("purged %v", f.purged)
	}
	// A start order carries no keep: that is not the leader saying it has heard.
	start := movedAway(1)
	start.Start = &lease.Start{Slug: "db-main"}
	if r := f.renew(start); r.Later != nil {
		t.Fatalf("reply %+v to a start order", r.Later)
	}
	if r := f.renew(o); r.Later == nil || len(r.Later.Saved) != 1 {
		t.Fatalf("the archive was forgotten before the leader heard of it: %+v", r.Later)
	}
}
