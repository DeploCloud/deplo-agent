//go:build linux

package server

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

func replicaStatus(t *testing.T, s *Service, appID string) *pb.ClusterReplicaState {
	t.Helper()
	res, err := s.ClusterReplicaStatus(context.Background(), &pb.ClusterReplicaStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.GetReplicas() {
		if r.GetAppId() == appID {
			return r
		}
	}
	return &pb.ClusterReplicaState{}
}

// replicating links two members, places "web" on a and hands both the table; a's volume is vol, b's live one live.
// directInTest is whether b's copies would go direct; dialsInTest counts the streams b dialed.
var (
	directInTest atomic.Bool
	dialsInTest  atomic.Int32
)

func replicating(t *testing.T, aRuns bool) (a, b *Service, vol, live string) {
	return replicatingHanded(t, aRuns, true)
}

// replicatingHanded is replicating where b, unless handB, holds the table only as read from its disk at a start.
func replicatingHanded(t *testing.T, aRuns, handB bool) (a, b *Service, vol, live string) {
	t.Helper()
	prev := replReconcileEvery
	replReconcileEvery = 50 * time.Millisecond
	t.Cleanup(func() { replReconcileEvery = prev })
	a, b, fpB := twoMembers(t, newImageHost(), newImageHost())
	vol, live = t.TempDir(), t.TempDir()
	a.replicas().mountpoint = func(context.Context, string, bool) (string, error) { return vol, nil }
	a.replicas().running = func(context.Context, string) bool { return aRuns }
	b.replicas().mountpoint = func(context.Context, string, bool) (string, error) { return live, nil }
	b.replicas().running = func(context.Context, string) bool { return false }
	b.replicas().direct = func(context.Context, string) bool { return directInTest.Load() }
	dial := b.replicas().dial
	b.replicas().dial = func(ctx context.Context, fp string) (net.Conn, error) {
		dialsInTest.Add(1)
		return dial(ctx, fp)
	}
	app := lease.Entry{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web", SingleWriter: true}
	placeOn(t, a, "srv_a", app)
	placeOn(t, b, "srv_b", app)
	req := &pb.SetClusterReplicasRequest{
		ClusterId: "cl1",
		Members: []*pb.ClusterForwarderMember{
			{ServerId: "srv_a", Fingerprint: sourcePrint[a]},
			{ServerId: "srv_b", Fingerprint: fpB},
		},
		Entries: []*pb.ClusterReplicaEntry{{AppId: "prj_1", Slug: "web", VolumeNames: []string{"deplo-web-data"}}},
	}
	for _, s := range []*Service{a, b} {
		if s == b && !handB {
			tab, err := validateReplicas(req)
			if err != nil {
				t.Fatal(err)
			}
			r := b.replicas()
			r.mu.Lock()
			r.tab = tab
			r.mu.Unlock()
			r.poke()
			continue
		}
		if _, err := s.SetClusterReplicas(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	return a, b, vol, live
}

func TestReplicas_aCopyFollowsTheAppOverTheRelayAndTakesItsPlaceAtAFailover(t *testing.T) {
	a, b, vol, live := replicating(t, true)
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("one"), 0o640); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to hold a whole copy", func() bool {
		st := replicaStatus(t, b, "prj_1")
		return st.GetRole() == "receiving" && st.GetSynced() && st.GetFromServerId() == "srv_a"
	})
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("two"), 0o640); err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(b.replicas().dir("prj_1"), "volume-deplo-web-data", "data.txt")
	eventually(t, "the change to reach b", func() bool {
		raw, _ := os.ReadFile(copied)
		return string(raw) == "two"
	})
	if st := replicaStatus(t, a, "prj_1"); st.GetRole() != "sending" || st.GetStreams() != 1 {
		t.Fatalf("a says %+v", st)
	}

	release, err := b.promoteReplica(context.Background(), "web")
	if release != nil {
		defer release()
	}
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(live, "data.txt"))
	if string(raw) != "two" {
		t.Fatalf("the live volume holds %q", raw)
	}
	if _, err := os.Stat(b.replicas().dir("prj_1")); !os.IsNotExist(err) {
		t.Fatal("the copy outlived its promotion")
	}
}

func TestReplicas_aServerThatRunsNothingOfTheAppSendsNothing(t *testing.T) {
	_, b, _, _ := replicating(t, false)
	eventually(t, "b to hear the refusal", func() bool {
		return strings.Contains(replicaStatus(t, b, "prj_1").GetError(), "does not run on this server")
	})
	if replicaStatus(t, b, "prj_1").GetSynced() {
		t.Fatal("b holds a copy of an App that runs nowhere")
	}
}

func TestReplicas_aServerThatStillHoldsTheAppsStackReceivesNothing(t *testing.T) {
	prev := replReconcileEvery
	replReconcileEvery = 50 * time.Millisecond
	defer func() { replReconcileEvery = prev }()
	_, b, _ := twoMembers(t, newImageHost(), newImageHost())
	if err := os.WriteFile(b.stackPath("web"), []byte("services: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	placeOn(t, b, "srv_b", lease.Entry{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web", SingleWriter: true})
	_, err := b.SetClusterReplicas(context.Background(), &pb.SetClusterReplicasRequest{
		ClusterId: "cl1",
		Members:   []*pb.ClusterForwarderMember{{ServerId: "srv_a", Fingerprint: strings.Repeat("a", 64)}},
		Entries:   []*pb.ClusterReplicaEntry{{AppId: "prj_1", Slug: "web", IncludeFiles: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to say why it waits", func() bool {
		st := replicaStatus(t, b, "prj_1")
		return st.GetRole() == "idle" && strings.Contains(st.GetError(), "still holds its own copy")
	})
}

func TestReplicas_aTableThatNamesAPathOrNothingIsRefused(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(t.TempDir())
	for _, e := range []*pb.ClusterReplicaEntry{
		{AppId: "prj_1", Slug: "web", VolumeNames: []string{"../etc"}},
		{AppId: "prj_1", Slug: "web"},
		{AppId: "prj/1", Slug: "web", IncludeFiles: true},
		{AppId: "prj_1", Slug: "../web", IncludeFiles: true},
	} {
		_, err := s.SetClusterReplicas(context.Background(), &pb.SetClusterReplicasRequest{ClusterId: "cl1", Entries: []*pb.ClusterReplicaEntry{e}})
		if code(err).String() != "InvalidArgument" {
			t.Errorf("%+v was taken: %v", e, err)
		}
	}
}

func TestReplicas_aPromotionWithoutAWholeCopyStartsNothing(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(t.TempDir())
	if _, err := s.SetClusterReplicas(context.Background(), &pb.SetClusterReplicasRequest{ClusterId: "cl1",
		Entries: []*pb.ClusterReplicaEntry{{AppId: "prj_1", Slug: "web", IncludeFiles: true}}}); err != nil {
		t.Fatal(err)
	}
	release, err := s.promoteReplica(context.Background(), "web")
	if release != nil {
		release()
	}
	if err == nil || !strings.Contains(err.Error(), "no whole copy") {
		t.Fatalf("promoted: %v", err)
	}
}

func TestReplica_aWholeCopyThatHearsNothingDialsAgain(t *testing.T) {
	now := time.Now()
	if replStalled(false, now.Add(-time.Hour), now) {
		t.Fatal("a copy still being made was cut off: its first pass may say nothing for long")
	}
	if replStalled(true, now.Add(-time.Minute), now) {
		t.Fatal("a whole copy was cut off within the wait")
	}
	if !replStalled(true, now.Add(-replStallAfter-time.Second), now) {
		t.Fatal("a whole copy that heard nothing past the wait kept waiting")
	}
}

// cluster eu, 23 Nov 2026: fise got its Volume after its copies had started on files only, and every copy refused
// the sender for holding "other data" until the receivers were started again with the new roots.
func TestReplicas_aCopyFollowsATableThatNamesMoreData(t *testing.T) {
	a, b, vol, _ := replicating(t, true)
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("one"), 0o640); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to hold a whole copy", func() bool { return replicaStatus(t, b, "prj_1").GetSynced() })
	req := &pb.SetClusterReplicasRequest{
		ClusterId: "cl1",
		Members: []*pb.ClusterForwarderMember{
			{ServerId: "srv_a", Fingerprint: sourcePrint[a]},
			{ServerId: "srv_b", Fingerprint: b.replicas().tab.Members["srv_b"]},
		},
		Entries: []*pb.ClusterReplicaEntry{{AppId: "prj_1", Slug: "web", VolumeNames: []string{"deplo-web-data", "deplo-web-db-data"}}},
	}
	for _, s := range []*Service{a, b} {
		if _, err := s.SetClusterReplicas(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	copied := filepath.Join(b.replicas().dir("prj_1"), "volume-deplo-web-db-data", "data.txt")
	eventually(t, "the new volume to reach b", func() bool {
		raw, _ := os.ReadFile(copied)
		st := replicaStatus(t, b, "prj_1")
		return string(raw) == "one" && st.GetSynced() && st.GetError() == ""
	})
}

// deplo ADR-0043, eu 24 Nov 2026: eu-3, back from a power cut with its copy of lab-fit set aside, took a whole new
// copy from the table it kept on disk, before Deplo could check it fit, and reached 91%. A new copy waits for Deplo.
func TestReplicas_aNewCopyWaitsForDeploAfterARestart(t *testing.T) {
	_, b, vol, _ := replicatingHanded(t, true, false)
	if err := os.WriteFile(filepath.Join(vol, "data.bin"), []byte("rows"), 0o644); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to wait for Deplo", func() bool {
		st := replicaStatus(t, b, "prj_1")
		return st.GetRole() == "idle" && strings.Contains(st.GetError(), "waiting for Deplo")
	})
	if _, err := os.Stat(b.replicas().dir("prj_1")); !os.IsNotExist(err) {
		t.Fatal("b started a copy Deplo did not check")
	}
	// Deplo hands it the table: the copy starts.
	r := b.replicas()
	r.mu.Lock()
	tab := r.tab
	r.mu.Unlock()
	req := &pb.SetClusterReplicasRequest{ClusterId: tab.Cluster, Entries: []*pb.ClusterReplicaEntry{{AppId: "prj_1", Slug: "web", VolumeNames: []string{"deplo-web-data"}}}}
	for id, fp := range tab.Members {
		req.Members = append(req.Members, &pb.ClusterForwarderMember{ServerId: id, Fingerprint: fp})
	}
	if _, err := b.SetClusterReplicas(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to receive", func() bool { return replicaStatus(t, b, "prj_1").GetRole() == "receiving" })
}

// The panel shows a copy's size on disk and whether it went through a relay, never the bytes it was sent.
func TestReplicas_aCopySaysItsSizeOnDiskAndThatARelayCarriesIt(t *testing.T) {
	a, b, vol, _ := replicating(t, true)
	if err := os.WriteFile(filepath.Join(vol, "data.bin"), make([]byte, 1<<20), 0o640); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to hold a whole copy", func() bool { return replicaStatus(t, b, "prj_1").GetSynced() })
	st := replicaStatus(t, b, "prj_1")
	if want := dirSize(b.replicas().dir("prj_1")); st.GetDiskBytes() != want || want < 1<<20 {
		t.Fatalf("the copy says %d bytes on disk, it takes %d", st.GetDiskBytes(), want)
	}
	if !st.GetRelayed() {
		t.Fatal("a copy through the relay says it went direct")
	}
	if replicaStatus(t, a, "prj_1").GetDiskBytes() != 0 {
		t.Fatal("the App's own data counted as a copy")
	}
}

// c38: lab-pd's copy on eu-4, handed while lab-pd was empty, took its 300 MB as followed changes beside a backup into
// eu-4. Deplo now holds a copy that would move data while its server copies something else: it stays whole, idle.
func TestReplicas_aHeldCopyStaysWholeAndReceivesNothingUntilItsTurn(t *testing.T) {
	a, b, vol, _ := replicating(t, true)
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("one"), 0o640); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to hold a whole copy", func() bool { return replicaStatus(t, b, "prj_1").GetSynced() })
	hand := func(hold string) {
		t.Helper()
		req := &pb.SetClusterReplicasRequest{
			ClusterId: "cl1",
			Members: []*pb.ClusterForwarderMember{
				{ServerId: "srv_a", Fingerprint: sourcePrint[a]},
				{ServerId: "srv_b", Fingerprint: b.replicas().tab.Members["srv_b"]},
			},
			Entries: []*pb.ClusterReplicaEntry{{AppId: "prj_1", Slug: "web", VolumeNames: []string{"deplo-web-data"}, Hold: hold}},
		}
		if _, err := b.SetClusterReplicas(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	hand("Queued: srv_b is busy with a backup of shop")
	eventually(t, "b to stop receiving", func() bool {
		st := replicaStatus(t, b, "prj_1")
		return st.GetRole() == "idle" && st.GetSynced() && strings.HasPrefix(st.GetError(), "Queued:")
	})
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("two"), 0o640); err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(b.replicas().dir("prj_1"), "volume-deplo-web-data", "data.txt")
	time.Sleep(300 * time.Millisecond)
	if raw, _ := os.ReadFile(copied); string(raw) != "one" {
		t.Fatalf("a held copy holds %q", raw)
	}
	hand("")
	eventually(t, "the change to reach b once its turn comes", func() bool {
		raw, _ := os.ReadFile(copied)
		return string(raw) == "two"
	})
}

// c39: each member's stream asked Docker on its own every 2 s whether the App still ran: three of them for one App.
func TestReplicas_everyStreamOfAnAppSharesOneLookAtWhetherItRuns(t *testing.T) {
	r := &replicas{}
	var asked atomic.Int32
	r.running = func(context.Context, string) bool { asked.Add(1); return true }
	for range 3 {
		if !r.stillRuns(context.Background(), "web") {
			t.Fatal("a running App reads as stopped")
		}
	}
	if n := asked.Load(); n != 1 {
		t.Fatalf("Docker was asked %d times for three streams", n)
	}
}

// c44: a move stopped lab-kit-d and copied all 64 MB again though eu-3's copy was current. The copy takes only the
// last changes from the member the App left, and only once it stopped there.
func TestReplicas_aMoveTakesOnlyTheLastChangesIntoTheCopyAndPutsItInPlace(t *testing.T) {
	var aRuns atomic.Bool
	aRuns.Store(true)
	a, b, vol, live := replicating(t, true)
	a.replicas().running = func(context.Context, string) bool { return aRuns.Load() }
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("one"), 0o640); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to hold a whole copy", func() bool { return replicaStatus(t, b, "prj_1").GetSynced() })
	moved := lease.Entry{Kind: "app", ID: "prj_1", Member: "srv_b", Slug: "web", SingleWriter: true}
	placeOn(t, b, "srv_b", moved)
	eventually(t, "b's stream to end", func() bool { return !replicaStatus(t, b, "prj_1").GetConnected() })
	b.replicas().mu.Lock()
	kept := b.replicas().indexes["prj_1"] != nil
	b.replicas().mu.Unlock()
	if !kept {
		t.Fatal("the stopped receiver forgot what its copy's files hold: the move would read them all again")
	}
	// What the App wrote as it stopped, after the copy last heard from a.
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("last"), 0o640); err != nil {
		t.Fatal(err)
	}
	req := &pb.PromoteClusterReplicaRequest{AppId: "prj_1", FromServerId: "srv_a"}
	placeOn(t, a, "srv_a", moved)
	if _, err := b.PromoteClusterReplica(context.Background(), req); err == nil || !strings.Contains(err.Error(), "still runs here") {
		t.Fatalf("a still runs the App, yet: %v", err)
	}
	if _, err := os.Stat(filepath.Join(live, "data.txt")); !os.IsNotExist(err) {
		t.Fatal("a refused move put the copy in place")
	}
	aRuns.Store(false)
	res, err := b.PromoteClusterReplica(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if raw, _ := os.ReadFile(filepath.Join(live, "data.txt")); string(raw) != "last" {
		t.Fatalf("the live volume holds %q", raw)
	}
	if res.GetBytes() != int64(len("last")) {
		t.Fatalf("%d bytes crossed for a 4-byte change", res.GetBytes())
	}
	if _, err := os.Stat(b.replicas().dir("prj_1")); !os.IsNotExist(err) {
		t.Fatal("the copy outlived its promotion")
	}
}

// c46: copies from eu-2 to eu-1 stayed on Deplo's relay for 5 minutes after eu-1 came back, until eu-2 restarted.
func TestReplicas_aCopyOnTheRelayLeavesItOnceADirectPathAnswers(t *testing.T) {
	prev := replStallCheck
	replStallCheck = 20 * time.Millisecond
	t.Cleanup(func() { replStallCheck = prev; directInTest.Store(false) })
	_, b, vol, _ := replicating(t, true)
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("one"), 0o640); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b to hold a whole copy over the relay", func() bool {
		st := replicaStatus(t, b, "prj_1")
		return st.GetSynced() && st.GetRelayed()
	})
	before := dialsInTest.Load()
	directInTest.Store(true)
	eventually(t, "the copy to dial again", func() bool { return dialsInTest.Load() == before+1 })
	time.Sleep(200 * time.Millisecond)
	if n := dialsInTest.Load() - before; n != 1 {
		t.Fatalf("a stream that landed on the relay again was left %d times", n)
	}
	if err := os.WriteFile(filepath.Join(vol, "data.txt"), []byte("two"), 0o640); err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(b.replicas().dir("prj_1"), "volume-deplo-web-data", "data.txt")
	eventually(t, "the change to reach b on the new stream", func() bool {
		raw, _ := os.ReadFile(copied)
		return string(raw) == "two"
	})
}
