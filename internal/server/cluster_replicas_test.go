//go:build linux

package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
func replicating(t *testing.T, aRuns bool) (a, b *Service, vol, live string) {
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
