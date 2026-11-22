package server

import (
	"context"
	"slices"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func TestRotations_areKeptPerClusterAndRefuseWhatIsNotOne(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.rotation") {
		t.Fatal("Hello does not advertise cluster.rotation")
	}
	dir := t.TempDir()
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(dir)
	a, b, c := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	set := func(cluster string, pairs ...string) error {
		req := &pb.SetClusterRotationsRequest{ClusterId: cluster}
		for i := 0; i+1 < len(pairs); i += 2 {
			req.Rotations = append(req.Rotations, &pb.ClusterKeyRotation{From: pairs[i], To: pairs[i+1]})
		}
		_, err := s.SetClusterRotations(context.Background(), req)
		return err
	}
	for why, err := range map[string]error{
		"no cluster":                  set("", a, b),
		"not a fingerprint":           set("cl1", a, "nope"),
		"a key to itself":             set("cl1", a, a),
		"a cluster id that is a path": set("cl1/..", a, b),
	} {
		if err == nil {
			t.Errorf("%s was taken", why)
		}
	}
	if err := set("cl1", strings.ToUpper(a), b); err != nil {
		t.Fatal(err)
	}
	if err := set("cl2", b, c); err != nil {
		t.Fatal(err)
	}
	again := New(t.TempDir(), t.TempDir(), "/", "")
	again.SetAgentDir(dir)
	if got := again.rotations().all(); len(got) != 2 || got[0].From != a || got[1].To != c {
		t.Fatalf("after a restart: %+v", got)
	}
	if err := set("cl1"); err != nil {
		t.Fatal(err)
	}
	if got := s.rotations().all(); len(got) != 1 || got[0].From != b {
		t.Fatalf("an empty set did not forget cl1: %+v", got)
	}
}
