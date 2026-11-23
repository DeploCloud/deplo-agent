package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// Every kind of order the leader sends (deplo-cluster's internal/leader/testdata/orders.golden.json, kept here too),
// at the epoch this member holds from that leader, passes this agent's epoch and leader checks.
type leaderOrder struct {
	Kind  string      `json:"kind"`
	Order lease.Order `json:"order"`
}

func readLeaderOrders(t *testing.T, path string) []leaderOrder {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []leaderOrder
	if err := json.Unmarshal(raw, &out); err != nil || len(out) == 0 {
		t.Fatalf("%s: %v", path, err)
	}
	return out
}

func ordersPass(t *testing.T, path string) {
	t.Helper()
	const leaderFP = "fp-leader"
	for _, lo := range readLeaderOrders(t, path) {
		t.Run(lo.Kind, func(t *testing.T) {
			f := newGuard(t)
			held := orderFor(7)
			held.From = leaderFP
			if r := f.g.Renew(held); !r.OK {
				t.Fatalf("renew: %+v", r)
			}
			o := lo.Order
			o.Cluster, o.Member, o.Epoch = held.Cluster, held.Member, held.Epoch
			if o.From != "" {
				o.From = leaderFP
			}
			raw, _ := json.Marshal(o)
			got := serveOrder(t, f.g, wire.RoleLeader, string(raw))
			if strings.Contains(got, lease.StaleEpoch) || strings.Contains(got, lease.LeaderAlive) || strings.Contains(got, "unreadable") {
				t.Fatalf("the leader's %s order at the epoch it holds was refused: %s", lo.Kind, got)
			}
		})
	}
}

func TestOrders_everyKindTheLeaderSendsPasses(t *testing.T) {
	ordersPass(t, filepath.Join("testdata", "leader-orders.golden.json"))
}

// clusterOrders is deplo-cluster's own file, from DEPLO_CLUSTER_REPO or a checkout beside this repo.
func clusterOrders(t *testing.T) string {
	t.Helper()
	repo := os.Getenv("DEPLO_CLUSTER_REPO")
	if repo == "" {
		repo = filepath.Join("..", "..", "..", "deplo-cluster")
	}
	path := filepath.Join(repo, "internal", "leader", "testdata", "orders.golden.json")
	if _, err := os.Stat(path); err != nil {
		t.Skip("no deplo-cluster beside this repo (set DEPLO_CLUSTER_REPO)")
	}
	return path
}

// What the leader sends today passes this agent: deplo-cluster's CI runs this against its own file.
func TestOrders_theLeadersOrdersPass(t *testing.T) {
	ordersPass(t, clusterOrders(t))
}

func TestOrders_theCopyAgreesWithTheLeaderRepo(t *testing.T) {
	theirs := readLeaderOrders(t, clusterOrders(t))
	if mine := readLeaderOrders(t, filepath.Join("testdata", "leader-orders.golden.json")); !reflect.DeepEqual(mine, theirs) {
		t.Fatal("testdata/leader-orders.golden.json differs from deplo-cluster's internal/leader/testdata/orders.golden.json")
	}
}
