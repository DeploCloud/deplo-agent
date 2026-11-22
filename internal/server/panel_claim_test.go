package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/mesh"
)

const (
	ourCluster   = "clu_ours"
	theirCluster = "clu_theirs"
)

// panelDir is an enrolled agent's directory whose CA is a fresh Deplo's; it answers that Deplo's stamp.
func panelDir(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "deplo-agent-ca"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{
		"ca.crt":    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		"agent.crt": []byte("x"), "agent.key": []byte("x"),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir, panelOf(dir)
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(v)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// seedTwoDeplos writes the state of a machine another Deplo ran before this one: both clusters in the shared files,
// one network key for both, that Deplo's leader and the restart policies it kept of its Apps.
func seedTwoDeplos(t *testing.T, dir, memberOf string) string {
	t.Helper()
	k, _ := mesh.NewPrivateKey()
	peer := func(fp string) []map[string]any {
		return []map[string]any{{"fingerprint": fp, "publicKey": k.String(), "address": "fd00::2"}}
	}
	writeJSON(t, filepath.Join(dir, "cluster-mesh.json"), map[string]any{
		"privateKey": k.String(), "port": 51820, "address": "fd00::1",
		"clusters": map[string]any{ourCluster: peer("ours"), theirCluster: peer("theirs")},
		"setAt":    map[string]any{ourCluster: 2, theirCluster: 1},
	})
	writeJSON(t, filepath.Join(dir, "cluster-lease.json"), map[string]any{
		ourCluster:   map[string]any{"epoch": 1, "placement": []map[string]any{{"kind": "app", "id": "prj_a", "slug": "fise"}}},
		theirCluster: map[string]any{"epoch": 18, "placement": []map[string]any{{"kind": "app", "id": "prj_b", "slug": "notes"}}},
	})
	writeJSON(t, filepath.Join(dir, "cluster-zone.json"), map[string]any{
		"clusters": map[string]any{ourCluster: map[string]any{"self": "a"}, theirCluster: map[string]any{"self": "b"}},
	})
	writeJSON(t, filepath.Join(dir, "cluster-member.json"), map[string]any{"member": true, "cluster": memberOf})
	writeJSON(t, filepath.Join(dir, "cluster-supervise.json"), map[string]any{
		"kept": map[string]any{"notes": map[string]string{"deplo-notes": "unless-stopped"}}, "stopped": map[string]any{"fise": []string{""}},
	})
	writeJSON(t, filepath.Join(dir, "cluster", "state.json"), clusterState{Version: "0.1.0", Relay: true})
	writeJSON(t, filepath.Join(dir, "cluster", "leader", "epochs.json"), map[string]int{theirCluster: 18})
	return k.String()
}

// stubHost lists these App objects and records every forwarder removed and every restart asked for.
func stubHost(t *testing.T, objs []appObject) (*[]string, *int) {
	t.Helper()
	var removed []string
	restarts := 0
	oldList, oldRestart, oldOps := listAppObjects, restartAgent, dockerForwardOps
	listAppObjects = func(context.Context) ([]appObject, error) { return objs, nil }
	restartAgent = func() { restarts++ }
	dockerForwardOps.list = func(context.Context) ([]string, error) { return []string{"deplo_fwd_1"}, nil }
	dockerForwardOps.remove = func(_ context.Context, n string) error { removed = append(removed, n); return nil }
	t.Cleanup(func() { listAppObjects, restartAgent, dockerForwardOps = oldList, oldRestart, oldOps })
	return &removed, &restarts
}

func onlySetAside(t *testing.T, dir string) string {
	t.Helper()
	entries, _ := os.ReadDir(filepath.Join(dir, setAsideRoot))
	if len(entries) != 1 {
		t.Fatalf("want one set-aside folder, got %d", len(entries))
	}
	return filepath.Join(dir, setAsideRoot, entries[0].Name())
}

func TestSettlePanel_anotherDeploWroteItAll(t *testing.T) {
	dir, panel := panelDir(t)
	seedTwoDeplos(t, dir, theirCluster)
	writeJSON(t, filepath.Join(dir, "link-addresses.json"), map[string]string{"primary": "https://old.example"})
	if err := writeStamp(dir, "another-deplo"); err != nil {
		t.Fatal(err)
	}
	apps := []appObject{{"container", "deplo-notes", "notes"}, {"volume", "deplo-notes-data", "notes"}}
	removed, _ := stubHost(t, apps)

	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(dir)
	s.SettlePanel()

	to := onlySetAside(t, dir)
	for _, f := range []string{"cluster-mesh.json", "cluster-lease.json", "cluster-zone.json", "cluster-member.json",
		"cluster-supervise.json", "link-addresses.json", "cluster"} {
		if exists(filepath.Join(dir, f)) {
			t.Errorf("%s is still loaded", f)
		}
		if !exists(filepath.Join(to, f)) {
			t.Errorf("%s was not set aside", f)
		}
	}
	if readStamp(dir) != panel {
		t.Fatal("the agent does not note the Deplo it belongs to now")
	}
	if len(*removed) != 1 || (*removed)[0] != "deplo_fwd_1" {
		t.Fatalf("another Deplo's name forwarders stay: %v", *removed)
	}
	var left []*pb.LeftWorkload
	raw, _ := os.ReadFile(filepath.Join(to, setAsideLeft))
	_ = json.Unmarshal(raw, &left)
	if len(left) != 2 || left[0].GetName() != "deplo-notes" || left[1].GetKind() != "volume" {
		t.Fatalf("the other Deplo's Apps are not listed for the owner: %v", left)
	}

	// A restart loads none of it, and sets nothing aside again.
	s2 := New(t.TempDir(), t.TempDir(), "/", "")
	s2.SetAgentDir(dir)
	s2.SettlePanel()
	if n := s2.network(); len(n.st.Clusters) != 0 || n.st.PrivateKey != "" {
		t.Fatalf("the network came back: %+v", n.st)
	}
	onlySetAside(t, dir)
}

func TestClaimServer_setsAsideOnlyAnotherDeplosClusters(t *testing.T) {
	dir, panel := panelDir(t)
	oldKey := seedTwoDeplos(t, dir, ourCluster)
	apps := []appObject{{"container", "deplo-notes", "notes"}, {"volume", "deplo-notes-data", "notes"},
		{"container", "deplo-fise-fise-1", "fise"}}
	removed, restarts := stubHost(t, apps)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(dir)

	// Before any stamp the agent loads what is there; this Deplo's claim says what is its own.
	s.SettlePanel()
	if exists(filepath.Join(dir, setAsideRoot)) || readStamp(dir) != "" {
		t.Fatal("an agent without a stamp judged its state before its Deplo claimed it")
	}
	resp, err := s.ClaimServer(context.Background(), &pb.ClaimServerRequest{ClusterIds: []string{ourCluster}})
	if err != nil || !resp.GetRestarting() {
		t.Fatalf("the claim does not restart the agent to set aside: %v %v", resp, err)
	}
	time.Sleep(selfUpdateGrace + 200*time.Millisecond)
	if *restarts != 1 {
		t.Fatalf("restarts = %d", *restarts)
	}

	s2 := New(t.TempDir(), t.TempDir(), "/", "")
	s2.SetAgentDir(dir)
	s2.SettlePanel()
	to := onlySetAside(t, dir)

	m := readJSON(t, filepath.Join(dir, "cluster-mesh.json"))
	if cl := m["clusters"].(map[string]any); cl[ourCluster] == nil || cl[theirCluster] != nil {
		t.Fatalf("network clusters kept: %v", cl)
	}
	if m["privateKey"] == oldKey || m["privateKey"] == "" {
		t.Fatal("the network key another Deplo's servers know is still used")
	}
	if g := readJSON(t, filepath.Join(to, "cluster-mesh.json")); g["privateKey"] != oldKey || g["clusters"].(map[string]any)[theirCluster] == nil {
		t.Fatalf("the old key and cluster were not set aside: %v", g)
	}
	for _, f := range []string{"cluster-lease.json", "cluster-zone.json"} {
		kept, gone := readJSON(t, filepath.Join(dir, f)), readJSON(t, filepath.Join(to, f))
		if f == "cluster-zone.json" {
			kept, gone = kept["clusters"].(map[string]any), gone["clusters"].(map[string]any)
		}
		if kept[ourCluster] == nil || kept[theirCluster] != nil || gone[theirCluster] == nil {
			t.Errorf("%s: kept %v, set aside %v", f, kept, gone)
		}
	}
	if !exists(filepath.Join(dir, "cluster-member.json")) {
		t.Error("this Deplo's membership was set aside")
	}
	if exists(filepath.Join(dir, "cluster")) || !exists(filepath.Join(to, "cluster", "leader", "epochs.json")) {
		t.Error("another Deplo's leader is still here")
	}
	sup := readJSON(t, filepath.Join(dir, "cluster-supervise.json"))
	if sup["kept"] != nil && sup["kept"].(map[string]any)["notes"] != nil || sup["stopped"].(map[string]any)["fise"] == nil {
		t.Fatalf("supervision kept: %v", sup)
	}
	if len(*removed) != 0 {
		t.Fatalf("this Deplo's name forwarders were removed: %v", *removed)
	}
	if readStamp(dir) != panel || exists(filepath.Join(dir, panelClaimFile)) {
		t.Fatal("the claim was not settled")
	}

	// The App the other Deplo left is listed while it is there, and this Deplo's is not.
	resp, err = s2.ClaimServer(context.Background(), &pb.ClaimServerRequest{ClusterIds: []string{ourCluster}})
	if err != nil || resp.GetRestarting() || resp.GetSetAside() != to {
		t.Fatalf("a settled claim: %v %v", resp, err)
	}
	if len(resp.GetLeft()) != 2 || resp.GetLeft()[0].GetApp() != "notes" {
		t.Fatalf("left: %v", resp.GetLeft())
	}
	listAppObjects = func(context.Context) ([]appObject, error) { return apps[2:], nil }
	if _, _, left := s2.lastSetAside(context.Background()); len(left) != 0 {
		t.Fatalf("an App the owner removed is still listed: %v", left)
	}
	if got := s2.leftSlugs(); len(got) != 0 {
		t.Fatalf("leftSlugs = %v", got)
	}

	// A restart loads only this Deplo's clusters.
	s3 := New(t.TempDir(), t.TempDir(), "/", "")
	s3.SetAgentDir(dir)
	s3.SettlePanel()
	if n := s3.network(); len(n.st.Clusters) != 1 || n.st.Clusters[ourCluster] == nil {
		t.Fatalf("the network after a restart: %v", n.st.Clusters)
	}
	onlySetAside(t, dir)
}

func TestClaimServer_nothingForeignStampsWithoutRestart(t *testing.T) {
	dir, panel := panelDir(t)
	writeJSON(t, filepath.Join(dir, "cluster-member.json"), map[string]any{"member": true, "cluster": ourCluster})
	writeJSON(t, filepath.Join(dir, "cluster", "state.json"), clusterState{Version: "0.1.0"})
	_, restarts := stubHost(t, nil)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(dir)
	resp, err := s.ClaimServer(context.Background(), &pb.ClaimServerRequest{ClusterIds: []string{ourCluster}, ClusterHost: true})
	if err != nil || resp.GetRestarting() || readStamp(dir) != panel || *restarts != 0 {
		t.Fatalf("resp %v err %v stamp %q restarts %d", resp, err, readStamp(dir), *restarts)
	}
	if exists(filepath.Join(dir, setAsideRoot)) {
		t.Fatal("this Deplo's own state was set aside")
	}
	if _, err := s.ClaimServer(context.Background(), &pb.ClaimServerRequest{ClusterIds: []string{"not a cluster"}}); err == nil {
		t.Fatal("a malformed cluster id was accepted")
	}
}

func TestSettlePanel_freshAgentStampsAtOnce(t *testing.T) {
	dir, panel := panelDir(t)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(dir)
	s.SettlePanel()
	if readStamp(dir) != panel {
		t.Fatal("a fresh agent waits for a claim it does not need")
	}
	// A reinstall on the same Deplo keeps its state: the CA's key is the same.
	writeJSON(t, filepath.Join(dir, "cluster-member.json"), map[string]any{"cluster": ourCluster})
	s.SettlePanel()
	if !exists(filepath.Join(dir, "cluster-member.json")) || exists(filepath.Join(dir, setAsideRoot)) {
		t.Fatal("the same Deplo's state was set aside")
	}
}
