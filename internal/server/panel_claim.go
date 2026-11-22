package server

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/dnszone"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/mesh"
)

// A server agent belongs to one Deplo (ADR-0050): what another Deplo left here is set aside on start, never loaded.
// https://deplo.build/docs/operations/servers/add-a-server
const (
	panelStampFile = "panel"
	panelClaimFile = "panel-claim.json"
	setAsideRoot   = "set-aside"
	setAsideLeft   = "left.json"
)

// oneClusterFiles each hold one cluster's state, named by their "cluster" field.
var oneClusterFiles = []string{"cluster-member.json", "cluster-forwarders.json", "cluster-replicas.json"}

// clusterKeyed files hold several clusters' state, by id under these keys ("" is the top level).
var clusterKeyed = []struct {
	file string
	keys []string
}{
	{"cluster-mesh.json", []string{"clusters", "setAt"}},
	{"cluster-lease.json", []string{""}},
	{"cluster-zone.json", []string{"clusters"}},
	{"cluster-rotations.json", []string{"clusters"}},
}

// panelOwned is what else a Deplo writes here, set aside whole when another Deplo wrote it.
var panelOwned = []string{"cluster", "replicas", "forwarders", "cluster-supervise.json", "link-addresses.json", panelClaimFile}

type panelClaim struct {
	Panel    string   `json:"panel"`
	Clusters []string `json:"clusters"`
	Host     bool     `json:"host"`
}

// aside is what one start sets aside: everything, or the clusters and the leader another Deplo left.
type aside struct {
	all      bool
	clusters map[string]bool
	host     bool
}

func (a aside) empty() bool { return !a.all && !a.host && len(a.clusters) == 0 }

// Seams for the tests: the host's App containers and volumes, and the restart that loads only this Deplo's state.
var (
	listAppObjects = dockerAppObjects
	restartAgent   = func() {
		exe, err := os.Executable()
		if err == nil {
			err = reexec(exe, append([]string{exe}, os.Args[1:]...), os.Environ())
		}
		log.Printf("deplo-agent: could not restart to set aside another Deplo's state, it is set aside on the next start: %v", err)
	}
)

// panelOf names the Deplo this agent's certificate comes from: its CA's public key, the same across a reinstall.
func panelOf(agentDir string) string {
	if agentDir == "" {
		return ""
	}
	raw, err := os.ReadFile(bootstrap.Paths(agentDir).CAPath)
	if err != nil {
		return ""
	}
	b, _ := pem.Decode(raw)
	if b == nil {
		return ""
	}
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

func readStamp(dir string) string {
	raw, _ := os.ReadFile(filepath.Join(dir, panelStampFile))
	return strings.TrimSpace(string(raw))
}

func writeStamp(dir, panel string) error {
	return writeFileAtomic(filepath.Join(dir, panelStampFile), []byte(panel+"\n"), 0o600)
}

// SettlePanel runs before anything loads state: what another Deplo left is set aside, so it can never come back.
func (s *Service) SettlePanel() {
	dir := s.agentDir
	panel := panelOf(dir)
	if panel == "" {
		return
	}
	stamp := readStamp(dir)
	var a aside
	switch claim := readClaim(dir, panel); {
	case stamp != "" && stamp != panel:
		a.all = true
	case claim != nil:
		a = judgeClaim(dir, claim)
	case stamp == panel:
		return
	case hasPanelState(dir):
		// Written before agents kept a stamp: this Deplo's claim says which of it is its own.
		return
	}
	if !a.empty() {
		to, err := s.setAside(context.Background(), a, time.Now())
		if err != nil {
			log.Printf("deplo-agent: could not set aside what another Deplo left here, tried again on the next start: %v", err)
			return
		}
		log.Printf("deplo-agent: set aside what another Deplo left on this machine in %s", to)
	}
	if err := writeStamp(dir, panel); err != nil {
		log.Printf("deplo-agent: could not note which Deplo this server belongs to: %v", err)
		return
	}
	_ = os.Remove(filepath.Join(dir, panelClaimFile))
}

func readClaim(dir, panel string) *panelClaim {
	raw, err := os.ReadFile(filepath.Join(dir, panelClaimFile))
	if err != nil {
		return nil
	}
	var c panelClaim
	if json.Unmarshal(raw, &c) != nil || c.Panel != panel {
		return nil
	}
	return &c
}

func hasPanelState(dir string) bool {
	for _, f := range oneClusterFiles {
		if exists(filepath.Join(dir, f)) {
			return true
		}
	}
	for _, k := range clusterKeyed {
		if exists(filepath.Join(dir, k.file)) {
			return true
		}
	}
	return exists(filepath.Join(dir, "cluster", "state.json"))
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// judgeClaim is what on disk the claiming Deplo does not own: clusters it does not have, a leader it does not run here.
func judgeClaim(dir string, c *panelClaim) aside {
	mine := map[string]bool{}
	for _, id := range c.Clusters {
		mine[id] = true
	}
	a := aside{clusters: map[string]bool{}}
	for _, f := range oneClusterFiles {
		if id := oneClusterOf(filepath.Join(dir, f)); id != "" && !mine[id] {
			a.clusters[id] = true
		}
	}
	for _, k := range clusterKeyed {
		top := readTop(filepath.Join(dir, k.file))
		for _, key := range k.keys {
			for id := range subMap(top, key) {
				if !mine[id] {
					a.clusters[id] = true
				}
			}
		}
	}
	a.host = !c.Host && exists(filepath.Join(dir, "cluster", "state.json"))
	return a
}

func oneClusterOf(path string) string {
	var v struct {
		Cluster string `json:"cluster"`
	}
	raw, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(raw, &v) != nil {
		return ""
	}
	return v.Cluster
}

func readTop(path string) map[string]json.RawMessage {
	top := map[string]json.RawMessage{}
	if raw, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(raw, &top)
	}
	return top
}

func subMap(top map[string]json.RawMessage, key string) map[string]json.RawMessage {
	if key == "" {
		return top
	}
	m := map[string]json.RawMessage{}
	if raw, ok := top[key]; ok {
		_ = json.Unmarshal(raw, &m)
	}
	return m
}

// setAside moves what a holds into one dated folder, after stopping what runs from it. Nothing is deleted, and no
// App container or volume is touched: those are listed in the folder for the owner.
func (s *Service) setAside(ctx context.Context, a aside, now time.Time) (string, error) {
	dir := s.agentDir
	to := filepath.Join(dir, setAsideRoot, now.UTC().Format("20060102T150405Z"))
	if err := os.MkdirAll(to, 0o700); err != nil {
		return "", err
	}
	left := leftApps(ctx, dir, a)
	if a.all || a.host {
		bin := filepath.Join(dir, "cluster", clusterBinaryName)
		for _, role := range []string{"leader", "relay"} {
			reapStale(filepath.Join(dir, "cluster", role+".pid"), bin)
		}
	}
	var moves []string
	if a.all {
		moves = append(append([]string{}, oneClusterFiles...), panelOwned...)
		for _, k := range clusterKeyed {
			moves = append(moves, k.file)
		}
	} else {
		for _, f := range oneClusterFiles {
			if a.clusters[oneClusterOf(filepath.Join(dir, f))] {
				moves = append(moves, f)
				switch f {
				case "cluster-forwarders.json":
					moves = append(moves, "forwarders")
				case "cluster-replicas.json":
					moves = append(moves, "replicas")
				}
			}
		}
		if a.host {
			moves = append(moves, "cluster")
		}
		for _, k := range clusterKeyed {
			if err := splitClusters(filepath.Join(dir, k.file), filepath.Join(to, k.file), k.keys, a.clusters); err != nil {
				return "", fmt.Errorf("%s: %w", k.file, err)
			}
		}
		if err := splitSupervise(dir, to, left); err != nil {
			return "", err
		}
	}
	if a.all || slices.Contains(moves, "forwarders") {
		removeForwarders(ctx)
	}
	// The name server answers what it was fed last; the zone left, if any, starts a new one.
	if a.all || len(a.clusters) > 0 {
		dnszone.EndLeftBehind(filepath.Join(dir, "dns.pid"))
	}
	for _, p := range moves {
		if err := os.Rename(filepath.Join(dir, p), filepath.Join(to, p)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	raw, _ := json.MarshalIndent(left, "", "  ")
	return to, writeFileAtomic(filepath.Join(to, setAsideLeft), raw, 0o600)
}

// splitClusters moves the foreign clusters' entries of one file to its copy in the set-aside folder. A network key
// another Deplo's servers know goes with them, and a new one takes its place.
func splitClusters(path, dst string, keys []string, foreign map[string]bool) error {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	top := map[string]json.RawMessage{}
	if json.Unmarshal(raw, &top) != nil {
		return os.Rename(path, dst)
	}
	gone := map[string]json.RawMessage{}
	for _, key := range keys {
		m, out := subMap(top, key), map[string]json.RawMessage{}
		for id, v := range m {
			if foreign[id] {
				out[id] = v
				delete(m, id)
			}
		}
		if len(out) == 0 {
			continue
		}
		if key == "" {
			for id, v := range out {
				gone[id] = v
			}
			continue
		}
		gone[key], _ = json.Marshal(out)
		top[key], _ = json.Marshal(m)
	}
	if len(gone) == 0 {
		return nil
	}
	if _, ok := top["privateKey"]; ok && filepath.Base(path) == "cluster-mesh.json" {
		gone["privateKey"] = top["privateKey"]
		delete(top, "privateKey")
		if len(subMap(top, "clusters")) > 0 {
			k, err := mesh.NewPrivateKey()
			if err != nil {
				return err
			}
			top["privateKey"], _ = json.Marshal(k.String())
		}
	}
	out, _ := json.Marshal(gone)
	if err := writeFileAtomic(dst, out, 0o600); err != nil {
		return err
	}
	kept, _ := json.Marshal(top)
	return writeFileAtomic(path, kept, 0o600)
}

// splitSupervise sets aside what the agent kept of another Deplo's Apps, so it never sets their restart policy again.
func splitSupervise(dir, to string, left []*pb.LeftWorkload) error {
	path := filepath.Join(dir, "cluster-supervise.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var st, gone struct {
		Kept    map[string]map[string]string `json:"kept,omitempty"`
		Stopped map[string][]string          `json:"stopped,omitempty"`
	}
	if json.Unmarshal(raw, &st) != nil {
		return nil
	}
	theirs := map[string]bool{}
	for _, l := range left {
		theirs[l.GetApp()] = true
	}
	for slug, v := range st.Kept {
		if theirs[slug] {
			if gone.Kept == nil {
				gone.Kept = map[string]map[string]string{}
			}
			gone.Kept[slug] = v
			delete(st.Kept, slug)
		}
	}
	for slug, v := range st.Stopped {
		if theirs[slug] {
			if gone.Stopped == nil {
				gone.Stopped = map[string][]string{}
			}
			gone.Stopped[slug] = v
			delete(st.Stopped, slug)
		}
	}
	if gone.Kept == nil && gone.Stopped == nil {
		return nil
	}
	out, _ := json.Marshal(gone)
	if err := writeFileAtomic(filepath.Join(to, "cluster-supervise.json"), out, 0o600); err != nil {
		return err
	}
	kept, _ := json.Marshal(st)
	return writeFileAtomic(path, kept, 0o600)
}

func removeForwarders(ctx context.Context) {
	names, err := dockerForwardOps.list(ctx)
	if err != nil {
		return
	}
	for _, n := range names {
		if err := dockerForwardOps.remove(ctx, n); err != nil {
			log.Printf("deplo-agent: another Deplo's name forwarder %s: %v", n, err)
		}
	}
}

// appObject is a container or volume of a Deplo stack: its compose project is "deplo-<slug>".
type appObject struct{ kind, name, app string }

func dockerAppObjects(ctx context.Context) ([]appObject, error) {
	var out []appObject
	ps, err := dockercli.Run(ctx, 15*time.Second, "ps", "-a", "--format",
		`{{.Names}}	{{.Label "com.docker.compose.project"}}	{{.Label "`+fwdLabelKey+`"}}`)
	if err != nil || ps.Code != 0 {
		return nil, errors.New("list the containers")
	}
	for _, line := range strings.Split(ps.Stdout, "\n") {
		f := strings.Split(line, "\t")
		if len(f) == 3 && f[2] == "" {
			if slug, ok := strings.CutPrefix(f[1], "deplo-"); ok && slug != "" {
				out = append(out, appObject{"container", f[0], slug})
			}
		}
	}
	vs, err := dockercli.Run(ctx, 15*time.Second, "volume", "ls", "--format", `{{.Name}}	{{.Label "com.docker.compose.project"}}`)
	if err != nil || vs.Code != 0 {
		return nil, errors.New("list the volumes")
	}
	for _, line := range strings.Split(vs.Stdout, "\n") {
		if f := strings.Split(line, "\t"); len(f) == 2 {
			if slug, ok := strings.CutPrefix(f[1], "deplo-"); ok && slug != "" {
				out = append(out, appObject{"volume", f[0], slug})
			}
		}
	}
	return out, nil
}

// leftApps is another Deplo's App containers and volumes on this host: every stack's when that Deplo wrote all of
// the state, else the Apps its clusters' leases placed here and this Deplo's do not.
func leftApps(ctx context.Context, dir string, a aside) []*pb.LeftWorkload {
	objs, err := listAppObjects(ctx)
	if err != nil {
		log.Printf("deplo-agent: could not list the Apps another Deplo left: %v", err)
		return nil
	}
	theirs := map[string]bool{}
	if !a.all {
		top := readTop(filepath.Join(dir, "cluster-lease.json"))
		ours := map[string]bool{}
		for id, raw := range top {
			var l struct {
				Placement []struct {
					Slug string `json:"slug"`
				} `json:"placement"`
			}
			_ = json.Unmarshal(raw, &l)
			for _, p := range l.Placement {
				if a.clusters[id] {
					theirs[p.Slug] = true
				} else {
					ours[p.Slug] = true
				}
			}
		}
		for slug := range ours {
			delete(theirs, slug)
		}
	}
	out := []*pb.LeftWorkload{}
	for _, o := range objs {
		if a.all || theirs[o.app] {
			out = append(out, &pb.LeftWorkload{Kind: o.kind, Name: o.name, App: o.app})
		}
	}
	return out
}

// lastSetAside is the newest set-aside folder, its time, and what every one listed that is still on this host.
func (s *Service) lastSetAside(ctx context.Context) (string, int64, []*pb.LeftWorkload) {
	root := filepath.Join(s.agentDir, setAsideRoot)
	entries, _ := os.ReadDir(root)
	var names []string
	for _, e := range entries {
		if _, err := time.Parse("20060102T150405Z", e.Name()); err == nil && e.IsDir() {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return "", 0, nil
	}
	sort.Strings(names)
	last := names[len(names)-1]
	at, _ := time.Parse("20060102T150405Z", last)
	var list []*pb.LeftWorkload
	seen := map[string]bool{}
	for _, n := range names {
		var some []*pb.LeftWorkload
		if raw, err := os.ReadFile(filepath.Join(root, n, setAsideLeft)); err == nil && json.Unmarshal(raw, &some) == nil {
			for _, l := range some {
				if k := l.GetKind() + "/" + l.GetName(); !seen[k] {
					seen[k] = true
					list = append(list, l)
				}
			}
		}
	}
	if objs, err := listAppObjects(ctx); err == nil {
		here := map[string]bool{}
		for _, o := range objs {
			here[o.kind+"/"+o.name] = true
		}
		kept := list[:0]
		for _, l := range list {
			if here[l.GetKind()+"/"+l.GetName()] {
				kept = append(kept, l)
			}
		}
		list = kept
	}
	return filepath.Join(root, last), at.UnixMilli(), list
}

// leftSlugs are the Apps another Deplo left here: a cleanup treats them as live, so their data stays.
func (s *Service) leftSlugs() []string {
	var out []string
	if s.agentDir == "" {
		return nil
	}
	_, _, list := s.lastSetAside(context.Background())
	for _, l := range list {
		out = append(out, l.GetApp())
	}
	return out
}

// ClaimServer is the Deplo this agent links to saying what is its own (ADR-0050).
func (s *Service) ClaimServer(ctx context.Context, req *pb.ClaimServerRequest) (*pb.ClaimServerResponse, error) {
	dir := s.agentDir
	panel := panelOf(dir)
	if panel == "" {
		return nil, status.Error(codes.FailedPrecondition, "this server agent has no certificate from a Deplo to belong to")
	}
	for _, id := range req.GetClusterIds() {
		if !fwdID.MatchString(id) {
			return nil, status.Errorf(codes.InvalidArgument, "%q is not a cluster id", id)
		}
	}
	resp := &pb.ClaimServerResponse{}
	resp.SetAside, resp.SetAsideAtMs, resp.Left = s.lastSetAside(ctx)
	// Settled once: what a Deplo hands the agent after that is its own.
	if readStamp(dir) == panel {
		return resp, nil
	}
	claim := &panelClaim{Panel: panel, Clusters: req.GetClusterIds(), Host: req.GetClusterHost()}
	if judgeClaim(dir, claim).empty() {
		if err := writeStamp(dir, panel); err != nil {
			return nil, status.Errorf(codes.Internal, "note which Deplo this server belongs to: %v", err)
		}
		return resp, nil
	}
	raw, _ := json.Marshal(claim)
	if err := writeFileAtomic(filepath.Join(dir, panelClaimFile), raw, 0o600); err != nil {
		return nil, status.Errorf(codes.Internal, "keep the claim: %v", err)
	}
	resp.Restarting = true
	go func() {
		time.Sleep(selfUpdateGrace)
		restartAgent()
	}()
	return resp, nil
}
