package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/mesh"
	"github.com/DeploCloud/deplo-agent/internal/replica"
)

// Replication of App data (ADR-0046 in DeploCloud/deplo): Deplo hands every member the table of the cluster's
// replicated Apps. The member that runs one sends its data as it changes; every other member receives it into a
// copy of its own, straight from member to member over the relay, with Deplo up or not. A copy is never the App's
// live data: only a failover start or a move puts it in place, and only on a member the leader places it on.
const (
	replVerb = "REPL"
	// replLast asks a REPL stream for one snapshot of a stopped App's data: its move's last changes.
	replLast       = "last"
	maxReplEntries = 256
	maxReplVolumes = 32
)

// Timing of the receivers; vars so tests can hurry them.
var (
	replReconcileEvery = 2 * time.Second
	replRetryMin       = 2 * time.Second
	replRetryMax       = 30 * time.Second
)

var replAppID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type replicaEntry struct {
	AppID   string   `json:"appId"`
	Slug    string   `json:"slug"`
	Volumes []string `json:"volumes"`
	Files   bool     `json:"files"`
	// Hold is why Deplo keeps this copy as it is for now: no stream runs, nothing on disk is removed.
	Hold string `json:"hold,omitempty"`
}

// roots are the entry's data, in the order both sides name it.
func (e replicaEntry) roots() []string {
	out := make([]string, 0, len(e.Volumes)+1)
	for _, v := range e.Volumes {
		out = append(out, replica.VolumeRoot(v))
	}
	if e.Files {
		out = append(out, replica.FilesRoot)
	}
	return out
}

type replicaTable struct {
	Cluster string            `json:"cluster"`
	Members map[string]string `json:"members"`
	Entries []replicaEntry    `json:"entries"`
}

type replicaRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	from   string
	// fingerprint is the sender's; relayed, whether a relay carries the stream itself.
	fingerprint string
	relayed     atomic.Bool
	// roots are the entry's roots the run receives; a table that names others starts it again.
	roots  []string
	status *replica.Status
	// index is what the copy's files hold, kept across the run's streams.
	index *replica.Index
}

type replicas struct {
	s       *Service
	mu      sync.Mutex
	tab     replicaTable
	runs    map[string]*replicaRun
	idle    map[string]string
	sending map[string]int
	// holding are the Apps a failover start is putting in place here: nothing receives for them meanwhile.
	holding map[string]bool
	// indexes are what each stopped receiver knew of its copy's files: a move's last changes need not read them again.
	indexes map[string]*replica.Index
	// handed is a table from Deplo in this run. Until then no new copy starts: only Deplo checks it fits on this
	// disk, and a member back from a power cut took a whole new copy beside its set-aside one (deplo ADR-0043).
	handed bool
	wake   chan struct{}
	// mountpoint reads where a volume's data lives on this host; tests replace it.
	mountpoint func(ctx context.Context, volume string, create bool) (string, error)
	// running says whether an App's stack runs here; tests replace it. ran is what each App's last look found.
	running func(ctx context.Context, slug string) bool
	ran     map[string]ranAt
	dial    func(ctx context.Context, fingerprint string) (net.Conn, error)
}

type ranAt struct {
	at time.Time
	ok bool
}

// stillRuns is running, asked once per App every replReconcileEvery however many members it sends to: each stream
// asked on its own, three docker processes every 2 s per App on a one-core server (c39).
func (r *replicas) stillRuns(ctx context.Context, slug string) bool {
	r.mu.Lock()
	last, ok := r.ran[slug]
	r.mu.Unlock()
	if ok && time.Since(last.at) < replReconcileEvery {
		return last.ok
	}
	runs := r.running(ctx, slug)
	r.mu.Lock()
	if r.ran == nil {
		r.ran = map[string]ranAt{}
	}
	r.ran[slug] = ranAt{time.Now(), runs}
	r.mu.Unlock()
	return runs
}

func (s *Service) replicas() *replicas {
	s.replOnce.Do(func() {
		r := &replicas{s: s, runs: map[string]*replicaRun{}, idle: map[string]string{}, sending: map[string]int{},
			holding: map[string]bool{}, indexes: map[string]*replica.Index{}, wake: make(chan struct{}, 1), mountpoint: volumeMountpoint, running: stackRuns}
		r.dial = r.dialMember
		if raw, err := os.ReadFile(r.file()); err == nil {
			_ = json.Unmarshal(raw, &r.tab)
		}
		s.repl = r
	})
	return s.repl
}

func (r *replicas) file() string { return filepath.Join(r.s.agentDir, "cluster-replicas.json") }

func (r *replicas) dirs() string { return filepath.Join(r.s.agentDir, "replicas") }

func (r *replicas) dir(appID string) string { return filepath.Join(r.dirs(), appID) }

// volumeMountpoint is where Docker keeps a volume's data on this host; create makes the volume first.
func volumeMountpoint(ctx context.Context, volume string, create bool) (string, error) {
	if create {
		if res, err := dockercli.Run(ctx, volumeExistsTimeout, "volume", "create", volume); err != nil || res.Code != 0 {
			return "", fmt.Errorf("create volume %s: %s", volume, oneLine(res.Stderr))
		}
	}
	res, err := dockercli.Run(ctx, volumeExistsTimeout, "volume", "inspect", "-f", "{{.Mountpoint}}", volume)
	if err != nil {
		return "", err
	}
	if res.Code != 0 {
		return "", fmt.Errorf("no volume named %s on this server", volume)
	}
	p := strings.TrimSpace(res.Stdout)
	if !filepath.IsAbs(p) {
		return "", fmt.Errorf("docker keeps %s where this server agent cannot read it", volume)
	}
	return p, nil
}

func validateReplicas(req *pb.SetClusterReplicasRequest) (replicaTable, error) {
	t := replicaTable{Cluster: req.GetClusterId(), Members: map[string]string{}}
	if len(t.Cluster) > 64 || len(req.GetEntries()) > maxReplEntries || len(req.GetMembers()) > maxFwdEntries {
		return t, status.Error(codes.InvalidArgument, "too many replicated Apps or members")
	}
	for _, m := range req.GetMembers() {
		if !replAppID.MatchString(m.GetServerId()) || !validFingerprint(m.GetFingerprint()) {
			return t, status.Errorf(codes.InvalidArgument, "%q is not a member", m.GetServerId())
		}
		t.Members[m.GetServerId()] = m.GetFingerprint()
	}
	seen := map[string]bool{}
	for _, e := range req.GetEntries() {
		if !replAppID.MatchString(e.GetAppId()) || seen[e.GetAppId()] {
			return t, status.Errorf(codes.InvalidArgument, "%q is not an App", e.GetAppId())
		}
		seen[e.GetAppId()] = true
		if err := validateSlug(e.GetSlug()); err != nil {
			return t, status.Error(codes.InvalidArgument, err.Error())
		}
		if len(e.GetVolumeNames()) > maxReplVolumes || (len(e.GetVolumeNames()) == 0 && !e.GetIncludeFiles()) {
			return t, status.Errorf(codes.InvalidArgument, "%s has nothing to replicate", e.GetSlug())
		}
		vols := map[string]bool{}
		for _, v := range e.GetVolumeNames() {
			if err := validateVolumeName(v); err != nil || vols[v] {
				return t, status.Errorf(codes.InvalidArgument, "%q is not a volume", v)
			}
			vols[v] = true
		}
		if len(e.GetHold()) > 512 {
			return replicaTable{}, status.Error(codes.InvalidArgument, "a replica's hold is too long")
		}
		t.Entries = append(t.Entries, replicaEntry{AppID: e.GetAppId(), Slug: e.GetSlug(), Volumes: e.GetVolumeNames(), Files: e.GetIncludeFiles(), Hold: e.GetHold()})
	}
	return t, nil
}

// SetClusterReplicas takes the cluster's replicated Apps; see the RPC's contract in proto/agent.proto.
func (s *Service) SetClusterReplicas(_ context.Context, req *pb.SetClusterReplicasRequest) (*pb.SetClusterReplicasResponse, error) {
	if s.agentDir == "" {
		return nil, status.Error(codes.FailedPrecondition, "this server agent keeps no state to replicate into")
	}
	t, err := validateReplicas(req)
	if err != nil {
		return nil, err
	}
	r := s.replicas()
	raw, _ := json.Marshal(t)
	if err := writeFileAtomic(r.file(), raw, 0o600); err != nil {
		return nil, status.Errorf(codes.Internal, "remember the replicated Apps: %v", err)
	}
	r.mu.Lock()
	r.tab, r.handed = t, true
	r.mu.Unlock()
	r.poke()
	return &pb.SetClusterReplicasResponse{Entries: int32(len(t.Entries))}, nil
}

// ClusterReplicaStatus says how each copy on this member stands; see the RPC's contract in proto/agent.proto.
func (s *Service) ClusterReplicaStatus(context.Context, *pb.ClusterReplicaStatusRequest) (*pb.ClusterReplicaStatusResponse, error) {
	r := s.replicas()
	out := r.status()
	// Measured outside the lock: a copy's size is a walk of its files.
	for _, st := range out.Replicas {
		if st.Role == "receiving" || st.Synced {
			st.DiskBytes = dirSize(r.dir(st.AppId))
		}
	}
	return out, nil
}

func (r *replicas) status() *pb.ClusterReplicaStatusResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := &pb.ClusterReplicaStatusResponse{}
	for _, e := range r.tab.Entries {
		st := &pb.ClusterReplicaState{AppId: e.AppID, Role: "idle", Error: r.idle[e.AppID]}
		if n := r.sending[e.AppID]; n > 0 {
			st.Role, st.Connected, st.Streams = "sending", true, int32(n)
		} else if _, here, ok := r.s.leaseGuard().locate(r.tab.Cluster, "app", e.AppID); ok && here {
			st.Role = "sending"
		}
		if run := r.runs[e.AppID]; run != nil {
			connected, synced, at, bytes, msg := run.status.Snapshot()
			st.Role, st.Connected, st.Synced, st.Bytes, st.Error, st.FromServerId = "receiving", connected, synced, bytes, msg, run.from
			st.Relayed = connected && (run.relayed.Load() || r.s.meshPath(run.fingerprint) == mesh.PathRelay)
			if !at.IsZero() {
				st.AppliedAt = at.UTC().Format(time.RFC3339Nano)
			}
		}
		if st.Role != "sending" && replica.Ready(r.dir(e.AppID)) {
			st.Synced = true
		}
		out.Replicas = append(out.Replicas, st)
	}
	return out
}

func (r *replicas) poke() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run keeps every copy this member should hold receiving, until ctx ends; then every receiver stops.
func (r *replicas) Run(ctx context.Context) {
	t := time.NewTicker(replReconcileEvery)
	defer t.Stop()
	defer r.stopAll()
	for {
		r.reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-r.wake:
		}
	}
}

func (r *replicas) stopAll() {
	r.mu.Lock()
	runs := r.runs
	r.runs = map[string]*replicaRun{}
	r.mu.Unlock()
	for _, run := range runs {
		run.cancel()
		<-run.done
	}
}

// reconcile decides, for each App, whether this member receives it now and from whom.
func (r *replicas) reconcile(ctx context.Context) {
	r.mu.Lock()
	tab := r.tab
	r.mu.Unlock()
	listed := map[string]bool{}
	for _, e := range tab.Entries {
		listed[e.AppID] = true
		member, here, ok := r.s.leaseGuard().locate(tab.Cluster, "app", e.AppID)
		r.mu.Lock()
		holding, handed := r.holding[e.AppID], r.handed
		r.mu.Unlock()
		switch {
		case holding:
			continue
		case !ok:
			r.stop(e.AppID, "the cluster's leader has not said where this App runs yet")
		case here:
			r.stop(e.AppID, "")
		case e.Hold != "":
			r.stop(e.AppID, e.Hold)
		case isFile(r.s.stackPath(e.Slug)):
			// The App ran here and its data may still be its own: it waits until the leader's return removed it.
			r.stop(e.AppID, "this server still holds its own copy of the App")
		case !validFingerprint(tab.Members[member]):
			r.stop(e.AppID, "this server does not know the server the App runs on")
		case !handed && !exists(r.dir(e.AppID)):
			r.stop(e.AppID, "waiting for Deplo to check that a copy fits on this server")
		default:
			r.mu.Lock()
			run := r.runs[e.AppID]
			r.mu.Unlock()
			if run != nil && run.from == member && slices.Equal(run.roots, e.roots()) {
				continue
			}
			r.stop(e.AppID, "")
			r.start(ctx, tab.Cluster, e, member, tab.Members[member])
		}
	}
	r.mu.Lock()
	for id := range r.runs {
		if !listed[id] {
			go r.stop(id, "")
		}
	}
	r.mu.Unlock()
	// A copy of an App no longer replicated is no use to anyone, and costs disk.
	entries, _ := os.ReadDir(r.dirs())
	for _, d := range entries {
		if d.IsDir() && !listed[d.Name()] && replAppID.MatchString(d.Name()) {
			r.stop(d.Name(), "")
			_ = os.RemoveAll(filepath.Join(r.dirs(), d.Name()))
		}
	}
}

// stop ends the receiver of one App, if one runs, and waits for it; why is shown until another starts.
func (r *replicas) stop(appID, why string) {
	r.mu.Lock()
	run := r.runs[appID]
	delete(r.runs, appID)
	if run != nil {
		if r.indexes == nil {
			r.indexes = map[string]*replica.Index{}
		}
		r.indexes[appID] = run.index
	}
	if why == "" {
		delete(r.idle, appID)
	} else {
		r.idle[appID] = why
	}
	r.mu.Unlock()
	if run != nil {
		run.cancel()
		<-run.done
	}
}

func (r *replicas) start(parent context.Context, cluster string, e replicaEntry, member, fingerprint string) {
	ctx, cancel := context.WithCancel(parent)
	run := &replicaRun{cancel: cancel, done: make(chan struct{}), from: member, fingerprint: fingerprint, roots: e.roots(), status: &replica.Status{}, index: replica.NewIndex()}
	r.mu.Lock()
	r.runs[e.AppID] = run
	r.mu.Unlock()
	go func() {
		defer close(run.done)
		wait := replRetryMin
		for ctx.Err() == nil {
			err := r.receiveOnce(ctx, cluster, e, fingerprint, run)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				run.status.Fail(oneLine(err.Error()))
			}
			// A stream that ran for a while was fine: the next try starts quick.
			if _, _, at, _, _ := run.status.Snapshot(); time.Since(at) < wait {
				wait = replRetryMin
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			wait = min(wait*2, replRetryMax)
		}
	}()
}

func (r *replicas) receiveOnce(ctx context.Context, cluster string, e replicaEntry, fingerprint string, run *replicaRun) error {
	conn, err := r.dial(ctx, fingerprint)
	if err != nil {
		return err
	}
	defer conn.Close()
	run.relayed.Store(r.s.copyPath(conn, "") == pathRelay)
	_ = conn.SetDeadline(time.Now().Add(wire.HeaderTimeout))
	if err := wire.WriteHeader(conn, replVerb, cluster+" "+e.AppID); err != nil {
		return err
	}
	verb, arg, err := wire.ReadHeader(conn)
	if err != nil {
		return err
	}
	if verb != wire.VerbOK {
		return fmt.Errorf("the server the App runs on refused: %s", arg)
	}
	_ = conn.SetDeadline(time.Time{})
	in := &activityConn{Conn: conn}
	in.last.Store(time.Now().UnixNano())
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(replStallCheck)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case now := <-t.C:
				_, synced, _, _, _ := run.status.Snapshot()
				if replStalled(synced, time.Unix(0, in.last.Load()), now) {
					fmt.Fprintf(os.Stderr, "deplo-agent: replica of %s heard nothing for %v: dialing again\n", e.Slug, replStallAfter)
					conn.Close()
					return
				}
			}
		}
	}()
	return atLowPriority(func() error { return replica.Receive(ctx, in, r.dir(e.AppID), e.roots(), run.status, run.index) })
}

// A whole copy whose sender says nothing at all, not even the beat it sends every 2 s, dials again: a stream that
// died without closing must never hold a copy behind. Generous, because a sender rescanning big files is quiet.
var (
	replStallAfter = 3 * time.Minute
	replStallCheck = 5 * time.Second
)

func replStalled(synced bool, last, now time.Time) bool {
	return synced && now.Sub(last) > replStallAfter
}

// activityConn notes when anything last arrived.
type activityConn struct {
	net.Conn
	last atomic.Int64
}

func (a *activityConn) Read(p []byte) (int, error) {
	n, err := a.Conn.Read(p)
	if n > 0 {
		a.last.Store(time.Now().UnixNano())
	}
	return n, err
}

func (r *replicas) dialMember(ctx context.Context, fingerprint string) (net.Conn, error) {
	m := r.s.clusterMember()
	m.mu.Lock()
	party := m.party
	m.mu.Unlock()
	if party == nil {
		return nil, errors.New("this server is not linked to its cluster")
	}
	return party.Dial(ctx, wire.Party{Fingerprint: fingerprint, Role: wire.RoleMember})
}

// serveReplica answers another member's REPL stream: only for an App the leader placed here and that runs here,
// to a member of this cluster, and only for as long as both stay true.
func (s *Service) serveReplica(from wire.Party, c net.Conn, arg string) {
	r := s.replicas()
	cluster, rest, _ := strings.Cut(arg, " ")
	appID, mode, _ := strings.Cut(rest, " ")
	last := mode == replLast
	r.mu.Lock()
	tab := r.tab
	r.mu.Unlock()
	var entry *replicaEntry
	for i := range tab.Entries {
		if tab.Entries[i].AppID == appID {
			entry = &tab.Entries[i]
		}
	}
	known := false
	for _, fp := range tab.Members {
		known = known || from.Is(fp)
	}
	refuse := func(why string) { _ = wire.WriteHeader(c, wire.VerbErr, why) }
	if tab.Cluster == "" || cluster != tab.Cluster || entry == nil || !known || from.Role != wire.RoleMember {
		refuse("this server does not replicate that App")
		return
	}
	stillHere := func(ctx context.Context) bool {
		_, here, ok := s.leaseGuard().locate(cluster, "app", appID)
		return ok && here && !s.leaseGuard().RanOut() && r.stillRuns(ctx, entry.Slug)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if last {
		// The last changes go only to the member the leader moved the App to, once it stopped here.
		member, here, ok := s.leaseGuard().locate(cluster, "app", appID)
		if !ok || here || !from.Is(tab.Members[member]) || r.running(ctx, entry.Slug) {
			refuse("the App has not moved to that server, or still runs here")
			return
		}
	} else if !stillHere(ctx) {
		refuse("the App does not run on this server")
		return
	}
	dirs := make([]string, 0, len(entry.Volumes)+1)
	for _, v := range entry.Volumes {
		p, err := r.mountpoint(ctx, v, false)
		if err != nil {
			refuse(oneLine(err.Error()))
			return
		}
		dirs = append(dirs, p)
	}
	if entry.Files {
		files := s.filesRoot(entry.Slug)
		if err := os.MkdirAll(files, 0o755); err != nil {
			refuse(oneLine(err.Error()))
			return
		}
		dirs = append(dirs, files)
	}
	if wire.WriteHeader(c, wire.VerbOK, "") != nil {
		return
	}
	if last {
		// The App is down until it lands: at full speed, never behind its Apps.
		if err := replica.SendOnce(ctx, c, entry.roots(), dirs); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: last changes of %s: %v\n", entry.Slug, err)
		}
		return
	}
	r.mu.Lock()
	r.sending[appID]++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.sending[appID]--
		if r.sending[appID] <= 0 {
			delete(r.sending, appID)
		}
		r.mu.Unlock()
	}()
	// The App moving away, its lease running out or its stack stopping ends the stream at once.
	go func() {
		t := time.NewTicker(replReconcileEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !stillHere(ctx) {
					cancel()
					c.Close()
					return
				}
			}
		}
	}()
	if err := atLowPriority(func() error { return replica.Send(ctx, c, entry.roots(), dirs) }); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: replica of %s: %v\n", entry.Slug, err)
	}
}

// promoteReplica puts this member's copy of an App in place of its Volumes and Files, before a failover starts it
// here. Nothing receives for the App until release is called, once the start is over: its stack exists here then.
func (s *Service) promoteReplica(ctx context.Context, slug string) (release func(), err error) {
	r := s.replicas()
	r.mu.Lock()
	var entry *replicaEntry
	for i := range r.tab.Entries {
		if r.tab.Entries[i].Slug == slug {
			entry = &r.tab.Entries[i]
		}
	}
	if entry == nil {
		r.mu.Unlock()
		return nil, errors.New("this server does not replicate that App")
	}
	e := *entry
	r.holding[e.AppID] = true
	r.mu.Unlock()
	release = func() {
		r.mu.Lock()
		delete(r.holding, e.AppID)
		r.mu.Unlock()
		r.poke()
	}
	r.stop(e.AppID, "")
	return release, s.putReplicaInPlace(ctx, e)
}

// putReplicaInPlace moves this member's copy of an App into its Volumes and Files; the caller holds it.
func (s *Service) putReplicaInPlace(ctx context.Context, e replicaEntry) error {
	r := s.replicas()
	live := map[string]string{}
	for _, v := range e.Volumes {
		p, err := r.mountpoint(ctx, v, true)
		if err != nil {
			return err
		}
		live[replica.VolumeRoot(v)] = p
	}
	if e.Files {
		files := s.filesRoot(e.Slug)
		if err := os.MkdirAll(files, 0o755); err != nil {
			return err
		}
		live[replica.FilesRoot] = files
	}
	if err := replica.Promote(r.dir(e.AppID), e.roots(), live); err != nil {
		return err
	}
	// The copy is the App's data now: this member sends from here on.
	_ = os.RemoveAll(r.dir(e.AppID))
	return nil
}

// PromoteClusterReplica takes a moved App's last changes from the member it left, then puts the copy in place: a
// move stops the App only for what changed since (c44: 64 MB copied again in 8 s). See proto/agent.proto.
func (s *Service) PromoteClusterReplica(ctx context.Context, req *pb.PromoteClusterReplicaRequest) (*pb.PromoteClusterReplicaResponse, error) {
	r := s.replicas()
	r.mu.Lock()
	tab := r.tab
	var entry *replicaEntry
	for i := range tab.Entries {
		if tab.Entries[i].AppID == req.GetAppId() {
			entry = &tab.Entries[i]
		}
	}
	if entry == nil || r.holding[req.GetAppId()] {
		r.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "this server does not replicate that App")
	}
	e := *entry
	ix := r.indexes[e.AppID]
	if run := r.runs[e.AppID]; run != nil {
		ix = run.index
	}
	r.holding[e.AppID] = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.holding, e.AppID)
		r.mu.Unlock()
		r.poke()
	}()
	if _, here, ok := s.leaseGuard().locate(tab.Cluster, "app", e.AppID); !ok || !here {
		return nil, status.Error(codes.FailedPrecondition, "the cluster's leader does not place that App on this server")
	}
	fp := tab.Members[req.GetFromServerId()]
	if !validFingerprint(fp) {
		return nil, status.Error(codes.FailedPrecondition, "this server does not know the server the App left")
	}
	if !replica.Ready(r.dir(e.AppID)) {
		return nil, status.Error(codes.FailedPrecondition, "this server has no whole copy of the App's data yet")
	}
	r.stop(e.AppID, "")
	if ix == nil {
		ix = replica.NewIndex()
	}
	defer func() {
		r.mu.Lock()
		delete(r.indexes, e.AppID)
		r.mu.Unlock()
	}()
	st := &replica.Status{}
	err := r.receiveLast(ctx, tab.Cluster, e, fp, st, ix)
	if _, synced, _, _, _ := st.Snapshot(); !synced {
		if err == nil {
			err = errors.New("the stream ended early")
		}
		return nil, status.Errorf(codes.Unavailable, "the last changes did not all arrive: %v", err)
	}
	if err := s.putReplicaInPlace(ctx, e); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	_, _, _, n, _ := st.Snapshot()
	return &pb.PromoteClusterReplicaResponse{Bytes: n}, nil
}

// receiveLast takes one snapshot of a stopped App's data into its copy here, at full speed.
func (r *replicas) receiveLast(ctx context.Context, cluster string, e replicaEntry, fingerprint string, st *replica.Status, ix *replica.Index) error {
	conn, err := r.dial(ctx, fingerprint)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(wire.HeaderTimeout))
	if err := wire.WriteHeader(conn, replVerb, cluster+" "+e.AppID+" "+replLast); err != nil {
		return err
	}
	verb, arg, err := wire.ReadHeader(conn)
	if err != nil {
		return err
	}
	if verb != wire.VerbOK {
		return fmt.Errorf("the server the App left refused: %s", arg)
	}
	_ = conn.SetDeadline(time.Time{})
	return replica.Receive(ctx, conn, r.dir(e.AppID), e.roots(), st, ix)
}
