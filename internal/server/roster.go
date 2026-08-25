package server

// https://deplo.build/docs/guides/monitoring

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// roster.go keeps `docker ps` OFF the hot path of the metrics stream. THE COST BEING
// AVOIDED, measured on a real host: one `docker ps --filter label=... --format '{{json
// .}}'` burns ~190ms of DOCKERD CPU per call. SCOPING IS BY LABEL, ALWAYS.

const (
	// One rebuild per window, measured from the FIRST event in it. Deliberately
	// not a sliding/resetting debounce: a host churning continuously would keep
	// resetting the window and never rebuild at all.
	rosterDebounce = 500 * time.Millisecond
	// Backstop so a dropped event cannot strand the roster indefinitely.
	rosterBackstop = 60 * time.Second
	// The label the control plane stamps on everything it creates.
	rosterManagedFilter = "label=deplo.managed=true"
	// cgroup v2 unified hierarchy mount point. Joined with the RELATIVE path read
	// out of /proc/<pid>/cgroup — never string-built from a container id.
	rosterCgroupRoot = "/sys/fs/cgroup"
	// Ceiling on the SYNCHRONOUS first rebuild only.
	rosterInitialRebuild = 10 * time.Second
)

// rosterEntry is one Deplo-managed container as the sampler sees it.
type rosterEntry struct {
	ID           string // full 64-hex docker id
	Name         string
	ProjectID    string // the deplo.project label; "" if absent
	State        string // running|restarting|exited|created|paused|dead|removing
	Health       string // healthy|unhealthy|starting; "" when the image has no healthcheck
	RestartCount int32
	PID          int    // 0 when not running or unknown
	CgroupPath   string // absolute /sys/fs/cgroup/... path; "" when unresolved
}

// roster is the live, event-driven set of Deplo-managed containers on this host.
type roster struct {
	mu      sync.RWMutex
	entries []rosterEntry
	// ids mirrors entries as a set, maintained under the same lock.
	ids map[string]struct{}
	// cgroups caches container id -> absolute cgroup path.
	cgroups map[string]string

	// dirty is a coalescing signal, not a queue: capacity 1, non-blocking send.
	// Eight starts in a burst leave exactly one token, which is the whole point.
	dirty chan struct{}

	// debounce / backstop are rosterDebounce and rosterBackstop in production.
	debounce time.Duration
	backstop time.Duration

	// procRoot / cgroupRoot are "/proc" and "/sys/fs/cgroup" in production. They
	// are fields rather than constants so the /proc-parse → stat resolution can
	// be driven against a t.TempDir tree, the same way cgroupSampler.procRoot is.
	procRoot   string
	cgroupRoot string

	// SEAMS. That half is where the failure modes are (a lost dirty token, a leaked child,
	// a rebuild storm), and a test that needs a live dockerd to exercise it would never
	// run on the machine where it broke.
	listFn      func(context.Context) ([]rosterPsRow, error)
	inspectFn   func(context.Context, []string) (map[string]rosterDetail, error)
	hostCountFn func(context.Context) (int, bool)
	rebuildFn   func(context.Context)
	watchFn     func(context.Context)

	// hostRunning is EVERY running container on the host, not just the deplo.managed ones
	// in `entries`. It exists because HostMetrics.running_containers must not change
	// meaning depending on which RPC served it.
	hostRunning int

	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// newRoster starts the docker events watcher bound to ctx and performs one synchronous
// initial rebuild so the first Entries() call is already populated.
func newRoster(ctx context.Context) *roster {
	r := newRosterDefaults()
	r.start(ctx)
	return r
}

// newRosterDefaults builds an UNSTARTED roster wired to the real docker calls.
// Split out from newRoster so a test can swap the seams before start() spawns
// anything; production has exactly one caller and it takes every default.
func newRosterDefaults() *roster {
	r := &roster{
		ids:        map[string]struct{}{},
		cgroups:    map[string]string{},
		dirty:      make(chan struct{}, 1),
		debounce:   rosterDebounce,
		backstop:   rosterBackstop,
		procRoot:   "/proc",
		cgroupRoot: rosterCgroupRoot,
	}
	r.listFn = listManagedContainers
	r.inspectFn = inspectRosterContainers
	r.hostCountFn = hostRunningCount
	r.rebuildFn = r.rebuild
	r.watchFn = r.watchEvents
	return r
}

// start spawns the watcher, performs the bounded initial rebuild, and spawns the
// rebuild loop. See newRoster for why the order is load-bearing.
func (r *roster) start(ctx context.Context) {
	cctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.watchFn(cctx)
	}()

	// First population, synchronous: the caller's next Entries() must not come back empty
	// just because the stream opened a millisecond ago.
	ictx, icancel := context.WithTimeout(cctx, rosterInitialRebuild)
	r.rebuildFn(ictx)
	icancel()

	// Started only now, so rebuild() has exactly one caller at a time and needs
	// no lock of its own.
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.rebuildLoop(cctx)
	}()
}

// Entries returns a snapshot COPY of the roster, safe for the caller to hold and
// iterate while the events goroutine rebuilds underneath it.
func (r *roster) Entries() []rosterEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]rosterEntry, len(r.entries))
	copy(out, r.entries)
	return out
}

// Snapshot returns the entries AND the running count read under a single lock.
func (r *roster) Snapshot() ([]rosterEntry, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]rosterEntry, len(r.entries))
	copy(out, r.entries)
	return out, countRunning(r.entries)
}

// RunningCount reports how many Deplo-managed containers are in the running
// state. Only for callers that want the gauge ALONE; pairing it with a separate
// Entries() call reintroduces the torn read Snapshot exists to prevent.
func (r *roster) RunningCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return countRunning(r.entries)
}

// HostRunningCount reports EVERY running container on the host, matching what the unary
// Metrics RPC puts in the same field.
func (r *roster) HostRunningCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hostRunning
}

// cachedCgroup exposes the cgroup cache under the lock. Only tests read it, and
// they must do so through here: the rebuild loop can be swapping the map at the
// same moment, which is a data race even when the assertion happens to pass.
func (r *roster) cachedCgroup(id string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.cgroups[id]
	return p, ok
}

func countRunning(entries []rosterEntry) int {
	n := 0
	for _, e := range entries {
		if e.State == "running" {
			n++
		}
	}
	return n
}

// Close stops the events child and both goroutines. Idempotent: a double Close
// (stream teardown racing an explicit close) must not panic.
func (r *roster) Close() {
	r.closeOnce.Do(func() {
		r.cancel()
		r.wg.Wait()
	})
}

// ---------------------------------------------------------------------------
// events watcher
// ---------------------------------------------------------------------------

// watchEvents supervises the `docker events` child, restarting it with backoff until
// ctx is done.
func (r *roster) watchEvents(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := r.streamEvents(ctx)
		if ctx.Err() != nil {
			return
		}
		// A watcher that survived a while was healthy; a fresh failure after a
		// long run deserves a fast retry, not the backoff a crash-loop earned.
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		if err != nil {
			log.Printf("deplo-agent: roster events watcher stopped (%v); retrying in %s", err, backoff)
		}
		// The daemon is likely coming back up; a rebuild on reconnect re-syncs
		// whatever churned while we were not listening.
		r.markDirty()
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

// streamEvents runs one `docker events` child to completion. It deliberately does NOT
// go through internal/dockercli: every entry point there forces a context.WithTimeout
// (there is no long-lived variant, by design), which would guillotine this stream.
func (r *roster) streamEvents(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker", "events",
		"--filter", "type=container",
		"--filter", "event=start",
		"--filter", "event=die",
		"--filter", "event=destroy",
		"--format", "{{json .}}")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	sc := bufio.NewScanner(stdout)
	// A compose container carries a lot of labels and they all ride the event's
	// actor attributes; the default 64KiB token limit is close enough to be worth
	// raising, since overflowing it would kill the watcher.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		ev, ok := parseEventLine(sc.Text())
		if !ok {
			continue
		}
		if !r.relevant(ev) {
			continue
		}
		r.markDirty()
	}

	// A scanner error (a token past the 1MiB limit, a read error on the pipe) ends the
	// loop with the child still RUNNING and its stdout no longer drained — cmd.Wait()
	// would then block forever on a `docker events` that never exits, and watchEvents
	// would never get to log, markDirty or restart.
	serr := sc.Err()
	if serr != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	werr := cmd.Wait()
	if ctx.Err() != nil {
		return nil // ordinary teardown, not a failure
	}
	if serr != nil {
		return serr
	}
	return werr
}

// relevant decides whether an event should cost us a rebuild. It covers REMOVALS ONLY.
// That lag is the price of not rebuilding on foreign churn, which would cost more than
// the per-tick `docker ps` this file replaces.
func (r *roster) relevant(ev dockerEvent) bool {
	if ev.Managed {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.ids[ev.ID]
	return ok
}

// markDirty records that the roster needs rebuilding, without blocking. A token
// already in the channel means a rebuild is pending and will observe this change
// too — dropping the send is correct, not a lost update.
func (r *roster) markDirty() {
	select {
	case r.dirty <- struct{}{}:
	default:
	}
}

// rebuildLoop is the only caller of rebuild after construction: churn (debounced)
// or the backstop, never a tick.
func (r *roster) rebuildLoop(ctx context.Context) {
	backstop := time.NewTicker(r.backstop)
	defer backstop.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.dirty:
			// Let the rest of the burst land before paying for the listing.
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.debounce):
			}
			// Drain the tokens this window collected: the rebuild that follows
			// covers them. Draining BEFORE rebuilding (not after) is deliberate —
			// an event arriving mid-rebuild must survive and trigger the next one.
			select {
			case <-r.dirty:
			default:
			}
			r.rebuildFn(ctx)
			backstop.Reset(r.backstop)
		case <-backstop.C:
			r.rebuildFn(ctx)
		}
	}
}

// ---------------------------------------------------------------------------
// rebuild
// ---------------------------------------------------------------------------

// rebuild re-lists the managed containers and swaps in a fresh snapshot. NEVER fatal,
// and — just as important — never PARTIAL. (RestartCount collapsing to 0 and back would
// likewise read as a counter reset to any delta consumer.)
func (r *roster) rebuild(ctx context.Context) {
	rows, err := r.listFn(ctx)
	if err != nil {
		// Close() cancelling an in-flight docker call is an ordinary teardown,
		// not an incident; logging it at error level on every stream close trains
		// the reader to ignore this line.
		if ctx.Err() == nil {
			log.Printf("deplo-agent: roster rebuild failed (%v); serving the last known roster", err)
		}
		return
	}

	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	details, err := r.inspectFn(ctx, ids)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("deplo-agent: roster inspect failed (%v); serving the last known roster", err)
		}
		return
	}

	// Resolve cgroup paths OUTSIDE the lock: /proc reads are fast but Entries()
	// is called from the sampling loop and must never wait on filesystem I/O.
	r.mu.RLock()
	known := make(map[string]string, len(r.cgroups))
	for k, v := range r.cgroups {
		known[k] = v
	}
	r.mu.RUnlock()

	cgroups := make(map[string]string, len(rows))
	for _, row := range rows {
		if p, ok := known[row.ID]; ok && p != "" {
			cgroups[row.ID] = p // fixed for the container's lifetime
			continue
		}
		// Resolve ONLY for a container the inspect reports as running.
		d := details[row.ID]
		if d.State != "running" {
			continue
		}
		// Only a non-empty result is cached, so a container inspected while it
		// was still starting (pid 0) is retried on the next rebuild instead of
		// being permanently marked unresolvable.
		if p := cgroupPathForPID(r.procRoot, r.cgroupRoot, d.PID); p != "" {
			cgroups[row.ID] = p
		}
	}

	entries := buildRosterEntries(rows, details, cgroups)
	ids2 := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		ids2[e.ID] = struct{}{}
	}
	// Unfiltered host count, taken outside the lock like everything else here.
	// ok distinguishes a genuine 0 from a read that failed.
	hostRunning, ok := r.hostCountFn(ctx)

	r.mu.Lock()
	r.entries = entries
	r.ids = ids2
	r.cgroups = cgroups // rebuilt from the live set, so destroyed ids drop out
	// Only publish a figure we actually READ. A failed `docker ps -q` (ok=false) means the
	// count is UNKNOWN, and fabricating a 0 on a host that plainly has containers is worse
	// than reporting the last known figure — keep the previous value.
	if ok {
		r.hostRunning = hostRunning
	}
	r.mu.Unlock()
}

// rosterPsRow is one `docker ps` line: enough to enumerate, not enough to report.
type rosterPsRow struct {
	ID    string
	Name  string
	State string
}

// listManagedContainers runs the ONE listing this file is allowed to run: label-scoped
// to deplo.managed=true, `-a` so stopped containers still appear (a stopped App must
// report "stopped", not vanish), and `--no-trunc` because the 12-hex short id docker
// prints by default is not the stable 64-hex identity the rate calculator keys on.
func listManagedContainers(ctx context.Context) ([]rosterPsRow, error) {
	res, err := dockercli.Run(ctx, 15*time.Second,
		"ps", "-a", "--no-trunc", "--filter", rosterManagedFilter, "--format", "{{json .}}")
	if err != nil {
		return nil, err
	}
	// A non-zero exit means docker ran but could not answer (daemon starting, permission
	// denied).
	if res.Code != 0 {
		return nil, &rosterCmdError{what: "docker ps", code: res.Code, stderr: strings.TrimSpace(res.Stderr)}
	}
	rows := []rosterPsRow{}
	for _, line := range strings.Split(res.Stdout, "\n") {
		if row, ok := parseRosterPsLine(line); ok {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// hostRunningCount counts EVERY running container on the host — the unfiltered `docker
// ps -q` the host gauge is built from — returning ok=false when the read itself failed.
func hostRunningCount(ctx context.Context) (int, bool) {
	res, err := dockercli.Run(ctx, 10*time.Second, "ps", "-q")
	if err != nil || res.Code != 0 {
		return 0, false
	}
	n := 0
	for _, l := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		if strings.TrimSpace(l) != "" {
			n++
		}
	}
	return n, true
}

type rosterCmdError struct {
	what   string
	code   int
	stderr string
}

func (e *rosterCmdError) Error() string {
	if e.stderr == "" {
		return e.what + " exited " + strconv.Itoa(e.code)
	}
	return e.what + " exited " + strconv.Itoa(e.code) + ": " + e.stderr
}

// rosterDetail is everything one batched `docker inspect` yields per container.
type rosterDetail struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ProjectID    string `json:"project"`
	State        string `json:"state"`
	Health       string `json:"health"`
	RestartCount int32  `json:"restartCount"`
	PID          int    `json:"pid"`
}

// The inspect template emits one JSON object per container, keyed by the FULL id so
// answers match back even when a container disappears mid-call.
const rosterInspectTemplate = `{"id":{{json .ID}},` +
	`"name":{{json .Name}},` +
	`"project":{{json (index .Config.Labels "deplo.project")}},` +
	`"state":{{json .State.Status}},` +
	`"health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}""{{end}},` +
	`"restartCount":{{json .RestartCount}},` +
	`"pid":{{json .State.Pid}}}`

// inspectRosterContainers inspects the whole managed set in ONE call, keyed by full id.
func inspectRosterContainers(ctx context.Context, ids []string) (map[string]rosterDetail, error) {
	if len(ids) == 0 {
		return map[string]rosterDetail{}, nil
	}
	args := append([]string{"inspect", "-f", rosterInspectTemplate}, ids...)
	res, err := dockercli.Run(ctx, 20*time.Second, args...)
	if err != nil {
		return nil, err
	}
	out := parseRosterInspectLines(res.Stdout)
	if res.Code != 0 && len(out) == 0 {
		return nil, &rosterCmdError{what: "docker inspect", code: res.Code, stderr: strings.TrimSpace(res.Stderr)}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// pure parsing / assembly — everything below is docker-free and table-tested
// ---------------------------------------------------------------------------

// parseRosterPsLine turns one `docker ps --format {{json .}}` line into a row.
// Pure (no docker) so it is unit-testable; ok=false for a blank line, malformed
// JSON, or a row with no id to key on.
func parseRosterPsLine(line string) (rosterPsRow, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return rosterPsRow{}, false
	}
	var raw struct {
		ID    string `json:"ID"`
		Names string `json:"Names"`
		State string `json:"State"`
	}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return rosterPsRow{}, false
	}
	if raw.ID == "" {
		return rosterPsRow{}, false
	}
	// `docker ps` can list several comma-joined names for one container; the
	// first is the canonical one every other RPC addresses it by.
	name := raw.Names
	if i := strings.IndexByte(name, ','); i >= 0 {
		name = name[:i]
	}
	return rosterPsRow{ID: raw.ID, Name: strings.TrimSpace(name), State: raw.State}, true
}

// parseRosterInspectLines turns the inspect template's output into details by id.
func parseRosterInspectLines(stdout string) map[string]rosterDetail {
	out := map[string]rosterDetail{}
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var d rosterDetail
		if err := json.Unmarshal([]byte(line), &d); err != nil {
			continue
		}
		if d.ID == "" {
			continue
		}
		// docker reports the name as "/deplo-foo".
		d.Name = strings.TrimPrefix(d.Name, "/")
		out[d.ID] = d
	}
	return out
}

// dockerEvent is the churn signal, reduced to the three things we act on.
type dockerEvent struct {
	Action  string
	ID      string
	Managed bool // the actor carried deplo.managed=true
}

// parseEventLine turns one `docker events --format {{json .}}` line into an
// event. ok=false for anything that is not a container churn event we watch for.
func parseEventLine(line string) (dockerEvent, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return dockerEvent{}, false
	}
	var raw struct {
		Type  string `json:"Type"`
		Act   string `json:"Action"`
		Actor struct {
			ID         string            `json:"ID"`
			Attributes map[string]string `json:"Attributes"`
		} `json:"Actor"`
		// The legacy top-level shape docker still emits alongside the typed one.
		// Read as a fallback so a daemon that only sends the old form is not
		// silently ignored (which would strand the roster on the 60s backstop).
		LegacyID     string `json:"id"`
		LegacyStatus string `json:"status"`
	}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return dockerEvent{}, false
	}
	// Type is absent on the legacy shape; only REJECT when it says something
	// other than container.
	if raw.Type != "" && raw.Type != "container" {
		return dockerEvent{}, false
	}
	action := raw.Act
	if action == "" {
		action = raw.LegacyStatus
	}
	// Some actions carry an argument ("exec_start: bash"); the verb is the head.
	if i := strings.IndexByte(action, ':'); i >= 0 {
		action = strings.TrimSpace(action[:i])
	}
	if !isChurnAction(action) {
		return dockerEvent{}, false
	}
	id := raw.Actor.ID
	if id == "" {
		id = raw.LegacyID
	}
	if id == "" {
		return dockerEvent{}, false
	}
	return dockerEvent{
		Action:  action,
		ID:      id,
		Managed: raw.Actor.Attributes["deplo.managed"] == "true",
	}, true
}

// isChurnAction reports whether an action changes WHICH containers exist or run
// — the only reason to pay for a rebuild.
func isChurnAction(action string) bool {
	switch action {
	case "start", "die", "destroy":
		return true
	}
	return false
}

// buildRosterEntries merges the ps rows, the inspect details and the cgroup cache into
// the snapshot, in a deterministic order. The reverse, synthesising an entry for a
// container nothing listed, is never done.
func buildRosterEntries(rows []rosterPsRow, details map[string]rosterDetail, cgroups map[string]string) []rosterEntry {
	entries := make([]rosterEntry, 0, len(rows))
	for _, row := range rows {
		d, ok := details[row.ID]
		e := rosterEntry{
			ID:         row.ID,
			Name:       row.Name,
			State:      row.State,
			CgroupPath: cgroups[row.ID],
		}
		if ok {
			// The inspect is the richer read from the same daemon: prefer it,
			// and fall back to the ps row only for what it did not answer.
			e.ProjectID = d.ProjectID
			e.Health = d.Health
			e.RestartCount = d.RestartCount
			e.PID = d.PID
			if d.State != "" {
				e.State = d.State
			}
			if d.Name != "" {
				e.Name = d.Name
			}
		}
		// A pid and a cgroup are only meaningful while the container RUNS, and they are
		// cleared together on purpose.
		if e.State != "running" {
			e.PID = 0
			e.CgroupPath = ""
		}
		entries = append(entries, e)
	}
	// Deterministic order so consecutive stream frames list containers the same
	// way; docker's own ordering is creation-time and shuffles across a restart.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Name != entries[j].Name {
			return entries[i].Name < entries[j].Name
		}
		return entries[i].ID < entries[j].ID
	})
	return entries
}

// cgroupPathForPID resolves a running container's absolute cgroup v2 path, or ""
// when it cannot be determined. "" is honest: the caller falls back to
// `docker stats` rather than reading a path that might not be the container's.
func cgroupPathForPID(procRoot, cgroupRoot string, pid int) string {
	if pid <= 0 {
		return ""
	}
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "" // the process exited between the inspect and this read
	}
	rel := parseCgroupV2Path(string(b))
	if rel == "" {
		return ""
	}
	path := filepath.Join(cgroupRoot, rel)
	// Verify it is really there rather than handing the backend a path that
	// silently reads nothing (e.g. the agent in a container with its own
	// cgroup namespace, where the host path does not exist in our view).
	if _, err := os.Stat(path); err != nil {
		return ""
	}
	return path
}

// parseCgroupV2Path extracts the container's cgroup path RELATIVE to the unified mount
// from /proc/<pid>/cgroup, where cgroup v2 writes a single `0::<relpath>` line.
func parseCgroupV2Path(content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		rel, ok := strings.CutPrefix(line, "0::")
		if !ok {
			continue
		}
		if rel == "" || rel == "/" || !strings.HasPrefix(rel, "/") {
			return ""
		}
		return rel
	}
	return ""
}
