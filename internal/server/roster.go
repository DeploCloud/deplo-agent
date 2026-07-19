package server

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

// roster.go keeps `docker ps` OFF the hot path of the metrics stream.
//
// THE COST BEING AVOIDED, measured on a real host: one
// `docker ps --filter label=... --format '{{json .}}'` burns ~190ms of DOCKERD
// CPU per call. StreamMetrics samples every 5s, so re-listing the containers on
// every tick spends ~3.6% of a core doing nothing but re-discovering a set that
// changes a handful of times a DAY — more than the container metrics the tick
// actually exists to collect. Multiply by a fleet and the telemetry costs more
// than the workload it reports on.
//
// THE CHEAP SUBSTITUTE: `docker events` is an idle PUSH stream whose measured
// marginal cost is ~0 — the daemon already computes these events, it just has
// nobody listening. So the roster is rebuilt on actual container CHURN (a start,
// a die, a destroy), never on a tick. Between two deploys the 5s loop does zero
// discovery work: it reads a slice out of memory.
//
// Three failure modes are designed against explicitly:
//
//   - A rebuild STORM. A compose stack coming up fires 8 start events in a
//     fraction of a second; a naive listener would pay the 190ms eight times.
//     Rebuilds are debounced into a fixed ~500ms window, which caps the cost at
//     one rebuild per window no matter how hard the host churns — including on a
//     host running non-Deplo containers in a loop, which is why foreign events
//     are dropped before they can even mark the roster dirty.
//
//   - A MISSED event stranding the roster. If dockerd restarts, or an event is
//     lost, an events-only design would serve a stale roster forever. A 60s
//     backstop rebuild bounds that staleness, and the events child is supervised
//     and restarted with backoff so "the daemon bounced" is a 1s outage, not a
//     permanent blindness.
//
//   - A LEAKED docker child per subscription. Close() cancels the context, which
//     SIGKILLs the events client and drains both goroutines. Without it every
//     control-plane reconnect would strand a `docker events` process forever.
//
// SCOPING IS BY LABEL, ALWAYS. Every listing carries
// `--filter label=deplo.managed=true`. It deliberately does NOT reuse
// listProjectContainers("") — that helper drops its filter when handed an empty
// project id and would enumerate EVERY container on the host, including ones
// Deplo does not own (which is why both of its callers hard-reject ""). The
// per-container deplo.project label comes back with the roster so the host-wide
// stream is demuxable without a second lookup.

const (
	// One rebuild per window, measured from the FIRST event in it. Deliberately
	// not a sliding/resetting debounce: a host churning continuously would keep
	// resetting the window and never rebuild at all.
	rosterDebounce = 500 * time.Millisecond
	// Backstop so a dropped event cannot strand the roster indefinitely. Also
	// what bounds drift in the fields no watched event reports — a healthcheck
	// flipping to unhealthy does not start/die/destroy anything, so Health can
	// lag by up to one backstop period.
	rosterBackstop = 60 * time.Second
	// The label the control plane stamps on everything it creates.
	rosterManagedFilter = "label=deplo.managed=true"
	// cgroup v2 unified hierarchy mount point. Joined with the RELATIVE path read
	// out of /proc/<pid>/cgroup — never string-built from a container id.
	rosterCgroupRoot = "/sys/fs/cgroup"
	// Ceiling on the SYNCHRONOUS first rebuild only. Without it newRoster inherits
	// the two dockercli deadlines back to back (15s ps + 20s inspect) and blocks
	// its caller for up to 35s against a wedged daemon — and if the roster is
	// built per StreamMetrics subscription, that is 35s of dead air before the
	// control plane's first frame. Timing out here costs at most one debounce
	// window: the watcher is already running and the backstop is already armed,
	// so the roster fills itself in without the caller waiting.
	rosterInitialRebuild = 10 * time.Second
)

// rosterEntry is one Deplo-managed container as the sampler sees it. Everything
// here is either read from docker or read from /proc; nothing is inferred from a
// container NAME, which is how sibling compose containers of one App get
// collapsed into a single bogus series.
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
	// ids mirrors entries as a set, maintained under the same lock. It exists
	// purely for relevant(): on a host that also runs CI, EVERY foreign event
	// reaches that lookup, and a linear scan of the entries slice would put an
	// O(n) walk on the busiest path in the file.
	ids map[string]struct{}
	// cgroups caches container id -> absolute cgroup path. A container's cgroup
	// path is fixed for its lifetime, so the /proc read happens once per
	// container rather than once per tick. Pruned to the live set on every
	// rebuild: an agent runs for months and a redeploy mints a NEW container id
	// every time, so an unpruned cache grows without bound.
	cgroups map[string]string

	// dirty is a coalescing signal, not a queue: capacity 1, non-blocking send.
	// Eight starts in a burst leave exactly one token, which is the whole point.
	dirty chan struct{}

	// debounce / backstop are rosterDebounce and rosterBackstop in production.
	// Fields so a test can compress them to milliseconds: the backstop is the one
	// guarantee that a MISSED event cannot strand the roster forever, and a
	// 60s-only version of it is a guarantee nothing ever asserts.
	debounce time.Duration
	backstop time.Duration

	// procRoot / cgroupRoot are "/proc" and "/sys/fs/cgroup" in production. They
	// are fields rather than constants so the /proc-parse → stat resolution can
	// be driven against a t.TempDir tree, the same way cgroupSampler.procRoot is.
	procRoot   string
	cgroupRoot string

	// SEAMS. Everything below is a function field with a real default assigned in
	// newRosterDefaults, so the concurrent half of this file — debounce
	// coalescing, the backstop, Close() draining both goroutines — can be tested
	// without a docker daemon. That half is where the failure modes are (a lost
	// dirty token, a leaked child, a rebuild storm), and a test that needs a live
	// dockerd to exercise it would never run on the machine where it broke.
	listFn    func(context.Context) ([]rosterPsRow, error)
	inspectFn func(context.Context, []string) (map[string]rosterDetail, error)
	rebuildFn func(context.Context)
	watchFn   func(context.Context)

	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// newRoster starts the docker events watcher bound to ctx and performs one
// synchronous initial rebuild so the first Entries() call is already populated.
//
// Order is load-bearing: the watcher starts BEFORE the initial rebuild, so a
// container that starts while that first `docker ps` is in flight leaves a dirty
// token behind rather than being missed until the backstop fires.
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

	// First population, synchronous: the caller's next Entries() must not come
	// back empty just because the stream opened a millisecond ago. Bounded by its
	// OWN deadline (see rosterInitialRebuild) — the caller must not inherit two
	// stacked dockercli timeouts from a daemon that is not answering.
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
//
// Use this, not Entries()+RunningCount(), whenever both are reported in the same
// frame: taken separately a rebuild can land between the two calls, and the
// stream would then publish a gauge that disagrees with the rows printed beside
// it — a host that reads as "3 running" next to 4 running containers, blamed on
// the sampler rather than on the read.
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

// watchEvents supervises the `docker events` child, restarting it with backoff
// until ctx is done. Without the restart loop a single dockerd bounce (a daemon
// reload, an apt upgrade) would leave the roster event-blind for the rest of the
// agent's life, silently degraded to the 60s backstop.
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

// streamEvents runs one `docker events` child to completion.
//
// It deliberately does NOT go through internal/dockercli: every entry point
// there forces a context.WithTimeout (there is no long-lived variant, by
// design), which would guillotine this stream. Same bypass FollowLogs uses —
// exec.CommandContext with the caller's context and no deadline, so the child
// dies exactly when the context is cancelled and not a moment before.
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

	// A scanner error (a token past the 1MiB limit, a read error on the pipe)
	// ends the loop with the child still RUNNING and its stdout no longer
	// drained — cmd.Wait() would then block forever on a `docker events` that
	// never exits, and watchEvents would never get to log, markDirty or restart.
	// The roster would silently degrade to the 60s backstop for the life of the
	// agent, with no log line. Kill the child first so Wait cannot wedge, and
	// return the error so the supervisor treats it as the failure it is rather
	// than as a clean EOF.
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

// relevant decides whether an event should cost us a rebuild.
//
// Foreign containers are ignored: on a host that also runs CI, unfiltered churn
// would trigger a rebuild every debounce window (~190ms of dockerd CPU each) —
// WORSE than the per-tick `docker ps` this file exists to eliminate.
//
// The second clause is a NARROW safety net, and its limit is worth stating
// plainly. Identity normally comes from the event's own deplo.managed attribute;
// an event for an id already in the roster counts regardless, so a `destroy` on
// a daemon that does not echo labels cannot strand a dead container. It covers
// REMOVALS ONLY. On such a daemon the start of a brand-new managed container is
// still dropped — it carries no label and is not yet tracked — so it stays
// invisible until the 60s backstop rebuild picks it up. That lag is the price of
// not rebuilding on foreign churn, which would cost more than the per-tick
// `docker ps` this file replaces.
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

// rebuild re-lists the managed containers and swaps in a fresh snapshot.
//
// NEVER fatal, and — just as important — never PARTIAL. A rebuild that fails
// (daemon restarting, docker socket briefly gone) logs and returns, leaving the
// last good roster in place. The containers did not stop existing because we
// could not ask about them, and reporting an empty roster would read on the
// control plane's charts as "everything went down" — a fabricated outage.
//
// The inspect gets the SAME discipline as the listing, and that is not
// symmetry for its own sake. ProjectID comes only from the inspect, and it is
// the demux key the host-wide stream is keyed on: publishing a snapshot built
// from a failed inspect blanks ProjectID for every container at once, the
// control plane cannot attribute a single sample to an App, and every chart on
// the host goes empty until the next successful rebuild — up to a full backstop
// period on a quiet host. (RestartCount collapsing to 0 and back would likewise
// read as a counter reset to any delta consumer.) A PARTIAL inspect is still
// accepted: ids destroyed between the ps and the inspect make docker exit
// non-zero while the found rows are on stdout, and dropping those rows would
// throw away a good answer.
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
		// Resolve ONLY for a container the inspect reports as running. A pid on a
		// non-running container (whether left behind by the daemon or simply
		// stale in our hands) resolves to the cgroup of whatever process reused
		// that pid, and the cache never re-resolves a hit — so one bad
		// resolution would keep feeding another workload's counters into this
		// container's series for its whole lifetime.
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

	r.mu.Lock()
	r.entries = entries
	r.ids = ids2
	r.cgroups = cgroups // rebuilt from the live set, so destroyed ids drop out
	r.mu.Unlock()
}

// rosterPsRow is one `docker ps` line: enough to enumerate, not enough to report.
type rosterPsRow struct {
	ID    string
	Name  string
	State string
}

// listManagedContainers runs the ONE listing this file is allowed to run:
// label-scoped to deplo.managed=true, `-a` so stopped containers still appear
// (a stopped App must report "stopped", not vanish), and `--no-trunc` because
// the 12-hex short id docker prints by default is not the stable 64-hex identity
// the rate calculator keys on.
func listManagedContainers(ctx context.Context) ([]rosterPsRow, error) {
	res, err := dockercli.Run(ctx, 15*time.Second,
		"ps", "-a", "--no-trunc", "--filter", rosterManagedFilter, "--format", "{{json .}}")
	if err != nil {
		return nil, err
	}
	// A non-zero exit means docker ran but could not answer (daemon starting,
	// permission denied). Its stdout is empty or partial, and treating that as
	// "no containers" would wipe a live roster — surface it as the failure it is
	// so the caller keeps the last good snapshot.
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

// The inspect template emits one JSON object per container, keyed by the FULL
// id so answers match back even when a container disappears mid-call. Mirrors
// instances.go's inspectTemplate field-for-field, plus the project label and the
// pid the cgroup backend needs. .State.Health is guarded because it is nil for
// an image with no healthcheck — a bare {{json .State.Health.Status}} would fail
// the whole template, taking every OTHER container's row down with it.
//
// `.ID`, NOT `.Id` — and the difference is not cosmetic. Docker executes an
// inspect template against the typed Go struct first and silently falls back to
// the RAW JSON MAP if any accessor does not resolve. `.Id` is the json TAG, so
// it only resolves on the map path — and on the map path `{{if .State.Health}}`
// stops protecting anything, because a container with no healthcheck simply has
// no "Health" KEY and the lookup errors out ("map has no entry for key Health")
// instead of yielding nil. Verified on docker 29.6.1: one wrong letter turned
// the whole batched inspect into an error for every container on the host.
const rosterInspectTemplate = `{"id":{{json .ID}},` +
	`"name":{{json .Name}},` +
	`"project":{{json (index .Config.Labels "deplo.project")}},` +
	`"state":{{json .State.Status}},` +
	`"health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}""{{end}},` +
	`"restartCount":{{json .RestartCount}},` +
	`"pid":{{json .State.Pid}}}`

// inspectRosterContainers inspects the whole managed set in ONE call, keyed by
// full id.
//
// One call regardless of container count is the invariant that matters here — a
// per-container inspect would reintroduce exactly the per-tick dockerd cost this
// file was written to remove.
//
// It reports an error rather than an empty map for a WHOLESALE failure (spawn
// error, 20s timeout, or a non-zero exit that produced no parsable row at all),
// because the caller cannot tell those apart from "the host has no containers"
// and would publish a roster with ProjectID blanked on every entry — see
// rebuild. A PARTIAL answer is not an error: docker exits non-zero when any id
// is gone, and the rows it did print are good data.
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

// buildRosterEntries merges the ps rows, the inspect details and the cgroup
// cache into the snapshot, in a deterministic order.
//
// The ps row is the SOURCE OF TRUTH for existence: a container docker listed is
// in the roster even if the inspect could not describe it (it was destroyed
// mid-call, or the inspect failed outright). In that case the fields we could
// not measure stay at their zero value — an unmeasurable FIELD is 0 — but the
// entry itself is real, seen in a real listing. The reverse, synthesising an
// entry for a container nothing listed, is never done.
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
		// A pid and a cgroup are only meaningful while the container RUNS, and
		// they are cleared together on purpose. Docker 29 reports pid 0 for an
		// exited container, but nothing in the API promises that across versions
		// or drivers, and a stale pid would send the cgroup backend reading
		// whatever process reused it. The cgroup path matters more: cgroupstats
		// samples any entry with a non-empty CgroupPath regardless of state, so
		// carrying a resolved path onto a stopped container would emit a
		// real-looking sample built from another workload's counters — a
		// fabricated reading, which is worse than a missing one.
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

// parseCgroupV2Path extracts the container's cgroup path RELATIVE to the unified
// mount from /proc/<pid>/cgroup, where cgroup v2 writes a single `0::<relpath>`
// line.
//
// READING the path is the entire point. The obvious shortcut — building
// /sys/fs/cgroup/system.slice/docker-<id>.scope from the container id — is wrong
// on any host that does not happen to match the author's setup: the systemd and
// cgroupfs drivers lay out different trees, and rootless docker nests everything
// under a user slice. Reading the relpath is driver-agnostic by construction,
// because the kernel is telling us where the process actually lives.
//
// Returns "" for cgroup v1 (no 0:: line — the caller falls back to docker stats)
// and, importantly, for the ROOT cgroup "/": that path resolves to
// /sys/fs/cgroup itself, whose counters describe the WHOLE HOST. Reporting the
// host's memory as one container's would not be a missing field, it would be a
// fabricated sample.
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
