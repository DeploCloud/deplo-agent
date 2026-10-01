package server

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const (
	rosterDebounce       = 500 * time.Millisecond
	rosterBackstop       = 60 * time.Second
	rosterManagedFilter  = "label=deplo.managed=true"
	rosterCgroupRoot     = "/sys/fs/cgroup"
	rosterInitialRebuild = 10 * time.Second
)

type rosterEntry struct {
	ID           string
	Name         string
	ProjectID    string
	State        string
	Health       string
	RestartCount int32
	PID          int
	CgroupPath   string
	OomKills     int32
}

type roster struct {
	mu      sync.RWMutex
	entries []rosterEntry
	ids     map[string]struct{}
	cgroups map[string]string
	// Docker clears State.OOMKilled on the next start, so the `oom` event is the only reliable record.
	ooms map[string]int32

	dirty chan struct{}

	debounce time.Duration
	backstop time.Duration

	procRoot   string
	cgroupRoot string

	listFn      func(context.Context) ([]rosterPsRow, error)
	inspectFn   func(context.Context, []string) (map[string]rosterDetail, error)
	hostCountFn func(context.Context) (int, bool)
	rebuildFn   func(context.Context)
	watchFn     func(context.Context)

	hostRunning int

	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func newRoster(ctx context.Context) *roster {
	r := newRosterDefaults()
	r.start(ctx)
	return r
}

func newRosterDefaults() *roster {
	r := &roster{
		ids:        map[string]struct{}{},
		cgroups:    map[string]string{},
		ooms:       map[string]int32{},
		dirty:      make(chan struct{}, 1),
		debounce:   rosterDebounce,
		backstop:   rosterBackstop,
		procRoot:   "/proc",
		cgroupRoot: rosterCgroupRoot,
	}
	r.listFn = listManagedContainers
	r.inspectFn = inspectRosterContainers
	r.hostCountFn = dockercli.CountRunning
	r.rebuildFn = r.rebuild
	r.watchFn = r.watchEvents
	return r
}

func (r *roster) start(ctx context.Context) {
	cctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.watchFn(cctx)
	}()

	ictx, icancel := context.WithTimeout(cctx, rosterInitialRebuild)
	r.rebuildFn(ictx)
	icancel()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.rebuildLoop(cctx)
	}()
}

// Entries returns a snapshot COPY of the roster, safe for the caller to hold and iterate while the events goroutine rebuilds underneath it.
func (r *roster) Entries() []rosterEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.copyEntries()
}

// copyEntries needs r.mu held.
func (r *roster) copyEntries() []rosterEntry {
	out := make([]rosterEntry, len(r.entries))
	copy(out, r.entries)
	for i := range out {
		out[i].OomKills = r.ooms[out[i].ID]
	}
	return out
}

// Snapshot returns the entries AND the running count read under a single lock.
func (r *roster) Snapshot() ([]rosterEntry, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.copyEntries(), countRunning(r.entries)
}

// RunningCount reports how many Deplo-managed containers are in the running state.
func (r *roster) RunningCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return countRunning(r.entries)
}

// HostRunningCount reports EVERY running container on the host, matching what the unary Metrics RPC puts in the same field.
func (r *roster) HostRunningCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hostRunning
}

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

// Close stops the events child and both goroutines.
func (r *roster) Close() {
	r.closeOnce.Do(func() {
		r.cancel()
		r.wg.Wait()
	})
}

func (r *roster) watchEvents(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := r.streamEvents(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		if err != nil {
			log.Printf("deplo-agent: roster events watcher stopped (%v); retrying in %s", err, backoff)
		}
		r.markDirty()
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (r *roster) streamEvents(ctx context.Context) error {
	cmd := dockercli.Command(ctx, "docker", "events",
		"--filter", "type=container",
		"--filter", "event=start",
		"--filter", "event=die",
		"--filter", "event=destroy",
		"--filter", "event=oom",
		"--format", "{{json .}}")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		ev, ok := parseEventLine(sc.Text())
		if !ok {
			continue
		}
		if !r.relevant(ev) {
			continue
		}
		if ev.Action == "oom" {
			r.recordOom(ev.ID)
			continue
		}
		r.markDirty()
	}

	serr := sc.Err()
	if serr != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	werr := cmd.Wait()
	if ctx.Err() != nil {
		return nil
	}
	if serr != nil {
		return serr
	}
	return werr
}

func (r *roster) relevant(ev dockerEvent) bool {
	if ev.Managed {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.ids[ev.ID]
	return ok
}

func (r *roster) recordOom(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ooms[id]++
}

func (r *roster) markDirty() {
	select {
	case r.dirty <- struct{}{}:
	default:
	}
}

func (r *roster) rebuildLoop(ctx context.Context) {
	backstop := time.NewTicker(r.backstop)
	defer backstop.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.dirty:
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.debounce):
			}
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

func (r *roster) rebuild(ctx context.Context) {
	rows, err := r.listFn(ctx)
	if err != nil {
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

	r.mu.RLock()
	known := make(map[string]string, len(r.cgroups))
	for k, v := range r.cgroups {
		known[k] = v
	}
	r.mu.RUnlock()

	cgroups := make(map[string]string, len(rows))
	for _, row := range rows {
		if p, ok := known[row.ID]; ok && p != "" {
			cgroups[row.ID] = p
			continue
		}
		d := details[row.ID]
		if d.State != "running" {
			continue
		}
		if p := cgroupPathForPID(r.procRoot, r.cgroupRoot, d.PID); p != "" {
			cgroups[row.ID] = p
		}
	}

	entries := buildRosterEntries(rows, details, cgroups)
	ids2 := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		ids2[e.ID] = struct{}{}
	}
	hostRunning, ok := r.hostCountFn(ctx)

	r.mu.Lock()
	r.entries = entries
	r.ids = ids2
	r.cgroups = cgroups
	for id := range r.ooms {
		if _, ok := ids2[id]; !ok {
			delete(r.ooms, id)
		}
	}
	if ok {
		r.hostRunning = hostRunning
	}
	r.mu.Unlock()
}

type rosterPsRow struct {
	ID    string
	Name  string
	State string
}

func listManagedContainers(ctx context.Context) ([]rosterPsRow, error) {
	res, err := dockercli.Run(ctx, 15*time.Second,
		"ps", "-a", "--no-trunc", "--filter", rosterManagedFilter, "--format", "{{json .}}")
	if err != nil {
		return nil, err
	}
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

type rosterDetail struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ProjectID    string `json:"project"`
	State        string `json:"state"`
	Health       string `json:"health"`
	RestartCount int32  `json:"restartCount"`
	PID          int    `json:"pid"`
}

const rosterInspectTemplate = `{"id":{{json .ID}},` +
	`"name":{{json .Name}},` +
	`"project":{{json (index .Config.Labels "deplo.project")}},` +
	`"state":{{json .State.Status}},` +
	`"health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}""{{end}},` +
	`"restartCount":{{json .RestartCount}},` +
	`"pid":{{json .State.Pid}}}`

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
	name := raw.Names
	if i := strings.IndexByte(name, ','); i >= 0 {
		name = name[:i]
	}
	return rosterPsRow{ID: raw.ID, Name: strings.TrimSpace(name), State: raw.State}, true
}

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
		d.Name = strings.TrimPrefix(d.Name, "/")
		out[d.ID] = d
	}
	return out
}

type dockerEvent struct {
	Action  string
	ID      string
	Managed bool
}

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
		LegacyID     string `json:"id"`
		LegacyStatus string `json:"status"`
	}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return dockerEvent{}, false
	}
	if raw.Type != "" && raw.Type != "container" {
		return dockerEvent{}, false
	}
	action := raw.Act
	if action == "" {
		action = raw.LegacyStatus
	}
	if i := strings.IndexByte(action, ':'); i >= 0 {
		action = strings.TrimSpace(action[:i])
	}
	if !isChurnAction(action) && action != "oom" {
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

func isChurnAction(action string) bool {
	switch action {
	case "start", "die", "destroy":
		return true
	}
	return false
}

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
		if e.State != "running" {
			e.PID = 0
			e.CgroupPath = ""
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Name != entries[j].Name {
			return entries[i].Name < entries[j].Name
		}
		return entries[i].ID < entries[j].ID
	})
	return entries
}

func cgroupPathForPID(procRoot, cgroupRoot string, pid int) string {
	if pid <= 0 {
		return ""
	}
	b, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return ""
	}
	rel := parseCgroupV2Path(string(b))
	if rel == "" {
		return ""
	}
	path := filepath.Join(cgroupRoot, rel)
	if _, err := os.Stat(path); err != nil {
		return ""
	}
	return path
}

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
