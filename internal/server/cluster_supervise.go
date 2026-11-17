package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A cluster's databases and Apps with stored data run with Docker's restart policy "no", so a server back from a power
// cut never starts a copy that runs elsewhere by now. The agent starts each one a current lease places here, and starts
// it again after it stops by itself, with a growing pause, for as long as that lease lasts (deplo#237).

// restartLabel is the restart policy Deplo rendered a single-writer service with, before it set "no" in its place.
const restartLabel = "deplo.restart"

var (
	superviseEvery   = 2 * time.Second
	policySweepEvery = 30 * time.Second
	superviseFirst   = time.Second
	superviseLongest = time.Minute
	// superviseSettled is how long a container runs before its next stop counts as a new one, not one more in a row.
	superviseSettled = time.Minute
)

type writerContainer struct {
	ID, Name, Service, State, Policy, Label string
	ExitCode                                int
	StartedAt, FinishedAt                   time.Time
}

// intent is the restart policy the container is meant to have, and where it was read. A record that is missing or
// unreadable means supervision: a writer is never kept down for want of one.
func (c writerContainer) intent(kept, rendered map[string]string) (policy, from string) {
	if p, ok := restartPolicy(c.Label); ok {
		return p, "Deplo's label"
	}
	if p, ok := restartPolicy(kept[c.Service]); ok {
		return p, "the policy this agent turned off"
	}
	if p, ok := restartPolicy(c.Policy); ok && p != "no" {
		return p, "Docker's policy"
	}
	if p, ok := restartPolicy(rendered[c.Service]); ok {
		return p, "the stack's file"
	}
	return "unless-stopped", "no record of its policy, so it is supervised"
}

// restartPolicy reads a restart policy as Docker or compose writes it; ok is false for anything else.
func restartPolicy(s string) (string, bool) {
	p, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(s)), ":")
	switch p {
	case "no", "always", "unless-stopped", "on-failure":
		return p, true
	}
	return "", false
}

// wantsStart says Docker's own policy would start the container now: one that ran and exited, never one only created.
func (c writerContainer) wantsStart(policy string) bool {
	if c.State != "exited" {
		return false
	}
	switch policy {
	case "always", "unless-stopped":
		return true
	case "on-failure":
		return c.ExitCode != 0
	}
	return false
}

type supervisor struct {
	file      string
	now       func() time.Time
	list      func(ctx context.Context, slug string) ([]writerContainer, error)
	setPolicy func(ctx context.Context, ids []string, policy string) error
	start     func(ctx context.Context, id string) error
	// rendered is the restart policy per service in the stack's own file, as Deplo rendered it.
	rendered func(slug string) map[string]string
	logf     func(format string, a ...any)

	// said is the last reason logged per stack or container, so each change is logged once.
	saidMu sync.Mutex
	said   map[string]string

	mu sync.Mutex
	// kept is the policy turned off, per stack and service; stopped the services stopped on purpose, "" the whole stack.
	kept    map[string]map[string]string
	stopped map[string][]string
	pause   map[string]time.Duration
	nextAt  map[string]time.Time
	busy    map[string]bool
	swept   time.Time
}

type supervisorState struct {
	Kept    map[string]map[string]string `json:"kept,omitempty"`
	Stopped map[string][]string          `json:"stopped,omitempty"`
}

func newSupervisor(file string, now func() time.Time) *supervisor {
	sv := &supervisor{file: file, now: now, list: listWriterContainers, setPolicy: setRestartPolicy, start: startContainer,
		kept: map[string]map[string]string{}, stopped: map[string][]string{}, pause: map[string]time.Duration{},
		nextAt: map[string]time.Time{}, busy: map[string]bool{}, said: map[string]string{}, logf: logCluster}
	var st supervisorState
	raw, err := os.ReadFile(file)
	if err == nil {
		err = json.Unmarshal(raw, &st)
	}
	switch {
	case err == nil:
		if st.Kept != nil {
			sv.kept = st.Kept
		}
		if st.Stopped != nil {
			sv.stopped = st.Stopped
		}
	case !errors.Is(err, os.ErrNotExist):
		sv.logf("could not read %s, so every writer here is supervised: %v", file, err)
	}
	return sv
}

// logCluster is the supervisor's log line: info level, in the agent's journal.
func logCluster(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "deplo-agent: cluster: "+format+"\n", a...)
}

// note logs why a writer is or is not started, once each time the reason changes; "" only forgets the last one.
func (sv *supervisor) note(key, why string) {
	sv.saidMu.Lock()
	same := sv.said[key] == why
	sv.said[key] = why
	sv.saidMu.Unlock()
	if !same && why != "" {
		sv.logf("%s", why)
	}
}

// saveLocked writes what must outlive this agent; the caller holds sv.mu.
func (sv *supervisor) saveLocked() {
	if sv.file == "" {
		return
	}
	raw, err := json.Marshal(supervisorState{Kept: sv.kept, Stopped: sv.stopped})
	if err == nil {
		err = writeFileAtomic(sv.file, raw, 0o600)
	}
	if err != nil {
		sv.logf("could not keep the restart policies in %s: %v", sv.file, err)
	}
}

// stoppedOnPurpose keeps a stop someone asked for, so the stack stays down as Docker would leave it.
func (sv *supervisor) stoppedOnPurpose(slug string, services []string, why string) {
	if len(services) == 0 {
		services = []string{""}
	}
	sv.mu.Lock()
	for _, name := range services {
		if !slices.Contains(sv.stopped[slug], name) {
			sv.stopped[slug] = append(sv.stopped[slug], name)
		}
	}
	sv.saveLocked()
	sv.mu.Unlock()
	sv.logf("%s stays down until it is started or deployed again: %s", slug, why)
}

// cleared ends the stops kept for a stack that was started or deployed again.
func (sv *supervisor) cleared(slug string) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	if _, ok := sv.stopped[slug]; ok {
		delete(sv.stopped, slug)
		sv.saveLocked()
		sv.logf("%s was started or deployed again: the supervisor looks after it again", slug)
	}
}

// forget drops everything kept for a stack that is gone from this host.
func (sv *supervisor) forget(slug string) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	_, k := sv.kept[slug]
	_, s := sv.stopped[slug]
	delete(sv.kept, slug)
	delete(sv.stopped, slug)
	if k || s {
		sv.saveLocked()
	}
}

// sweepSoon has the next pass turn Docker's policy off again: a deploy has just made new containers.
func (sv *supervisor) sweepSoon() {
	sv.mu.Lock()
	sv.swept = time.Time{}
	sv.mu.Unlock()
}

// sweep turns Docker's restart policy off for every single-writer stack on this host, at most every policySweepEvery.
func (sv *supervisor) sweep(ctx context.Context, slugs []string) {
	sv.mu.Lock()
	if !sv.swept.IsZero() && sv.now().Sub(sv.swept) < policySweepEvery {
		sv.mu.Unlock()
		return
	}
	sv.swept = sv.now()
	sv.mu.Unlock()
	for _, slug := range slugs {
		sv.quiet(ctx, slug)
	}
}

// quiet turns Docker's restart policy off for one stack's containers, once what it was is on disk.
func (sv *supervisor) quiet(ctx context.Context, slug string) {
	cs, err := sv.list(ctx, slug)
	if err != nil || len(cs) == 0 {
		return
	}
	var ids []string
	sv.mu.Lock()
	changed := false
	for _, c := range cs {
		if c.Policy == "no" || c.Policy == "" {
			continue
		}
		ids = append(ids, c.ID)
		if c.Label == "" && sv.kept[slug][c.Service] != c.Policy {
			if sv.kept[slug] == nil {
				sv.kept[slug] = map[string]string{}
			}
			sv.kept[slug][c.Service], changed = c.Policy, true
		}
	}
	if changed {
		sv.saveLocked()
	}
	sv.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	if err := sv.setPolicy(ctx, ids, "no"); err != nil {
		sv.logf("could not turn Docker's restart policy off for %s: %v", slug, err)
		return
	}
	sv.logf("%s starts only under its lease now: Docker's restart policy is off for %d container(s)", slug, len(ids))
}

// due lists what of a stack Docker's own policy would start now; true reserves the stack for one start.
func (sv *supervisor) due(ctx context.Context, slug string) bool {
	sv.mu.Lock()
	if sv.busy[slug] {
		sv.mu.Unlock()
		return false
	}
	sv.mu.Unlock()
	cs, err := sv.list(ctx, slug)
	if err != nil {
		sv.note(slug, fmt.Sprintf("%s is not looked after this time: Docker did not list its containers: %v", slug, err))
		return false
	}
	if len(cs) == 0 {
		sv.note(slug, slug+" has no container on this server")
		return false
	}
	if len(sv.startable(slug, cs)) == 0 {
		return false
	}
	sv.mu.Lock()
	defer sv.mu.Unlock()
	if sv.busy[slug] {
		return false
	}
	sv.busy[slug] = true
	return true
}

func (sv *supervisor) done(slug string) {
	sv.mu.Lock()
	delete(sv.busy, slug)
	sv.mu.Unlock()
}

// startable is the stopped containers of a stack Docker would start again now: not stopped on purpose, past their
// pause. Why each other one is left as it is goes to the log.
func (sv *supervisor) startable(slug string, cs []writerContainer) []writerContainer {
	var rendered map[string]string
	if sv.rendered != nil {
		rendered = sv.rendered(slug)
	}
	type said struct{ key, why string }
	var notes []said
	var out []writerContainer
	sv.mu.Lock()
	now := sv.now()
	if slices.Contains(sv.stopped[slug], "") {
		notes = append(notes, said{slug, slug + " stays down: it was stopped on purpose"})
	} else {
		notes = append(notes, said{slug, ""})
		for _, c := range cs {
			policy, from := c.intent(sv.kept[slug], rendered)
			switch {
			case slices.Contains(sv.stopped[slug], c.Service):
				notes = append(notes, said{c.ID, c.Name + " stays down: it was stopped on purpose"})
			case c.State != "exited":
				notes = append(notes, said{c.ID, fmt.Sprintf("%s is %s", c.Name, c.State)})
			case !c.wantsStart(policy):
				notes = append(notes, said{c.ID, fmt.Sprintf("%s stays down: its restart policy %s (%s) does not start it after exit %d",
					c.Name, policy, from, c.ExitCode)})
			case now.Before(sv.nextAt[c.ID]):
				notes = append(notes, said{c.ID, fmt.Sprintf("%s is started again at %s, after a pause (restart policy %s, %s)",
					c.Name, sv.nextAt[c.ID].Format(time.TimeOnly), policy, from)})
			default:
				out = append(out, c)
			}
		}
	}
	sv.mu.Unlock()
	for _, n := range notes {
		sv.note(n.key, n.why)
	}
	return out
}

// startDue starts what of a stack is due, each pause twice the last one up to superviseLongest. The caller holds the
// stack's lock and has seen the lease place it here.
func (sv *supervisor) startDue(ctx context.Context, slug string) {
	cs, err := sv.list(ctx, slug)
	if err != nil {
		return
	}
	for _, c := range sv.startable(slug, cs) {
		sv.mu.Lock()
		p := sv.pause[c.ID]
		if p == 0 || (!c.StartedAt.IsZero() && c.FinishedAt.Sub(c.StartedAt) >= superviseSettled) {
			p = superviseFirst
		}
		sv.nextAt[c.ID], sv.pause[c.ID] = sv.now().Add(p), min(2*p, superviseLongest)
		sv.mu.Unlock()
		if err := sv.start(ctx, c.ID); err != nil {
			sv.note(c.ID, fmt.Sprintf("could not start %s again: %v", c.Name, err))
			continue
		}
		sv.note(c.ID, "")
		sv.logf("started %s again under its lease: it had stopped by itself (exit %d)", c.Name, c.ExitCode)
	}
}

// release gives the containers of these stacks back their restart policy, for a server that left its cluster.
func (sv *supervisor) release(ctx context.Context, slugs []string) {
	sv.mu.Lock()
	kept := maps.Clone(sv.kept)
	sv.mu.Unlock()
	for slug := range kept {
		if !slices.Contains(slugs, slug) {
			slugs = append(slugs, slug)
		}
	}
	for _, slug := range slugs {
		cs, err := sv.list(ctx, slug)
		if err != nil {
			continue
		}
		by := map[string][]string{}
		for _, c := range cs {
			p, ok := restartPolicy(c.Label)
			if !ok {
				p, ok = restartPolicy(kept[slug][c.Service])
			}
			if ok && p != "no" && p != c.Policy {
				by[p] = append(by[p], c.ID)
			}
		}
		for p, ids := range by {
			if err := sv.setPolicy(ctx, ids, p); err != nil {
				sv.logf("could not give %s its restart policy back: %v", slug, err)
			}
		}
	}
	sv.mu.Lock()
	sv.kept, sv.stopped = map[string]map[string]string{}, map[string][]string{}
	sv.saveLocked()
	sv.mu.Unlock()
}

func (s *Service) supervisorFile() string {
	if s.agentDir == "" {
		return ""
	}
	return filepath.Join(s.agentDir, "cluster-supervise.json")
}

func (s *Service) supervision() *supervisor {
	s.supOnce.Do(func() {
		s.sup = newSupervisor(s.supervisorFile(), time.Now)
		s.sup.rendered = s.renderedRestart
	})
	return s.sup
}

// renderedRestart is the restart policy of each service in the stack's file: Deplo's label there, else `restart:`,
// else Docker's own default. A file that cannot be read says nothing, and nothing is taken from it.
func (s *Service) renderedRestart(slug string) map[string]string {
	raw, err := os.ReadFile(s.stackPath(slug))
	if err != nil {
		return nil
	}
	return renderedPolicies(raw)
}

func renderedPolicies(raw []byte) map[string]string {
	var doc struct {
		Services map[string]struct {
			Restart string `yaml:"restart"`
			Labels  any    `yaml:"labels"`
		} `yaml:"services"`
	}
	if yaml.Unmarshal(raw, &doc) != nil {
		return nil
	}
	out := map[string]string{}
	for name, svc := range doc.Services {
		p := svc.Restart
		if l := composeLabel(svc.Labels, restartLabel); l != "" {
			p = l
		}
		if p == "" {
			p = "no"
		}
		out[name] = p
	}
	return out
}

// composeLabel reads one label of a compose service, written as a map or as a list of key=value.
func composeLabel(labels any, key string) string {
	switch l := labels.(type) {
	case map[string]any:
		if v, ok := l[key]; ok && v != nil {
			return fmt.Sprint(v)
		}
	case []any:
		for _, item := range l {
			if k, v, ok := strings.Cut(fmt.Sprint(item), "="); ok && k == key {
				return v
			}
		}
	}
	return ""
}

// superviseWriters keeps this member's single-writer stacks to their lease until ctx ends.
func (s *Service) superviseWriters(ctx context.Context) {
	t := time.NewTicker(superviseEvery)
	defer t.Stop()
	for {
		s.superviseOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Service) superviseOnce(ctx context.Context) {
	g, sv := s.leaseGuard(), s.supervision()
	all, mine := g.writers()
	var here []string
	for _, slug := range all {
		if g.has == nil || g.has(slug) {
			here = append(here, slug)
		}
	}
	// What the leases name is turned off before any lease is heard: a power cut can come at any time.
	sv.sweep(ctx, here)
	for _, slug := range here {
		if !slices.Contains(mine, slug) {
			if why := g.whyNotMine(slug); why != "" {
				sv.note("lease "+slug, slug+" is not started here: "+why)
			}
			continue
		}
		sv.note("lease "+slug, "")
		if sv.due(ctx, slug) {
			go s.restartWriter(slug)
		}
	}
}

// restartWriter starts what of a stack is due, under its lock and only while a current lease still places it here.
func (s *Service) restartWriter(slug string) {
	sv := s.supervision()
	defer sv.done(slug)
	done, ok := s.lockToStart(slug)
	if !ok {
		sv.note("lease "+slug, slug+" is not started here: its lease ran out")
		return
	}
	defer done()
	if why := s.leaseGuard().whyNotMine(slug); why != "" {
		sv.note("lease "+slug, slug+" is not started here: "+why)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sv.startDue(ctx, slug)
}

type stopReasonKey struct{}

// stoppedBecause tells StopStack who stops the stack and why, for the log; a stop with none is one Deplo asked for.
func stoppedBecause(ctx context.Context, why string) context.Context {
	return context.WithValue(ctx, stopReasonKey{}, why)
}

// writerStopped keeps a stop someone asked for of a cluster's single-writer stack.
func (s *Service) writerStopped(ctx context.Context, slug string, services []string) {
	if !s.leaseGuard().isWriter(slug) {
		return
	}
	why, _ := ctx.Value(stopReasonKey{}).(string)
	if why == "" {
		why = "Deplo stopped it"
	}
	s.supervision().stoppedOnPurpose(slug, services, why)
}

// writerRedeployed follows a deploy or reroute of a single-writer stack: what runs now is wanted, and its new
// containers come with the policy Deplo rendered, turned off at the next pass.
func (s *Service) writerRedeployed(ctx context.Context, slug string) {
	if !s.leaseGuard().isWriter(slug) {
		return
	}
	if stackRuns(ctx, slug) {
		s.supervision().cleared(slug)
	}
	s.supervision().sweepSoon()
}

// restoringWriter keeps a stack a restore stops as stopped on purpose, until the restore has it running again.
func (s *Service) restoringWriter(slug string) func() {
	if slug == "" || !s.leaseGuard().isWriter(slug) {
		return func() {}
	}
	s.supervision().stoppedOnPurpose(slug, nil, "a restore stopped it")
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if stackRuns(ctx, slug) {
			s.supervision().cleared(slug)
		}
	}
}

func listWriterContainers(ctx context.Context, slug string) ([]writerContainer, error) {
	res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-a", "-q", "--no-trunc", "--filter", "label=com.docker.compose.project=deplo-"+slug)
	if err == nil && res.Code != 0 {
		err = errors.New(strings.TrimSpace(res.Stderr))
	}
	if err != nil {
		return nil, err
	}
	ids := splitLines(res.Stdout)
	if len(ids) == 0 {
		return nil, nil
	}
	format := strings.Join([]string{"{{.Id}}", "{{.Name}}", `{{index .Config.Labels "com.docker.compose.service"}}`,
		"{{.State.Status}}", "{{.HostConfig.RestartPolicy.Name}}", `{{index .Config.Labels "` + restartLabel + `"}}`,
		"{{.State.ExitCode}}", "{{.State.StartedAt}}", "{{.State.FinishedAt}}", `{{index .Config.Labels "com.docker.compose.oneoff"}}`}, "\t")
	// A container removed in between fails the inspect; the others still print.
	res, err = dockercli.Run(ctx, 15*time.Second, append([]string{"inspect", "--format", format}, ids...)...)
	if err != nil {
		return nil, err
	}
	return parseWriterContainers(res.Stdout), nil
}

func parseWriterContainers(out string) []writerContainer {
	var cs []writerContainer
	for _, line := range splitLines(out) {
		f := strings.Split(line, "\t")
		if len(f) != 10 || f[9] == "True" {
			continue
		}
		code, _ := strconv.Atoi(f[6])
		started, _ := parseDockerTime(f[7])
		finished, _ := parseDockerTime(f[8])
		cs = append(cs, writerContainer{ID: f[0], Name: strings.TrimPrefix(f[1], "/"), Service: labelValue(f[2]), State: f[3],
			Policy: f[4], Label: labelValue(f[5]), ExitCode: code, StartedAt: started, FinishedAt: finished})
	}
	return cs
}

// labelValue is a label as this listing prints it: Docker writes "<no value>" for one the container lacks.
func labelValue(s string) string {
	if s == "<no value>" {
		return ""
	}
	return s
}

func setRestartPolicy(ctx context.Context, ids []string, policy string) error {
	res, err := dockercli.Run(ctx, 30*time.Second, append([]string{"update", "--restart=" + policy}, ids...)...)
	if err == nil && res.Code != 0 {
		err = errors.New(strings.TrimSpace(res.Stderr))
	}
	return err
}

func startContainer(ctx context.Context, id string) error {
	res, err := dockercli.Run(ctx, 30*time.Second, "start", id)
	if err == nil && res.Code != 0 {
		err = errors.New(strings.TrimSpace(res.Stderr))
	}
	return err
}
