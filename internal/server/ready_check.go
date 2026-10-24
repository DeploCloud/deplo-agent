package server

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// .ID keeps docker on its typed view; .Id switches it to the raw map, where a container with no
// health check has no Health key and the whole inspect fails.
const readyInspectTemplate = `{"id":{{json .ID}},"name":{{json .Name}},` +
	`"service":{{json (index .Config.Labels "com.docker.compose.service")}},` +
	`"status":{{json .State.Status}},"restarts":{{json .RestartCount}},"created":{{json .Created}},` +
	`"startedAt":{{json .State.StartedAt}},"pid":{{json .State.Pid}},` +
	`"exitCode":{{json .State.ExitCode}},"oomKilled":{{json .State.OOMKilled}},` +
	`"health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}""{{end}},` +
	`"restartPolicy":{{json .HostConfig.RestartPolicy.Name}},` +
	`"ip":"{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}"}`

// waitReady holds the deploy until every container has stayed up through the settle window and
// the app listens where it is routed, failing fast on a restart, an exit or a failing health check.
func (s *Service) waitReady(ctx context.Context, req *pb.DeployRequest, stack bool, since time.Time, commitSha string, e *emitter) {
	ready := req.GetReady()
	rc := newReadyCheck(ready.GetPort(), ready.GetPortRequired(), ready.GetService(), stack)
	rc.since = since
	rc.speaksHTTP = speaksHTTP
	timeout := time.Duration(req.GetReadyTimeoutMs()) * time.Millisecond
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	e.log("info", "Waiting for the app to start and stay up…")
	deadline := time.Now().Add(timeout)
	for {
		v := rc.observe(time.Now(), sampleReady(ctx, req.GetSlug(), stack))
		if !v.done && !time.Now().Before(deadline) {
			v = rc.expire(v)
		}
		if v.done {
			s.reportReady(ctx, rc, v, commitSha, e)
			return
		}
		select {
		case <-ctx.Done():
			e.finish(&pb.DeployResult{Error: "the deploy was canceled while the app was starting", CommitSha: commitSha})
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *Service) reportReady(ctx context.Context, rc *readyCheck, v readyVerdict, commitSha string, e *emitter) {
	res := &pb.DeployResult{Ready: v.ok, CommitSha: commitSha, ListeningPorts: v.listening}
	if !v.ok {
		res.Error = v.err
		if v.culprit != "" {
			lines := containerLogTail(ctx, v.culprit, 40)
			if len(lines) == 0 {
				e.log("warn", "It printed nothing before it stopped")
			} else {
				e.log("warn", "The last lines it printed:")
			}
			for _, l := range lines {
				e.log("info", l)
			}
			if hint := startHint(v.exitCode, lines); hint != "" {
				res.Error += ". " + hint
			}
		}
		e.finish(res)
		return
	}
	switch {
	case v.observed > 0:
		res.ObservedPort = v.observed
		e.log("warn", fmt.Sprintf("It listens on %s, not on port %d", portList(v.listening), rc.port))
	case v.note != "":
		e.log("warn", v.note)
	case len(v.listening) > 0:
		e.log("info", "Up and listening on "+portList(v.listening))
	default:
		e.log("info", "Up")
	}
	e.finish(res)
}

func portList(ports []int32) string {
	s := make([]string, len(ports))
	for i, p := range ports {
		s[i] = strconv.Itoa(int(p))
	}
	if len(s) == 1 {
		return "port " + s[0]
	}
	return "ports " + strings.Join(s, ", ")
}

// startHint names the usual cause of a start that failed at once: a program or a script missing from the image.
func startHint(exitCode int, lines []string) string {
	joined := strings.Join(lines, "\n")
	text := strings.ToLower(joined)
	if exitCode == 127 || strings.Contains(text, "command not found") || strings.Contains(text, "executable file not found") {
		return "The start command runs a program that is not in the image: check the Start command in Settings -> Build & output."
	}
	if m := pyModuleMissing.FindStringSubmatch(joined); m != nil {
		mod, _, _ := strings.Cut(m[1], ".")
		if mod == "pkg_resources" {
			return "An old package still needs setuptools, which new Python versions no longer include: add setuptools " +
				"to requirements.txt, or update that package."
		}
		return "The app imports " + mod + ", which is not installed: add it to requirements.txt if it is a package, " +
			"or check the Start command in Settings -> Build & output."
	}
	for _, re := range missingStart {
		m := re.FindStringSubmatch(joined)
		switch {
		case m == nil:
			continue
		case !strings.Contains(m[1], "/"):
			return "The app runs " + m[1] + ", a program its image does not have: add the package that provides it " +
				"(for a Railpack build, list it in RAILPACK_DEPLOY_APT_PACKAGES), or change the Start command in Settings -> Build & output."
		default:
			return "The start command runs " + strings.TrimPrefix(m[1], "/app/") + ", which is not in the app: check the Start command in Settings -> Build & output."
		}
	}
	return ""
}

var pyModuleMissing = regexp.MustCompile(`ModuleNotFoundError: No module named '([A-Za-z0-9_.]+)'`)

// What Node, Python, bash and dash print for a start script or program that is not there (Railpack builds into /app).
var missingStart = []*regexp.Regexp{
	regexp.MustCompile(`(?:Cannot find module|can't open file) '(/app/[^']+)'`),
	regexp.MustCompile(`/bin/(?:ba)?sh: (?:line \d+: )?(\S+): No such file or directory`),
	regexp.MustCompile(`\bsh: \d+: (\S+): not found`),
}

// sampleReady inspects the app's container, or every container of its stack.
func sampleReady(ctx context.Context, slug string, stack bool) []readySample {
	targets := []string{"deplo-" + slug}
	if stack {
		res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-aq", "--filter", "label=deplo.slug="+slug)
		if err != nil || res.Code != 0 {
			return nil
		}
		if targets = splitLines(res.Stdout); len(targets) == 0 {
			return nil
		}
	}
	res, err := dockercli.Run(ctx, 15*time.Second, append([]string{"inspect", "-f", readyInspectTemplate}, targets...)...)
	if err != nil {
		return nil
	}
	return parseReadySamples(res.Stdout)
}

func parseReadySamples(out string) []readySample {
	var samples []readySample
	for _, line := range splitLines(out) {
		var raw struct {
			ID, Name, Service, Status, Created, StartedAt, Health, RestartPolicy, IP string
			Restarts, Pid, ExitCode                                                  int
			OOMKilled                                                                bool
		}
		if json.Unmarshal([]byte(line), &raw) != nil || raw.ID == "" {
			continue
		}
		created, _ := parseDockerTime(raw.Created)
		started, _ := parseDockerTime(raw.StartedAt)
		if started.Year() <= 1 {
			started = time.Time{}
		}
		c := readySample{id: raw.ID, name: strings.TrimPrefix(raw.Name, "/"), service: raw.Service, status: raw.Status,
			restarts: raw.Restarts, created: created, startedAt: started, pid: raw.Pid, exitCode: raw.ExitCode,
			oomKilled: raw.OOMKilled, health: raw.Health, restartPolicy: raw.RestartPolicy}
		if ips := strings.Fields(raw.IP); len(ips) > 0 {
			c.ip = ips[0]
		}
		if c.status == "running" {
			c.listeners = containerListeners(c.pid)
		}
		samples = append(samples, c)
	}
	return samples
}

// containerLogTail is the last n lines a container printed, stdout and stderr together.
func containerLogTail(ctx context.Context, id string, n int) []string {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	tail := &lineTail{max: n}
	cmd := dockercli.Command(cctx, "docker", "logs", "--tail", strconv.Itoa(n), id)
	_ = dockercli.RunLines(cctx, cmd, func(l string) {
		if r := []rune(l); len(r) > 2000 {
			l = string(r[:2000])
		}
		tail.add(l)
	})
	return tail.lines
}
