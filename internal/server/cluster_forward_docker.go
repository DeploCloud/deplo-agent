package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// The docker side of name forwarders (#200). `.` is in no deploy key, so no build can be tagged as this image.
const (
	fwdImage     = "deplo/name.forwarder:1"
	fwdLabelKey  = "deplo.forwarder"
	fwdSpecLabel = "deplo.forwarder.spec"
	fwdSocketDir = "/run/deplo-fwd"
	fwdSocket    = "s"
)

var dockerForwardOps = forwardOps{
	network:   ensureTenantNetwork,
	ensure:    ensureForwarderContainer,
	remove:    removeForwarderContainer,
	list:      listForwarderContainers,
	target:    workloadAddress,
	listen:    listenForwarder,
	pids:      forwarderPids,
	redirect:  redirectAllPorts,
	proxyDial: dialProxyLoopback,
	ready:     appReady,
	runs:      appRunsHere,
}

// fwdProto names what a forwarder container speaks to this agent: a container of another one is replaced.
const fwdProto = "2"

// listenForwarder opens the socket one forwarder container reaches this agent on. The directory is what gets
// mounted, so the container still finds the socket after this agent restarts and makes it again.
func listenForwarder(dir string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o711); err != nil {
		return nil, err
	}
	sock := filepath.Join(dir, fwdSocket)
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return nil, err
	}
	// The forwarder runs as nobody; the 0700 directory above still keeps every other host user out.
	_ = os.Chmod(sock, 0o666)
	return ln, nil
}

func forwarderSpecTag(s forwardSpec) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%s", fwdProto, s.Network, s.Alias, s.Port, s.Dir)))
	return hex.EncodeToString(sum[:6])
}

// forwarderRunArgs is the whole container: this binary and one socket, no privilege, no label a stack carries.
func forwarderRunArgs(exe string, s forwardSpec) []string {
	return []string{
		"run", "-d", "--name", s.Container,
		"--network", s.Network, "--network-alias", s.Alias, "--restart", "unless-stopped",
		"--user", "65534:65534", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only",
		"--sysctl", "net.ipv4.ip_unprivileged_port_start=0",
		"--memory", "32m", "--pids-limit", "64", "--log-driver", "none",
		"--label", fwdLabelKey + "=1", "--label", fwdSpecLabel + "=" + forwarderSpecTag(s),
		"-v", exe + ":/deplo-agent:ro", "-v", s.Dir + ":" + fwdSocketDir + ":ro",
		"--entrypoint", "/deplo-agent", fwdImage,
		"forward", "--listen", fmt.Sprintf(":%d", s.Port), "--to", fwdSocketDir + "/" + fwdSocket,
	}
}

// ensureForwarderContainer starts the container that holds one name, and leaves a running one of the same spec.
func ensureForwarderContainer(ctx context.Context, s forwardSpec) error {
	if !fwdContainer.MatchString(s.Container) {
		return fmt.Errorf("%q is not a name forwarder", s.Container)
	}
	format := fmt.Sprintf(`{{index .Config.Labels %q}} {{index .Config.Labels %q}} {{.State.Running}}`, fwdLabelKey, fwdSpecLabel)
	if res, err := dockercli.Run(ctx, 10*time.Second, "inspect", "--format", format, s.Container); err == nil && res.Code == 0 {
		got := strings.Fields(res.Stdout)
		// Never remove a container that only has a forwarder's name.
		if len(got) != 3 || got[0] != "1" {
			return fmt.Errorf("a container named %s is not a name forwarder", s.Container)
		}
		if got[1] == forwarderSpecTag(s) && got[2] == "true" {
			return nil
		}
		if err := removeForwarderContainer(ctx, s.Container); err != nil {
			return err
		}
	}
	if res, err := dockercli.Run(ctx, 10*time.Second, "image", "inspect", fwdImage); err != nil || res.Code != 0 {
		empty := strings.NewReader(strings.Repeat("\x00", 1024))
		if code, err := dockercli.PipeIn(ctx, time.Minute, empty, nil, "import", "-", fwdImage); err != nil || code != 0 {
			return fmt.Errorf("prepare the forwarder image")
		}
	}
	exe, err := captureExe()
	if err != nil {
		return err
	}
	res, err := dockercli.Run(ctx, time.Minute, forwarderRunArgs(exe, s)...)
	if err != nil {
		return err
	}
	if res.Code != 0 {
		return fmt.Errorf("start the forwarder for %s: %s", s.Alias, oneLine(res.Stderr))
	}
	return nil
}

func removeForwarderContainer(ctx context.Context, name string) error {
	if !fwdContainer.MatchString(name) {
		return fmt.Errorf("%q is not a name forwarder", name)
	}
	res, err := dockercli.Run(ctx, 30*time.Second, "rm", "-f", name)
	if err == nil && res.Code != 0 {
		err = fmt.Errorf("remove %s: %s", name, oneLine(res.Stderr))
	}
	return err
}

// listForwarderContainers is every forwarder on this host: the label AND the name, so a stack that copies the label is not one.
func listForwarderContainers(ctx context.Context) ([]string, error) {
	res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-a", "--filter", "label="+fwdLabelKey+"=1", "--format", "{{.Names}}")
	if err != nil || res.Code != 0 {
		return nil, fmt.Errorf("list the name forwarders")
	}
	var out []string
	for _, name := range strings.Fields(res.Stdout) {
		if fwdContainer.MatchString(name) {
			out = append(out, name)
		}
	}
	return out, nil
}

// workloadAddress finds a running container of the workload by the id Deplo stamps on it, on that network only;
// for a compose service, the container compose labels with that service.
func workloadAddress(ctx context.Context, workloadID, network, service string) (string, error) {
	if !fwdID.MatchString(workloadID) || !forwardableNetwork(network) || (service != "" && !fwdService.MatchString(service)) {
		return "", fmt.Errorf("not a workload on a tenant network")
	}
	args := []string{"ps", "-q", "--filter", "label=deplo.project=" + workloadID, "--filter", "network=" + network,
		"--filter", "status=running"}
	if service != "" {
		args = append(args, "--filter", "label=com.docker.compose.service="+service)
	}
	res, err := dockercli.Run(ctx, 10*time.Second, args...)
	if err != nil || res.Code != 0 {
		return "", fmt.Errorf("list the workload's containers")
	}
	ids := strings.Fields(res.Stdout)
	if len(ids) == 0 {
		return "", fmt.Errorf("the workload is not running on %s here", network)
	}
	res, err = dockercli.Run(ctx, 10*time.Second, "inspect", "--format",
		fmt.Sprintf(`{{(index .NetworkSettings.Networks %q).IPAddress}}`, network), ids[0])
	ip := strings.TrimSpace(res.Stdout)
	if err != nil || res.Code != 0 || net.ParseIP(ip) == nil {
		return "", fmt.Errorf("the workload has no address on %s", network)
	}
	return ip, nil
}

// forwarderPids is the process of each running forwarder container, read in one call.
func forwarderPids(ctx context.Context, containers []string) (map[string]int, error) {
	if len(containers) == 0 {
		return map[string]int{}, nil
	}
	// .ID, never .Id: a field the typed container lacks makes docker read the raw map, where a missing Health fails.
	args := append([]string{"inspect", "--format", "{{.Name}} {{.State.Pid}}"}, containers...)
	res, err := dockercli.Run(ctx, 15*time.Second, args...)
	if err != nil {
		return nil, err
	}
	// A missing container fails the call but the others are still printed.
	out := map[string]int{}
	for _, line := range strings.Split(res.Stdout, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		name := strings.TrimPrefix(f[0], "/")
		if pid, err := strconv.Atoi(f[1]); err == nil && fwdContainer.MatchString(name) {
			out[name] = pid
		}
	}
	return out, nil
}

// appReady reads every container of the App here: it answers when one runs, every one with a health check is
// healthy, and none is still being created or restarting. One that exited (a job that ran) does not count.
func appRunsHere(ctx context.Context, appID string) (bool, error) {
	if !fwdID.MatchString(appID) {
		return false, nil
	}
	res, err := dockercli.Run(ctx, 10*time.Second, "ps", "-q", "--filter", "label=deplo.project="+appID,
		"--filter", "status=running")
	if err != nil {
		return false, err
	}
	if res.Code != 0 {
		return false, fmt.Errorf("docker ps: %s", firstLine(res.Stderr))
	}
	return strings.TrimSpace(res.Stdout) != "", nil
}

func appReady(ctx context.Context, appID string) appReadiness {
	if !fwdID.MatchString(appID) {
		return appReadiness{}
	}
	res, err := dockercli.Run(ctx, 10*time.Second, "ps", "-aq", "--filter", "label=deplo.project="+appID)
	ids := strings.Fields(res.Stdout)
	if err != nil || res.Code != 0 || len(ids) == 0 {
		return appReadiness{}
	}
	args := append([]string{"inspect", "--format", `{"id":{{json .ID}},"name":{{json .Name}},"status":{{json .State.Status}},"started":{{json .State.StartedAt}},"created":{{json .Created}},` +
		`"health":{{if .State.Health}}{{json .State.Health}}{{else}}null{{end}}}`}, ids...)
	res, err = dockercli.Run(ctx, 10*time.Second, args...)
	if err != nil || res.Code != 0 {
		return appReadiness{}
	}
	return readinessOf(strings.Split(strings.TrimSpace(res.Stdout), "\n"))
}

type containerState struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Status  string    `json:"status"`
	Started time.Time `json:"started"`
	Created time.Time `json:"created"`
	Health  *struct {
		Status        string
		FailingStreak int
		Log           []struct {
			End      time.Time
			ExitCode int
		}
	} `json:"health"`
}

// recreated is the name compose gives a container's replacement until the old one is gone.
var recreated = regexp.MustCompile(`^/([0-9a-f]{12})_(.+)$`)

// readinessOf is the rule appReady applies, one container per line. A container without a health check counts
// from its start; one with a check from the oldest check docker keeps, when every one kept passed. One whose check
// fails, or one created answerStuck ago and never started (its dependency never came up), makes the App broken.
func readinessOf(lines []string) appReadiness {
	var r appReadiness
	// starting: something will still change by itself; broken: something will not, whatever comes next.
	running, broken, starting := false, false, false
	later := func(t time.Time) {
		if t.After(r.Since) {
			r.Since = t
		}
	}
	all := make([]containerState, 0, len(lines))
	runs := map[string]string{}
	for _, line := range lines {
		var c containerState
		if json.Unmarshal([]byte(line), &c) != nil {
			return appReadiness{}
		}
		all = append(all, c)
		if c.Status == "running" {
			runs[c.Name] = c.ID
		}
	}
	for _, c := range all {
		switch c.Status {
		case "exited", "dead":
			continue
		case "running":
		case "created":
			// A routing change recreates a container: the copy that will replace it waits beside it as
			// <its id>_<its name>, and the one running answers until then (eu, 24 Nov 2026: 13 s held on a new domain).
			if m := recreated.FindStringSubmatch(c.Name); m != nil && strings.HasPrefix(runs["/"+m[2]], m[1]) {
				continue
			}
			if !c.Created.IsZero() && time.Since(c.Created) >= answerStuck {
				broken = true
			} else {
				starting = true
			}
			continue
		default:
			starting = true
			continue
		}
		running = true
		h := c.Health
		if h == nil {
			later(c.Started)
			continue
		}
		if h.Status == "unhealthy" {
			broken = true
			continue
		}
		if h.Status != "healthy" {
			starting = true
			continue
		}
		passed := h.FailingStreak == 0 && len(h.Log) > 0
		for _, l := range h.Log {
			passed = passed && l.ExitCode == 0
		}
		if passed {
			later(h.Log[0].End)
		} else {
			r.Health = true
		}
	}
	if broken && running {
		return appReadiness{Broken: true}
	}
	if !running || starting {
		return appReadiness{}
	}
	r.OK = true
	return r
}

var proxyPid struct {
	sync.Mutex
	pid int
	// missed is when the last look found no proxy: for a second after it, nobody asks Docker again.
	missed time.Time
}

// proxyMissFor is how long a look that found no proxy answers for every dial: while a proxy is recreated, each
// passed-on visitor ran its own docker inspect, up to 59 a second on a one-core server (c39).
var proxyMissFor = time.Second

// lookupProxyPid is the proxy's process, looked up one at a time: a caller whose stale pid another caller already
// replaced takes the new one, and none asks Docker within proxyMissFor of a look that found no proxy.
func lookupProxyPid(ctx context.Context, stale int) (int, error) {
	proxyPid.Lock()
	defer proxyPid.Unlock()
	if proxyPid.pid != 0 && proxyPid.pid != stale {
		return proxyPid.pid, nil
	}
	if time.Since(proxyPid.missed) < proxyMissFor {
		return 0, fmt.Errorf("this server's proxy is not running")
	}
	res, err := dockercli.Run(ctx, 10*time.Second, "inspect", "--format", "{{.State.Pid}}", traefikContainer)
	pid := 0
	if err == nil && res.Code == 0 {
		pid, _ = strconv.Atoi(strings.TrimSpace(res.Stdout))
	}
	if pid <= 0 {
		proxyPid.pid, proxyPid.missed = 0, time.Now()
		return 0, fmt.Errorf("this server's proxy is not running")
	}
	proxyPid.pid = pid
	return pid, nil
}

// dialProxyLoopback connects to the proxy's internal entrypoint, which listens on its own loopback: only a process
// inside its network, as this agent is for that one call, can reach it, and it trusts the PROXY header from there.
func dialProxyLoopback(ctx context.Context, port uint32) (net.Conn, error) {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))
	proxyPid.Lock()
	pid := proxyPid.pid
	proxyPid.Unlock()
	for attempt := 0; attempt < 2; attempt++ {
		if pid == 0 || attempt > 0 {
			var err error
			if pid, err = lookupProxyPid(ctx, pid); err != nil {
				return nil, err
			}
		}
		// A proxy that restarted has a new process: the second attempt looks it up again.
		if c, err := dialInNetwork(ctx, pid, addr); err == nil {
			return c, nil
		} else if attempt > 0 {
			return nil, err
		}
	}
	return nil, fmt.Errorf("this server's proxy cannot be reached")
}
