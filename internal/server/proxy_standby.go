package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// While this server's proxy restarts, a copy of it with no port of its own takes its visitors: it starts and reads
// its routes before the proxy stops (c42: an App on one server waited the whole restart, 3-8 s on one core).

const (
	standbyContainer = "deplo-traefik-standby"
	standbyLabel     = "deplo.proxy.standby"
)

// standbyDialer reaches a port of the standby proxy, inside its own network.
type standbyDialer func(ctx context.Context, port uint32) (net.Conn, error)

var (
	// standbyUp starts the standby and returns how to reach it and how to end it; tests replace it.
	standbyUp = startStandbyProxy
	// standbyWait bounds the standby's start; standbySettle is what it gets to read its routes once it answers.
	standbyWait   = 20 * time.Second
	standbySettle = time.Second
	standbyMu     sync.Mutex
)

// proxyShape is what the standby copies of the running proxy.
type proxyShape struct {
	Image  string
	Config struct {
		Cmd, Entrypoint, Env []string
		User                 string
	}
	HostConfig struct {
		Binds []string
	}
	NetworkSettings struct {
		Networks map[string]json.RawMessage
	}
}

// standbyRunArgs is the standby: the proxy's image, command, environment and networks, no port, no alias, no label of
// the proxy's, and every bind it may write replaced by a copy (a proxy erases from its store what it did not write).
func standbyRunArgs(p proxyShape, copyDir string) (args []string, copies [][2]string, networks []string) {
	for n := range p.NetworkSettings.Networks {
		networks = append(networks, n)
	}
	slices.Sort(networks)
	args = []string{"run", "-d", "--name", standbyContainer, "--label", standbyLabel + "=1", "--restart", "no",
		"--log-driver", "none"}
	if len(networks) > 0 {
		args = append(args, "--network", networks[0])
	}
	for _, e := range p.Config.Env {
		args = append(args, "--env", e)
	}
	if p.Config.User != "" {
		args = append(args, "--user", p.Config.User)
	}
	for i, b := range p.HostConfig.Binds {
		parts := strings.Split(b, ":")
		if len(parts) >= 3 && strings.Contains(parts[2], "ro") {
			args = append(args, "-v", b)
			continue
		}
		if len(parts) < 2 {
			continue
		}
		cp := filepath.Join(copyDir, strconv.Itoa(i))
		copies = append(copies, [2]string{parts[0], cp})
		args = append(args, "-v", cp+":"+parts[1])
	}
	cmd := p.Config.Cmd
	if len(p.Config.Entrypoint) > 0 {
		args = append(args, "--entrypoint", p.Config.Entrypoint[0])
		cmd = append(slices.Clone(p.Config.Entrypoint[1:]), cmd...)
	}
	args = append(args, p.Image)
	return append(args, cmd...), copies, networks
}

// startStandbyProxy runs the standby beside this server's proxy and waits until it answers and read its routes.
func startStandbyProxy(ctx context.Context, agentDir string) (standbyDialer, func(), error) {
	standbyMu.Lock()
	defer standbyMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, standbyWait)
	defer cancel()
	res, err := dockercli.Run(ctx, 10*time.Second, "inspect", traefikContainer)
	if err != nil || res.Code != 0 {
		return nil, nil, fmt.Errorf("no proxy runs to copy")
	}
	var shapes []proxyShape
	if err := json.Unmarshal([]byte(res.Stdout), &shapes); err != nil || len(shapes) != 1 || shapes[0].Image == "" {
		return nil, nil, fmt.Errorf("could not read the proxy")
	}
	copyDir := filepath.Join(agentDir, "proxy-standby")
	_ = os.RemoveAll(copyDir)
	if err := os.MkdirAll(copyDir, 0o700); err != nil {
		return nil, nil, err
	}
	args, copies, networks := standbyRunArgs(shapes[0], copyDir)
	for _, c := range copies {
		if out, err := exec.CommandContext(ctx, "cp", "-a", c[0], c[1]).CombinedOutput(); err != nil {
			return nil, nil, fmt.Errorf("copy %s: %s", c[0], strings.TrimSpace(string(out)))
		}
	}
	_, _ = dockercli.Run(ctx, 30*time.Second, "rm", "-f", standbyContainer)
	res, err = dockercli.Run(ctx, time.Minute, args...)
	if err != nil || res.Code != 0 {
		return nil, nil, fmt.Errorf("start the standby proxy: %s", strings.TrimSpace(res.Stderr))
	}
	id := strings.TrimSpace(res.Stdout)
	end := func() { endStandby(id, copyDir) }
	for _, n := range networks[min(1, len(networks)):] {
		if err := dockercli.ConnectNetwork(ctx, n, id); err != nil {
			end()
			return nil, nil, err
		}
	}
	res, err = dockercli.Run(ctx, 10*time.Second, "inspect", "--format", "{{.State.Pid}}", id)
	pid := 0
	if err == nil && res.Code == 0 {
		pid, _ = strconv.Atoi(strings.TrimSpace(res.Stdout))
	}
	if pid == 0 {
		end()
		return nil, nil, fmt.Errorf("the standby proxy did not start")
	}
	dial := func(ctx context.Context, port uint32) (net.Conn, error) {
		return dialInNetwork(ctx, pid, net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))))
	}
	for _, port := range frontDoorPorts {
		for {
			if c, err := dial(ctx, port); err == nil {
				c.Close()
				break
			}
			select {
			case <-ctx.Done():
				end()
				return nil, nil, fmt.Errorf("the standby proxy did not answer in %s", standbyWait)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	time.Sleep(standbySettle)
	return dial, end, nil
}

// endStandby stops the standby, letting what it carries finish, and removes it and its copies.
func endStandby(id, copyDir string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, _ = dockercli.Run(ctx, 30*time.Second, "stop", "-t", "10", id)
	_, _ = dockercli.Run(ctx, 30*time.Second, "rm", "-f", id)
	_ = os.RemoveAll(copyDir)
}

// ClearStandbyProxy removes a standby an agent that stopped mid-restart left running.
func ClearStandbyProxy(ctx context.Context) {
	res, err := dockercli.Run(ctx, 10*time.Second, "ps", "-aq", "--filter", "label="+standbyLabel+"=1",
		"--filter", "name=^"+standbyContainer+"$")
	if err == nil && res.Code == 0 && strings.TrimSpace(res.Stdout) != "" {
		_, _ = dockercli.Run(ctx, 30*time.Second, "rm", "-f", standbyContainer)
	}
}
