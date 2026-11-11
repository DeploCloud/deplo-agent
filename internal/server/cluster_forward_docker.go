package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
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
	network: ensureTenantNetwork,
	ensure:  ensureForwarderContainer,
	remove:  removeForwarderContainer,
	list:    listForwarderContainers,
	target:  workloadAddress,
	listen:  listenForwarder,
}

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
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%s", s.Network, s.Alias, s.Port, s.Dir)))
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

// workloadAddress finds a running container of the workload by the id Deplo stamps on it, on that network only.
func workloadAddress(ctx context.Context, workloadID, network string) (string, error) {
	if !fwdID.MatchString(workloadID) || !forwardableNetwork(network) {
		return "", fmt.Errorf("not a workload on a tenant network")
	}
	res, err := dockercli.Run(ctx, 10*time.Second, "ps", "-q",
		"--filter", "label=deplo.project="+workloadID, "--filter", "network="+network, "--filter", "status=running")
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
