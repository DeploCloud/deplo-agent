package server

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var testSpec = forwardSpec{
	Container: "deplo_fwd_0123456789abcdef", Network: envNet, Alias: "db-shop", Port: 5432,
	Dir: "/var/lib/deplo-agent/forwarders/deplo_fwd_0123456789abcdef",
}

func agentBinaryAt(t *testing.T, path string) {
	t.Helper()
	was := captureExe
	captureExe = func() (string, error) { return path, nil }
	t.Cleanup(func() { captureExe = was })
}

func TestForwarderContainer_isThisBinaryAndOneSocketWithNoPrivilege(t *testing.T) {
	got := strings.Join(forwarderRunArgs("/usr/local/bin/deplo-agent", testSpec), " ")
	want := "run -d --name deplo_fwd_0123456789abcdef" +
		" --network deplo-env-e1 --network-alias db-shop --restart unless-stopped" +
		" --user 65534:65534 --cap-drop ALL --security-opt no-new-privileges --read-only" +
		" --sysctl net.ipv4.ip_unprivileged_port_start=0" +
		" --memory 32m --pids-limit 64 --log-driver none" +
		" --label deplo.forwarder=1 --label deplo.forwarder.spec=" + forwarderSpecTag(testSpec) +
		" -v /usr/local/bin/deplo-agent:/deplo-agent:ro" +
		" -v /var/lib/deplo-agent/forwarders/deplo_fwd_0123456789abcdef:/run/deplo-fwd:ro" +
		" --entrypoint /deplo-agent deplo/name.forwarder:1" +
		" forward --listen :5432 --to /run/deplo-fwd/s"
	if got != want {
		t.Fatalf("docker would run\n%s\nwant\n%s", got, want)
	}
	// Nothing the proxy, the roster, the console or a cleanup reads as a stack; no port on the host; no privilege.
	for _, never := range []string{"traefik", "deplo.managed", "deplo.project", "deplo.slug", "com.docker.compose", " -p ", "--publish", "--privileged", "--cap-add", "docker.sock", "--network host", "--pid ", "--ipc", "--device"} {
		if strings.Contains(got, never) {
			t.Errorf("the forwarder is started with %q", never)
		}
	}
}

func TestForwarderContainer_holdsANameNoStackCanHave(t *testing.T) {
	name := containerFor(heldName{Network: envNet, Name: "db-shop"})
	if !fwdContainer.MatchString(name) {
		t.Fatalf("%q is not a forwarder's name", name)
	}
	// Every stack's container is deplo-<slug>, deplo-<slug>-<service>-<n> or db-<slug>; a slug has no single `_`.
	if strings.HasPrefix(name, "deplo-") || strings.HasPrefix(name, "db-") || validateSlug(strings.TrimPrefix(name, "deplo")) == nil {
		t.Fatalf("a stack could be named %q", name)
	}
	if other := containerFor(heldName{Network: teamBNet, Name: "db-shop"}); other == name {
		t.Fatal("two Environments' forwarders share a container")
	}
	// The image is one no build can be tagged as: a deploy key has no dot.
	repo := strings.TrimSuffix(strings.TrimPrefix(fwdImage, "deplo/"), ":1")
	if validateSlug(repo) == nil || !regexp.MustCompile(`^[a-z0-9]+\.[a-z0-9]+$`).MatchString(repo) {
		t.Fatalf("an app's build could be tagged %s", fwdImage)
	}
}

func TestForwarderContainer_startsFromAnEmptyImage(t *testing.T) {
	agentBinaryAt(t, "/opt/agent/deplo-agent")
	argv := dockerOnPath(t, `case "$1" in inspect|image) exit 1;; import) cat >/dev/null;; esac; exit 0`)
	if err := ensureForwarderContainer(context.Background(), testSpec); err != nil {
		t.Fatal(err)
	}
	got := argv()
	if len(got) != 4 || !strings.HasPrefix(got[0], "inspect --format ") || !strings.HasSuffix(got[0], " "+testSpec.Container) ||
		got[1] != "image inspect "+fwdImage || got[2] != "import - "+fwdImage ||
		got[3] != strings.Join(forwarderRunArgs("/opt/agent/deplo-agent", testSpec), " ") {
		t.Fatalf("docker was run as\n%s", strings.Join(got, "\n"))
	}
}

func TestForwarderContainer_isLeftAloneWhileItRunsTheSameSpec(t *testing.T) {
	agentBinaryAt(t, "/opt/agent/deplo-agent")
	argv := dockerOnPath(t, `[ "$1" = inspect ] && echo "1 `+forwarderSpecTag(testSpec)+` true"; exit 0`)
	if err := ensureForwarderContainer(context.Background(), testSpec); err != nil {
		t.Fatal(err)
	}
	if got := argv(); len(got) != 1 {
		t.Fatalf("docker was run as\n%s", strings.Join(got, "\n"))
	}
}

func TestForwarderContainer_isReplacedWhenItsSpecChangedOrItStopped(t *testing.T) {
	agentBinaryAt(t, "/opt/agent/deplo-agent")
	for name, seen := range map[string]string{"another spec": "1 000000000000 true", "stopped": "1 " + forwarderSpecTag(testSpec) + " false"} {
		argv := dockerOnPath(t, `[ "$1" = inspect ] && echo "`+seen+`"; exit 0`)
		if err := ensureForwarderContainer(context.Background(), testSpec); err != nil {
			t.Fatal(err)
		}
		got := argv()
		if len(got) != 4 || got[1] != "rm -f "+testSpec.Container || !strings.HasPrefix(got[3], "run -d --name "+testSpec.Container+" ") {
			t.Fatalf("%s: docker was run as\n%s", name, strings.Join(got, "\n"))
		}
	}
}

func TestForwarderContainer_neverRemovesWhatIsNotAForwarder(t *testing.T) {
	agentBinaryAt(t, "/opt/agent/deplo-agent")
	// A container that has the name but not the label: someone else's.
	argv := dockerOnPath(t, `[ "$1" = inspect ] && echo "  true"; exit 0`)
	if err := ensureForwarderContainer(context.Background(), testSpec); err == nil {
		t.Fatal("a container that is not a forwarder was taken over")
	}
	if got := argv(); len(got) != 1 {
		t.Fatalf("docker was run as\n%s", strings.Join(got, "\n"))
	}
	// A stack's own name is refused before docker is asked anything.
	argv = dockerOnPath(t, "exit 0")
	for _, stack := range []string{"deplo-shop", "db-shop", "deplo-traefik", "deplo-shop-web-1", "deplo_fwd_x"} {
		spec := testSpec
		spec.Container = stack
		if ensureForwarderContainer(context.Background(), spec) == nil || removeForwarderContainer(context.Background(), stack) == nil {
			t.Errorf("%s was treated as a forwarder", stack)
		}
	}
	if got := argv(); len(got) != 1 || got[0] != "" {
		t.Fatalf("docker was run as\n%s", strings.Join(got, "\n"))
	}
}

func TestForwarderContainer_listsOnlyForwarders(t *testing.T) {
	// A stack may copy the label; it never has the name.
	argv := dockerOnPath(t, `[ "$1" = ps ] && printf 'deplo_fwd_0123456789abcdef\ndeplo-shop-web-1\ndb-shop\n'; exit 0`)
	got, err := listForwarderContainers(context.Background())
	if err != nil || len(got) != 1 || got[0] != "deplo_fwd_0123456789abcdef" {
		t.Fatalf("listed %v, %v", got, err)
	}
	if a := argv(); a[0] != "ps -a --filter label=deplo.forwarder=1 --format {{.Names}}" {
		t.Fatalf("docker was run as %q", a[0])
	}
}

func TestForwarderTarget_isTheWorkloadsContainerOnThatNetwork(t *testing.T) {
	argv := dockerOnPath(t, `case "$1" in ps) echo abc123;; inspect) echo 172.20.0.5;; esac; exit 0`)
	ip, err := workloadAddress(context.Background(), "db1", envNet)
	if err != nil || ip != "172.20.0.5" {
		t.Fatalf("got %q, %v", ip, err)
	}
	got := argv()
	if got[0] != "ps -q --filter label=deplo.project=db1 --filter network=deplo-env-e1 --filter status=running" ||
		got[1] != `inspect --format {{(index .NetworkSettings.Networks "deplo-env-e1").IPAddress}} abc123` {
		t.Fatalf("docker was run as\n%s", strings.Join(got, "\n"))
	}
	// Nothing of the platform's, and nothing that is not an id, is ever looked up.
	argv = dockerOnPath(t, "exit 0")
	for _, c := range [][2]string{{"db1", "deplo"}, {"db1", "deplo-socket"}, {"db1", "deplo-preview-web__pr-1"}, {"db1 --filter x", envNet}, {"", envNet}} {
		if _, err := workloadAddress(context.Background(), c[0], c[1]); err == nil {
			t.Errorf("%q on %q was looked up", c[0], c[1])
		}
	}
	if got := argv(); len(got) != 1 || got[0] != "" {
		t.Fatalf("docker was run as\n%s", strings.Join(got, "\n"))
	}
	// A workload that is not running here on that network has no address.
	dockerOnPath(t, "exit 0")
	if _, err := workloadAddress(context.Background(), "db1", envNet); err == nil {
		t.Fatal("a workload with no container was given an address")
	}
}

func TestForwarderSocket_survivesAnAgentRestartInTheSameDirectory(t *testing.T) {
	root, err := os.MkdirTemp("", "fw")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	dir := filepath.Join(root, "forwarders", "deplo_fwd_0123456789abcdef")
	first, err := listenForwarder(dir)
	if err != nil {
		t.Fatal(err)
	}
	inode := func() os.FileInfo { fi, _ := os.Stat(dir); return fi }
	before := inode()
	// The agent dies without closing: the socket file is left behind.
	if u, ok := first.(*net.UnixListener); ok {
		u.SetUnlinkOnClose(false)
	}
	first.Close()
	second, err := listenForwarder(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if !os.SameFile(before, inode()) {
		t.Fatal("the directory the container mounts was replaced")
	}
	if before.Mode().Perm() != 0o711 {
		t.Fatalf("the mounted directory is %v", before.Mode().Perm())
	}
	sock, _ := os.Stat(filepath.Join(dir, fwdSocket))
	if sock.Mode().Perm() != 0o666 {
		t.Fatalf("the socket is %v", sock.Mode().Perm())
	}
	c, err := net.Dial("unix", filepath.Join(dir, fwdSocket))
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}
