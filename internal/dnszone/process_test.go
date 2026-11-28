package dnszone

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/miekg/dns"

	"github.com/DeploCloud/deplo-agent/internal/dnszone/geotest"
)

// The test binary is also the name server process the supervisor starts: the role says which part it plays.
const roleEnv = "DEPLO_DNS_TEST_ROLE"

func TestMain(m *testing.M) {
	switch os.Getenv(roleEnv) {
	case "serve":
		os.Exit(Main(os.Stdin, os.Stdout, os.Stderr))
	case "probe":
		os.Exit(probe())
	}
	os.Exit(m.Run())
}

// probe is the process locked down as the name server is, trying what it must not be able to do.
type probeReport struct {
	UID, GID int
	Groups   []int
	Reached  []string
	// ReadGeo: it read the location table it was handed, as the name server must.
	ReadGeo bool
}

func probe() int {
	if err := lockDown(); err != nil {
		fmt.Fprintf(os.Stderr, "PROBE-ERROR %v\n", err)
		return 2
	}
	r := probeReport{UID: os.Getuid(), GID: os.Getgid()}
	r.Groups, _ = os.Getgroups()
	try := func(what string, err error) {
		if err == nil {
			r.Reached = append(r.Reached, what)
		}
	}
	for _, sock := range []string{"/var/run/docker.sock", "/run/docker.sock", os.Getenv("PROBE_SOCK")} {
		c, err := net.DialTimeout("unix", sock, time.Second)
		if err == nil {
			c.Close()
		}
		try("unix "+sock, err)
	}
	_, err := os.ReadFile(os.Getenv("PROBE_SECRET"))
	try("read "+os.Getenv("PROBE_SECRET"), err)
	c, err := net.DialTimeout("tcp", os.Getenv("PROBE_TCP"), time.Second)
	if err == nil {
		c.Close()
	}
	try("tcp "+os.Getenv("PROBE_TCP"), err)
	try("write its root", os.WriteFile("/planted", []byte("x"), 0o644))
	// The location table: handed open for reading, so it reads it and can neither write it nor open it by its path.
	for i, k := range strings.Split(os.Getenv(EnvSockets), ",") {
		if k != "g" {
			continue
		}
		f := os.NewFile(uintptr(3+i), "geo")
		head := make([]byte, 8)
		if n, err := f.ReadAt(head, 0); err == nil && n == 8 && string(head) == geoMagic {
			r.ReadGeo = true
		}
		_, err := f.WriteAt([]byte("x"), 0)
		try("write the location table", err)
		try("truncate the location table", f.Truncate(0))
	}
	if p := os.Getenv("PROBE_GEO"); p != "" {
		g, err := os.OpenFile(p, os.O_RDWR, 0)
		if err == nil {
			g.Close()
		}
		try("open "+p, err)
	}
	raw, _ := json.Marshal(r)
	fmt.Fprintf(os.Stderr, "PROBE %s\n", raw)
	_, _ = io.Copy(io.Discard, os.Stdin)
	return 0
}

type logs struct {
	mu    sync.Mutex
	lines []string
}

func (l *logs) logf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(f, a...))
}

func (l *logs) find(prefix string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}

func eventually(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("not within %v: %s", within, what)
}

func supervise(t *testing.T, role string, env ...string) (*Supervisor, *logs) {
	t.Helper()
	return superviseWith(t, role, "", env...)
}

// superviseWith hands the process the location table at geo too.
func superviseWith(t *testing.T, role, geo string, env ...string) (*Supervisor, *logs) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	l := &logs{}
	s := Start(Config{
		Path: exe, Env: append([]string{roleEnv + "=" + role}, env...), GeoPath: geo,
		Root:      filepath.Join(t.TempDir(), "root"),
		Addresses: func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("127.0.0.1")} },
		Logf:      l.logf,
	})
	t.Cleanup(func() {
		s.Close()
		if t.Failed() {
			l.mu.Lock()
			for _, line := range l.lines {
				t.Log(line)
			}
			l.mu.Unlock()
		}
	})
	return s, l
}

func listeningAt(t *testing.T, s *Supervisor) string {
	t.Helper()
	addrs, problem := s.Listening()
	if len(addrs) != 1 {
		t.Fatalf("listening on %v (%s)", addrs, problem)
	}
	return addrs[0]
}

func answers(addr, network string) int {
	c := &dns.Client{Net: network, Timeout: 500 * time.Millisecond}
	r, _, err := c.Exchange(query("shop-example-com.go.example.com.", dns.TypeA), addr)
	if err != nil {
		return -1
	}
	if r.Rcode != dns.RcodeSuccess {
		return -r.Rcode - 100
	}
	return len(r.Answer)
}

func TestSupervisor_runsTheNameServerAndStartsItAgain(t *testing.T) {
	s, l := supervise(t, "serve")
	addr := listeningAt(t, s)
	if err := s.SetTable(testTable()); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "an answer over UDP", func() bool { return answers(addr, "udp") == 3 })
	if n := answers(addr, "tcp"); n != 3 {
		t.Fatalf("TCP: %d", n)
	}
	// A new table reaches the running process.
	tb := testTable()
	tb.Zones[0].Fail = true
	if err := s.SetTable(tb); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "SERVFAIL", func() bool { return answers(addr, "udp") == -dns.RcodeServerFailure-100 })
	// The process dies: it is started again on the same sockets, with the last table.
	pid := s.PID()
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "a new process", func() bool { p := s.PID(); return p != 0 && p != pid })
	if err := s.SetTable(testTable()); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "answers after the restart", func() bool { return answers(addr, "udp") == 3 })
	if again := listeningAt(t, s); again != addr {
		t.Fatalf("the address moved from %s to %s", addr, again)
	}
	if l.find("dns: the name server stopped") == "" {
		t.Fatalf("the stop was not logged: %v", l.lines)
	}
	// A table that does not hold together is refused before it reaches the process.
	if err := s.SetTable(&Table{Zones: []Zone{{Name: "x"}}}); err == nil {
		t.Fatal("a bad table was taken")
	}
}

// A check's record must be served the moment the proxy hears it was set: it asks the certificate authority to look then.
func TestSupervisor_servedReturnsOnceTheProcessAnswersFromTheTable(t *testing.T) {
	s, _ := supervise(t, "serve")
	addr := listeningAt(t, s)
	for i := range 20 {
		tb := testTable()
		want := fmt.Sprint("check-", i)
		tb.Zones[0].TXT = []TXT{{Name: "_acme-challenge.go.example.com", Values: []string{want}}}
		if err := s.SetTable(tb); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := s.Served(ctx)
		cancel()
		if err != nil {
			t.Fatalf("table %d: %v", i, err)
		}
		if got := checkRecord(addr); got != want {
			t.Fatalf("table %d is served as %q", i, got)
		}
	}
}

func TestSupervisor_saysWhenPort53IsTaken(t *testing.T) {
	taken, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	port := taken.LocalAddr().(*net.UDPAddr).Port
	exe, _ := os.Executable()
	s := Start(Config{Path: exe, Env: []string{roleEnv + "=serve"}, Port: port, Root: filepath.Join(t.TempDir(), "root"),
		Addresses: func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("127.0.0.1")} }})
	defer s.Close()
	addrs, problem := s.Listening()
	if len(addrs) != 0 || !strings.Contains(problem, "taken by another program") {
		t.Fatalf("listening %v, problem %q", addrs, problem)
	}
	// Once the other program lets go, the next pass takes the port.
	taken.Close()
	s.Rebind()
	if addrs, problem = s.Listening(); len(addrs) != 1 || problem != "" {
		t.Fatalf("after the port was freed: %v %q", addrs, problem)
	}
}

// statusOf reads fields of /proc/<pid>/status.
func statusOf(t *testing.T, pid int) map[string]string {
	t.Helper()
	f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, _ := strings.Cut(sc.Text(), ":")
		out[k] = strings.Join(strings.Fields(v), " ")
	}
	return out
}

// The agent runs as root with the Docker socket and its keys: the name server must have none of that (ADR-0049).
func TestProcess_hasNoneOfTheAgentsPrivileges(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root, as the agent runs: CI runs it with sudo")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// What the process must not reach, made as reachable as possible for anyone on the host itself.
	sockPath := filepath.Join(dir, "docker.sock")
	sock, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sock.Close()
	_ = os.Chmod(sockPath, 0o777)
	secret := filepath.Join(dir, "agent.key")
	if err := os.WriteFile(secret, []byte("key"), 0o600); err != nil {
		t.Fatal(err)
	}
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	go func() {
		for {
			c, err := tcp.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	// The test itself reaches all three, so a refusal below is the lockdown's doing.
	if c, err := net.Dial("unix", sockPath); err != nil {
		t.Fatal(err)
	} else {
		c.Close()
	}
	if c, err := net.Dial("tcp", tcp.Addr().String()); err != nil {
		t.Fatal(err)
	} else {
		c.Close()
	}

	// The location table, kept as the agent keeps it: root's alone.
	geo := filepath.Join(dir, "dns-geo.bin")
	if err := os.WriteFile(geo, geotest.World(), 0o600); err != nil {
		t.Fatal(err)
	}
	s, l := superviseWith(t, "probe", geo, "PROBE_SOCK="+sockPath, "PROBE_SECRET="+secret,
		"PROBE_TCP="+tcp.Addr().String(), "PROBE_GEO="+geo)
	eventually(t, 10*time.Second, "the probe's report", func() bool { return l.find("dns: PROBE ") != "" })
	var r probeReport
	if err := json.Unmarshal([]byte(l.find("dns: PROBE ")), &r); err != nil {
		t.Fatal(err)
	}
	if r.UID != nobody || r.GID != nobody || len(r.Groups) != 0 {
		t.Fatalf("runs as uid %d gid %d groups %v", r.UID, r.GID, r.Groups)
	}
	if len(r.Reached) != 0 {
		t.Fatalf("the name server reached %v", r.Reached)
	}
	if !r.ReadGeo {
		t.Fatal("the name server could not read the location table it was handed")
	}
	if raw, _ := os.ReadFile(geo); string(raw) != string(geotest.World()) {
		t.Fatal("the location table changed")
	}
	if _, err := os.Stat(filepath.Join(s.cfg.Root, "planted")); err == nil {
		t.Fatal("the name server wrote into its root")
	}

	// The real name server, as the agent starts it, placing visitors with the table and answering a check's record.
	s, _ = superviseWith(t, "serve", geo)
	addr := listeningAt(t, s)
	tb := geoTable()
	tb.Zones[0].TXT = []TXT{{Name: "_acme-challenge.go.example.com", Values: []string{"check-value"}}}
	if err := s.SetTable(tb); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "an answer", func() bool { return answers(addr, "udp") == 3 })
	eventually(t, 5*time.Second, "Milan first", func() bool { return nearestFrom(addr, geotest.MilanV4) == "203.0.113.1 203.0.113.2" })
	eventually(t, 5*time.Second, "the check's record", func() bool { return checkRecord(addr) == "check-value" })
	pid := s.PID()
	st := statusOf(t, pid)
	for k, want := range map[string]string{"Uid": "65534 65534 65534 65534", "Gid": "65534 65534 65534 65534",
		"Groups": "", "CapEff": "0000000000000000", "CapPrm": "0000000000000000", "NoNewPrivs": "1"} {
		if st[k] != want {
			t.Errorf("%s: %q, want %q", k, st[k], want)
		}
	}
	if root, err := os.Readlink(fmt.Sprintf("/proc/%d/root", pid)); err != nil || root != s.cfg.Root {
		t.Errorf("its root is %q (%v), want %q", root, err, s.cfg.Root)
	}
	own, _ := os.Readlink("/proc/self/ns/net")
	theirs, _ := os.Readlink(fmt.Sprintf("/proc/%d/ns/net", pid))
	if own == "" || own == theirs {
		t.Errorf("it shares the host's network: %q", theirs)
	}
	entries, _ := os.ReadDir(s.cfg.Root)
	if len(entries) != 1 || entries[0].Name() != "deplo-agent" {
		t.Errorf("its root holds %v", entries)
	}
}

// checkRecord is the TXT record the name server answers at the zone's certificate check.
func checkRecord(addr string) string {
	c := &dns.Client{Timeout: 500 * time.Millisecond}
	r, _, err := c.Exchange(query("_acme-challenge.go.example.com.", dns.TypeTXT), addr)
	if err != nil || r.Rcode != dns.RcodeSuccess || len(r.Answer) != 1 {
		return ""
	}
	return strings.Join(r.Answer[0].(*dns.TXT).Txt, "")
}

// nearestFrom asks for the A records as a resolver for a client in subnet does, and says them in the order given.
func nearestFrom(addr, subnet string) string {
	c := &dns.Client{Timeout: 500 * time.Millisecond}
	r, _, err := c.Exchange(withSubnet(query("shop-example-com.go.example.com.", dns.TypeA), subnet), addr)
	if err != nil {
		return "error: " + err.Error()
	}
	if r.Rcode != dns.RcodeSuccess {
		return dns.RcodeToString[r.Rcode]
	}
	return inOrder(r.Answer)
}

func TestSupervisor_placesVisitorsWithTheTableItIsHanded(t *testing.T) {
	geo := filepath.Join(t.TempDir(), "dns-geo.bin")
	s, _ := superviseWith(t, "serve", geo)
	addr := listeningAt(t, s)
	if err := s.SetTable(geoTable()); err != nil {
		t.Fatal(err)
	}
	// No table yet: every server, the nearest unknown.
	eventually(t, 5*time.Second, "every server", func() bool { return answers(addr, "udp") == 3 })
	if got := nearestFrom(addr, geotest.MilanV4); len(strings.Fields(got)) != 3 {
		t.Fatalf("no table: %s", got)
	}
	// The agent keeps a table and has the process read it: visitors are placed.
	if err := os.WriteFile(geo, geotest.World(), 0o600); err != nil {
		t.Fatal(err)
	}
	s.Reload()
	eventually(t, 5*time.Second, "Milan first", func() bool { return nearestFrom(addr, geotest.MilanV4) == "203.0.113.1 203.0.113.2" })
	if got := nearestFrom(addr, geotest.TokyoV4); got != "203.0.113.3 203.0.113.1" {
		t.Fatalf("from Tokyo: %s", got)
	}
	// From this machine, with no subnet and an address the table places nowhere: every server.
	if n := answers(addr, "udp"); n != 3 {
		t.Fatalf("an unplaced resolver: %d", n)
	}
	// A damaged table is refused by the process, which goes on answering every server.
	raw := geotest.World()
	raw[40] ^= 1
	if err := os.WriteFile(geo, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	s.Reload()
	eventually(t, 5*time.Second, "every server again", func() bool { return len(strings.Fields(nearestFrom(addr, geotest.MilanV4))) == 3 })
}

// The flaky "port already in use": with any port, the TCP half of the one UDP got may be taken by another program.
func TestBind_anyPortTriesAnotherWhenItsTCPHalfIsTaken(t *testing.T) {
	was := listenTCP
	calls := 0
	listenTCP = func(network string, a *net.TCPAddr) (*net.TCPListener, error) {
		if calls++; calls == 1 {
			return nil, &net.OpError{Op: "listen", Net: network, Err: syscall.EADDRINUSE}
		}
		return was(network, a)
	}
	t.Cleanup(func() { listenTCP = was })
	s, err := bind(netip.MustParseAddr("127.0.0.1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	s.close()
}
