package dnszone

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// nobody is the user and group the name server runs as when the agent is root.
const nobody = 65534

// Config is how the agent starts its name server.
type Config struct {
	// Path and Args start the process: the agent's own binary, and "dns".
	Path string
	Args []string
	Env  []string
	// Root is the empty directory the process is locked into when the agent is root; the binary is put there.
	Root string
	// Port is 53; 0 picks one, the same for UDP and TCP of an address.
	Port int
	// Addresses lists where to answer; nil is ListenAddresses.
	Addresses func() []netip.Addr
	// PIDFile names the running process, so the next run of the agent ends one an earlier run left behind.
	PIDFile string
	// GeoPath is the location table, handed to every process started, open for reading only; none when missing.
	GeoPath string
	Logf    func(string, ...any)
}

type socket struct {
	port     int
	udp, tcp *os.File
}

func (s *socket) close() {
	_ = s.udp.Close()
	_ = s.tcp.Close()
}

// Supervisor keeps the name server process running on sockets the agent bound, and hands it every new table.
type Supervisor struct {
	cfg     Config
	mu      sync.Mutex
	socks   map[netip.Addr]*socket
	problem map[netip.Addr]string
	table   []byte
	// gen counts the tables handed in, applied the newest the process acknowledged; served closes at each ack.
	gen, applied uint64
	served       chan struct{}
	pid          int
	noNS         bool
	notify       chan struct{}
	restart      chan struct{}
	stop         chan struct{}
	done         chan struct{}
	once         sync.Once
}

// Start binds the sockets and starts the process; it keeps it running until Close.
func Start(cfg Config) *Supervisor {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Addresses == nil {
		cfg.Addresses = ListenAddresses
	}
	s := &Supervisor{cfg: cfg, socks: map[netip.Addr]*socket{}, problem: map[netip.Addr]string{},
		served: make(chan struct{}), notify: make(chan struct{}, 1), restart: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	// After a self-update the process the earlier run started still holds port 53: end it before binding.
	if pid := leftBehind(cfg.PIDFile); pid > 0 {
		endLeftBehind(pid)
		cfg.Logf("dns: ended the name server an earlier run of the agent left (pid %d)", pid)
	}
	s.Rebind()
	go s.run()
	return s
}

// Rebind answers on the addresses the host has now: a new one is bound, a gone one closed, a taken one tried again.
func (s *Supervisor) Rebind() {
	want := map[netip.Addr]bool{}
	for _, a := range s.cfg.Addresses() {
		want[a.Unmap()] = true
	}
	s.mu.Lock()
	changed := false
	for a := range want {
		if s.socks[a] != nil {
			continue
		}
		sock, err := bind(a, s.cfg.Port)
		if err != nil {
			if errors.Is(err, syscall.EADDRNOTAVAIL) {
				continue
			}
			s.problem[a] = bindProblem(a, s.cfg.Port, err)
			continue
		}
		delete(s.problem, a)
		s.socks[a] = sock
		changed = true
	}
	for a, sock := range s.socks {
		if !want[a] {
			sock.close()
			delete(s.socks, a)
			changed = true
		}
	}
	for a := range s.problem {
		if !want[a] {
			delete(s.problem, a)
		}
	}
	s.mu.Unlock()
	if changed {
		signal(s.restart)
	}
}

func bind(a netip.Addr, port int) (*socket, error) {
	for try := 1; ; try++ {
		s, err := bindOnce(a, port)
		// Any port (tests): the one UDP got may be taken for TCP, so another one is tried.
		if port != 0 || try == 8 || !errors.Is(err, syscall.EADDRINUSE) {
			return s, err
		}
	}
}

// listenTCP is net.ListenTCP; a test makes it find a port taken.
var listenTCP = net.ListenTCP

func bindOnce(a netip.Addr, port int) (*socket, error) {
	uc, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.AddrPortFrom(a, uint16(port))))
	if err != nil {
		return nil, err
	}
	defer uc.Close()
	port = uc.LocalAddr().(*net.UDPAddr).Port
	tl, err := listenTCP("tcp", net.TCPAddrFromAddrPort(netip.AddrPortFrom(a, uint16(port))))
	if err != nil {
		return nil, err
	}
	defer tl.Close()
	uf, err := uc.File()
	if err != nil {
		return nil, err
	}
	tf, err := tl.File()
	if err != nil {
		uf.Close()
		return nil, err
	}
	return &socket{port: port, udp: uf, tcp: tf}, nil
}

func bindProblem(a netip.Addr, port int, err error) string {
	switch {
	case errors.Is(err, syscall.EADDRINUSE):
		return fmt.Sprintf("port %d on %s is taken by another program", port, a)
	case errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return fmt.Sprintf("the server agent may not open port %d on %s", port, a)
	}
	return fmt.Sprintf("port %d on %s: %v", port, a, err)
}

// Listening is every address answered on, by "address:port", and why any other one is not.
func (s *Supervisor) Listening() (addrs []string, problem string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for a, sock := range s.socks {
		addrs = append(addrs, netip.AddrPortFrom(a, uint16(sock.port)).String())
	}
	var problems []string
	for _, p := range s.problem {
		problems = append(problems, p)
	}
	sort.Strings(addrs)
	sort.Strings(problems)
	return addrs, strings.Join(problems, "; ")
}

// PID is the running process, or 0.
func (s *Supervisor) PID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pid
}

// SetTable hands the process a new table now, and every process started after.
func (s *Supervisor) SetTable(t *Table) error {
	if _, err := compile(t); err != nil {
		return err
	}
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if len(raw) >= MaxTableBytes {
		return fmt.Errorf("a table of %d bytes", len(raw))
	}
	s.mu.Lock()
	s.table = raw
	s.gen++
	s.mu.Unlock()
	signal(s.notify)
	return nil
}

// Served waits until the process answers from the newest table handed in, or ctx ends.
func (s *Supervisor) Served(ctx context.Context) error {
	s.mu.Lock()
	want := s.gen
	for s.applied < want {
		ch := s.served
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		s.mu.Lock()
	}
	s.mu.Unlock()
	return nil
}

// Reload starts the process again, so it reads the location table anew; the sockets stay open meanwhile.
func (s *Supervisor) Reload() { signal(s.restart) }

func (s *Supervisor) openGeo() *os.File {
	if s.cfg.GeoPath == "" {
		return nil
	}
	f, err := os.Open(s.cfg.GeoPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			s.cfg.Logf("dns: the location table cannot be read (%v); every healthy server is answered", err)
		}
		return nil
	}
	return f
}

// Close stops the process and closes every socket.
func (s *Supervisor) Close() {
	s.once.Do(func() { close(s.stop) })
	<-s.done
	s.mu.Lock()
	for a, sock := range s.socks {
		sock.close()
		delete(s.socks, a)
	}
	s.mu.Unlock()
}

func signal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func (s *Supervisor) run() {
	defer close(s.done)
	backoff := time.Second
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		began := time.Now()
		restarted, err := s.runOnce()
		select {
		case <-s.stop:
			return
		default:
		}
		if restarted {
			continue
		}
		if time.Since(began) > time.Minute {
			backoff = time.Second
		}
		s.cfg.Logf("dns: the name server stopped (%v); starting it again in %v", err, backoff)
		select {
		case <-s.stop:
			return
		case <-s.restart:
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

// runOnce runs one process until it exits, or until it is replaced (restarted) or stopped.
func (s *Supervisor) runOnce() (restarted bool, err error) {
	s.mu.Lock()
	addrs := make([]netip.Addr, 0, len(s.socks))
	for a := range s.socks {
		addrs = append(addrs, a)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].Less(addrs[j]) })
	var files []*os.File
	var kinds []string
	if geo := s.openGeo(); geo != nil {
		// The process holds it only open for reading: it can read the table and nothing else of the agent's directory.
		defer geo.Close()
		files, kinds = append(files, geo), append(kinds, "g")
	}
	for _, a := range addrs {
		files = append(files, s.socks[a].udp, s.socks[a].tcp)
		kinds = append(kinds, "u", "t")
	}
	s.mu.Unlock()
	if len(addrs) == 0 {
		select {
		case <-s.stop:
		case <-s.restart:
		}
		return true, nil
	}
	path, err := s.prepareRoot()
	if err != nil {
		return false, err
	}
	cmd, stdin, acks, stderr, err := s.startProcess(path, files, strings.Join(kinds, ","))
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	s.pid = cmd.Process.Pid
	s.mu.Unlock()
	if s.cfg.PIDFile != "" {
		_ = os.WriteFile(s.cfg.PIDFile, []byte(fmt.Sprint(cmd.Process.Pid)), 0o600)
	}
	defer func() {
		s.mu.Lock()
		s.pid = 0
		s.mu.Unlock()
		if s.cfg.PIDFile != "" {
			_ = os.Remove(s.cfg.PIDFile)
		}
	}()

	go s.forward(stderr)
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	writeDone := make(chan struct{})
	defer close(writeDone)
	go s.feed(stdin, acks, cmd, writeDone)

	select {
	case err := <-exited:
		return false, err
	case <-s.stop:
	case <-s.restart:
		restarted = true
	}
	_ = cmd.Process.Kill()
	<-exited
	return restarted, nil
}

func (s *Supervisor) startProcess(path string, files []*os.File, kinds string) (*exec.Cmd, io.WriteCloser, *os.File, io.ReadCloser, error) {
	for {
		cmd := &exec.Cmd{
			Path:        path,
			Args:        append([]string{"deplo-agent"}, s.cfg.Args...),
			Env:         append(append([]string{}, s.cfg.Env...), EnvSockets+"="+kinds),
			ExtraFiles:  files,
			SysProcAttr: procAttr(s.cfg.Root, !s.noNS),
		}
		// An os.Pipe of our own, not StdinPipe: its write end takes a deadline, so a stuck process cannot hold the agent.
		r, stdin, err := os.Pipe()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		acks, ackW, err := os.Pipe()
		if err != nil {
			r.Close()
			stdin.Close()
			return nil, nil, nil, nil, err
		}
		cmd.Stdin, cmd.Stdout = r, ackW
		stderr, err := cmd.StderrPipe()
		if err == nil {
			err = cmd.Start()
		}
		r.Close()
		ackW.Close()
		if err == nil {
			return cmd, stdin, acks, stderr, nil
		}
		stdin.Close()
		acks.Close()
		if s.noNS || cmd.SysProcAttr.Cloneflags == 0 || !namespaceRefused(err) {
			return nil, nil, nil, nil, err
		}
		s.noNS = true
		s.cfg.Logf("dns: this host gives the name server no network of its own (%v); it still runs as nobody, locked in an empty directory", err)
	}
}

// feed writes the latest table to the process at start and on every change; a process that stops reading or
// acknowledging is killed.
func (s *Supervisor) feed(stdin io.WriteCloser, acks *os.File, cmd *exec.Cmd, done <-chan struct{}) {
	defer stdin.Close()
	defer acks.Close()
	ack := bufio.NewReader(acks)
	f, _ := stdin.(*os.File)
	for first := true; ; first = false {
		if !first {
			select {
			case <-done:
				return
			case <-s.notify:
			}
		}
		s.mu.Lock()
		raw, gen := s.table, s.gen
		s.mu.Unlock()
		if raw == nil {
			continue
		}
		if f != nil {
			_ = f.SetWriteDeadline(time.Now().Add(5 * time.Second))
		}
		_ = acks.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err := stdin.Write(append(append([]byte{}, raw...), '\n'))
		if err == nil {
			_, err = ack.ReadString('\n')
		}
		if err != nil {
			s.cfg.Logf("dns: the name server took no table (%v); it is started again", err)
			_ = cmd.Process.Kill()
			return
		}
		s.mu.Lock()
		if gen > s.applied {
			s.applied = gen
			close(s.served)
			s.served = make(chan struct{})
		}
		s.mu.Unlock()
	}
}

// forward passes what the process says on to the agent's log.
func (s *Supervisor) forward(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 4096), 4096)
	for sc.Scan() {
		line := sc.Text()
		if len(line) > 512 {
			line = line[:512]
		}
		s.cfg.Logf("dns: %s", line)
	}
}

// prepareRoot puts the binary alone in the process's root and says where it is from inside; no root when not root.
func (s *Supervisor) prepareRoot() (string, error) {
	if os.Geteuid() != 0 || s.cfg.Root == "" {
		return s.cfg.Path, nil
	}
	if err := os.MkdirAll(s.cfg.Root, 0o755); err != nil {
		return "", err
	}
	if err := os.Chmod(s.cfg.Root, 0o755); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(s.cfg.Root)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(s.cfg.Root, e.Name())); err != nil {
			return "", err
		}
	}
	dst := filepath.Join(s.cfg.Root, "deplo-agent")
	if err := os.Link(s.cfg.Path, dst); err != nil {
		if err := copyFile(s.cfg.Path, dst); err != nil {
			if err := copyFile("/proc/self/exe", dst); err != nil {
				return "", fmt.Errorf("put the name server in %s: %v", s.cfg.Root, err)
			}
		}
	}
	return "/deplo-agent", nil
}

// leftBehind is the name server an earlier run of this process started, when it is still this process's child.
func leftBehind(pidFile string) int {
	if pidFile == "" {
		return 0
	}
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		return 0
	}
	var pid int
	if _, err := fmt.Sscan(strings.TrimSpace(string(raw)), &pid); err != nil || pid <= 1 {
		return 0
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	// pid (comm) state ppid ...: comm may hold spaces, so read past its closing parenthesis.
	rest := string(stat)
	if i := strings.LastIndexByte(rest, ')'); i >= 0 {
		rest = rest[i+1:]
	}
	f := strings.Fields(rest)
	if len(f) < 2 || f[1] != fmt.Sprint(os.Getpid()) {
		return 0
	}
	return pid
}

// EndLeftBehind ends the name server an earlier run of this agent started, before its zones are set aside.
func EndLeftBehind(pidFile string) {
	if pid := leftBehind(pidFile); pid > 0 {
		endLeftBehind(pid)
		_ = os.Remove(pidFile)
	}
}

func endLeftBehind(pid int) {
	_ = syscall.Kill(pid, syscall.SIGKILL)
	var ws syscall.WaitStatus
	_, _ = syscall.Wait4(pid, &ws, 0, nil)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
