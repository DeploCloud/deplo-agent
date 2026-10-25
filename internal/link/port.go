package link

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
)

// NoListenerPath marks an agent that stopped listening once its link worked; it overrides --addr.
func NoListenerPath(agentDir string) string {
	return filepath.Join(agentDir, "no-listener")
}

// ListenerStopped reports whether the agent under agentDir closed its own port for good.
func ListenerStopped(agentDir string) bool {
	_, err := os.Stat(NoListenerPath(agentDir))
	return err == nil
}

// Port is the agent's own port, which the panel dials until the agent link replaces it (ADR-0040).
type Port struct {
	net.Listener
	agentDir string

	mu      sync.Mutex
	stopped bool
	closed  chan struct{}
	once    sync.Once
}

// NewPort serves lis until Stop closes it for good, remembered under agentDir.
func NewPort(lis net.Listener, agentDir string) *Port {
	return &Port{Listener: lis, agentDir: agentDir, closed: make(chan struct{})}
}

// Accept hands over each connection; once stopped it waits for Close, so serving goes on over the link.
func (p *Port) Accept() (net.Conn, error) {
	c, err := p.Listener.Accept()
	if err != nil && !p.Listening() {
		<-p.closed
		return nil, net.ErrClosed
	}
	return c, err
}

func (p *Port) Close() error {
	p.once.Do(func() { close(p.closed) })
	return p.Listener.Close()
}

// Listening reports whether the port is still open; a nil Port is an agent with none.
func (p *Port) Listening() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.stopped
}

// Stop saves that the agent no longer listens, then closes the port; connections already made carry on.
func (p *Port) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil
	}
	if p.agentDir == "" {
		return errors.New("no agent dir to remember a closed port in")
	}
	if err := os.WriteFile(NoListenerPath(p.agentDir), nil, 0o600); err != nil {
		return err
	}
	p.stopped = true
	if err := p.Listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}
