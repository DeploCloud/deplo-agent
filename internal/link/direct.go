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

// Direct is the agent's own port, which the panel dials until the agent link replaces it (ADR-0040).
type Direct struct {
	net.Listener
	agentDir string

	mu      sync.Mutex
	stopped bool
	closed  chan struct{}
	once    sync.Once
}

// NewDirect serves lis until Stop closes it for good, remembered under agentDir.
func NewDirect(lis net.Listener, agentDir string) *Direct {
	return &Direct{Listener: lis, agentDir: agentDir, closed: make(chan struct{})}
}

// Accept hands over each connection; once stopped it waits for Close, so serving goes on over the link.
func (d *Direct) Accept() (net.Conn, error) {
	c, err := d.Listener.Accept()
	if err != nil && !d.Listening() {
		<-d.closed
		return nil, net.ErrClosed
	}
	return c, err
}

func (d *Direct) Close() error {
	d.once.Do(func() { close(d.closed) })
	return d.Listener.Close()
}

// Listening reports whether the port is still open; a nil Direct is an agent with none.
func (d *Direct) Listening() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.stopped
}

// Stop saves that the agent no longer listens, then closes the port; connections already made carry on.
func (d *Direct) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return nil
	}
	if d.agentDir == "" {
		return errors.New("no agent dir to remember a closed port in")
	}
	if err := os.WriteFile(NoListenerPath(d.agentDir), nil, 0o600); err != nil {
		return err
	}
	d.stopped = true
	if err := d.Listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}
