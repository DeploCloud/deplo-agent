package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/forwarder"
)

// While this server's proxy restarts, its ports 80 and 443 lead to the agent, which holds each visitor and hands it
// to the new proxy once that routes: a restart turns no visitor away (c41: every App unanswered 8-10 s).

var (
	// holdRedirect sends this server's own ports to the agent's (front port -> hold port) until undo; a var so tests
	// redirect nothing.
	holdRedirect = redirectFrontDoor
	// frontDoorDial reaches the proxy's own port inside its network.
	frontDoorDial = func(ctx context.Context, port uint32) (net.Conn, error) { return dialProxyLoopback(ctx, port) }
	// frontDoorWait is how long a release waits for the new proxy, holdSettle how long it lets it read its routes.
	frontDoorWait = 60 * time.Second
	holdSettle    = 500 * time.Millisecond
	// holdLinger keeps the hold's listeners a moment past the redirect's end, for visitors already on their way.
	holdLinger = time.Second
)

var frontDoorPorts = []uint32{80, 443}

type frontHold struct {
	mu   sync.Mutex
	held []heldVisitor
	open bool
}

type heldVisitor struct {
	c    net.Conn
	port uint32
}

// holdFrontDoor starts holding the visitors of this server's ports; release, after the new proxy is up, hands them
// over. A hold that could not start is said once and costs nothing: visitors are turned away, as before.
func holdFrontDoor() (release func()) {
	h := &frontHold{open: true}
	redirect := map[uint16]uint16{}
	var lns []net.Listener
	for _, port := range frontDoorPorts {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: could not hold visitors while the proxy restarts: %v\n", err)
			closeAll(lns)
			return func() {}
		}
		lns = append(lns, ln)
		redirect[uint16(port)] = uint16(ln.Addr().(*net.TCPAddr).Port)
		go h.accept(ln, port)
	}
	undo, err := holdRedirect(redirect)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not hold visitors while the proxy restarts: %v\n", err)
		closeAll(lns)
		return func() {}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			h.release(undo, lns)
		})
	}
}

func (h *frontHold) accept(ln net.Listener, port uint32) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		h.mu.Lock()
		if h.open {
			h.held = append(h.held, heldVisitor{c, port})
			h.mu.Unlock()
			continue
		}
		h.mu.Unlock()
		go handOver(c, port)
	}
}

// release waits for the new proxy's front door and its routes, ends the redirect, then hands over every visitor held.
func (h *frontHold) release(undo func(), lns []net.Listener) {
	ctx, cancel := context.WithTimeout(context.Background(), frontDoorWait)
	defer cancel()
	for ctx.Err() == nil {
		if c, err := frontDoorDial(ctx, 443); err == nil {
			c.Close()
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	time.Sleep(holdSettle)
	undo()
	h.mu.Lock()
	h.open = false
	held := h.held
	h.held = nil
	h.mu.Unlock()
	for _, v := range held {
		go handOver(v.c, v.port)
	}
	time.AfterFunc(holdLinger, func() { closeAll(lns) })
}

func handOver(c net.Conn, port uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	up, err := frontDoorDial(ctx, port)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: a visitor held on port %s while the proxy restarted could not reach it: %v\n", strconv.Itoa(int(port)), err)
		c.Close()
		return
	}
	forwarder.Join(c, up)
}

// ClearFrontDoorHold ends what an agent that stopped mid-restart left holding this server's ports.
func ClearFrontDoorHold() { clearFrontDoorHold() }

func closeAll(lns []net.Listener) {
	for _, ln := range lns {
		ln.Close()
	}
}
