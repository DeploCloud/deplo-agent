//go:build !linux

package server

import (
	"context"
	"errors"
	"net"
)

func dialInNetwork(context.Context, int, string) (net.Conn, error) {
	return nil, errors.New("reaching the proxy's own network needs Linux")
}

func listenInNetwork(int, string) (net.Listener, error) {
	return nil, errors.New("listening in the proxy's own network needs Linux")
}
