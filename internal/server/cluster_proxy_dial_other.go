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
