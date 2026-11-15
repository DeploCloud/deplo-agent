//go:build !linux

package server

import "errors"

// redirectAllPorts needs Linux; elsewhere a name answers on its own port only.
func redirectAllPorts(int, uint32) error { return errors.New("redirecting ports needs Linux") }
