//go:build !linux

package logpipe

// Unblock needs Linux; elsewhere the log is written as it is.
func Unblock(int) error { return nil }

// Restore has nothing to put back elsewhere.
func Restore() {}

// Revive needs Linux; elsewhere nothing is changed.
func Revive(int, string) bool { return false }
