//go:build !linux

package logpipe

// Unblock needs Linux; elsewhere the log is written as it is.
func Unblock(int) error { return nil }
