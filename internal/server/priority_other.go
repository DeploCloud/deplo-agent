//go:build !linux

package server

import "context"

// TakePriority needs Linux and systemd; elsewhere the agent keeps its defaults.
func TakePriority(context.Context) {}

func helperPriority() []string { return []string{"--cpu-shares=2"} }

func atLowPriority(fn func() error) error { return fn() }
