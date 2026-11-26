//go:build !linux

package server

import (
	"context"
	"errors"
)

// TakePriority needs Linux and systemd; elsewhere the agent keeps its defaults.
func TakePriority(context.Context) {}

func helperPriority() []string { return []string{"--cpu-shares=2"} }

func atLowPriority(fn func() error) error { return fn() }

func EnsureRestart(context.Context) {}

var startGuard = func(prev, exe, version, answered string) error { return errors.New("no systemd") }

func UpdateGuard([]string) int { return 2 }

func proxyFirst(context.Context, string) {}

func proxyAside(context.Context, string) {}
