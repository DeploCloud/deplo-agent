//go:build !linux

package dnszone

import "syscall"

func lockDown() error { return nil }

// procAttr drops nothing off Linux; a process started as root there refuses to run (Main).
func procAttr(string, bool) *syscall.SysProcAttr { return &syscall.SysProcAttr{} }

func namespaceRefused(error) bool { return false }
