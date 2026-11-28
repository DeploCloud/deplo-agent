//go:build !linux

package dockercli

import "syscall"

// UseCgroup is Linux's: elsewhere every child runs beside the agent.
func UseCgroup(string) bool { return false }

func intoCgroup(*syscall.SysProcAttr) {}
