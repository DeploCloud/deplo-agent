package main

import "testing"

func TestLoopbackAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:9443": true,
		"[::1]:9443":     true,
		"localhost:9443": true,
		":9443":          false,
		"0.0.0.0:9443":   false,
		"10.0.0.5:9443":  false,
		"[::]:9443":      false,
		"127.0.0.1":      false,
	} {
		if got := loopbackAddr(addr); got != want {
			t.Errorf("loopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}
