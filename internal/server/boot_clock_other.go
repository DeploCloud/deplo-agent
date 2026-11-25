//go:build !linux

package server

import "time"

func bootClock() time.Time { return time.Now() }
