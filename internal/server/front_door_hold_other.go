//go:build !linux

package server

import "errors"

func redirectFrontDoor(map[uint16]uint16) (func(), error) {
	return nil, errors.New("holding visitors needs Linux")
}

func clearFrontDoorHold() {}
