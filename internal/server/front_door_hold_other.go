//go:build !linux

package server

import "errors"

func redirectFrontDoor(map[uint16]uint16) (stop, drop func(), err error) {
	return nil, nil, errors.New("holding visitors needs Linux")
}

func clearFrontDoorHold() {}
