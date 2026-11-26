//go:build !linux

package replica

import (
	"context"
	"errors"
	"io"
)

var errLinuxOnly = errors.New("replication needs a Linux server")

func Send(context.Context, io.ReadWriter, []string, []string) error { return errLinuxOnly }

func SendOnce(context.Context, io.ReadWriter, []string, []string) error { return errLinuxOnly }

func Receive(context.Context, io.ReadWriter, string, []string, *Status, *Index) error {
	return errLinuxOnly
}

func ReceiveFirst(context.Context, io.ReadWriter, string, []string, *Status, *Index) error {
	return errLinuxOnly
}

func Promote(string, []string, map[string]string) error { return errLinuxOnly }
