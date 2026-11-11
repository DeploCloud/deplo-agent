package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A standby copy of a database (spec #176) is a stack like any other until its leader has it promoted: the
// control plane renders the engine's own command, this only runs it inside the copy's container.

// promoteTimeout bounds the engine's promote command.
const promoteTimeout = 2 * time.Minute

// runFailoverPromote runs the promote command in the copy's container; a var so a test can answer without Docker.
var runFailoverPromote = func(ctx context.Context, projectID string, p *lease.Promote) error {
	// The container must be the workload's own, so an order cannot run a command anywhere else.
	if err := assertOwned(ctx, p.Container, projectID); err != nil {
		return errors.New(statusMessage(err))
	}
	args := []string{"exec"}
	if p.User != "" {
		args = append(args, "-u", p.User)
	}
	res, err := dockercli.Run(ctx, promoteTimeout, append(append(args, p.Container), p.Command...)...)
	if err != nil {
		return err
	}
	if res.Code != 0 {
		return fmt.Errorf("the standby copy could not be promoted: %s", oneLine(res.Stderr+" "+res.Stdout))
	}
	return nil
}

// promoteStandby makes the copy this member holds the workload, before its stack is started over the same data.
func promoteStandby(ctx context.Context, st *lease.Start) error {
	p := st.Promote
	if p == nil {
		return nil
	}
	if p.Container == "" || len(p.Command) == 0 {
		return errors.New("the bundle does not say how to promote the standby copy")
	}
	return runFailoverPromote(ctx, st.ProjectID, p)
}
