package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// bundleJSON reads a message a newer control plane wrote: a field this agent does not know is skipped.
var bundleJSON = protojson.UnmarshalOptions{DiscardUnknown: true}

// runFailoverDeploy runs one deploy request to its result; a var so a test can answer without Docker.
var runFailoverDeploy = func(s *Service, ctx context.Context, req *pb.DeployRequest, e *emitter) {
	s.runDeploy(ctx, req, e)
}

// runFailoverRestore restores one backup the way the Restore RPC does; a var so a test can answer without Docker.
var runFailoverRestore func(s *Service, ctx context.Context, req *pb.RestoreRequest) error

// Set here, not in the declaration: a restore reroutes, and Reroute reads the lease guard that starts this.
func init() { runFailoverRestore = restoreLikeTheRPC }

func restoreLikeTheRPC(s *Service, ctx context.Context, req *pb.RestoreRequest) error {
	src, err := sourceFromRestore(s, req)
	if err != nil {
		return errors.New(statusMessage(err))
	}
	// The stack is started from its bundle, which is newer than the one the backup holds.
	src.dataOnly = restoresBeforeStart(req)
	var res *pb.RestoreResult
	e := &rsEmitter{send: func(ev *pb.RestoreEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return nil
	}}
	s.restoreArtifact(ctx, req.GetKind(), req.GetDatabase(), req.GetProject(), src, e)
	switch {
	case res == nil:
		return errors.New("the restore ended without a result")
	case !res.GetOk():
		return errors.New(res.GetError())
	}
	return nil
}

// stopFailoverStack stops a stack whose restore failed, so it never serves empty; a var for the same reason.
var stopFailoverStack = func(s *Service, ctx context.Context, slug string) {
	if _, err := s.StopStack(ctx, &pb.StackRef{Slug: slug}); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not stop %s after its restore failed: %v\n", slug, err)
	}
}

// ponytail: a database is up before its engine takes a restore, so a failed one is tried again for half a
// minute. Wait on the engine itself if a big artifact makes a retry expensive.
var (
	failoverRestoreTries = 6
	failoverRestoreWait  = 5 * time.Second
)

// failoverRestore reads the restore a bundle asks for, before anything starts: an unreadable one starts nothing.
func failoverRestore(st *lease.Start) (*pb.RestoreRequest, error) {
	if st.Restore == nil {
		return nil, nil
	}
	req := &pb.RestoreRequest{}
	if err := bundleJSON.Unmarshal(st.Restore.Request, req); err != nil {
		return nil, fmt.Errorf("the bundle's restore is unreadable: %w", err)
	}
	switch req.GetKind() {
	case pb.BackupKind_BACKUP_KIND_DATABASE, pb.BackupKind_BACKUP_KIND_PROJECT:
		return req, nil
	}
	return nil, errors.New("this server cannot restore that kind of backup on a failover")
}

// restoresBeforeStart is an App's backup: its Volumes and Files are put back before its stack starts, so it
// never runs on empty ones. A database is the other way round, restored into its running engine.
func restoresBeforeStart(req *pb.RestoreRequest) bool {
	return req.GetKind() == pb.BackupKind_BACKUP_KIND_PROJECT
}

// restoreAfterStart restores the bundle's backup into the stack that just came up, and stops it when that fails.
func (s *Service) restoreAfterStart(ctx context.Context, slug string, req *pb.RestoreRequest) error {
	var err error
	for try := 0; try < failoverRestoreTries && ctx.Err() == nil; try++ {
		if try > 0 {
			time.Sleep(failoverRestoreWait)
		}
		if err = runFailoverRestore(s, ctx, req); err == nil {
			return nil
		}
	}
	fmt.Fprintf(os.Stderr, "deplo-agent: failover restore of %s failed: %v\n", slug, err)
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stopFailoverStack(s, stopCtx, slug)
	return err
}

// startFromBundle brings a moved workload up through the same stack deploy a deploy from the control plane
// takes: the image is already here (copied or pulled by compose), so nothing is built. A stack that never
// becomes ready is an error, and the leader tries another member.
func (s *Service) startFromBundle(ctx context.Context, st *lease.Start) error {
	if st.Slug == "" || st.ComposeYAML == "" {
		return errors.New("the bundle holds no stack to start")
	}
	restore, err := failoverRestore(st)
	if err != nil {
		return err
	}
	first := restore != nil && restoresBeforeStart(restore)
	if first {
		if err := runFailoverRestore(s, ctx, restore); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: failover restore of %s failed: %v\n", st.Slug, err)
			return err
		}
	}
	if err := promoteStandby(ctx, st); err != nil {
		return err
	}
	req := &pb.DeployRequest{
		DeployId:         fmt.Sprintf("failover-%s-%d", st.DeploymentID, time.Now().Unix()),
		Slug:             st.Slug,
		ProjectId:        st.ProjectID,
		ImageRef:         st.ImageRef,
		SourceKind:       pb.SourceKind_SOURCE_KIND_IMAGE,
		ComposeYaml:      st.ComposeYAML,
		Env:              st.Env,
		Network:          st.Network,
		ComposeUpArgs:    st.ComposeUpArgs,
		StartAfterReady:  st.StartAfterReady,
		FilesBindSources: st.FilesBindSources,
	}
	// A stack with no image of its own (a database) is a compose stack: its images are pulled by the names it gives.
	if st.ImageRef == "" {
		req.SourceKind = pb.SourceKind_SOURCE_KIND_COMPOSE
	}
	for _, m := range st.Mounts {
		req.Mounts = append(req.Mounts, &pb.MountFile{Path: m.Path, Content: m.Content})
	}
	// Both arrive as the deploy's own messages, so a moved App is checked and restarted exactly as before.
	if len(st.Ready) > 0 {
		req.Ready = &pb.ReadyCheck{}
		if err := bundleJSON.Unmarshal(st.Ready, req.Ready); err != nil {
			return fmt.Errorf("the bundle's ready check is unreadable: %w", err)
		}
	}
	if len(st.RestartRule) > 0 {
		req.RestartRule = &pb.RestartRule{}
		if err := bundleJSON.Unmarshal(st.RestartRule, req.RestartRule); err != nil {
			return fmt.Errorf("the bundle's restart rule is unreadable: %w", err)
		}
	}
	for _, a := range st.RegistryAuth {
		req.RegistryAuth = append(req.RegistryAuth, &pb.RegistryAuth{Host: a.Host, Username: a.Username, Password: a.Password})
	}
	var result *pb.DeployResult
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			result = r
		}
		return nil
	}}
	runFailoverDeploy(s, ctx, req, e)
	if result == nil {
		return errors.New("the deploy ended without a result")
	}
	if !result.GetReady() {
		msg := result.GetError()
		if msg == "" {
			msg = "the stack did not become ready"
		}
		fmt.Fprintf(os.Stderr, "deplo-agent: failover start of %s failed: %s\n", st.Slug, msg)
		if first {
			// The leader tries another member: a copy that half started on restored data must not stay up here.
			stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			stopFailoverStack(s, stopCtx, st.Slug)
		}
		return errors.New(msg)
	}
	if restore != nil && !first {
		return s.restoreAfterStart(ctx, st.Slug, restore)
	}
	return nil
}
