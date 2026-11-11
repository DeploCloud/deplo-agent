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

// startFromBundle brings a moved workload up through the same stack deploy a deploy from the control plane
// takes: the image is already here (copied or pulled by compose), so nothing is built. A stack that never
// becomes ready is an error, and the leader tries another member.
func (s *Service) startFromBundle(ctx context.Context, st *lease.Start) error {
	if st.Slug == "" || st.ComposeYAML == "" {
		return errors.New("the bundle holds no stack to start")
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
		return errors.New(msg)
	}
	return nil
}
