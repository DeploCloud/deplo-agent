package server

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A member copies a built image to the other members of its cluster over the relay, mutual TLS end to end (ADR-0044).
// Stream: "IMAGE <ref> <keep>", the target answers HAVE, SEND or ERR, then the gzipped archive, then PRUNED lines and OK or ERR.
const (
	imageVerb   = "IMAGE"
	imageHave   = "HAVE"
	imageSend   = "SEND"
	imagePruned = "PRUNED"
	defaultKeep = 2
	maxKeep     = 20
)

// clusterImageOps is what an image copy asks of docker.
type clusterImageOps struct {
	has    func(ctx context.Context, ref string) (bool, error)
	save   func(ctx context.Context, ref string, w io.Writer) error
	load   func(ctx context.Context, ref string, feed func(w io.Writer) error) (int64, string, error)
	tags   func(ctx context.Context, repo string) ([]string, error)
	remove func(ctx context.Context, ref string) error
}

var dockerImageOps = clusterImageOps{imageHeld, saveImage, loadImageArchive, imageTagsOf, removeImage}

func (s *Service) images() clusterImageOps {
	if s.imageOps.has != nil {
		return s.imageOps
	}
	return dockerImageOps
}

func imageHeld(ctx context.Context, ref string) (bool, error) {
	res, err := dockercli.Run(ctx, imageRemoveTimeout, "image", "inspect", "--format", "{{.Id}}", ref)
	if err != nil {
		return false, err
	}
	return res.Code == 0, nil
}

func saveImage(ctx context.Context, ref string, w io.Writer) error {
	code, err := dockercli.PipeOut(ctx, imageCopyTimeout, w, nil, "save", ref)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("docker save exited %d", code)
	}
	return nil
}

// imageTagsOf lists the tags of one repository, newest first (docker's own order).
func imageTagsOf(ctx context.Context, repo string) ([]string, error) {
	res, err := dockercli.Run(ctx, imageRemoveTimeout, "image", "ls", "--filter", "reference="+repo, "--format", "{{.Repository}}:{{.Tag}}")
	if err != nil {
		return nil, err
	}
	if res.Code != 0 {
		return nil, fmt.Errorf("docker image ls exited %d", res.Code)
	}
	var out []string
	for _, line := range strings.Split(res.Stdout, "\n") {
		tag := strings.TrimSpace(line)
		if tag != "" && !strings.Contains(tag, "<none>") && repoOf(tag) == repo {
			out = append(out, tag)
		}
	}
	return out, nil
}

func removeImage(ctx context.Context, ref string) error {
	res, err := dockercli.Run(ctx, imageRemoveTimeout, "rmi", ref)
	if err != nil {
		return err
	}
	if res.Code != 0 {
		return fmt.Errorf("docker rmi exited %d", res.Code)
	}
	return nil
}

// pruneReleases keeps ref and the newest others up to `keep` releases of its App; one in use stays and is not reported.
func (s *Service) pruneReleases(ctx context.Context, ref string, keep int) []string {
	tags, err := s.images().tags(ctx, repoOf(ref))
	if err != nil {
		return nil
	}
	kept := 1
	var gone []string
	for _, t := range tags {
		if t == ref {
			continue
		}
		if kept < keep {
			kept++
			continue
		}
		if s.images().remove(ctx, t) == nil {
			gone = append(gone, t)
		}
	}
	return gone
}

// acceptMemberStream serves what another member of the cluster opened toward this agent.
func (s *Service) acceptMemberStream(from wire.Party, c net.Conn) {
	defer c.Close()
	if from.Role != wire.RoleMember {
		return
	}
	_ = c.SetReadDeadline(time.Now().Add(wire.HeaderTimeout))
	verb, arg, err := wire.ReadHeader(c)
	if err != nil {
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	switch verb {
	case imageVerb:
		s.receiveImage(c, arg)
	case fwdVerb:
		s.serveForward(from, c, arg)
	}
}

func (s *Service) receiveImage(c net.Conn, arg string) {
	refArg, keepArg, _ := strings.Cut(arg, " ")
	keep, kerr := strconv.Atoi(keepArg)
	if err := validateImageRef(refArg); err != nil || kerr != nil || keep < 1 || keep > maxKeep {
		_ = wire.WriteHeader(c, wire.VerbErr, "not an image this member accepts")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), imageCopyTimeout)
	defer cancel()
	_ = c.SetDeadline(time.Now().Add(imageCopyTimeout))
	if held, _ := s.images().has(ctx, refArg); held {
		_ = wire.WriteHeader(c, imageHave, "")
		return
	}
	if wire.WriteHeader(c, imageSend, "") != nil {
		return
	}
	n, _, err := s.images().load(ctx, refArg, func(w io.Writer) error {
		_, cerr := io.Copy(w, c)
		return cerr
	})
	if err != nil {
		_ = wire.WriteHeader(c, wire.VerbErr, oneLine(err.Error()))
		return
	}
	for _, p := range s.pruneReleases(ctx, refArg, keep) {
		_ = wire.WriteHeader(c, imagePruned, p)
	}
	_ = wire.WriteHeader(c, wire.VerbOK, strconv.FormatInt(n, 10))
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

// CopyClusterImage sends a built image to one member of this agent's cluster; see the RPC's contract in proto/agent.proto.
func (s *Service) CopyClusterImage(ctx context.Context, req *pb.CopyClusterImageRequest) (*pb.CopyClusterImageResponse, error) {
	ref := req.GetImageRef()
	if err := validateImageRef(ref); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	to := wire.Party{Fingerprint: strings.ToLower(req.GetTargetFingerprint()), Role: wire.RoleMember}
	if !to.Valid() {
		return nil, status.Error(codes.InvalidArgument, "the target is not a certificate fingerprint")
	}
	keep := int(req.GetKeep())
	if keep == 0 {
		keep = defaultKeep
	}
	if keep < 1 || keep > maxKeep {
		return nil, status.Errorf(codes.InvalidArgument, "keep must be between 1 and %d", maxKeep)
	}
	m := s.clusterMember()
	m.mu.Lock()
	party := m.party
	m.mu.Unlock()
	if party == nil {
		return nil, status.Error(codes.FailedPrecondition, "this agent is not a cluster member")
	}
	if held, err := s.images().has(ctx, ref); err != nil || !held {
		return nil, status.Errorf(codes.FailedPrecondition, "this server does not hold %s", ref)
	}

	cctx, cancel := context.WithTimeout(ctx, imageCopyTimeout)
	defer cancel()
	conn, err := party.Dial(cctx, to)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reach the member through the relay: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(imageCopyTimeout))
	if err := wire.WriteHeader(conn, imageVerb, fmt.Sprintf("%s %d", ref, keep)); err != nil {
		return nil, status.Errorf(codes.Unavailable, "reach the member: %v", err)
	}
	verb, arg, err := wire.ReadHeader(conn)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "the member did not answer: %v", err)
	}
	switch verb {
	case imageHave:
		return &pb.CopyClusterImageResponse{AlreadyPresent: true}, nil
	case imageSend:
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "the member refused the image: %s", arg)
	}

	gz, _ := gzip.NewWriterLevel(conn, gzip.BestSpeed)
	serr := s.images().save(cctx, ref, gz)
	if cerr := gz.Close(); serr == nil {
		serr = cerr
	}
	if serr != nil {
		return nil, status.Errorf(codes.Internal, "send %s: %v", ref, serr)
	}
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
	resp := &pb.CopyClusterImageResponse{}
	for {
		verb, arg, err = wire.ReadHeader(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errors.New("the member closed the stream before it confirmed")
			}
			return nil, status.Errorf(codes.Unavailable, "the member did not confirm %s: %v", ref, err)
		}
		switch verb {
		case imagePruned:
			resp.Pruned = append(resp.Pruned, arg)
		case wire.VerbOK:
			resp.Bytes, _ = strconv.ParseInt(arg, 10, 64)
			return resp, nil
		default:
			return nil, status.Errorf(codes.Aborted, "the member could not load %s: %s", ref, arg)
		}
	}
}
